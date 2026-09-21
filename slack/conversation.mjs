import path from "node:path";
import { slackAgentInput } from "./identity.mjs";

export const SLACK_INBOX = "/workspace/inbox/slack";
const STOPPED_LINE = "\n\n_Stopped._";
const BUSY_RETRY_LIMIT = 150;
const MAX_POLL_MS = 5000;
const MAX_RETRY_AFTER_SECONDS = 3600;

function noticeFor(message) {
  return `The Agent could not finish this reply: ${message}`;
}

/** Spaces Management API reads across every thread this process drives so
 * their sum stays under the per-bearer limit (300 requests a minute), leaving
 * room for submissions, aborts and operation reads. */
export function createPollBudget({ perMinute = 120, now = () => Date.now() } = {}) {
  const interval = 60_000 / perMinute;
  let nextAt = 0;
  return {
    /** Milliseconds the caller must wait before its next read. */
    reserve() {
      const current = now();
      const at = Math.max(current, nextAt);
      nextAt = at + interval;
      return at - current;
    },
  };
}

/** Drives one Slack thread's Turns: submits, follows the event feed, streams
 * assistant text into Slack, honours stop, and reports failure. State lives in
 * memory only; a restart forgets in-flight streams and the next message on a
 * thread is new work. */
export function createConversations({
  tonbo,
  slack,
  agentId,
  teamId,
  botUserId,
  inbox = SLACK_INBOX,
  log = () => {},
  pollMs = 1000,
  budget = createPollBudget(),
  sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
}) {
  const threads = new Map();
  const turns = new Map();
  let revoked = false;
  const key = (channelId, threadTs) => `${channelId}:${threadTs}`;

  async function attachFiles(prompt, files) {
    let text = prompt;
    for (const file of files) {
      try {
        const saved = await slack.downloadFile(file, inbox);
        text += `\n\nAttached file saved at ${saved}`;
        log("slack_file_saved", { file: file.id, bytes: file.size });
      } catch (error) {
        const reason = typeof error?.code === "string" ? error.code : "download_failed";
        text += `\n\nAttached file ${JSON.stringify(file.name || file.id)} could not be downloaded (${reason}).`;
        log("slack_file_failed", { file: file.id, reason });
      }
    }
    return text;
  }

  /** One Slack write, retried once when Slack asks for a pause. */
  async function slackWrite(operation) {
    try {
      return await operation();
    } catch (error) {
      if (error?.code !== "ratelimited") throw error;
      await sleep(Math.min(error.retryAfterSeconds ?? 5, 60) * 1000);
      return operation();
    }
  }

  async function submitWithBusyRetry(turn, prompt) {
    for (let attempt = 0; ; attempt += 1) {
      const result = await tonbo.submitTurn(turn.sessionId, turn.turnId, prompt);
      if (
        result.state === "failed" &&
        result.code === "agent_busy" &&
        attempt < BUSY_RETRY_LIMIT &&
        !turn.cancelled
      ) {
        await sleep((result.retryAfterSeconds ?? 2) * 1000);
        continue;
      }
      return result;
    }
  }

  async function awaitOutcome(turn, first) {
    let outcome = first;
    while (outcome.state === "pending" && !turn.cancelled) {
      await sleep(Math.max(pollMs, 1000) + budget.reserve());
      if (!outcome.operationId) return outcome;
      outcome = await tonbo.operation(outcome.operationId);
    }
    return outcome;
  }

  async function stream(turn, text) {
    if (!text || turn.cancelled) return;
    turn.streamed += text;
    if (turn.streamTs === null) {
      turn.streamTs = await slackWrite(() =>
        slack.startStream({
          channelId: turn.channelId,
          threadTs: turn.threadTs,
          userId: turn.userId,
          teamId,
          text,
        }),
      );
      // A stop that arrived while the stream was opening found nothing to
      // close; close it now so Slack does not keep an open stream.
      if (turn.cancelled) await closeStopped(turn);
    } else await slackWrite(() => slack.appendStream(turn.channelId, turn.streamTs, text));
  }

  async function closeStopped(turn) {
    if (turn.closed || turn.streamTs === null) return;
    turn.closed = true;
    await slackWrite(() => slack.stopStream(turn.channelId, turn.streamTs, STOPPED_LINE, "active"));
  }

  async function follow(turn, outcomePromise) {
    let settled = null;
    const settling = outcomePromise.then(
      (value) => {
        settled = value;
        return value;
      },
      (error) => {
        settled = { state: "failed", message: error?.message || "request failed" };
        return settled;
      },
    );
    // The feed can report completion before the submission answers with the
    // recorded text; give that answer a moment so the remainder is exact.
    const outcome = async (status) => {
      if (!settled) {
        const grace = new Promise((resolve) => setTimeout(resolve, 5000).unref());
        await Promise.race([settling, grace]);
      }
      return { status, settled };
    };
    let missing = 0;
    let idle = 0;
    while (!turn.cancelled) {
      let page;
      let delay = null;
      let progressed = false;
      do {
        await sleep(budget.reserve());
        page = await tonbo.turnEvents(turn.sessionId, turn.turnId, turn.cursor).catch((error) => {
          log("turn_events_failed", { code: error?.code, status: error?.status });
          if (error?.status === 429 || error?.retryAfterSeconds)
            delay = Math.min(error.retryAfterSeconds ?? 60, MAX_RETRY_AFTER_SECONDS) * 1000;
          return { events: [], status: "pending" };
        });
        if (page === null) {
          missing += 1;
          break;
        }
        for (const event of page.events) {
          turn.cursor = Math.max(turn.cursor, Number(event.sequence) || 0);
          progressed = true;
          if (event.event_type === "assistant.delta" && typeof event.payload?.text === "string")
            await stream(turn, event.payload.text);
        }
        if (page.status !== "pending") return outcome(page.status);
      } while (page.events.length >= 100 && !turn.cancelled);
      // A refused submission never creates the Turn, so the feed stays absent
      // and the settled answer is the only signal. A pending answer with an
      // absent feed is a Turn the coordinator has not claimed yet.
      if (settled && settled.state !== "pending") return outcome(settled.state);
      if (missing > 600)
        return {
          status: "failed",
          settled: { state: "failed", message: "The Turn never started." },
        };
      // Back off while nothing arrives; a delta resets the cadence.
      // A refusal already imposed its own pause; start the backoff afresh after it.
      idle = progressed || delay !== null ? 0 : idle + 1;
      await sleep(delay ?? Math.min(pollMs * 2 ** Math.min(idle, 3), MAX_POLL_MS));
    }
    return { status: "cancelled", settled };
  }

  async function finish(turn, result) {
    if (turn.cancelled) return;
    const settled = result.settled;
    if (result.status === "completed") {
      const finalText =
        typeof settled?.data?.assistant_text === "string" ? settled.data.assistant_text : "";
      // The feed may lag the final text or, on an older platform, carry no
      // deltas at all; the recorded answer is authoritative for the remainder.
      const remainder = finalText.startsWith(turn.streamed)
        ? finalText.slice(turn.streamed.length)
        : "";
      if (turn.streamTs === null)
        await stream(
          turn,
          finalText || remainder || "The Agent completed without a text response.",
        );
      else if (remainder) await stream(turn, remainder);
      if (turn.cancelled) return;
      turn.closed = true;
      await slackWrite(() => slack.stopStream(turn.channelId, turn.streamTs, "", "active"));
      return;
    }
    const message = settled?.message || `Turn ${result.status}`;
    const notice = noticeFor(message);
    if (turn.streamTs === null) await stream(turn, notice);
    if (turn.cancelled) return;
    turn.closed = true;
    await slackWrite(() =>
      slack.stopStream(
        turn.channelId,
        turn.streamTs,
        turn.streamed === notice ? "" : `\n\n${notice}`,
        "active",
      ),
    );
  }

  async function handlePrompt(command) {
    // Slack delivers one DM message as both message.im and app_mention with
    // different event ids; both derive the same Turn. Drive it once.
    if (turns.has(command.idempotencyKey)) {
      log("turn_already_driven", { turn: command.idempotencyKey });
      return;
    }
    const id = key(command.channelId, command.threadTs);
    const turn = {
      channelId: command.channelId,
      threadTs: command.threadTs,
      userId: command.userId,
      sessionId: command.sessionId,
      turnId: command.idempotencyKey,
      streamTs: null,
      streamed: "",
      cursor: 0,
      cancelled: false,
      closed: false,
    };
    turns.set(turn.turnId, turn);
    threads.set(id, turn);
    try {
      await slack.setStatus(command.channelId, command.threadTs, "processing");
      const prompt = await attachFiles(
        command.prompt || "Please look at the attached file.",
        command.files,
      );
      const outcome = submitWithBusyRetry(turn, prompt).then((first) => awaitOutcome(turn, first));
      outcome.catch(() => {});
      const result = await follow(turn, outcome);
      await finish(turn, result);
      log("turn_finished", { turn: turn.turnId, status: result.status });
    } catch (error) {
      log("turn_failed", { turn: turn.turnId, reason: error?.code || error?.message });
      if (turn.cancelled) return;
      // Never leave a stream open: close it with the reason, or clear the status.
      try {
        if (turn.streamTs !== null && !turn.closed) {
          turn.closed = true;
          await slackWrite(() =>
            slack.stopStream(
              turn.channelId,
              turn.streamTs,
              `\n\n${noticeFor(error?.code || error?.message || "unexpected error")}`,
              "active",
            ),
          );
        } else await slack.setStatus(command.channelId, command.threadTs, "active");
      } catch (closing) {
        log("turn_close_failed", { turn: turn.turnId, reason: closing?.code || closing?.message });
      }
    } finally {
      if (threads.get(id) === turn) threads.delete(id);
      if (turns.get(turn.turnId) === turn) turns.delete(turn.turnId);
    }
  }

  async function handleStop(command) {
    const id = key(command.channelId, command.threadTs);
    const turn = threads.get(id);
    if (!turn || turn.cancelled) {
      await slack.setStatus(command.channelId, command.threadTs, "active");
      return;
    }
    turn.cancelled = true;
    threads.delete(id);
    const abort = tonbo
      .abortTurn(turn.sessionId, turn.turnId, command.idempotencyKey)
      .catch((error) => {
        log("turn_abort_failed", { turn: turn.turnId, reason: error?.code || error?.message });
        return null;
      });
    if (turn.streamTs !== null) await closeStopped(turn);
    else await slack.setStatus(command.channelId, command.threadTs, "active");
    const result = await abort;
    log("turn_stopped", { turn: turn.turnId, abort: result?.state ?? "failed" });
  }

  return {
    /** Handles one verified, deduplicated event. Resolves when the work for
     * that event is done; the HTTP layer never waits for it. */
    async handle(event) {
      if (event?.type === "tokens_revoked" || event?.type === "app_uninstalled") {
        revoked = true;
        log("slack_revoked", { type: event.type });
        return;
      }
      if (revoked) return;
      let command;
      try {
        command = slackAgentInput({ agentId, teamId, botUserId, event });
      } catch (error) {
        log("slack_event_invalid", { reason: error.message });
        return;
      }
      if (command.kind === "ignore") return;
      if (command.kind === "stop") return handleStop(command);
      return handlePrompt(command);
    },
    get active() {
      return threads.size;
    },
    get revoked() {
      return revoked;
    },
    inboxFor: (fileId) => path.join(inbox, fileId),
  };
}
