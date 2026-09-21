import path from "node:path";
import { slackAgentInput } from "./identity.mjs";

export const SLACK_INBOX = "/workspace/inbox/slack";
const STOPPED_LINE = "\n\n_Stopped._";
const BUSY_RETRY_LIMIT = 150;
const MAX_POLL_MS = 5000;
const MAX_RETRY_AFTER_SECONDS = 3600;
const TRANSPORT_RETRY_MS = 5000;
/** Slack accepts at most 12,000 characters per stream call and truncates a
 * message silently past roughly 40,000; a reply longer than that continues
 * as another message in the same thread. */
export const STREAM_CHUNK_CHARS = 12_000;
export const MESSAGE_CHARS = 38_000;

/** What of the recorded answer is still to send after the streamed deltas.
 * The feed carries every assistant message of the Turn while the recorded
 * answer is the last one, so the overlap is the longest suffix of the
 * streamed text that begins the answer. */
export function remainderAfter(streamed, finalText) {
  if (!finalText) return { text: "", branch: "empty" };
  if (!streamed) return { text: finalText, branch: "unstreamed" };
  if (finalText.startsWith(streamed))
    return { text: finalText.slice(streamed.length), branch: "prefix" };
  // The longest suffix starts at the earliest position where the answer's
  // first character occurs and the rest of the streamed text begins the answer.
  for (
    let start = streamed.indexOf(finalText[0], 1);
    start !== -1;
    start = streamed.indexOf(finalText[0], start + 1)
  ) {
    const suffix = streamed.slice(start);
    if (suffix.length > finalText.length) continue;
    if (finalText.startsWith(suffix))
      return { text: finalText.slice(suffix.length), branch: "suffix" };
  }
  if (streamed.includes(finalText)) return { text: "", branch: "contained" };
  return { text: `\n\n${finalText}`, branch: "disjoint" };
}

/** Unicode-safe pieces of at most `size` characters. */
export function chunkText(text, size = STREAM_CHUNK_CHARS) {
  const characters = [...text];
  const chunks = [];
  for (let offset = 0; offset < characters.length; offset += size)
    chunks.push(characters.slice(offset, offset + size).join(""));
  return chunks;
}
/** A Turn that has produced nothing for this long is given up on; the
 * platform's own recovery owns whatever is still running. */
export const NO_PROGRESS_DEADLINE_MS = 30 * 60 * 1000;

function noticeFor(message) {
  return `The Agent could not finish this reply: ${[...String(message)].slice(0, 1000).join("")}`;
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
  deadlineMs = NO_PROGRESS_DEADLINE_MS,
  now = () => Date.now(),
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

  const isTransport = (error) => error?.code === "transport";

  /** Drives the submission until the Management API states the Turn's
   * outcome. The Turn id is the idempotency key, so a request that never
   * answered (our timeout, a reset) is simply sent again: the server answers
   * a running Turn by waiting for it and a settled one at once. A transport
   * failure is never the Turn's outcome; only the API's own answer is. */
  async function driveSubmission(turn, prompt) {
    for (let busy = 0; !turn.cancelled && !turn.done;) {
      let result;
      try {
        result = await tonbo.submitTurn(turn.sessionId, turn.turnId, prompt);
      } catch (error) {
        if (!isTransport(error)) throw error;
        log("turn_submit_unanswered", { turn: turn.turnId, reason: error.message });
        await sleep(TRANSPORT_RETRY_MS);
        continue;
      }
      if (result.state === "failed" && result.code === "agent_busy" && busy < BUSY_RETRY_LIMIT) {
        busy += 1;
        await sleep((result.retryAfterSeconds ?? 2) * 1000);
        continue;
      }
      if (result.state !== "pending") return result;
      if (!result.operationId) {
        await sleep(TRANSPORT_RETRY_MS);
        continue;
      }
      const outcome = await awaitOperation(turn, result.operationId);
      if (outcome) return outcome;
    }
    return { state: "pending" };
  }

  /** Polls a 202 operation. Resolves null when the operation vanished or the
   * Turn ended meanwhile, so the caller resubmits and gets the recorded answer. */
  async function awaitOperation(turn, operationId) {
    while (!turn.cancelled && !turn.done) {
      await sleep(Math.max(pollMs, 1000) + budget.reserve());
      let outcome;
      try {
        outcome = await tonbo.operation(operationId);
      } catch (error) {
        if (!isTransport(error)) throw error;
        log("turn_operation_unanswered", { turn: turn.turnId, reason: error.message });
        continue;
      }
      if (outcome.state === "failed" && outcome.status === 404) return null;
      if (outcome.state !== "pending") return outcome;
    }
    return null;
  }

  async function openStream(turn, text) {
    turn.streamTs = null;
    turn.messageChars = 0;
    const streamTs = await slackWrite(() =>
      slack.startStream({
        channelId: turn.channelId,
        threadTs: turn.threadTs,
        userId: turn.userId,
        teamId,
        text,
      }),
    );
    turn.streamTs = streamTs;
    turn.messageChars = [...text].length;
    // A stop that arrived while the stream was opening found nothing to
    // close; close it now so Slack does not keep an open stream.
    if (turn.cancelled) await closeStopped(turn);
  }

  /** Streams text in Slack-sized chunks. When the open message would grow
   * past Slack's cap, it is closed and the reply continues as a new message
   * in the same thread; `turn.streamTs` always names the open one. */
  async function stream(turn, text) {
    if (!text || turn.cancelled) return;
    turn.streamed += text;
    for (const chunk of chunkText(text)) {
      if (turn.cancelled) return;
      if (turn.streamTs !== null && turn.messageChars + [...chunk].length > MESSAGE_CHARS) {
        const full = turn.streamTs;
        turn.streamTs = null;
        await slackWrite(() => slack.stopStream(turn.channelId, full, "", "processing"));
      }
      if (turn.streamTs === null) await openStream(turn, chunk);
      else {
        await slackWrite(() => slack.appendStream(turn.channelId, turn.streamTs, chunk));
        turn.messageChars += [...chunk].length;
      }
    }
  }

  /** Closes the open message with a trailing text, streaming the text first
   * when it would not fit in one call or in the message. */
  async function closeStream(turn, text, sessionStatus) {
    const length = [...text].length;
    if (length > STREAM_CHUNK_CHARS || turn.messageChars + length > MESSAGE_CHARS) {
      await stream(turn, text);
      text = "";
    }
    if (turn.cancelled || turn.streamTs === null) return;
    turn.closed = true;
    await slackWrite(() => slack.stopStream(turn.channelId, turn.streamTs, text, sessionStatus));
  }

  async function closeStopped(turn) {
    if (turn.closed || turn.streamTs === null) return;
    turn.closed = true;
    const trailer =
      turn.messageChars + [...STOPPED_LINE].length > MESSAGE_CHARS ? "" : STOPPED_LINE;
    await slackWrite(() => slack.stopStream(turn.channelId, turn.streamTs, trailer, "active"));
  }

  async function drain(turn) {
    while (!turn.cancelled) {
      await sleep(budget.reserve());
      const page = await tonbo
        .turnEvents(turn.sessionId, turn.turnId, turn.cursor)
        .catch((error) => {
          log("turn_events_failed", { code: error?.code, status: error?.status });
          return null;
        });
      // Only rows past the cursor count; a page that adds nothing ends the drain.
      const fresh =
        page?.events.filter((event) => (Number(event.sequence) || 0) > turn.cursor) ?? [];
      if (fresh.length === 0) return;
      for (const event of fresh) {
        turn.cursor = Number(event.sequence);
        if (event.event_type === "assistant.delta" && typeof event.payload?.text === "string")
          await stream(turn, event.payload.text);
      }
    }
  }

  async function follow(turn, outcomePromise, prompt) {
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
    // The feed says the Turn is over; the recorded answer comes from the
    // submission. If that request is still open or was lost, replay it: the
    // idempotent key answers a settled Turn at once.
    const outcome = async (status) => {
      turn.done = true;
      if (!settled || settled.state === "pending") await Promise.race([settling, sleep(5000)]);
      if (status === "completed" && settled?.state !== "completed") {
        settled = await tonbo
          .submitTurn(turn.sessionId, turn.turnId, prompt)
          .catch((error) => ({ state: "unknown", message: error?.message }));
      }
      return { status, settled };
    };
    let progressAt = now();
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
        if (page === null) break;
        for (const event of page.events) {
          turn.cursor = Math.max(turn.cursor, Number(event.sequence) || 0);
          progressed = true;
          progressAt = now();
          // TODO: consecutive assistant messages of one Turn arrive without a
          // boundary and are joined as-is; the coordinator's projection will
          // mark `assistant.delta` boundaries, so no separator is invented here.
          if (event.event_type === "assistant.delta" && typeof event.payload?.text === "string")
            await stream(turn, event.payload.text);
        }
        if (page.status !== "pending") {
          // The status can settle on a page that is not the last: drain
          // every later row before the answer is finished.
          await drain(turn);
          return outcome(page.status);
        }
      } while (page.events.length >= 100 && !turn.cancelled);
      // A refused submission never creates the Turn, so the feed stays absent
      // and the settled answer is the only signal. A pending answer with an
      // absent feed is a Turn the coordinator has not claimed yet.
      if (settled && settled.state !== "pending") return outcome(settled.state);
      if (now() - progressAt > deadlineMs) {
        turn.done = true;
        log("turn_no_progress", { turn: turn.turnId, minutes: Math.round(deadlineMs / 60_000) });
        return {
          status: "failed",
          settled: {
            state: "failed",
            message: `no progress for ${Math.round(deadlineMs / 60_000)} minutes`,
          },
        };
      }
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
      // The feed may lag the recorded answer or, on an older platform, carry
      // no deltas at all; the recorded answer is authoritative for its tail.
      const remainder = remainderAfter(turn.streamed, finalText);
      log("turn_remainder", {
        turn: turn.turnId,
        branch: remainder.branch,
        chars: [...remainder.text].length,
      });
      if (turn.streamTs === null && !remainder.text)
        await stream(turn, "The Agent completed without a text response.");
      else if (remainder.text) await stream(turn, remainder.text);
      await closeStream(turn, "", "active");
      return;
    }
    const message = settled?.message || `Turn ${result.status}`;
    const notice = noticeFor(message);
    if (turn.streamTs === null) await stream(turn, notice);
    await closeStream(turn, turn.streamed === notice ? "" : `\n\n${notice}`, "active");
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
      messageChars: 0,
      streamed: "",
      cursor: 0,
      cancelled: false,
      closed: false,
      done: false,
    };
    turns.set(turn.turnId, turn);
    threads.set(id, turn);
    try {
      await slack.setStatus(command.channelId, command.threadTs, "processing");
      const prompt = await attachFiles(
        command.prompt || "Please look at the attached file.",
        command.files,
      );
      const outcome = driveSubmission(turn, prompt);
      outcome.catch(() => {});
      const result = await follow(turn, outcome, prompt);
      await finish(turn, result);
      log("turn_finished", { turn: turn.turnId, status: result.status });
    } catch (error) {
      log("turn_failed", { turn: turn.turnId, reason: error?.code || error?.message });
      if (turn.cancelled) return;
      // Never leave a stream open: close it with the reason, or clear the status.
      try {
        if (turn.streamTs !== null && !turn.closed)
          await closeStream(
            turn,
            `\n\n${noticeFor(error?.code || error?.message || "unexpected error")}`,
            "active",
          );
        else await slack.setStatus(command.channelId, command.threadTs, "active");
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
    turn.done = true;
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
