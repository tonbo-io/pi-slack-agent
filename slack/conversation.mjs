import path from "node:path";
import { slackAgentInput } from "./identity.mjs";
import { createInflightStore } from "./inflight.mjs";

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
/** A Turn that has produced nothing for this long is given up on in Slack;
 * whatever is still running settles in the platform's ledger on its own. */
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
 * a durable checkpoint under exclusive named Activity ownership. */
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
  store = createInflightStore(),
  activities,
}) {
  if (!activities) throw new Error("Named Activity client is required.");
  const accepting = new Map();
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
      turn.lease.assertOwned();
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

  function effect(turn, kind, invoke, apply) {
    const operation = (turn.effectQueue || Promise.resolve()).then(async () => {
      turn.lease.assertOwned();
      if (turn.pendingEffect) throw new Error("Slack write requires reconciliation.");
      if ((kind === "close" || kind === "stop") && turn.closed) return;
      turn.pendingEffect = { kind, streamTs: turn.streamTs };
      await store.save(turn);
      turn.lease.assertOwned();
      const result = await slackWrite(invoke);
      apply(result);
      turn.pendingEffect = null;
      await store.save(turn);
    });
    turn.effectQueue = operation;
    return operation;
  }

  async function openStream(turn, text) {
    await effect(
      turn,
      "start",
      () =>
        slack.startStream({
          channelId: turn.channelId,
          threadTs: turn.threadTs,
          userId: turn.userId,
          teamId,
          text,
        }),
      (ts) => {
        turn.streamTs = ts;
        turn.messageChars = [...text].length;
        turn.streamed += text;
        if (turn.processingEvent) turn.processingEvent.offset += [...text].length;
      },
    );
    if (turn.cancelled) await closeStopped(turn);
  }

  async function stream(turn, text, sequence = null) {
    if (!text || turn.cancelled) return;
    if (sequence !== null) {
      turn.processingEvent ||= { sequence, text, offset: 0 };
      if (turn.processingEvent.sequence !== sequence || turn.processingEvent.text !== text)
        throw new Error("Checkpoint event differs from the authoritative feed.");
      await store.save(turn);
      text = [...text].slice(turn.processingEvent.offset).join("");
    }
    for (const chunk of chunkText(text)) {
      if (turn.cancelled) return;
      if (turn.streamTs !== null && turn.messageChars + [...chunk].length > MESSAGE_CHARS) {
        await effect(
          turn,
          "rotate",
          () => slack.stopStream(turn.channelId, turn.streamTs, "", "processing"),
          () => {
            turn.streamTs = null;
            turn.messageChars = 0;
          },
        );
      }
      if (turn.streamTs === null) await openStream(turn, chunk);
      else
        await effect(
          turn,
          "append",
          () => slack.appendStream(turn.channelId, turn.streamTs, chunk),
          () => {
            turn.messageChars += [...chunk].length;
            turn.streamed += chunk;
            if (turn.processingEvent) turn.processingEvent.offset += [...chunk].length;
          },
        );
    }
    if (sequence !== null && !turn.cancelled) {
      turn.cursor = sequence;
      turn.processingEvent = null;
      await store.save(turn);
    }
  }

  async function closeStream(turn, text, sessionStatus) {
    if (
      [...text].length > STREAM_CHUNK_CHARS ||
      turn.messageChars + [...text].length > MESSAGE_CHARS
    ) {
      await stream(turn, text);
      text = "";
    }
    if (turn.cancelled || turn.streamTs === null || turn.closed) return;
    await effect(
      turn,
      "close",
      () => slack.stopStream(turn.channelId, turn.streamTs, text, sessionStatus),
      () => {
        turn.closed = true;
      },
    );
  }

  async function closeStopped(turn) {
    if (turn.closed || turn.streamTs === null) return;
    const trailer =
      turn.messageChars + [...STOPPED_LINE].length > MESSAGE_CHARS ? "" : STOPPED_LINE;
    await effect(
      turn,
      "stop",
      () => slack.stopStream(turn.channelId, turn.streamTs, trailer, "active"),
      () => {
        turn.closed = true;
      },
    );
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
        if (event.event_type === "assistant.delta" && typeof event.payload?.text === "string")
          await stream(turn, event.payload.text, Number(event.sequence));
        turn.cursor = Number(event.sequence);
        await store.save(turn);
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
      if (status === "completed" && settled?.state !== "completed" && prompt !== null) {
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
        if (page === null && turn.resumed && turn.cursor > 0) {
          // A resumed Turn existed before the restart; an absent feed means
          // the platform no longer has it.
          turn.done = true;
          return {
            status: "failed",
            settled: { state: "failed", message: "the Turn no longer exists" },
          };
        }
        if (page === null) break;
        for (const event of page.events) {
          progressed = true;
          progressAt = now();
          // TODO: consecutive assistant messages of one Turn arrive without a
          // boundary and are joined as-is; the coordinator's projection will
          // mark `assistant.delta` boundaries, so no separator is invented here.
          if (event.event_type === "assistant.delta" && typeof event.payload?.text === "string")
            await stream(turn, event.payload.text, Number(event.sequence));
          turn.cursor = Math.max(turn.cursor, Number(event.sequence) || 0);
          await store.save(turn);
        }
        if (progressed) await store.save(turn);
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
        throw new Error("Turn progress deadline exceeded; ownership and checkpoint retained.");
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
      if (settled?.state !== "completed")
        throw new Error("Authoritative final answer remains unavailable.");
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
      if (turn.streamTs === null && !remainder.text && !turn.streamed)
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

  function releaseLocal(turn) {
    const id = key(turn.channelId, turn.threadTs);
    if (threads.get(id) === turn) threads.delete(id);
    if (turns.get(turn.turnId) === turn) turns.delete(turn.turnId);
  }

  async function drive(turn) {
    try {
      turn.lease.assertOwned();
      if (turn.pendingEffect) throw new Error("Uncertain Slack write retained for reconciliation.");
      await slack.setStatus(turn.channelId, turn.threadTs, "processing");
      if (turn.prompt === null) {
        turn.prompt = await attachFiles(
          turn.command.prompt || "Please look at the attached file.",
          turn.command.files,
        );
        await store.save(turn);
      }
      turn.lease.assertOwned();
      const outcome = driveSubmission(turn, turn.prompt);
      outcome.catch(() => {});
      const result = await follow(turn, outcome, turn.prompt);
      await finish(turn, result);
      if (turn.cancelled) await closeStopped(turn);
      turn.lease.assertOwned();
      if (!turn.closed) await slack.setStatus(turn.channelId, turn.threadTs, "active");
      await store.complete(turn);
      await turn.lease.release();
      log("turn_finished", { turn: turn.turnId, status: result.status, resumed: turn.resumed });
    } catch (error) {
      // A failed/uncertain provider write is not completion. Preserve the
      // checkpoint and named ownership; never guess or emit a second reply.
      turn.done = true;
      turn.lease.abandon();
      log("turn_reconciliation_required", { turn: turn.turnId, reason: error?.message });
    } finally {
      releaseLocal(turn);
    }
  }

  async function claim(command, saved = null) {
    const turnId = saved?.turnId || command.idempotencyKey;
    if (turns.has(turnId)) return () => {};
    const lease = await activities.acquire(`slack-turn:${turnId}`);
    if (!lease) {
      if (await store.get(turnId)) return () => {};
      throw new Error("Turn ownership was acquired before durable acceptance; retry.");
    }
    try {
      const record = await store.get(turnId);
      if (record?.completed) {
        await lease.release();
        return () => {};
      }
      const turn = {
        channelId: command?.channelId,
        threadTs: command?.threadTs,
        userId: command?.userId,
        sessionId: command?.sessionId,
        turnId,
        streamTs: null,
        messageChars: 0,
        streamed: "",
        cursor: 0,
        startedAt: now(),
        prompt: null,
        command,
        closed: false,
        pendingEffect: null,
        ...record,
        resumed: Boolean(record),
        cancelled: false,
        done: false,
        lease,
      };
      await store.save(turn);
      turns.set(turnId, turn);
      threads.set(key(turn.channelId, turn.threadTs), turn);
      return () => drive(turn);
    } catch (error) {
      lease.abandon();
      throw error;
    }
  }

  async function accept(event) {
    if (event?.type === "tokens_revoked" || event?.type === "app_uninstalled") {
      revoked = true;
      return () => {};
    }
    if (revoked) return () => {};
    const command = slackAgentInput({ agentId, teamId, botUserId, event });
    if (command.kind === "ignore") return () => {};
    if (command.kind === "stop") {
      await handleStop(command);
      return () => {};
    }
    if (accepting.has(command.idempotencyKey)) {
      await accepting.get(command.idempotencyKey);
      return () => {};
    }
    const admission = claim(command);
    accepting.set(command.idempotencyKey, admission);
    try {
      return await admission;
    } finally {
      accepting.delete(command.idempotencyKey);
    }
  }

  async function resume() {
    const records = await store.list();
    for (const record of records) {
      if (turns.has(record.turnId)) continue;
      if (accepting.has(record.turnId)) continue;
      const admission = claim(record.command, record);
      accepting.set(record.turnId, admission);
      let run;
      try {
        run = await admission;
      } finally {
        accepting.delete(record.turnId);
      }
      Promise.resolve()
        .then(run)
        .catch((error) => log("turn_resume_failed", { reason: error?.message }));
    }
    return records.length;
  }

  async function handleStop(command) {
    const id = key(command.channelId, command.threadTs);
    let turn = threads.get(id);
    if (!turn) {
      // A Turn the previous process left in flight and this one has not
      // resumed yet is still worth stopping.
      const record = (await store.list()).find(
        (item) => item.channelId === command.channelId && item.threadTs === command.threadTs,
      );
      if (record) {
        await tonbo.abortTurn(record.sessionId, record.turnId, command.idempotencyKey);
        return;
      }
    }
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
    accept,
    async handle(event) {
      const run = await accept(event);
      await run();
    },
    resume,
    get active() {
      return threads.size;
    },
    get revoked() {
      return revoked;
    },
    inboxFor: (fileId) => path.join(inbox, fileId),
  };
}
