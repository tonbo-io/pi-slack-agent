import assert from "node:assert/strict";
import test from "node:test";
import {
  chunkText,
  createConversations as createConversationsImpl,
  createPollBudget,
  remainderAfter,
  rejectedLegacyFirstAppend,
} from "../conversation.mjs";

function createConversations(options) {
  const records = new Map();
  return createConversationsImpl({
    activities: {
      acquire: async () => ({
        assertOwned() {},
        abandon() {},
        async release() {},
      }),
    },
    store: {
      get: async (id) => records.get(id) ?? null,
      save: async (turn) => {
        const { lease: _lease, effectQueue: _effectQueue, ...record } = turn;
        records.set(turn.turnId, structuredClone(record));
      },
      complete: async (turn) => records.set(turn.turnId, { turnId: turn.turnId, completed: true }),
      list: async () => [...records.values()].filter((record) => !record.completed),
    },
    ...options,
  });
}

const agentId = "0f1e2d3c-4b5a-4968-8778-695a4b3c2d1e";
const base = {
  agentId,
  teamId: "T0AAA",
  botUserId: "U0BOT",
  pollMs: 0,
  budget: { reserve: () => 0 },
  sleep: async () => {},
};
const message = {
  type: "message",
  channel_type: "im",
  channel: "D0CHAN",
  user: "U0USER",
  text: "hello",
  ts: "1782234671.392669",
};
const stop = {
  type: "agent_session_stopped",
  channel: "D0CHAN",
  thread_ts: message.ts,
  user: "U0USER",
  event_ts: "1782234690.000001",
};

function fakeSlack() {
  const calls = [];
  return {
    calls,
    setStatus: async (channel, thread, status) =>
      calls.push(["setStatus", channel, thread, status]),
    startStream: async ({ channelId, threadTs, userId, teamId, text, chunks }) => {
      calls.push([
        "startStream",
        channelId,
        threadTs,
        userId,
        teamId,
        text,
        ...(chunks ? [chunks] : []),
      ]);
      return "1782234700.000100";
    },
    appendStream: async (channel, ts, text, chunks) =>
      calls.push(["appendStream", channel, ts, text, ...(chunks ? [chunks] : [])]),
    stopStream: async (channel, ts, text, status, chunks) =>
      calls.push(["stopStream", channel, ts, text, status, ...(chunks ? [chunks] : [])]),
    downloadFile: async (file, directory) => {
      calls.push(["downloadFile", file.id, directory]);
      if (file.id === "FBAD") throw Object.assign(new Error("too large"), { code: "too_large" });
      return `${directory}/${file.id}/${file.name}`;
    },
  };
}
/** A scripted Turn: the feed pages and the submission answer are queued. */
function fakeTonbo({
  pages = [],
  submit,
  abort = { state: "pending" },
  operation,
  settleAfterFeed = false,
}) {
  const calls = [];
  let page = 0;
  let releaseFeed;
  const feedDone = new Promise((resolve) => (releaseFeed = resolve));
  return {
    calls,
    submitTurn: async (sessionId, turnId, prompt) => {
      calls.push(["submitTurn", sessionId, turnId, prompt]);
      // The real answer arrives only once the Turn settled, after its feed.
      if (settleAfterFeed) await feedDone;
      return typeof submit === "function" ? submit() : submit;
    },
    operation: async (id) => {
      calls.push(["operation", id]);
      return operation;
    },
    turnEvents: async (sessionId, turnId, after) => {
      calls.push(["turnEvents", after]);
      const next = pages[Math.min(page, pages.length - 1)];
      page += 1;
      if (next && next.status !== "pending") releaseFeed();
      return next ?? null;
    },
    abortTurn: async (sessionId, turnId, requestId) => {
      calls.push(["abortTurn", sessionId, turnId, requestId]);
      return abort;
    },
  };
}
const delta = (sequence, text) => ({
  sequence,
  event_type: "assistant.delta",
  payload: { text },
});

test("streams deltas into Slack as they arrive and closes with the exact remainder", async () => {
  const slack = fakeSlack();
  const tonbo = fakeTonbo({
    pages: [
      null,
      { events: [delta(1, "Hel"), delta(2, "lo")], status: "pending" },
      { events: [delta(3, " wor")], status: "pending" },
      {
        events: [{ sequence: 4, event_type: "assistant.completed", payload: {} }],
        status: "completed",
      },
    ],
    submit: { state: "completed", data: { assistant_text: "Hello world" } },
    settleAfterFeed: true,
  });
  const conversations = createConversations({ ...base, tonbo, slack });
  await conversations.handle(message);
  assert.deepEqual(
    slack.calls.map((call) => call[0]),
    ["setStatus", "startStream", "appendStream", "appendStream", "stopStream"],
  );
  assert.deepEqual(slack.calls[0], ["setStatus", "D0CHAN", message.ts, "processing"]);
  assert.deepEqual(slack.calls[1].slice(1), ["D0CHAN", message.ts, "U0USER", "T0AAA", "Hello"]);
  assert.deepEqual(
    slack.calls.slice(2, 4).map((call) => call[3]),
    [" wor", "ld"],
  );
  assert.deepEqual(slack.calls[4], ["stopStream", "D0CHAN", "1782234700.000100", "", "active"]);
  assert.equal(tonbo.calls[0][0], "submitTurn");
  assert.equal(tonbo.calls[0][3], "hello");
  // The last read drains anything past the settling page.
  assert.deepEqual(
    tonbo.calls.filter((c) => c[0] === "turnEvents").map((c) => c[1]),
    [0, 0, 2, 3, 4],
  );
  assert.equal(conversations.active, 0);
});

test("pages a long feed within one poll tick before sleeping", async () => {
  const slack = fakeSlack();
  const full = Array.from({ length: 100 }, (_, index) => delta(index + 1, "x"));
  let sleeps = 0;
  const tonbo = fakeTonbo({
    pages: [
      { events: full, status: "pending" },
      { events: [delta(101, "y")], status: "completed" },
    ],
    submit: {
      state: "completed",
      data: { assistant_text: "x".repeat(100) + "y" },
    },
    settleAfterFeed: true,
  });
  const conversations = createConversations({
    ...base,
    tonbo,
    slack,
    sleep: async (ms) => {
      if (ms > 0) sleeps += 1;
    },
  });
  await conversations.handle(message);
  // Two pages, then one drain read that finds nothing new; no sleep between them.
  assert.equal(tonbo.calls.filter((c) => c[0] === "turnEvents").length, 3);
  assert.equal(sleeps, 0);
  assert.equal(slack.calls.at(-1)[0], "stopStream");
});

test("replays the recorded answer when the feed carries no text", async () => {
  const slack = fakeSlack();
  const tonbo = fakeTonbo({
    pages: [
      {
        events: [{ sequence: 1, event_type: "run.started", payload: {} }],
        status: "completed",
      },
    ],
    submit: { state: "completed", data: { assistant_text: "Final answer" } },
  });
  await createConversations({ ...base, tonbo, slack }).handle(message);
  assert.equal(slack.calls[1][0], "startStream");
  assert.equal(slack.calls[1][5], "Final answer");
  assert.deepEqual(slack.calls[2].slice(0, 1), ["stopStream"]);
});

test("a failed Turn ends the thread with the coordinator's reason", async () => {
  const slack = fakeSlack();
  const tonbo = fakeTonbo({
    pages: [null],
    submit: {
      state: "failed",
      status: 409,
      code: "turn_failed",
      message: "turn failed: PI assistant failed: insufficient credit",
    },
  });
  await createConversations({ ...base, tonbo, slack }).handle(message);
  assert.equal(slack.calls[1][0], "startStream");
  assert.match(
    slack.calls[1][5],
    /^The Agent could not finish this reply: turn failed: PI assistant failed: insufficient credit$/,
  );
  assert.deepEqual(slack.calls[2], ["stopStream", "D0CHAN", "1782234700.000100", "", "active"]);
});

test("a failure after text was streamed appends the notice to the open stream", async () => {
  const slack = fakeSlack();
  const tonbo = fakeTonbo({
    pages: [
      { events: [delta(1, "partial")], status: "pending" },
      { events: [], status: "failed" },
    ],
    submit: {
      state: "failed",
      code: "turn_failed",
      message: "turn failed: process lost",
    },
  });
  await createConversations({ ...base, tonbo, slack }).handle(message);
  assert.equal(
    slack.calls.at(-1)[3],
    "\n\nThe Agent could not finish this reply: turn failed: process lost",
  );
});

test("stop aborts the Turn and closes the stream at once without waiting for Pi", async () => {
  const slack = fakeSlack();
  let release;
  const gate = new Promise((resolve) => (release = resolve));
  let served = 0;
  const tonbo = fakeTonbo({
    submit: () =>
      gate.then(() => ({
        state: "completed",
        data: { assistant_text: "late" },
      })),
  });
  tonbo.turnEvents = async () => {
    served += 1;
    if (served === 1) return { events: [delta(1, "Working on")], status: "pending" };
    await new Promise((resolve) => setTimeout(resolve, 5));
    return { events: [], status: "pending" };
  };
  const conversations = createConversations({
    ...base,
    tonbo,
    slack,
    sleep: (ms) => new Promise((r) => setTimeout(r, ms || 1)),
  });
  const prompt = conversations.handle(message);
  while (!slack.calls.some((call) => call[0] === "startStream"))
    await new Promise((r) => setTimeout(r, 1));
  await conversations.handle(stop);
  assert.deepEqual(slack.calls.at(-1), [
    "stopStream",
    "D0CHAN",
    "1782234700.000100",
    "\n\n_Stopped._",
    "active",
  ]);
  const abort = tonbo.calls.find((call) => call[0] === "abortTurn");
  assert.ok(abort);
  assert.notEqual(abort[3], abort[2]);
  release();
  await prompt;
  assert.equal(slack.calls.filter((call) => call[0] === "stopStream").length, 1);
  assert.equal(slack.calls.filter((call) => call[0] === "appendStream").length, 0);
  assert.equal(conversations.active, 0);
});

test("a stop with nothing running only clears the status", async () => {
  const slack = fakeSlack();
  await createConversations({ ...base, tonbo: fakeTonbo({}), slack }).handle(stop);
  assert.deepEqual(slack.calls, [["setStatus", "D0CHAN", message.ts, "active"]]);
});

test("shared files are saved into the inbox and named in the prompt", async () => {
  const slack = fakeSlack();
  const tonbo = fakeTonbo({
    pages: [{ events: [], status: "completed" }],
    submit: { state: "completed", data: { assistant_text: "ok" } },
  });
  await createConversations({
    ...base,
    tonbo,
    slack,
    inbox: "/workspace/inbox/slack",
  }).handle({
    ...message,
    subtype: "file_share",
    text: "please read",
    files: [
      {
        id: "F1",
        name: "brief.md",
        size: 12,
        url_private_download: "https://files.slack.com/F1",
      },
      {
        id: "FBAD",
        name: "huge.bin",
        size: 99,
        url_private_download: "https://files.slack.com/FBAD",
      },
    ],
  });
  const prompt = tonbo.calls.find((call) => call[0] === "submitTurn")[3];
  assert.equal(
    prompt,
    'please read\n\nAttached file saved at /workspace/inbox/slack/F1/brief.md\n\nAttached file "huge.bin" could not be downloaded (too_large).',
  );
  assert.deepEqual(slack.calls[1], ["downloadFile", "F1", "/workspace/inbox/slack"]);
});

test("waits through a busy Agent and resumes a pending operation", async () => {
  const slack = fakeSlack();
  let submissions = 0;
  const tonbo = fakeTonbo({
    pages: [null],
    submit: () =>
      (submissions += 1) < 3
        ? {
            state: "failed",
            code: "agent_busy",
            status: 409,
            message: "busy",
            retryAfterSeconds: 0,
          }
        : {
            state: "pending",
            operationId: "3f1e2d3c-4b5a-4968-8778-695a4b3c2d1e",
          },
    operation: { state: "completed", data: { assistant_text: "after wait" } },
  });
  await createConversations({ ...base, tonbo, slack }).handle(message);
  assert.equal(submissions, 3);
  assert.ok(tonbo.calls.some((call) => call[0] === "operation"));
  assert.equal(slack.calls[1][5], "after wait");
});

test("ignores bots and stops answering after the App is uninstalled", async () => {
  const slack = fakeSlack();
  const tonbo = fakeTonbo({});
  const conversations = createConversations({ ...base, tonbo, slack });
  await conversations.handle({ ...message, bot_id: "B1" });
  await conversations.handle({ type: "app_uninstalled" });
  await conversations.handle(message);
  assert.equal(conversations.revoked, true);
  assert.deepEqual(slack.calls, []);
  assert.deepEqual(tonbo.calls, []);
});

test("drives one Turn once when Slack delivers the same message twice", async () => {
  const slack = fakeSlack();
  let release;
  const gate = new Promise((resolve) => (release = resolve));
  const tonbo = fakeTonbo({
    submit: () =>
      gate.then(() => ({
        state: "completed",
        data: { assistant_text: "once" },
      })),
  });
  let served = 0;
  tonbo.turnEvents = async () => {
    served += 1;
    return served > 3 ? { events: [], status: "completed" } : { events: [], status: "pending" };
  };
  const conversations = createConversations({ ...base, tonbo, slack });
  const first = conversations.handle(message);
  const second = conversations.handle({
    ...message,
    type: "app_mention",
    channel_type: undefined,
  });
  await second;
  release();
  await first;
  assert.equal(tonbo.calls.filter((call) => call[0] === "submitTurn").length, 1);
  assert.equal(slack.calls.filter((call) => call[0] === "startStream").length, 1);
  assert.equal(slack.calls.filter((call) => call[0] === "stopStream").length, 1);
});

test("a stop that lands while the stream is opening still closes it", async () => {
  const slack = fakeSlack();
  let openStream;
  const opening = new Promise((resolve) => (openStream = resolve));
  slack.startStream = async (input) => {
    slack.calls.push(["startStream", input.text]);
    await opening;
    return "1782234700.000100";
  };
  let submitted;
  const tonbo = fakeTonbo({
    submit: () => new Promise((resolve) => (submitted = resolve)),
  });
  let served = 0;
  tonbo.turnEvents = async () =>
    (served += 1) === 1
      ? { events: [delta(1, "Hel")], status: "pending" }
      : new Promise((resolve) => setTimeout(() => resolve({ events: [], status: "pending" }), 5));
  const conversations = createConversations({
    ...base,
    tonbo,
    slack,
    sleep: (ms) => new Promise((r) => setTimeout(r, ms || 1)),
  });
  const prompt = conversations.handle(message);
  while (!slack.calls.some((call) => call[0] === "startStream"))
    await new Promise((r) => setTimeout(r, 1));
  await conversations.handle(stop);
  assert.ok(tonbo.calls.some((call) => call[0] === "abortTurn"));
  assert.deepEqual(slack.calls.at(-1), ["setStatus", "D0CHAN", message.ts, "active"]);
  openStream();
  submitted({ state: "completed", data: { assistant_text: "Hello" } });
  await prompt;
  assert.deepEqual(slack.calls.at(-1), [
    "stopStream",
    "D0CHAN",
    "1782234700.000100",
    "\n\n_Stopped._",
    "active",
  ]);
  assert.equal(slack.calls.filter((call) => call[0] === "stopStream").length, 1);
});

test("an uncertain Slack append preserves ownership without another provider write", async () => {
  const slack = fakeSlack();
  const tonbo = fakeTonbo({
    submit: { state: "completed", data: { assistant_text: "partial done" } },
  });
  tonbo.turnEvents = async () => ({
    events: [delta(1, "partial"), delta(2, "more")],
    status: "pending",
  });
  slack.appendStream = async () => {
    throw new Error("slack exploded");
  };
  const records = new Map();
  let released = false;
  await createConversations({
    ...base,
    tonbo,
    slack,
    activities: {
      acquire: async () => ({
        assertOwned() {},
        abandon() {},
        async release() {
          released = true;
        },
      }),
    },
    store: {
      get: async (id) => records.get(id),
      save: async (turn) => records.set(turn.turnId, { pendingEffect: turn.pendingEffect }),
      complete: async () => {
        throw new Error("Uncertain write must not complete");
      },
    },
  }).handle(message);
  assert.equal(slack.calls.at(-1)[0], "startStream");
  assert.equal([...records.values()][0].pendingEffect.kind, "append");
  assert.equal(released, false);
});

test("retries a rate-limited Slack write once after the pause Slack asks for", async () => {
  const slack = fakeSlack();
  const sleeps = [];
  let attempts = 0;
  const start = slack.startStream;
  slack.startStream = async (input) => {
    if ((attempts += 1) === 1)
      throw Object.assign(new Error("ratelimited"), {
        code: "ratelimited",
        retryAfterSeconds: 3,
      });
    return start(input);
  };
  const tonbo = fakeTonbo({
    pages: [{ events: [], status: "completed" }],
    submit: { state: "completed", data: { assistant_text: "ok" } },
  });
  await createConversations({
    ...base,
    tonbo,
    slack,
    sleep: async (ms) => void sleeps.push(ms),
  }).handle(message);
  assert.equal(attempts, 2);
  assert.ok(sleeps.includes(3000));
  assert.equal(slack.calls.at(-1)[0], "stopStream");
});

test("sleeps for Retry-After when the event feed is rate limited and bounds idle latency", async () => {
  const slack = fakeSlack();
  const sleeps = [];
  let reads = 0;
  let done;
  const feedDone = new Promise((resolve) => (done = resolve));
  // The submission settles only once the feed has reported completion, so
  // the feed alone drives the loop.
  const tonbo = fakeTonbo({
    submit: () =>
      feedDone.then(() => ({
        state: "completed",
        data: { assistant_text: "ok" },
      })),
  });
  tonbo.turnEvents = async () => {
    reads += 1;
    if (reads === 1)
      throw Object.assign(new Error("limited"), {
        status: 429,
        retryAfterSeconds: 60,
      });
    if (reads < 5) return { events: [], status: "pending" };
    done();
    return { events: [], status: "completed" };
  };
  await createConversations({
    ...base,
    tonbo,
    slack,
    pollMs: 1000,
    sleep: async (ms) => void sleeps.push(ms),
  }).handle(message);
  // The trailing grace wait for the recorded answer is not part of the cadence.
  assert.deepEqual(sleeps.filter((ms) => ms > 0).slice(0, 4), [60000, 1000, 1000, 1000]);
});

test("the poll budget spaces reads across threads", () => {
  let clock = 0;
  const budget = createPollBudget({ perMinute: 120, now: () => clock });
  assert.equal(budget.reserve(), 0);
  assert.equal(budget.reserve(), 500);
  assert.equal(budget.reserve(), 1000);
  clock = 5000;
  assert.equal(budget.reserve(), 0);
});

const transport = () =>
  Object.assign(new Error("The operation was aborted due to timeout"), {
    code: "transport",
  });

test("a submission the network lost after text streamed completes through the feed", async () => {
  const slack = fakeSlack();
  let submissions = 0;
  const tonbo = fakeTonbo({
    pages: [
      { events: [delta(1, "First sentence.")], status: "pending" },
      { events: [delta(2, " Second.")], status: "completed" },
    ],
    submit: () => {
      submissions += 1;
      if (submissions === 1) throw transport();
      return {
        state: "completed",
        data: { assistant_text: "First sentence. Second. Third." },
      };
    },
  });
  const logs = [];
  await createConversations({
    ...base,
    tonbo,
    slack,
    log: (event) => logs.push(event),
  }).handle(message);
  assert.ok(submissions >= 2, "the idempotent submission is sent again");
  assert.equal(tonbo.calls.filter((call) => call[0] === "abortTurn").length, 0);
  const stops = slack.calls.filter((call) => call[0] === "stopStream");
  assert.equal(stops.length, 1);
  assert.deepEqual(stops[0], ["stopStream", "D0CHAN", "1782234700.000100", "", "active"]);
  const streamed =
    slack.calls.find((call) => call[0] === "startStream")[5] +
    slack.calls
      .filter((call) => call[0] === "appendStream")
      .map((call) => call[3])
      .join("");
  assert.equal(streamed, "First sentence. Second. Third.");
  assert.ok(!slack.calls.some((call) => String(call[3] ?? call[5]).includes("could not finish")));
  assert.ok(logs.includes("turn_submit_unanswered"));
  assert.ok(!logs.includes("turn_failed") && !logs.includes("turn_close_failed"));
});

test("a submission still open when the feed ends is replayed for the recorded answer", async () => {
  const slack = fakeSlack();
  let submissions = 0;
  let releaseFirst;
  const first = new Promise((resolve) => (releaseFirst = resolve));
  const tonbo = fakeTonbo({
    pages: [
      { events: [delta(1, "Hello")], status: "pending" },
      { events: [], status: "completed" },
    ],
    submit: () => {
      submissions += 1;
      return submissions === 1
        ? first
        : { state: "completed", data: { assistant_text: "Hello world" } };
    },
  });
  const done = createConversations({ ...base, tonbo, slack }).handle(message);
  await done;
  releaseFirst({ state: "completed", data: { assistant_text: "Hello world" } });
  assert.equal(submissions, 2);
  assert.equal(slack.calls.filter((call) => call[0] === "appendStream").at(-1)[3], " world");
  assert.deepEqual(slack.calls.at(-1), ["stopStream", "D0CHAN", "1782234700.000100", "", "active"]);
});

test("a lost operation poll is retried instead of failing the Turn", async () => {
  const slack = fakeSlack();
  let polls = 0;
  const tonbo = fakeTonbo({
    pages: [null],
    submit: {
      state: "pending",
      operationId: "3f1e2d3c-4b5a-4968-8778-695a4b3c2d1e",
    },
  });
  tonbo.operation = async (id) => {
    tonbo.calls.push(["operation", id]);
    polls += 1;
    if (polls === 1) throw transport();
    return { state: "completed", data: { assistant_text: "after the hiccup" } };
  };
  await createConversations({ ...base, tonbo, slack }).handle(message);
  assert.equal(polls, 2);
  assert.equal(slack.calls[1][5], "after the hiccup");
  assert.equal(slack.calls.at(-1)[0], "stopStream");
});

test("a stalled Turn retains its checkpoint and ownership without claiming completion", async () => {
  const slack = fakeSlack();
  let clock = 0;
  let reads = 0;
  const tonbo = fakeTonbo({ submit: () => new Promise(() => {}) });
  tonbo.turnEvents = async () => {
    tonbo.calls.push(["turnEvents"]);
    reads += 1;
    clock += 10 * 60 * 1000;
    return reads === 1
      ? { events: [delta(1, "Started")], status: "pending" }
      : { events: [], status: "pending" };
  };
  const logs = [];
  await createConversations({
    ...base,
    tonbo,
    slack,
    now: () => clock,
    log: (event) => logs.push(event),
  }).handle(message);
  assert.ok(reads <= 5, `gave up after ${reads} reads`);
  assert.ok(logs.includes("turn_no_progress"));
  const last = slack.calls.at(-1);
  assert.equal(last[0], "appendStream");
  assert.equal(last[4][0].status, "in_progress");
  assert.match(last[4][0].details, /No new progress/);
  assert.ok(logs.includes("turn_reconciliation_required"));
  assert.equal(slack.calls.filter((call) => call[0] === "stopStream").length, 0);
  assert.equal(tonbo.calls.filter((call) => call[0] === "abortTurn").length, 0);
});

/** A Slack fake whose streams get distinct timestamps, for multi-message replies. */
function numberedSlack() {
  const slack = fakeSlack();
  let opened = 0;
  slack.startStream = async ({ channelId, threadTs, userId, teamId, text }) => {
    opened += 1;
    const ts = `1782234700.${String(opened).padStart(6, "0")}`;
    slack.calls.push(["startStream", channelId, threadTs, userId, teamId, text, ts]);
    return ts;
  };
  return slack;
}
const chars = (n, c = "x") => c.repeat(n);
const streamedText = (slack) =>
  slack.calls
    .filter(
      (call) => call[0] === "startStream" || call[0] === "appendStream" || call[0] === "stopStream",
    )
    .map((call) => (call[0] === "startStream" ? call[5] : call[3]))
    .join("");

test("a long recorded remainder is appended in Slack-sized chunks", async () => {
  const slack = numberedSlack();
  const remainder = chars(30_000, "é");
  const tonbo = fakeTonbo({
    pages: [{ events: [delta(1, "Intro. ")], status: "completed" }],
    submit: {
      state: "completed",
      data: { assistant_text: "Intro. " + remainder },
    },
    settleAfterFeed: true,
  });
  await createConversations({ ...base, tonbo, slack }).handle(message);
  const appends = slack.calls.filter((call) => call[0] === "appendStream");
  assert.deepEqual(
    appends.map((call) => [...call[3]].length),
    [12_000, 12_000, 6_000],
  );
  assert.equal(slack.calls.filter((call) => call[0] === "startStream").length, 1);
  assert.equal(slack.calls.filter((call) => call[0] === "stopStream").length, 1);
  assert.equal(streamedText(slack), "Intro. " + remainder);
});

test("a reply past Slack's message cap continues as another message in the thread", async () => {
  const slack = numberedSlack();
  const text = chars(90_000);
  const tonbo = fakeTonbo({
    pages: [{ events: [delta(1, text.slice(0, 5_000))], status: "completed" }],
    submit: { state: "completed", data: { assistant_text: text } },
    settleAfterFeed: true,
  });
  await createConversations({ ...base, tonbo, slack }).handle(message);
  const starts = slack.calls.filter((call) => call[0] === "startStream");
  const stops = slack.calls.filter((call) => call[0] === "stopStream");
  assert.ok(starts.length >= 2, "the reply continued in a second message");
  assert.equal(stops.length, starts.length);
  for (const start of starts) assert.equal(start[2], message.ts);
  assert.deepEqual(
    stops.map((call) => call[4]),
    [...stops.slice(0, -1).map(() => "processing"), "active"],
  );
  // Each closed message names the stream that was open at the time, in order.
  assert.deepEqual(
    stops.map((call) => call[2]),
    starts.map((call) => call[6]),
  );
  for (const call of slack.calls)
    if (call[0] === "appendStream" || call[0] === "startStream")
      assert.ok([...(call[0] === "startStream" ? call[5] : call[3])].length <= 12_000);
  assert.equal(streamedText(slack), text);
  assert.equal([...streamedText(slack)].length, 90_000);
});

test("a stop during the continuation closes the message that is open", async () => {
  const slack = numberedSlack();
  let releaseSecond;
  const secondOpening = new Promise((resolve) => (releaseSecond = resolve));
  const open = slack.startStream;
  let opened = 0;
  slack.startStream = async (input) => {
    opened += 1;
    if (opened === 2) await secondOpening;
    return open(input);
  };
  let submitted;
  const tonbo = fakeTonbo({
    submit: () => new Promise((resolve) => (submitted = resolve)),
  });
  let served = 0;
  tonbo.turnEvents = async () =>
    (served += 1) === 1
      ? {
          events: [delta(1, chars(36_000)), delta(2, chars(12_000))],
          status: "pending",
        }
      : new Promise((resolve) => setTimeout(() => resolve({ events: [], status: "pending" }), 5));
  const conversations = createConversations({
    ...base,
    tonbo,
    slack,
    sleep: (ms) => new Promise((r) => setTimeout(r, ms || 1)),
  });
  const prompt = conversations.handle(message);
  while (slack.calls.filter((call) => call[0] === "stopStream").length < 1)
    await new Promise((r) => setTimeout(r, 1));
  // The first message is closed and the second is opening: stop now.
  await conversations.handle(stop);
  releaseSecond();
  submitted({ state: "completed", data: { assistant_text: chars(48_000) } });
  await prompt;
  const stops = slack.calls.filter((call) => call[0] === "stopStream");
  assert.equal(stops.length, 2);
  assert.deepEqual(stops[0].slice(2), ["1782234700.000001", "", "processing"]);
  assert.deepEqual(stops[1].slice(2), ["1782234700.000002", "\n\n_Stopped._", "active"]);
  assert.ok(tonbo.calls.some((call) => call[0] === "abortTurn"));
});

test("a failure notice that would overflow the message goes into a new one, bounded", async () => {
  const slack = numberedSlack();
  const tonbo = fakeTonbo({
    submit: {
      state: "failed",
      code: "turn_failed",
      message: "turn failed: " + chars(5_000, "m"),
    },
  });
  let served = 0;
  tonbo.turnEvents = async () =>
    (served += 1) === 1
      ? { events: [delta(1, chars(37_990))], status: "pending" }
      : { events: [], status: "failed" };
  await createConversations({ ...base, tonbo, slack }).handle(message);
  const stops = slack.calls.filter((call) => call[0] === "stopStream");
  const starts = slack.calls.filter((call) => call[0] === "startStream");
  assert.equal(starts.length, 2);
  assert.deepEqual(stops[0].slice(2), ["1782234700.000001", "", "processing"]);
  assert.match(starts[1][5], /^\n\nThe Agent could not finish this reply: turn failed: m+$/);
  assert.ok([...starts[1][5]].length <= 12_000);
  assert.ok([...starts[1][5]].length < 1_100, "the reason is bounded");
  assert.deepEqual(stops[1].slice(2), ["1782234700.000002", "", "active"]);
});

test("splits on characters, never inside a surrogate pair", () => {
  const text = "😀".repeat(12_001);
  const chunks = chunkText(text);
  assert.equal(chunks.length, 2);
  assert.equal([...chunks[0]].length, 12_000);
  assert.equal(chunks[1], "😀");
  assert.deepEqual(chunkText(""), []);
});

test("the recorded answer is the last assistant message; its unstreamed tail is appended once", async () => {
  const slack = numberedSlack();
  const last = "Read both. Here's my exploration of the brief in full.";
  const tonbo = fakeTonbo({
    pages: [
      { events: [delta(1, "I'll read the brief first.")], status: "pending" },
      { events: [delta(2, "Read both. Here's my")], status: "completed" },
      { events: [] },
    ],
    submit: { state: "completed", data: { assistant_text: last } },
    settleAfterFeed: true,
  });
  const logs = [];
  await createConversations({
    ...base,
    tonbo,
    slack,
    log: (e, f) => logs.push([e, f]),
  }).handle(message);
  assert.equal(streamedText(slack), "I'll read the brief first." + last);
  assert.deepEqual(logs.find(([e]) => e === "turn_remainder")[1].branch, "suffix");
});

test("events that land after the settling page are drained before the answer is closed", async () => {
  const slack = numberedSlack();
  const full = "Alpha. Beta. Gamma.";
  const tonbo = fakeTonbo({
    pages: [
      { events: [delta(1, "Alpha. ")], status: "completed" },
      { events: [delta(2, "Beta. ")], status: "completed" },
      { events: [delta(3, "Gamma.")], status: "completed" },
      { events: [], status: "completed" },
    ],
    submit: { state: "completed", data: { assistant_text: full } },
    settleAfterFeed: true,
  });
  await createConversations({ ...base, tonbo, slack }).handle(message);
  assert.deepEqual(
    tonbo.calls.filter((c) => c[0] === "turnEvents").map((c) => c[1]),
    [0, 1, 2, 3],
  );
  assert.equal(streamedText(slack), full);
  assert.equal(slack.calls.filter((c) => c[0] === "stopStream").length, 1);
});

test("computes the tail of the recorded answer against what was streamed", () => {
  assert.deepEqual(remainderAfter("Hello", "Hello world"), {
    text: " world",
    branch: "prefix",
  });
  assert.deepEqual(remainderAfter("first.Read both. Here", "Read both. Here's more"), {
    text: "'s more",
    branch: "suffix",
  });
  assert.deepEqual(remainderAfter("", "Answer"), {
    text: "Answer",
    branch: "unstreamed",
  });
  assert.deepEqual(remainderAfter("Whole answer here", "answer"), {
    text: "",
    branch: "contained",
  });
  assert.deepEqual(remainderAfter("Something else", "Answer"), {
    text: "\n\nAnswer",
    branch: "disjoint",
  });
  assert.deepEqual(remainderAfter("Streamed", ""), {
    text: "",
    branch: "empty",
  });
  assert.deepEqual(remainderAfter("Same", "Same"), {
    text: "",
    branch: "prefix",
  });
});

test("an empty recorded answer with nothing streamed says so, and with text streamed closes quietly", async () => {
  const quiet = numberedSlack();
  await createConversations({
    ...base,
    tonbo: fakeTonbo({
      pages: [{ events: [], status: "completed" }],
      submit: { state: "completed", data: { assistant_text: "" } },
    }),
    slack: quiet,
  }).handle(message);
  assert.equal(
    quiet.calls.find((c) => c[0] === "startStream")[5],
    "The Agent completed without a text response.",
  );
  const partial = numberedSlack();
  await createConversations({
    ...base,
    tonbo: fakeTonbo({
      pages: [{ events: [delta(1, "Only streamed")], status: "completed" }],
      submit: { state: "completed", data: { assistant_text: "" } },
    }),
    slack: partial,
  }).handle(message);
  assert.equal(streamedText(partial), "Only streamed");
  assert.equal(partial.calls.filter((c) => c[0] === "appendStream").length, 0);
});

import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createInflightStore } from "../inflight.mjs";
import { slackAgentInput } from "../identity.mjs";

async function durableFixture(run) {
  const directory = await mkdtemp(path.join(os.tmpdir(), "slack-owner-"));
  try {
    await run(createInflightStore({ directory }));
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}
function ownerships() {
  const owners = new Map();
  return (owner, released = () => {}) => ({
    async acquire(work) {
      if (owners.has(work) && owners.get(work) !== owner) return null;
      owners.set(work, owner);
      let valid = true;
      return {
        assertOwned() {
          assert.ok(valid && owners.get(work) === owner);
        },
        abandon() {
          valid = false;
        },
        async release() {
          owners.delete(work);
          released();
        },
      };
    },
  });
}
test("overlapping application processes neither replay owned work nor completed tombstones", () =>
  durableFixture(async (store) => {
    const activity = ownerships();
    const oldSlack = fakeSlack();
    const newSlack = fakeSlack();
    const tonbo = fakeTonbo({
      pages: [{ events: [], status: "completed" }],
      submit: { state: "completed", data: { assistant_text: "once" } },
    });
    const old = createConversations({
      ...base,
      store,
      activities: activity("old"),
      slack: oldSlack,
      tonbo,
    });
    const next = createConversations({
      ...base,
      store,
      activities: activity("new"),
      slack: newSlack,
      tonbo,
    });
    const run = await old.accept(message);
    assert.equal((await store.list()).length, 1);
    assert.equal(oldSlack.calls.length, 0, "acceptance must precede provider effects");
    await next.handle(message);
    assert.equal(newSlack.calls.length, 0);
    await run();
    await next.handle(message);
    assert.equal(newSlack.calls.length, 0);
    assert.equal(tonbo.calls.filter(([name]) => name === "submitTurn").length, 1);
  }));
test("restart resumes partial event offset and fetches the original Turn's final answer", () =>
  durableFixture(async (store) => {
    const command = slackAgentInput({
      agentId,
      teamId: base.teamId,
      botUserId: base.botUserId,
      event: message,
    });
    await store.save({
      ...command,
      turnId: command.idempotencyKey,
      command,
      prompt: "hello",
      streamed: "He",
      streamTs: "1782234700.000100",
      messageChars: 2,
      cursor: 0,
      startedAt: Date.now(),
      processingEvent: { sequence: 1, text: "Hello", offset: 2 },
      pendingEffect: null,
      closed: false,
    });
    const slack = fakeSlack();
    const tonbo = fakeTonbo({
      pages: [
        { events: [delta(1, "Hello")], status: "completed" },
        { events: [], status: "completed" },
      ],
      submit: { state: "completed", data: { assistant_text: "Hello" } },
    });
    let resolve;
    const completed = new Promise((done) => {
      resolve = done;
    });
    const conversations = createConversations({
      ...base,
      store,
      slack,
      tonbo,
      activities: ownerships()("new", resolve),
    });
    await conversations.resume();
    await completed;
    assert.deepEqual(
      slack.calls.filter(([name]) => name === "appendStream").map((call) => call[3]),
      ["llo"],
    );
    assert.equal(slack.calls.filter(([name]) => name === "startStream").length, 0);
    assert.equal(tonbo.calls.find(([name]) => name === "submitTurn")[3], "hello");
    assert.equal((await store.get(command.idempotencyKey)).completed, true);
  }));
test("new runtime routes Stop to the persisted old Turn without taking its stream", () =>
  durableFixture(async (store) => {
    const command = slackAgentInput({
      agentId,
      teamId: base.teamId,
      botUserId: base.botUserId,
      event: message,
    });
    await store.save({
      ...command,
      turnId: command.idempotencyKey,
      command,
      streamTs: "1782234700.000100",
    });
    const slack = fakeSlack();
    const tonbo = fakeTonbo({});
    await createConversations({
      ...base,
      store,
      slack,
      tonbo,
      activities: {
        acquire() {
          throw new Error("Stop must not seize response ownership");
        },
      },
    }).handle(stop);
    assert.equal(tonbo.calls[0][0], "abortTurn");
    assert.equal(slack.calls.length, 0);
    assert.equal((await store.list()).length, 1);
  }));

test("900 tiny deltas are delivered exactly in nine batches with bounded durable writes", async () => {
  const slack = numberedSlack();
  const events = Array.from({ length: 900 }, (_, i) => delta(i + 1, "中😀x"));
  const text = events.map((e) => e.payload.text).join("");
  const pages = Array.from({ length: 9 }, (_, i) => ({
    events: events.slice(i * 100, (i + 1) * 100),
    status: i === 8 ? "completed" : "pending",
  }));
  let saves = 0;
  let clock = 0;
  const records = new Map();
  const realStart = slack.startStream;
  const realAppend = slack.appendStream;
  slack.startStream = async (args) => {
    clock += 150;
    return realStart(args);
  };
  slack.appendStream = async (...args) => {
    clock += 150;
    return realAppend(...args);
  };
  await createConversations({
    ...base,
    slack,
    now: () => clock,
    tonbo: fakeTonbo({
      pages,
      submit: { state: "completed", data: { assistant_text: text } },
      settleAfterFeed: true,
    }),
    store: {
      get: async (id) => records.get(id),
      list: async () => [],
      save: async (turn) => {
        saves++;
        clock += 20;
        records.set(turn.turnId, { cursor: turn.cursor });
      },
      complete: async () => {},
    },
  }).handle(message);
  assert.equal(streamedText(slack), text);
  assert.equal(
    slack.calls.filter(([kind]) => ["startStream", "appendStream"].includes(kind)).length,
    9,
  );
  assert.ok(saves <= 24, `too many checkpoint writes: ${saves}`);
  assert.ok(clock < 2000, `simulated delivery took ${clock}ms`);
});

test("an explicitly expired Slack stream continues only its rejected text", async () => {
  const slack = numberedSlack();
  const append = slack.appendStream;
  let rejected = false;
  slack.appendStream = async (...args) => {
    if (!rejected) {
      rejected = true;
      throw Object.assign(new Error("closed"), {
        code: "message_not_in_streaming_state",
      });
    }
    return append(...args);
  };
  const tonbo = fakeTonbo({
    pages: [
      { events: [delta(1, "first ")], status: "pending" },
      { events: [delta(2, "second")], status: "completed" },
    ],
    submit: { state: "completed", data: { assistant_text: "first second" } },
    settleAfterFeed: true,
  });
  await createConversations({ ...base, slack, tonbo }).handle(message);
  assert.equal(streamedText(slack), "first second");
  assert.equal(slack.calls.filter(([kind]) => kind === "startStream").length, 2);
  assert.equal(slack.calls.at(-1)[0], "stopStream");
});

test("a stopped stream can finish without another append or a duplicate message", async () => {
  const slack = numberedSlack();
  slack.stopStream = async () => {
    throw Object.assign(new Error("closed"), {
      code: "message_not_in_streaming_state",
    });
  };
  const logs = [];
  await createConversations({
    ...base,
    slack,
    log: (event) => logs.push(event),
    tonbo: fakeTonbo({
      pages: [{ events: [delta(1, "done")], status: "completed" }],
      submit: { state: "completed", data: { assistant_text: "done" } },
      settleAfterFeed: true,
    }),
  }).handle(message);
  assert.equal(streamedText(slack), "done");
  assert.ok(logs.includes("turn_finished"));
});

test("an uncertain append is retained and never retried on a fresh stream", async () => {
  const slack = numberedSlack();
  slack.appendStream = async () => {
    throw new Error("response lost after provider acceptance");
  };
  const logs = [];
  await createConversations({
    ...base,
    slack,
    log: (event) => logs.push(event),
    tonbo: fakeTonbo({
      pages: [
        { events: [delta(1, "first")], status: "pending" },
        { events: [delta(2, "tail")], status: "completed" },
      ],
      submit: { state: "completed", data: { assistant_text: "firsttail" } },
      settleAfterFeed: true,
    }),
  }).handle(message);
  assert.equal(slack.calls.filter(([kind]) => kind === "startStream").length, 1);
  assert.ok(logs.includes("turn_reconciliation_required"));
  assert.ok(!logs.includes("turn_finished"));
});

test("a long tool pause rotates the stream before sending the next batch", async () => {
  let clock = 0;
  const slack = numberedSlack();
  let reads = 0;
  const tonbo = fakeTonbo({
    pages: [
      { events: [delta(1, "first")], status: "pending" },
      { events: [delta(2, "tail")], status: "completed" },
    ],
    submit: { state: "completed", data: { assistant_text: "firsttail" } },
    settleAfterFeed: true,
  });
  const read = tonbo.turnEvents;
  tonbo.turnEvents = async (...args) => {
    if (++reads > 1) clock = 241000;
    return read(...args);
  };
  await createConversations({ ...base, slack, tonbo, now: () => clock }).handle(message);
  assert.equal(streamedText(slack), "firsttail");
  // The recorded answer may settle early, but the injected pause must still
  // rotate the stream if the second page is read by the feed/drain.
  assert.equal(slack.calls.filter(([kind]) => kind === "startStream").length, 2);
});

test("restart resumes an acknowledged prefix of a multi-event batch without duplication", () =>
  durableFixture(async (store) => {
    const command = slackAgentInput({
      agentId,
      teamId: base.teamId,
      botUserId: base.botUserId,
      event: message,
    });
    await store.save({
      ...command,
      turnId: command.idempotencyKey,
      command,
      prompt: "hello",
      streamed: "He",
      streamTs: "1782234700.000100",
      messageChars: 2,
      cursor: 0,
      startedAt: Date.now(),
      processingEvent: { sequence: 2, text: "Hello", offset: 2 },
      pendingEffect: null,
      closed: false,
    });
    const slack = fakeSlack();
    const tonbo = fakeTonbo({
      pages: [
        {
          events: [delta(1, "Hel"), delta(2, "lo"), delta(3, "!")],
          status: "completed",
        },
        { events: [], status: "completed" },
      ],
      submit: { state: "completed", data: { assistant_text: "Hello!" } },
    });
    let done;
    const completed = new Promise((resolve) => {
      done = resolve;
    });
    await createConversations({
      ...base,
      store,
      slack,
      tonbo,
      activities: ownerships()("restored", done),
    }).resume();
    await completed;
    assert.equal(
      slack.calls
        .filter(([kind]) => kind === "appendStream")
        .map((call) => call[3])
        .join(""),
      "llo!",
    );
    assert.equal((await store.get(command.idempotencyKey)).completed, true);
  }));

test("delivery policy is customizable while Slack request limits stay enforced", async () => {
  const slack = fakeSlack();
  await createConversations({
    ...base,
    batchChars: 2,
    streamMaxAgeMs: 10_000,
    slack,
    tonbo: fakeTonbo({
      pages: [{ events: [delta(1, "abcde")], status: "completed" }],
      submit: { state: "completed", data: { assistant_text: "abcde" } },
      settleAfterFeed: true,
    }),
  }).handle(message);
  assert.equal(streamedText(slack), "abcde");
  assert.deepEqual(
    slack.calls.filter(([kind]) => kind === "appendStream").map((call) => call[3]),
    ["cd", "e"],
  );
  for (const batchChars of [0, 12_001, NaN])
    assert.throws(() => createConversations({ ...base, batchChars }), /batchChars/);
  assert.throws(() => createConversations({ ...base, streamMaxAgeMs: 0 }), /streamMaxAgeMs/);
});

test("cancelled work archives its checkpoint without Slack or Turn effects", async () => {
  const slack = fakeSlack();
  const tonbo = fakeTonbo({});
  const cancelled = [];
  const conversations = createConversations({
    ...base,
    slack,
    tonbo,
    activities: { acquire: async () => ({ cancelled: true, cancellationId: "cancel-id" }) },
    store: {
      cancel: async (...args) => cancelled.push(args),
      list: async () => [{ turnId: "old-turn", command: {} }],
    },
  });
  await conversations.resume();
  assert.deepEqual(cancelled, [["old-turn", "cancel-id"]]);
  assert.deepEqual(slack.calls, []);
  assert.deepEqual(tonbo.calls, []);
});

test("a long tool wait updates one native task before the exact final answer", async () => {
  const slack = fakeSlack();
  let clock = 0;
  let reads = 0;
  let finish;
  const submitted = new Promise((resolve) => {
    finish = resolve;
  });
  const tonbo = fakeTonbo({ submit: () => submitted });
  tonbo.turnEvents = async () => {
    reads += 1;
    clock += 11_000;
    if (reads === 1)
      return {
        status: "pending",
        events: [
          {
            sequence: 1,
            event_type: "tool.started",
            created_at: new Date(clock).toISOString(),
            payload: { tool_call_id: "a", tool_name: "bash", arguments: "SECRET" },
          },
        ],
      };
    if (reads < 5) return { status: "pending", events: [] };
    finish({ state: "completed", data: { assistant_text: "Done." } });
    return { status: "completed", events: [] };
  };
  await createConversations({ ...base, tonbo, slack, now: () => clock }).handle(message);
  const opened = slack.calls.filter((call) => call[0] === "startStream");
  assert.equal(opened.length, 1);
  assert.equal(opened[0][5], "");
  assert.equal(opened[0][6][0].title, "Running tools");
  const updates = slack.calls.filter((call) => call[0] === "appendStream" && call[4]);
  assert.ok(updates.length >= 2);
  assert.match(updates.at(-1)[4][0].details, /No new progress/);
  assert.ok(updates.every((call) => call[4][0].id === "execution"));
  const answers = slack.calls.filter((call) => call[0] === "appendStream" && !call[4]);
  assert.deepEqual(
    answers.map((call) => call[3]),
    ["Done."],
  );
  const closed = slack.calls.find((call) => call[0] === "stopStream");
  assert.equal(closed[5][0].status, "complete");
  assert.ok(!JSON.stringify(slack.calls).includes("SECRET"));
});

test("only the legacy first plain-text append to a progress-only stream is provably rejected", () => {
  const record = {
    version: 2,
    streamTs: "1.0",
    pendingEffect: { kind: "append", streamTs: "1.0" },
    streamed: "",
    messageChars: 0,
    progressShownAt: 10,
    streamOpenedAt: 10,
    processingEvent: { sequence: 2, text: "Hello", offset: 0 },
  };
  assert.equal(rejectedLegacyFirstAppend(record), true);
  for (const change of [
    { streamFormat: "chunks" },
    { streamed: "H" },
    { messageChars: 1 },
    { pendingEffect: { kind: "start" } },
    { pendingEffect: { kind: "append", streamTs: "2.0" } },
    { progressShownAt: undefined },
    { streamOpenedAt: 11 },
    { cancelled: true },
    { processingEvent: { sequence: 2, text: "Hello", offset: 1 } },
  ]) {
    assert.equal(rejectedLegacyFirstAppend({ ...record, ...change }), false);
  }
});

test("restart recovers the rejected legacy first append without regenerating or duplicating text", () =>
  durableFixture(async (store) => {
    const command = slackAgentInput({
      agentId,
      teamId: base.teamId,
      botUserId: base.botUserId,
      event: message,
    });
    const started = Date.now();
    await store.save({
      ...command,
      turnId: command.idempotencyKey,
      command,
      prompt: "hello",
      streamed: "",
      streamTs: "1782234700.000100",
      messageChars: 0,
      cursor: 0,
      startedAt: Date.now(),
      processingEvent: { sequence: 1, text: "Hello", offset: 0 },
      pendingEffect: { kind: "append", streamTs: "1782234700.000100" },
      streamOpenedAt: started,
      progressShownAt: started,
      closed: false,
    });
    const slack = fakeSlack();
    const tonbo = fakeTonbo({
      pages: [
        { events: [delta(1, "Hello")], status: "completed" },
        { events: [], status: "completed" },
      ],
      submit: { state: "completed", data: { assistant_text: "Hello" } },
    });
    let resolve;
    const completed = new Promise((done) => {
      resolve = done;
    });
    const conversations = createConversations({
      ...base,
      store,
      slack,
      tonbo,
      activities: ownerships()("new", resolve),
    });
    await conversations.resume();
    await completed;
    assert.deepEqual(
      slack.calls.filter(([name]) => name === "appendStream").map((call) => call[3]),
      ["Hello"],
    );
    assert.equal(slack.calls.filter(([name]) => name === "startStream").length, 0);
    assert.equal(tonbo.calls.find(([name]) => name === "submitTurn")[3], "hello");
    assert.equal((await store.get(command.idempotencyKey)).completed, true);
  }));
