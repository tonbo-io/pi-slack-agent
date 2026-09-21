import assert from "node:assert/strict";
import test from "node:test";
import { createConversations, createPollBudget } from "../conversation.mjs";

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
    startStream: async ({ channelId, threadTs, userId, teamId, text }) => {
      calls.push(["startStream", channelId, threadTs, userId, teamId, text]);
      return "1782234700.000100";
    },
    appendStream: async (channel, ts, text) => calls.push(["appendStream", channel, ts, text]),
    stopStream: async (channel, ts, text, status) =>
      calls.push(["stopStream", channel, ts, text, status]),
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
const delta = (sequence, text) => ({ sequence, event_type: "assistant.delta", payload: { text } });

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
    ["setStatus", "startStream", "appendStream", "appendStream", "appendStream", "stopStream"],
  );
  assert.deepEqual(slack.calls[0], ["setStatus", "D0CHAN", message.ts, "processing"]);
  assert.deepEqual(slack.calls[1].slice(1), ["D0CHAN", message.ts, "U0USER", "T0AAA", "Hel"]);
  assert.deepEqual(
    slack.calls.slice(2, 5).map((call) => call[3]),
    ["lo", " wor", "ld"],
  );
  assert.deepEqual(slack.calls[5], ["stopStream", "D0CHAN", "1782234700.000100", "", "active"]);
  assert.equal(tonbo.calls[0][0], "submitTurn");
  assert.equal(tonbo.calls[0][3], "hello");
  assert.deepEqual(
    tonbo.calls.filter((c) => c[0] === "turnEvents").map((c) => c[1]),
    [0, 0, 2, 3],
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
    submit: { state: "completed", data: { assistant_text: "x".repeat(100) + "y" } },
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
  assert.equal(tonbo.calls.filter((c) => c[0] === "turnEvents").length, 2);
  assert.equal(sleeps, 0);
  assert.equal(slack.calls.at(-1)[0], "stopStream");
});

test("replays the recorded answer when the feed carries no text", async () => {
  const slack = fakeSlack();
  const tonbo = fakeTonbo({
    pages: [
      { events: [{ sequence: 1, event_type: "run.started", payload: {} }], status: "completed" },
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
    submit: { state: "failed", code: "turn_failed", message: "turn failed: process lost" },
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
    submit: () => gate.then(() => ({ state: "completed", data: { assistant_text: "late" } })),
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
  await createConversations({ ...base, tonbo, slack, inbox: "/workspace/inbox/slack" }).handle({
    ...message,
    subtype: "file_share",
    text: "please read",
    files: [
      { id: "F1", name: "brief.md", size: 12, url_private_download: "https://files.slack.com/F1" },
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
        : { state: "pending", operationId: "3f1e2d3c-4b5a-4968-8778-695a4b3c2d1e" },
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
    submit: () => gate.then(() => ({ state: "completed", data: { assistant_text: "once" } })),
  });
  let served = 0;
  tonbo.turnEvents = async () => {
    served += 1;
    return served > 3 ? { events: [], status: "completed" } : { events: [], status: "pending" };
  };
  const conversations = createConversations({ ...base, tonbo, slack });
  const first = conversations.handle(message);
  const second = conversations.handle({ ...message, type: "app_mention", channel_type: undefined });
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
  const tonbo = fakeTonbo({ submit: () => new Promise((resolve) => (submitted = resolve)) });
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

test("an unexpected error after the stream opened closes it with a notice", async () => {
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
  await createConversations({ ...base, tonbo, slack }).handle(message);
  const last = slack.calls.at(-1);
  assert.equal(last[0], "stopStream");
  assert.match(last[3], /could not finish this reply: slack exploded/);
  assert.equal(last[4], "active");
});

test("retries a rate-limited Slack write once after the pause Slack asks for", async () => {
  const slack = fakeSlack();
  const sleeps = [];
  let attempts = 0;
  const start = slack.startStream;
  slack.startStream = async (input) => {
    if ((attempts += 1) === 1)
      throw Object.assign(new Error("ratelimited"), { code: "ratelimited", retryAfterSeconds: 3 });
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

test("sleeps for Retry-After when the event feed is rate limited and backs off while idle", async () => {
  const slack = fakeSlack();
  const sleeps = [];
  let reads = 0;
  let done;
  const feedDone = new Promise((resolve) => (done = resolve));
  // The submission settles only once the feed has reported completion, so
  // the feed alone drives the loop.
  const tonbo = fakeTonbo({
    submit: () => feedDone.then(() => ({ state: "completed", data: { assistant_text: "ok" } })),
  });
  tonbo.turnEvents = async () => {
    reads += 1;
    if (reads === 1)
      throw Object.assign(new Error("limited"), { status: 429, retryAfterSeconds: 60 });
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
  assert.deepEqual(sleeps.filter((ms) => ms > 0).slice(0, 4), [60000, 2000, 4000, 5000]);
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
  Object.assign(new Error("The operation was aborted due to timeout"), { code: "transport" });

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
      return { state: "completed", data: { assistant_text: "First sentence. Second. Third." } };
    },
  });
  const logs = [];
  await createConversations({ ...base, tonbo, slack, log: (event) => logs.push(event) }).handle(
    message,
  );
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
    submit: { state: "pending", operationId: "3f1e2d3c-4b5a-4968-8778-695a4b3c2d1e" },
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

test("a Turn that produces nothing for the deadline is given up on", async () => {
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
  assert.equal(last[0], "stopStream");
  assert.match(last[3], /could not finish this reply: no progress for 30 minutes/);
  assert.equal(tonbo.calls.filter((call) => call[0] === "abortTurn").length, 0);
});
