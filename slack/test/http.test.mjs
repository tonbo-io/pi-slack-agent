import assert from "node:assert/strict";
import { createHmac } from "node:crypto";
import { createServer } from "node:http";
import test from "node:test";
import { SeenEvents } from "../dedupe.mjs";
import { createSlackRequestHandler } from "../http.mjs";

const secret = "signing-secret";
async function withHandler(options, run) {
  const events = [];
  let release;
  const gate = new Promise((resolve) => (release = resolve));
  const handler = createSlackRequestHandler({
    signingSecret: secret,
    teamId: "T0AAA",
    seen: new SeenEvents(),
    onAccept: async (event) => async () => {
      events.push(event);
      await gate;
    },
    ...options,
  });
  const server = createServer((request, response) => {
    handler(request, response).catch(() => {
      if (!response.headersSent) response.writeHead(500);
      response.end();
    });
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const origin = `http://127.0.0.1:${server.address().port}`;
  const post = async (
    payload,
    {
      timestamp = String(Math.floor(Date.now() / 1000)),
      sign = true,
      retry,
      path = "/slack/events",
    } = {},
  ) => {
    const body = typeof payload === "string" ? payload : JSON.stringify(payload);
    const signature = `v0=${createHmac("sha256", secret).update(`v0:${timestamp}:${body}`).digest("hex")}`;
    return fetch(origin + path, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-slack-request-timestamp": timestamp,
        ...(sign ? { "x-slack-signature": signature } : {}),
        ...(retry ? { "x-slack-retry-num": retry } : {}),
      },
      body,
    });
  };
  try {
    await run({ post, events, release, origin });
  } finally {
    release();
    server.close();
  }
}
const callback = (eventId, event) => ({
  type: "event_callback",
  team_id: "T0AAA",
  event_id: eventId,
  event,
});

test("answers the URL challenge and rejects unsigned or stale requests", async () => {
  await withHandler({}, async ({ post }) => {
    const challenge = await post({ type: "url_verification", challenge: "abc" });
    assert.equal(challenge.status, 200);
    assert.deepEqual(await challenge.json(), { challenge: "abc" });
    assert.equal(
      (await post({ type: "url_verification", challenge: "abc" }, { sign: false })).status,
      401,
    );
    assert.equal(
      (await post({ type: "url_verification", challenge: "abc" }, { timestamp: "1000" })).status,
      401,
    );
    assert.equal((await post("{not json", {})).status, 400);
    assert.equal((await post({ type: "event_callback" }, { path: "/other" })).status, 404);
    assert.equal(
      (await fetch(`${new URL((await post({ type: "x" })).url).origin}/healthz`)).status,
      200,
    );
  });
});

test("acknowledges before the work finishes and drops retries of a seen event", async () => {
  await withHandler({}, async ({ post, events, release }) => {
    const event = {
      type: "message",
      channel_type: "im",
      channel: "D1",
      user: "U1",
      text: "hi",
      ts: "1.000000",
    };
    const first = await post(callback("Ev1", event));
    assert.equal(first.status, 200);
    assert.equal(events.length, 1);
    const retry = await post(callback("Ev1", event), { retry: "1" });
    assert.equal(retry.status, 200);
    assert.equal(events.length, 1);
    assert.equal((await post(callback("Ev2", event))).status, 200);
    assert.equal(events.length, 2);
    release();
  });
});

test("ignores events for another workspace and malformed callbacks", async () => {
  await withHandler({}, async ({ post, events }) => {
    assert.equal(
      (
        await post({
          type: "event_callback",
          team_id: "T9",
          event_id: "Ev3",
          event: { type: "message" },
        })
      ).status,
      200,
    );
    assert.equal(
      (await post({ type: "event_callback", team_id: "T0AAA", event: { type: "message" } })).status,
      400,
    );
    assert.equal(events.length, 0);
  });
});

test("failed durable acceptance is not ACKed or deduplicated away on retry", async () => {
  let accepted = 0;
  let delivered = 0;
  const seen = new SeenEvents();
  await withHandler(
    {
      seen,
      onAccept: async () => {
        accepted += 1;
        if (accepted === 1) throw new Error("checkpoint fsync failed");
        return async () => {
          delivered += 1;
        };
      },
    },
    async ({ post }) => {
      const event = callback("Ev-durable", { type: "message" });
      assert.equal((await post(event)).status, 500);
      assert.equal(seen.has("Ev-durable"), false);
      assert.equal(delivered, 0);
      assert.equal((await post(event, { retry: "1" })).status, 200);
      assert.equal(accepted, 2);
      assert.equal(delivered, 1);
    },
  );
});

test("acceptance persistence completes before the event enters the ACK dedupe set", async () => {
  let persisted = false;
  let entered;
  let persist;
  const started = new Promise((resolve) => {
    entered = resolve;
  });
  const gate = new Promise((resolve) => {
    persist = resolve;
  });
  const seen = new SeenEvents();
  const remember = seen.remember.bind(seen);
  seen.remember = (id) => {
    assert.equal(persisted, true);
    remember(id);
  };
  await withHandler(
    {
      seen,
      onAccept: async () => {
        entered();
        await gate;
        persisted = true;
        return async () => {};
      },
    },
    async ({ post }) => {
      const response = post(callback("Ev-persist", { type: "message" }));
      await started;
      assert.equal(seen.has("Ev-persist"), false);
      persist();
      assert.equal((await response).status, 200);
      assert.equal(seen.has("Ev-persist"), true);
    },
  );
});
