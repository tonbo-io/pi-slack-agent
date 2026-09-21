import assert from "node:assert/strict";
import test from "node:test";
import { createTonboClient, problemMessage } from "../tonbo.mjs";

const agentId = "0f1e2d3c-4b5a-4968-8778-695a4b3c2d1e";
const sessionId = "1f1e2d3c-4b5a-4968-8778-695a4b3c2d1e";
const turnId = "2f1e2d3c-4b5a-4968-8778-695a4b3c2d1e";

function harness(routes) {
  const calls = [];
  const fetchImpl = async (url, init = {}) => {
    const target = new URL(url);
    calls.push({ origin: target.origin, path: target.pathname + target.search, init });
    const route = routes[`${init.method ?? "GET"} ${target.pathname}`];
    if (!route) return new Response("{}", { status: 404 });
    const answer = typeof route === "function" ? route(init) : route;
    return new Response(JSON.stringify(answer.body ?? {}), {
      status: answer.status ?? 200,
      headers: answer.headers,
    });
  };
  return { calls, fetchImpl };
}

test("exchanges the key once and reuses the token until it nears expiry", async () => {
  let clock = 1_000_000;
  let exchanges = 0;
  const { calls, fetchImpl } = harness({
    "POST /api/iam/token": () => ({
      body: { access_token: `tok${(exchanges += 1)}`, expires_in: 300 },
    }),
    [`POST /v1/agents/${agentId}/turns`]: {
      body: { data: { turn_id: turnId, assistant_text: "hi" } },
    },
  });
  const client = createTonboClient({
    origin: "https://api.example.test",
    iamOrigin: "https://app.example.test",
    apiKey: "tbo_x",
    agentId,
    fetch: fetchImpl,
    now: () => clock,
  });
  await client.submitTurn(sessionId, turnId, "hello");
  await client.submitTurn(sessionId, turnId, "hello");
  assert.equal(exchanges, 1);
  const exchange = calls[0];
  assert.equal(exchange.origin, "https://app.example.test");
  assert.equal(calls[1].origin, "https://api.example.test");
  assert.deepEqual(JSON.parse(exchange.init.body), {
    api_key: "tbo_x",
    audience: "https://api.tonbo.dev",
  });
  assert.equal(calls[1].init.headers.authorization, "Bearer tok1");
  assert.equal(calls[1].init.headers["idempotency-key"], turnId);
  assert.deepEqual(JSON.parse(calls[1].init.body), { session_id: sessionId, prompt: "hello" });
  clock += 241_000;
  await client.submitTurn(sessionId, turnId, "hello");
  assert.equal(exchanges, 2);
  assert.equal(calls.at(-1).init.headers.authorization, "Bearer tok2");
});

test("settles completed, pending and failed answers with the coordinator's failure text", async () => {
  const { fetchImpl } = harness({
    "POST /api/iam/token": { body: { access_token: "tok", expires_in: 300 } },
    [`POST /v1/agents/${agentId}/turns`]: (init) => {
      const prompt = JSON.parse(init.body).prompt;
      if (prompt === "pending")
        return {
          status: 202,
          body: { data: { id: "op" } },
          headers: { "x-operation-id": "3f1e2d3c-4b5a-4968-8778-695a4b3c2d1e" },
        };
      if (prompt === "fail")
        return {
          status: 409,
          body: {
            error: {
              code: "turn_failed",
              message: "turn failed: PI assistant failed: insufficient credit",
            },
          },
        };
      return { body: { data: { turn_id: turnId, assistant_text: "done" } } };
    },
    [`GET /v1/agents/${agentId}/operations/3f1e2d3c-4b5a-4968-8778-695a4b3c2d1e`]: {
      body: { data: { turn_id: turnId, assistant_text: "later" } },
    },
    [`GET /v1/agents/${agentId}/sessions/${sessionId}/turns/${turnId}/events`]: {
      body: {
        data: [{ sequence: 7, event_type: "assistant.delta", payload: { text: "d" } }],
        status: "pending",
      },
    },
    [`POST /v1/agents/${agentId}/sessions/${sessionId}/turns/${turnId}/abort`]: {
      status: 202,
      body: { data: { state: "aborting" } },
    },
  });
  const client = createTonboClient({
    origin: "https://api.example.test",
    apiKey: "tbo_x",
    agentId,
    fetch: fetchImpl,
  });
  assert.deepEqual(await client.submitTurn(sessionId, turnId, "ok"), {
    state: "completed",
    data: { turn_id: turnId, assistant_text: "done" },
  });
  const pending = await client.submitTurn(sessionId, turnId, "pending");
  assert.equal(pending.state, "pending");
  assert.equal((await client.operation(pending.operationId)).data.assistant_text, "later");
  const failed = await client.submitTurn(sessionId, turnId, "fail");
  assert.equal(failed.state, "failed");
  assert.equal(failed.code, "turn_failed");
  assert.equal(failed.message, "turn failed: PI assistant failed: insufficient credit");
  assert.deepEqual(await client.turnEvents(sessionId, turnId, 0), {
    events: [{ sequence: 7, event_type: "assistant.delta", payload: { text: "d" } }],
    status: "pending",
  });
  assert.equal(await client.turnEvents(sessionId, "4f1e2d3c-4b5a-4968-8778-695a4b3c2d1e", 0), null);
  assert.equal(
    (await client.abortTurn(sessionId, turnId, "5f1e2d3c-4b5a-4968-8778-695a4b3c2d1e")).state,
    "pending",
  );
  await assert.rejects(() => client.submitTurn("nope", turnId, "x"));
});

test("bounds the failure text and refuses insecure origins", () => {
  assert.equal(problemMessage({ error: { message: "x".repeat(2000) } }, 409).length, 1000);
  assert.equal(problemMessage({ title: "Permission denied" }, 403), "Permission denied");
  assert.equal(problemMessage(null, 502), "Management API answered 502");
  assert.throws(() =>
    createTonboClient({ origin: "http://api.example.test", apiKey: "k", agentId }),
  );
  assert.throws(() =>
    createTonboClient({
      origin: "https://api.example.test",
      iamOrigin: "http://app.example.test",
      apiKey: "k",
      agentId,
    }),
  );
  assert.throws(() =>
    createTonboClient({ origin: "https://api.example.test", apiKey: "k", agentId: "x" }),
  );
});

test("re-exchanges the key and retries once when a cached token is refused", async () => {
  let exchanges = 0;
  let turns = 0;
  const { calls, fetchImpl } = harness({
    "POST /api/iam/token": () => ({
      body: { access_token: `tok${(exchanges += 1)}`, expires_in: 300 },
    }),
    [`POST /v1/agents/${agentId}/turns`]: () =>
      (turns += 1) === 1
        ? { status: 401, body: { code: "unauthenticated" } }
        : { body: { data: { turn_id: turnId, assistant_text: "hi" } } },
  });
  const client = createTonboClient({
    origin: "https://api.example.test",
    apiKey: "tbo_x",
    agentId,
    fetch: fetchImpl,
  });
  assert.equal((await client.submitTurn(sessionId, turnId, "hello")).state, "completed");
  assert.equal(exchanges, 2);
  assert.equal(calls.filter((call) => call.path.endsWith("/turns")).length, 2);
  assert.equal(calls.at(-1).init.headers.authorization, "Bearer tok2");
  turns = 0;
  const refused = createTonboClient({
    origin: "https://api.example.test",
    apiKey: "tbo_x",
    agentId,
    fetch: harness({
      "POST /api/iam/token": { body: { access_token: "tok", expires_in: 300 } },
      [`POST /v1/agents/${agentId}/turns`]: { status: 401, body: { code: "unauthenticated" } },
    }).fetchImpl,
  });
  assert.equal((await refused.submitTurn(sessionId, turnId, "hello")).status, 401);
});

test("carries Retry-After from a refused event read", async () => {
  const { fetchImpl } = harness({
    "POST /api/iam/token": { body: { access_token: "tok", expires_in: 300 } },
    [`GET /v1/agents/${agentId}/sessions/${sessionId}/turns/${turnId}/events`]: {
      status: 429,
      body: { code: "rate_limited" },
      headers: { "retry-after": "60" },
    },
  });
  const client = createTonboClient({
    origin: "https://api.example.test",
    apiKey: "tbo_x",
    agentId,
    fetch: fetchImpl,
  });
  await assert.rejects(() => client.turnEvents(sessionId, turnId, 0), {
    status: 429,
    retryAfterSeconds: 60,
  });
});
