import assert from "node:assert/strict";
import test from "node:test";
import { createActivityClient } from "../activity.mjs";
test("named Activity carries stable process identity and releases only explicitly", async () => {
  const calls = [];
  const client = createActivityClient({
    origin: "http://localhost/activity",
    owner: "process-one",
    fetchImpl: async (url, init) => {
      calls.push([url, init]);
      return new Response(JSON.stringify({ lease_id: "lease", lease_seconds: 45 }));
    },
  });
  const lease = await client.acquire("work-one");
  lease.assertOwned();
  assert.deepEqual(JSON.parse(calls[0][1].body), {
    work_key: "work-one",
    owner_instance: "process-one",
  });
  lease.abandon();
  assert.throws(() => lease.assertOwned(), /uncertain/);
  assert.equal(calls.length, 1);
  await lease.release();
  assert.equal(calls[1][1].method, "DELETE");
});
test("another owner is a conflict, not permission to replay", async () => {
  const client = createActivityClient({
    origin: "http://localhost/activity",
    fetchImpl: async () => new Response("", { status: 409 }),
  });
  assert.equal(await client.acquire("work-one"), null);
});

test("failed heartbeat stops effects without releasing uncertain ownership", async (t) => {
  t.mock.timers.enable({ apis: ["Date", "setInterval"], now: 1000 });
  const methods = [];
  const client = createActivityClient({
    origin: "http://localhost/activity",
    fetchImpl: async (_url, init) => {
      methods.push(init.method);
      return init.method === "POST"
        ? new Response(JSON.stringify({ lease_id: "lease", lease_seconds: 6 }))
        : new Response("", { status: 409 });
    },
  });
  const lease = await client.acquire("owned-work");
  t.mock.timers.tick(2000);
  await new Promise(setImmediate);
  assert.throws(() => lease.assertOwned(), /uncertain/);
  lease.abandon();
  assert.deepEqual(methods, ["POST", "PATCH"]);
});

test("a delayed admission response cannot extend ownership beyond its original expiry", async (t) => {
  t.mock.timers.enable({ apis: ["Date", "setInterval"], now: 1000 });
  const client = createActivityClient({
    origin: "http://localhost/activity",
    fetchImpl: async () => {
      t.mock.timers.tick(7000);
      return new Response(JSON.stringify({ lease_id: "lease", lease_seconds: 6 }));
    },
  });
  const lease = await client.acquire("owned-work");
  assert.throws(() => lease.assertOwned(), /uncertain/);
  lease.abandon();
});
