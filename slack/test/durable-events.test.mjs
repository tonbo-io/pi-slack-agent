import assert from "node:assert/strict";
import test from "node:test";
import { durableEvents } from "../durable-events.mjs";

function response(text, piece = 3) {
  const bytes = new TextEncoder().encode(text);
  let offset = 0;
  return new Response(
    new ReadableStream({
      pull(controller) {
        if (offset >= bytes.length) controller.close();
        else {
          controller.enqueue(bytes.slice(offset, offset + piece));
          offset += piece;
        }
      },
    }),
    { headers: { "content-type": "text/event-stream" } },
  );
}
const frame = (type, payload) => `event: ${type}\ndata:${JSON.stringify(payload)}\n\n`;
const record = (n, value) => frame("data", { record: n, value });
const control = (n, extra = {}) =>
  frame("control", {
    streamNextOffset: "00000000000000000042",
    streamFirstRecord: 0,
    streamNextRecord: n,
    ...extra,
  });
async function collect(source, options) {
  const values = [];
  for await (const value of durableEvents(source, options)) values.push(value);
  return values;
}

test("reads pinned Ursula envelope/control frames across UTF-8 network boundaries", async () => {
  const source = response(record(4, { text: "你好" }) + control(5, { streamClosed: true }), 1);
  assert.deepEqual(await collect(source, { nextRecord: 4 }), [
    {
      record: 4,
      nextRecord: 5,
      value: { text: "你好" },
    },
  ]);
});

test("a record is not yielded without its replay cursor", async () => {
  const iterator = durableEvents(response(record(0, { text: "partial" })));
  await assert.rejects(iterator.next(), /mid-record/);
});

test("retention and coordinate gaps require resynchronization", async () => {
  await assert.rejects(collect(new Response(null, { status: 410 })), /retention gap/);
  await assert.rejects(collect(response(record(2, {}) + control(3))), /record gap/);
  await assert.rejects(collect(response(record(0, {}) + control(2))), /cursor disagrees/);
});

test("heartbeat controls produce no fabricated progress", async () => {
  assert.deepEqual(await collect(response(control(0) + control(0, { streamClosed: true }))), []);
});

test("oversized frames and upstream errors are bounded failures", async () => {
  await assert.rejects(
    collect(response(record(0, "x".repeat(100))), { maxFrameBytes: 40 }),
    /limit/,
  );
  await assert.rejects(collect(response(frame("error", "SECRET"))), /upstream error/);
});

test("ending iteration cancels the HTTP body instead of leaving a subscriber running", async () => {
  let cancelled = false;
  const source = new Response(
    new ReadableStream({
      start(controller) {
        controller.enqueue(new TextEncoder().encode(record(0, {}) + control(1)));
      },
      cancel() {
        cancelled = true;
      },
    }),
    { headers: { "content-type": "text/event-stream" } },
  );
  for await (const _event of durableEvents(source)) break;
  assert.equal(cancelled, true);
});
