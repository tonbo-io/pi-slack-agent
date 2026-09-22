import assert from "node:assert/strict";
import test from "node:test";
import { eventBatches } from "../event-batches.mjs";
const event = (sequence, text) => ({
  sequence,
  event_type: "assistant.delta",
  payload: { text },
});
async function collect(source, options) {
  const batches = [];
  for await (const batch of eventBatches(source, { maxChars: 100, ...options }))
    batches.push(batch);
  return batches;
}
test("async source is batched without losing Unicode or control boundaries", async () => {
  async function* source() {
    yield event(1, "中😀");
    yield event(2, "abc");
    yield { sequence: 3, event_type: "run.completed" };
  }
  assert.deepEqual(await collect(source(), { maxChars: 4 }), [
    { sequence: 1, text: "中😀" },
    { sequence: 2, text: "abc" },
    { sequence: 3, text: null },
  ]);
});
test("restart retains the exact pending batch boundary even when the next page grew", async () => {
  assert.deepEqual(
    await collect([event(1, "a"), event(2, "b"), event(3, "c")], {
      pending: { sequence: 2, text: "ab", offset: 1 },
    }),
    [
      { sequence: 2, text: "ab" },
      { sequence: 3, text: "c" },
    ],
  );
});
test("incomplete or changed pending batches fail closed", async () => {
  await assert.rejects(
    collect([event(1, "a")], { pending: { sequence: 2, text: "ab" } }),
    /incomplete/,
  );
  await assert.rejects(
    collect([event(1, "a"), event(2, "x")], {
      pending: { sequence: 2, text: "ab" },
    }),
    /differs/,
  );
});
