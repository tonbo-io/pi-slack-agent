import assert from "node:assert/strict";
import test from "node:test";
import { SeenEvents } from "../dedupe.mjs";

test("reports a repeated event id and forgets the oldest past the limit", () => {
  const seen = new SeenEvents(3);
  assert.equal(seen.remember("Ev1"), false);
  assert.equal(seen.remember("Ev1"), true);
  assert.equal(seen.remember("Ev2"), false);
  assert.equal(seen.remember("Ev3"), false);
  assert.equal(seen.remember("Ev4"), false);
  assert.equal(seen.size, 3);
  assert.equal(seen.remember("Ev1"), false);
  assert.equal(seen.remember("Ev4"), true);
});
