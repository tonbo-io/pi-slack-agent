import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { createInflightStore } from "../inflight.mjs";
const turnId = "00000000-0000-4000-8000-000000000001";
const sessionId = "00000000-0000-4000-8000-000000000002";
async function fixture(run) {
  const directory = await mkdtemp(path.join(os.tmpdir(), "slack-checkpoint-"));
  try {
    await run(createInflightStore({ directory }), directory);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}
test("repeated restart preserves acknowledged prefix, partial event and pending effect", () =>
  fixture(async (store, directory) => {
    const turn = {
      turnId,
      sessionId,
      streamed: "first",
      cursor: 2,
      processingEvent: { sequence: 3, text: "second", offset: 2 },
      pendingEffect: { kind: "append" },
    };
    await store.save(turn);
    const restarted = createInflightStore({ directory });
    const recovered = await restarted.get(turnId);
    assert.equal(recovered.streamed, "first");
    assert.equal(recovered.processingEvent.offset, 2);
    assert.equal(recovered.pendingEffect.kind, "append");
    await restarted.save(recovered);
    assert.deepEqual(await createInflightStore({ directory }).get(turnId), recovered);
  }));
test("concurrent completion and late saves cannot resurrect a completed Turn", () =>
  fixture(async (store, directory) => {
    const turn = { turnId, sessionId, streamed: "done" };
    await Promise.all([
      store.save(turn),
      store.complete(turn),
      store.save({ ...turn, streamed: "stale" }),
    ]);
    assert.deepEqual(await store.list(), []);
    assert.equal((await createInflightStore({ directory }).get(turnId)).completed, true);
  }));
test("corrupt checkpoints are preserved and fail closed", () =>
  fixture(async (store, directory) => {
    const filename = path.join(directory, `${turnId}.json`);
    await writeFile(filename, "incomplete");
    await assert.rejects(store.list());
    assert.equal(await readFile(filename, "utf8"), "incomplete");
    await assert.rejects(store.get("../other"));
  }));
