import assert from "node:assert/strict";
import test from "node:test";
import { progressChunk } from "../progress-chunk.mjs";
import { streamPayload } from "../slack-api.mjs";

test("native progress and answer text use chunks without incompatible markdown_text", () => {
  const chunk = progressChunk({ phase: "tools", elapsedSeconds: 15, toolsCompleted: 2 });
  assert.deepEqual(streamPayload("hello", [chunk]), {
    chunks: [chunk, { type: "markdown_text", text: "hello" }],
  });
  assert.deepEqual(streamPayload("hello"), { markdown_text: "hello" });
  assert.deepEqual(streamPayload("", [chunk]), { chunks: [chunk] });
});

test("progress stays bounded and never exposes tool data or an invented estimate", () => {
  const chunk = progressChunk({
    phase: "tools",
    elapsedSeconds: 90,
    toolsCompleted: 12,
    activeTools: ["SECRET"],
    stale: true,
    quietSeconds: 35,
  });
  const serialized = JSON.stringify(chunk);
  assert.ok(serialized.length <= 256);
  assert.ok(!serialized.includes("SECRET"));
  assert.match(chunk.details, /No new progress for 35s/);
  assert.equal(chunk.status, "in_progress");
});

test("outcomes retain one task identity without presenting cancellation as success", () => {
  const chunks = ["completed", "failed", "cancelled"].map((phase) =>
    progressChunk({ phase, terminal: true, elapsedSeconds: 15, toolsCompleted: 2 }),
  );
  assert.deepEqual(
    chunks.map((chunk) => chunk.status),
    ["complete", "error", "error"],
  );
  assert.equal(new Set(chunks.map((chunk) => chunk.id)).size, 1);
  assert.equal(chunks[2].title, "Stopped");
});
