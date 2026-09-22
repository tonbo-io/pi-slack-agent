import assert from "node:assert/strict";
import test from "node:test";
import { initialProgress, progressView, reduceProgress } from "../progress.mjs";

const start = Date.parse("2026-09-22T00:00:00Z");
const event = (sequence, event_type, payload = {}, seconds = sequence) => ({
  sequence,
  event_type,
  payload,
  created_at: new Date(start + seconds * 1000).toISOString(),
});
const replay = (events) => events.reduce(reduceProgress, initialProgress(start));

test("parallel tools remain active until their matching completions", () => {
  let state = replay([
    event(1, "tool.started", { tool_call_id: "a", tool_name: "read" }),
    event(2, "tool.started", { tool_call_id: "b", tool_name: "bash" }),
    event(3, "tool.completed", { tool_call_id: "a", is_error: false }),
  ]);
  assert.equal(state.phase, "tools");
  assert.deepEqual(state.tools, { b: "bash" });
  state = reduceProgress(state, event(4, "tool.completed", { tool_call_id: "b", is_error: true }));
  assert.equal(state.phase, "waiting");
  assert.equal(state.toolsCompleted, 2);
  assert.equal(state.toolsFailed, 1);
});

test("replay and out-of-order delivery cannot double count or resurrect completion", () => {
  const events = [
    event(1, "tool.started", { tool_call_id: "a", tool_name: "read" }),
    event(2, "tool.completed", { tool_call_id: "a", is_error: false }),
    event(3, "run.completed"),
  ];
  const state = replay(events);
  assert.deepEqual(replay([...events, ...events, event(4, "run.started")]), state);
});

test("silence is stale information, not thinking or failure", () => {
  const state = initialProgress(start);
  const view = progressView(state, start + 45_000);
  assert.equal(view.phase, "waiting");
  assert.equal(view.stale, true);
  assert.equal(view.elapsedSeconds, 45);
  assert.equal(view.terminal, false);
});

test("only explicit model metadata reports thinking; private fields are discarded", () => {
  const state = replay([event(1, "model.phase", { phase: "thinking", text: "SECRET" })]);
  assert.equal(state.phase, "thinking");
  assert.ok(!JSON.stringify(state).includes("SECRET"));
  const tool = reduceProgress(
    state,
    event(2, "tool.started", {
      tool_call_id: "a",
      tool_name: "read",
      arguments: { path: "SECRET" },
      output: "SECRET",
    }),
  );
  assert.ok(!JSON.stringify(tool).includes("SECRET"));
});

test("assistant settlement is not authoritative Turn completion", () => {
  const state = replay([
    event(1, "assistant.delta", { text: "hello" }),
    event(2, "assistant.completed"),
  ]);
  assert.equal(state.phase, "answering");
  assert.equal(state.terminal, false);
  assert.equal(reduceProgress(state, event(3, "run.failed")).phase, "failed");
});

test("out-of-order timestamps never move activity time backwards", () => {
  const state = replay([
    event(1, "run.started", {}, 10),
    event(2, "assistant.delta", { text: "hi" }, 5),
  ]);
  assert.equal(state.changedAt, start + 10_000);
  assert.equal(progressView(state, start + 9000).phaseSeconds, 0);
});

test("invalid identities and unknown events cannot spoof tool activity or heartbeat", () => {
  const initial = initialProgress(start);
  assert.equal(
    reduceProgress(initial, { ...event(1, "run.started"), created_at: "invalid" }),
    initial,
  );
  const state = replay([event(1, "heartbeat", { text: "thinking" }, 20)]);
  assert.equal(state.observedAt, start);
  assert.equal(progressView(state, start + 30_000).stale, true);
});
