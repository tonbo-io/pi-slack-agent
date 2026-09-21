import assert from "node:assert/strict";
import test from "node:test";
import { agentTurnSessionId, slackAgentInput } from "../identity.mjs";

const agentId = "0f1e2d3c-4b5a-4968-8778-695a4b3c2d1e";
const base = { agentId, teamId: "T0AAA", botUserId: "U0BOT" };
const message = {
  type: "message",
  channel_type: "im",
  channel: "D0CHAN",
  user: "U0USER",
  text: "hi",
  ts: "1782234671.392669",
};

test("derives version 4 UUIDs that differ by Agent", () => {
  const one = agentTurnSessionId(agentId, "key");
  assert.match(one, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  assert.equal(one, agentTurnSessionId(agentId, "key"));
  assert.notEqual(one, agentTurnSessionId("1f1e2d3c-4b5a-4968-8778-695a4b3c2d1e", "key"));
});

test("a thread is one Session and a message is one Turn across retries and surfaces", () => {
  const root = slackAgentInput({ ...base, event: message });
  const reply = slackAgentInput({
    ...base,
    event: { ...message, ts: "1782234680.000001", thread_ts: message.ts },
  });
  const mention = slackAgentInput({
    ...base,
    event: { ...message, type: "app_mention", channel_type: undefined, channel: "C0CHAN" },
  });
  assert.equal(root.kind, "prompt");
  assert.equal(root.sessionId, reply.sessionId);
  assert.notEqual(root.idempotencyKey, reply.idempotencyKey);
  assert.equal(slackAgentInput({ ...base, event: message }).idempotencyKey, root.idempotencyKey);
  assert.notEqual(mention.sessionId, root.sessionId);
  assert.equal(root.threadTs, message.ts);
  assert.equal(reply.threadTs, message.ts);
});

test("a stop names the same Session as the thread it interrupts", () => {
  const prompt = slackAgentInput({ ...base, event: message });
  const stop = slackAgentInput({
    ...base,
    event: {
      type: "agent_session_stopped",
      channel: "D0CHAN",
      thread_ts: message.ts,
      user: "U0USER",
      event_ts: "1782234690.000001",
    },
  });
  assert.equal(stop.kind, "stop");
  assert.equal(stop.sessionId, prompt.sessionId);
  assert.notEqual(stop.idempotencyKey, prompt.idempotencyKey);
});

test("ignores bots, edits, unsupported events and empty messages", () => {
  assert.equal(slackAgentInput({ ...base, event: { ...message, bot_id: "B1" } }).reason, "bot");
  assert.equal(slackAgentInput({ ...base, event: { ...message, user: "U0BOT" } }).reason, "bot");
  assert.equal(
    slackAgentInput({ ...base, event: { ...message, subtype: "message_changed" } }).reason,
    "message_update",
  );
  assert.equal(
    slackAgentInput({ ...base, event: { ...message, type: "reaction_added" } }).reason,
    "unsupported_event",
  );
  assert.equal(
    slackAgentInput({ ...base, event: { ...message, channel_type: "channel" } }).reason,
    "unsupported_event",
  );
  assert.equal(
    slackAgentInput({ ...base, event: { ...message, text: "  " } }).reason,
    "empty_message",
  );
  assert.throws(() => slackAgentInput({ ...base, event: { ...message, channel: "bad" } }));
});

test("keeps bounded file references from a file_share message", () => {
  const files = Array.from({ length: 12 }, (_, index) => ({
    id: `F${index}`,
    name: `report-${index}.pdf`,
    size: 1024,
    url_private_download: `https://files.slack.com/${index}`,
  }));
  const shared = slackAgentInput({
    ...base,
    event: { ...message, subtype: "file_share", text: "", files },
  });
  assert.equal(shared.kind, "prompt");
  assert.equal(shared.prompt, "");
  assert.equal(shared.files.length, 10);
  assert.deepEqual(shared.files[0], {
    id: "F0",
    name: "report-0.pdf",
    size: 1024,
    url: "https://files.slack.com/0",
  });
  assert.equal(
    slackAgentInput({ ...base, event: { ...message, files: [{ id: "nope" }] } }).files.length,
    0,
  );
});
