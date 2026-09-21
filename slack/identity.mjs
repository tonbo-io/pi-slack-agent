import { createHash } from "node:crypto";

/** The Agent-scoped UUID derivation the platform uses for Sessions and Turns:
 * a SHA-256 over the Agent id and a caller key, shaped as a version 4 UUID.
 * The Agent id is part of the digest, so the same key in another tenant can
 * never collide. Mirrors the Management API's own derivation. */
export function agentTurnSessionId(agentId, key) {
  const bytes = createHash("sha256")
    .update(agentId)
    .update("\0")
    .update(key)
    .digest()
    .subarray(0, 16);
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const value = bytes.toString("hex");
  return `${value.slice(0, 8)}-${value.slice(8, 12)}-${value.slice(12, 16)}-${value.slice(16, 20)}-${value.slice(20)}`;
}

const TS = /^\d{1,16}\.\d{6}$/;
const CHANNEL = /^[CDG][A-Z0-9]+$/;
const USER = /^U[A-Z0-9]+$/;

/** Maps a verified Slack event to Agent work. A thread is one Session, a
 * message is one Turn, a stop names the Session it interrupts. Slack retries
 * and duplicate mention plus DM delivery of one message resolve to the same
 * Turn because the message timestamp is the key. */
export function slackAgentInput({ agentId, teamId, botUserId, event }) {
  if (!event || typeof event !== "object" || Array.isArray(event))
    throw new Error("Invalid Slack event.");
  const stop = event.type === "agent_session_stopped";
  const message =
    event.type === "app_mention" || (event.type === "message" && event.channel_type === "im");
  if (!stop && !message) return { kind: "ignore", reason: "unsupported_event" };
  if (
    event.bot_id ||
    event.bot_profile ||
    event.user === botUserId ||
    event.subtype === "bot_message"
  )
    return { kind: "ignore", reason: "bot" };
  if (message && event.subtype && event.subtype !== "file_share")
    return { kind: "ignore", reason: "message_update" };
  const text = typeof event.text === "string" ? event.text.trim() : "";
  const files = message ? slackFiles(event.files) : [];
  if (message && !text && files.length === 0) return { kind: "ignore", reason: "empty_message" };
  const threadTs = stop ? event.thread_ts : (event.thread_ts ?? event.ts);
  if (
    typeof event.channel !== "string" ||
    !CHANNEL.test(event.channel) ||
    typeof event.user !== "string" ||
    !USER.test(event.user) ||
    typeof threadTs !== "string" ||
    !TS.test(threadTs)
  )
    throw new Error("Invalid Slack conversation.");
  const timestamp = stop ? event.event_ts : event.ts;
  if (typeof timestamp !== "string" || !TS.test(timestamp))
    throw new Error("Invalid Slack event timestamp.");
  const identity = [teamId, event.channel];
  const conversation = {
    channelId: event.channel,
    threadTs,
    userId: event.user,
    sessionId: agentTurnSessionId(
      agentId,
      JSON.stringify(["slack-session-v2", ...identity, threadTs]),
    ),
  };
  const idempotencyKey = agentTurnSessionId(
    agentId,
    JSON.stringify([stop ? "slack-stop-v2" : "slack-message-v2", ...identity, timestamp]),
  );
  return stop
    ? { kind: "stop", ...conversation, eventTs: timestamp, idempotencyKey }
    : {
        kind: "prompt",
        ...conversation,
        prompt: text,
        files,
        messageTs: timestamp,
        idempotencyKey,
      };
}

const FILE_ID = /^F[A-Z0-9]+$/;
function slackFiles(value) {
  if (!Array.isArray(value)) return [];
  return value
    .filter(
      (file) =>
        file && typeof file === "object" && typeof file.id === "string" && FILE_ID.test(file.id),
    )
    .slice(0, 10)
    .map((file) => ({
      id: file.id,
      name: typeof file.name === "string" ? file.name : "",
      size: Number.isSafeInteger(file.size) ? file.size : null,
      url: typeof file.url_private_download === "string" ? file.url_private_download : null,
    }));
}
