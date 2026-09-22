import { createWriteStream } from "node:fs";
import { mkdir, rm } from "node:fs/promises";
import path from "node:path";
import { pipeline } from "node:stream/promises";
import { Transform } from "node:stream";

export const MAX_FILE_BYTES = 50 * 1024 * 1024;

/** Slack refuses markdown_text together with chunks. Keep text and native
 * progress in one ordered stream request when both are available. */
export function streamPayload(text, chunks = []) {
  if (chunks.length)
    return { chunks: [...chunks, ...(text ? [{ type: "markdown_text", text }] : [])] };
  return text ? { markdown_text: text } : {};
}

export class SlackApiError extends Error {
  constructor(method, code, retryAfterSeconds) {
    super(`Slack ${method} failed: ${code}`);
    this.method = method;
    this.code = code;
    this.retryAfterSeconds = retryAfterSeconds;
  }
}

/** Only Slack's own file hosts may receive the bot token. Remote files and
 * forged events carry arbitrary `url_private` values. */
export function isSlackFileUrl(value, origin = "https://slack.com") {
  try {
    const url = new URL(value);
    if (url.origin === new URL(origin).origin) return true;
    return (
      url.protocol === "https:" &&
      (url.hostname === "slack.com" || url.hostname.endsWith(".slack.com"))
    );
  } catch {
    return false;
  }
}

/** A file name that is safe as one path segment under the inbox. */
export function safeFileName(name, fallback = "file") {
  const base = path
    .basename(String(name || ""))
    .replace(/[^A-Za-z0-9._-]/g, "_")
    .slice(0, 128);
  return base && base !== "." && base !== ".." ? base : fallback;
}

export function createSlackClient({
  botToken,
  fetch: fetchImpl = globalThis.fetch,
  timeoutMs = 15_000,
  origin = "https://slack.com",
}) {
  if (!botToken) throw new Error("SLACK_BOT_TOKEN is required.");
  async function call(method, payload) {
    const response = await fetchImpl(`${origin}/api/${method}`, {
      method: "POST",
      redirect: "manual",
      signal: AbortSignal.timeout(timeoutMs),
      headers: { authorization: `Bearer ${botToken}`, "content-type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (response.status === 429) {
      const delay = Number(response.headers.get("retry-after"));
      throw new SlackApiError(
        method,
        "ratelimited",
        Number.isFinite(delay) && delay > 0 ? delay : 5,
      );
    }
    const body = await response.json().catch(() => null);
    if (!response.ok || body?.ok !== true)
      throw new SlackApiError(
        method,
        typeof body?.error === "string" ? body.error : `http_${response.status}`,
      );
    return body;
  }
  return {
    call,
    setStatus: (channelId, threadTs, status) =>
      call("agents.sessions.setStatus", { channel_id: channelId, thread_ts: threadTs, status }),
    async startStream({ channelId, threadTs, userId, teamId, text, chunks }) {
      const body = await call("chat.startStream", {
        channel: channelId,
        thread_ts: threadTs,
        recipient_user_id: userId,
        recipient_team_id: teamId,
        ...streamPayload(text, chunks),
      });
      if (typeof body.ts !== "string") throw new SlackApiError("chat.startStream", "missing_ts");
      return body.ts;
    },
    appendStream: (channelId, streamTs, text, chunks) =>
      call("chat.appendStream", {
        channel: channelId,
        ts: streamTs,
        ...streamPayload(text, chunks),
      }),
    stopStream: (channelId, streamTs, text, sessionStatus = "active", chunks) =>
      call("chat.stopStream", {
        channel: channelId,
        ts: streamTs,
        ...streamPayload(text, chunks),
        session_status: sessionStatus,
      }),
    /** Downloads one shared file into `<directory>/<file id>/<safe name>` with
     * the bot token, refusing anything over the byte limit. Resolves the path. */
    async downloadFile(file, directory, { maxBytes = MAX_FILE_BYTES } = {}) {
      let url = file.url;
      let name = file.name;
      if (!url) {
        const info = await fetchImpl(
          `${origin}/api/files.info?file=${encodeURIComponent(file.id)}`,
          {
            headers: { authorization: `Bearer ${botToken}` },
            redirect: "manual",
            signal: AbortSignal.timeout(timeoutMs),
          },
        );
        const body = await info.json().catch(() => null);
        if (!info.ok || body?.ok !== true || typeof body.file?.url_private_download !== "string")
          throw new SlackApiError(
            "files.info",
            typeof body?.error === "string" ? body.error : "unavailable",
          );
        url = body.file.url_private_download;
        name ||= body.file.name;
      }
      if (!isSlackFileUrl(url, origin)) throw new SlackApiError("files.download", "untrusted_host");
      if (Number.isSafeInteger(file.size) && file.size > maxBytes)
        throw new SlackApiError("files.download", "too_large");
      const target = path.join(directory, file.id, safeFileName(name, file.id));
      const response = await fetchImpl(url, {
        headers: { authorization: `Bearer ${botToken}` },
        redirect: "manual",
        signal: AbortSignal.timeout(Math.max(timeoutMs, 120_000)),
      });
      if (!response.ok || !response.body)
        throw new SlackApiError("files.download", `http_${response.status}`);
      const declared = Number(response.headers.get("content-length"));
      if (Number.isFinite(declared) && declared > maxBytes)
        throw new SlackApiError("files.download", "too_large");
      await mkdir(path.dirname(target), { recursive: true });
      let total = 0;
      const limit = new Transform({
        transform(chunk, _encoding, callback) {
          total += chunk.length;
          if (total > maxBytes) callback(new SlackApiError("files.download", "too_large"));
          else callback(null, chunk);
        },
      });
      try {
        await pipeline(
          response.body,
          limit,
          createWriteStream(target, { flags: "w", mode: 0o600 }),
        );
      } catch (error) {
        await rm(target, { force: true });
        throw error;
      }
      return target;
    },
  };
}
