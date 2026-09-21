import { verifySlackSignature } from "./signature.mjs";

const MAX_BODY_BYTES = 1024 * 1024;

function readBody(request, limit) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    request.on("data", (chunk) => {
      size += chunk.length;
      if (size > limit) {
        reject(new Error("body_too_large"));
        request.destroy();
        return;
      }
      chunks.push(chunk);
    });
    request.on("end", () => resolve(Buffer.concat(chunks)));
    request.on("error", reject);
  });
}

/** The Slack events endpoint: verify, deduplicate, acknowledge, then hand the
 * event to the conversations without waiting. Anything else is 404. */
export function createSlackRequestHandler({
  signingSecret,
  teamId,
  seen,
  onAccept,
  log = () => {},
  now,
}) {
  return async (request, response) => {
    const end = (status, body = "", type = "text/plain") => {
      response.writeHead(status, { "content-type": type, "cache-control": "no-store" });
      response.end(body);
    };
    const url = new URL(request.url ?? "/", "http://localhost");
    if (request.method === "GET" && url.pathname === "/healthz") return end(200, "ok\n");
    if (request.method !== "POST" || url.pathname !== "/slack/events")
      return end(404, "not found\n");
    let body;
    try {
      body = await readBody(request, MAX_BODY_BYTES);
    } catch {
      return end(413);
    }
    if (
      !verifySlackSignature({
        signingSecret,
        timestamp: request.headers["x-slack-request-timestamp"],
        signature: request.headers["x-slack-signature"],
        body,
        ...(now ? { now: now() } : {}),
      })
    )
      return end(401);
    let payload;
    try {
      payload = JSON.parse(body.toString("utf8"));
      if (!payload || typeof payload !== "object" || Array.isArray(payload)) throw new Error();
    } catch {
      return end(400);
    }
    if (payload.type === "url_verification")
      return typeof payload.challenge === "string" && payload.challenge.length <= 1024
        ? end(200, JSON.stringify({ challenge: payload.challenge }), "application/json")
        : end(400);
    if (payload.type !== "event_callback") return end(200);
    if (payload.team_id !== teamId) {
      log("slack_event_foreign_team", {});
      return end(200);
    }
    const event = payload.event;
    if (typeof payload.event_id !== "string" || !event || typeof event !== "object")
      return end(400);
    const retry = request.headers["x-slack-retry-num"];
    if (seen.has(payload.event_id)) {
      log("slack_event_duplicate", { retry: typeof retry === "string" ? retry : null });
      return end(200);
    }
    const run = await onAccept(event);
    seen.remember(payload.event_id);
    end(200);
    // Durable acceptance precedes ACK; response delivery owns background Activity.
    Promise.resolve()
      .then(run)
      .catch((error) => log("slack_event_failed", { type: event.type, reason: error?.message }));
  };
}
