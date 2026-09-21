/** A client for the Agent's own Management API. It exchanges the channel-scoped
 * API key for a short-lived access token and caches it, then creates and
 * follows Turns. Every path is pinned to the one Agent this runtime hosts. */
export const MANAGEMENT_API_AUDIENCE = "https://api.tonbo.dev";
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export class TonboRequestError extends Error {
  constructor(status, code, message, retryAfterSeconds) {
    super(message || code || `Management API answered ${status}`);
    this.status = status;
    this.code = code;
    this.retryAfterSeconds = retryAfterSeconds;
  }
}

/** The failure text a problem response carries, bounded for a Slack notice. */
export function problemMessage(body, status) {
  const error = body && typeof body === "object" ? body.error : null;
  const message =
    (error && typeof error === "object" && typeof error.message === "string" && error.message) ||
    (typeof body?.detail === "string" && body.detail) ||
    (typeof body?.title === "string" && body.title) ||
    (typeof body?.code === "string" && body.code) ||
    `Management API answered ${status}`;
  return [...message].slice(0, 1000).join("");
}
function problemCode(body) {
  const error = body && typeof body === "object" ? body.error : null;
  if (error && typeof error === "object" && typeof error.code === "string") return error.code;
  return typeof body?.code === "string" ? body.code : null;
}

export function createTonboClient({
  origin,
  iamOrigin = origin,
  apiKey,
  agentId,
  audience = MANAGEMENT_API_AUDIENCE,
  fetch: fetchImpl = globalThis.fetch,
  now = () => Date.now(),
  timeoutMs = 20_000,
}) {
  const base = new URL(origin);
  // The token route lives on the application origin; the Management API origin admits only /v1.
  const iam = new URL(iamOrigin);
  for (const [name, url] of [
    ["Management API", base],
    ["IAM", iam],
  ])
    if (url.protocol !== "https:" && url.hostname !== "127.0.0.1" && url.hostname !== "localhost")
      throw new Error(`The ${name} origin must use HTTPS.`);
  if (!UUID.test(agentId)) throw new Error("TONBO_AGENT_ID must be a UUID.");
  if (!apiKey) throw new Error("TONBO_AGENT_API_KEY is required.");
  let cached = null;
  let exchanging = null;

  async function exchange() {
    const response = await fetchImpl(new URL("/api/iam/token", iam), {
      method: "POST",
      redirect: "manual",
      signal: AbortSignal.timeout(timeoutMs),
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ api_key: apiKey, audience }),
    });
    const body = await response.json().catch(() => null);
    if (!response.ok || typeof body?.access_token !== "string")
      throw new TonboRequestError(response.status, "token_exchange_failed", body?.error);
    const expiresIn = Number.isFinite(body.expires_in) ? Number(body.expires_in) : 300;
    return { token: body.access_token, expiresAt: now() + Math.max(expiresIn - 60, 30) * 1000 };
  }
  async function token() {
    if (cached && cached.expiresAt > now()) return cached.token;
    exchanging ??= exchange().finally(() => {
      exchanging = null;
    });
    cached = await exchanging;
    return cached.token;
  }
  async function request(path, { method = "GET", idempotencyKey, body } = {}, retried = false) {
    const bearer = await token();
    const response = await fetchImpl(new URL(`/v1/agents/${agentId}${path}`, base), {
      method,
      redirect: "manual",
      signal: AbortSignal.timeout(timeoutMs),
      headers: {
        authorization: `Bearer ${bearer}`,
        ...(body === undefined ? {} : { "content-type": "application/json" }),
        ...(idempotencyKey ? { "idempotency-key": idempotencyKey } : {}),
      },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    if (response.status === 401 && !retried) {
      // The cached token was revoked or expired early: exchange again once.
      // Every request is idempotent by key or read-only, so a retry is safe.
      cached = null;
      return request(path, { method, idempotencyKey, body }, true);
    }
    const parsed = await response.json().catch(() => null);
    return { status: response.status, body: parsed, headers: response.headers };
  }
  function retryAfter(headers) {
    const seconds = Number(headers.get("retry-after"));
    return Number.isFinite(seconds) && seconds > 0 ? Math.min(seconds, 3600) : undefined;
  }
  function settle(result) {
    if (result.status === 200) return { state: "completed", data: result.body?.data ?? {} };
    if (result.status === 202) {
      const operationId =
        result.headers.get("x-operation-id") ||
        result.body?.data?.operation_id ||
        result.body?.data?.id;
      return {
        state: "pending",
        operationId: typeof operationId === "string" ? operationId : null,
      };
    }
    return {
      state: "failed",
      status: result.status,
      code: problemCode(result.body),
      message: problemMessage(result.body, result.status),
      retryAfterSeconds: retryAfter(result.headers),
    };
  }
  return {
    /** Creates the Turn (and its Session on first use). Resolves when the
     * Management API answers: completed with the final text, pending with an
     * operation to poll, or failed with the problem the coordinator recorded. */
    async submitTurn(sessionId, turnId, prompt) {
      if (!UUID.test(sessionId) || !UUID.test(turnId)) throw new Error("Invalid Turn identity.");
      return settle(
        await request("/turns", {
          method: "POST",
          idempotencyKey: turnId,
          body: { session_id: sessionId, prompt },
        }),
      );
    },
    async operation(operationId) {
      if (!UUID.test(operationId)) throw new Error("Invalid operation identity.");
      return settle(await request(`/operations/${operationId}`));
    },
    /** One page of the Turn's event feed after a cursor. `null` while the Turn
     * does not exist yet; the coordinator creates it on claim. */
    async turnEvents(sessionId, turnId, after) {
      const result = await request(`/sessions/${sessionId}/turns/${turnId}/events?after=${after}`);
      if (result.status === 404) return null;
      if (result.status !== 200)
        throw new TonboRequestError(
          result.status,
          problemCode(result.body),
          problemMessage(result.body, result.status),
          retryAfter(result.headers),
        );
      const events = Array.isArray(result.body?.data) ? result.body.data : [];
      return {
        events,
        status: typeof result.body?.status === "string" ? result.body.status : "pending",
      };
    },
    async abortTurn(sessionId, turnId, requestId) {
      if (!UUID.test(requestId)) throw new Error("Invalid abort identity.");
      return settle(
        await request(`/sessions/${sessionId}/turns/${turnId}/abort`, {
          method: "POST",
          idempotencyKey: requestId,
        }),
      );
    },
  };
}
