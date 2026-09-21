import { randomUUID } from "node:crypto";

/** Generic named Activity over the runtime's existing local broker. A lost
 * heartbeat ends this owner's permission to emit effects; it never licenses
 * another process to take ownership. */
export function createActivityClient({ origin, fetchImpl = fetch, owner = randomUUID() }) {
  if (!origin) throw new Error("TONBO_ACTIVITY_URL is required.");
  async function request(method, suffix = "", body) {
    const response = await fetchImpl(`${origin}${suffix}`, {
      method,
      signal: AbortSignal.timeout(2000),
      headers: { "content-type": "application/json" },
      ...(body ? { body: JSON.stringify(body) } : {}),
    });
    if (response.status === 409 && method === "POST") return null;
    if (!response.ok) throw new Error(`Activity ${method} failed: ${response.status}`);
    return method === "DELETE" ? true : response.json();
  }
  return {
    async acquire(workKey) {
      const startedAt = Date.now();
      const admitted = await request("POST", "", { work_key: workKey, owner_instance: owner });
      if (!admitted) return null;
      const { lease_id: id, lease_seconds: seconds } = admitted;
      if (typeof id !== "string" || !Number.isFinite(seconds) || seconds < 5)
        throw new Error("Invalid Activity admission response.");
      let valid = true;
      let expiresAt = startedAt + seconds * 1000;
      let renewing = false;
      const timer = setInterval(
        async () => {
          if (renewing || !valid) return;
          renewing = true;
          try {
            const renewalStartedAt = Date.now();
            await request("PATCH", `/${id}`);
            expiresAt = renewalStartedAt + seconds * 1000;
          } catch {
            valid = false;
          } finally {
            renewing = false;
          }
        },
        Math.floor((seconds * 1000) / 3),
      );
      timer.unref();
      return {
        assertOwned() {
          if (!valid || Date.now() >= expiresAt)
            throw new Error("Activity ownership is uncertain.");
        },
        abandon() {
          valid = false;
          clearInterval(timer);
        },
        async release() {
          valid = false;
          clearInterval(timer);
          await request("DELETE", `/${id}`);
        },
      };
    },
  };
}
