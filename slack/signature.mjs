import { createHmac, timingSafeEqual } from "node:crypto";

export const SLACK_SIGNATURE_WINDOW_SECONDS = 5 * 60;

/** Slack signs `v0:<timestamp>:<raw body>` with the App's signing secret. A
 * request outside the replay window or with a malformed header is rejected
 * before the body is parsed. */
export function verifySlackSignature({
  signingSecret,
  timestamp,
  signature,
  body,
  now = Date.now(),
}) {
  if (!signingSecret || typeof timestamp !== "string" || typeof signature !== "string")
    return false;
  if (!/^[0-9]{1,12}$/.test(timestamp)) return false;
  const skew = Math.abs(Math.floor(now / 1000) - Number(timestamp));
  if (skew > SLACK_SIGNATURE_WINDOW_SECONDS) return false;
  const expected = createHmac("sha256", signingSecret)
    .update(`v0:${timestamp}:`)
    .update(body)
    .digest("hex");
  const presented = signature.startsWith("v0=") ? signature.slice(3) : "";
  if (presented.length !== expected.length) return false;
  return timingSafeEqual(Buffer.from(presented, "utf8"), Buffer.from(expected, "utf8"));
}
