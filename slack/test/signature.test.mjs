import assert from "node:assert/strict";
import { createHmac } from "node:crypto";
import test from "node:test";
import { verifySlackSignature } from "../signature.mjs";

const secret = "8f742231b10e8888abcd99yyyzzz85a5";
const body = Buffer.from('{"type":"event_callback","event":{"type":"message"}}');
const now = 1_700_000_000_000;
function sign(timestamp, key = secret) {
  return `v0=${createHmac("sha256", key).update(`v0:${timestamp}:`).update(body).digest("hex")}`;
}

test("accepts a fresh v0 signature and rejects a wrong secret", () => {
  const timestamp = String(Math.floor(now / 1000));
  assert.equal(
    verifySlackSignature({
      signingSecret: secret,
      timestamp,
      signature: sign(timestamp),
      body,
      now,
    }),
    true,
  );
  assert.equal(
    verifySlackSignature({
      signingSecret: secret,
      timestamp,
      signature: sign(timestamp, "other"),
      body,
      now,
    }),
    false,
  );
});

test("rejects requests outside the five-minute replay window", () => {
  const stale = String(Math.floor(now / 1000) - 301);
  assert.equal(
    verifySlackSignature({
      signingSecret: secret,
      timestamp: stale,
      signature: sign(stale),
      body,
      now,
    }),
    false,
  );
  const future = String(Math.floor(now / 1000) + 301);
  assert.equal(
    verifySlackSignature({
      signingSecret: secret,
      timestamp: future,
      signature: sign(future),
      body,
      now,
    }),
    false,
  );
});

test("rejects malformed headers without throwing", () => {
  const timestamp = String(Math.floor(now / 1000));
  for (const signature of [undefined, "", "v1=abc", "v0=", "v0=zz", sign(timestamp).slice(0, 10)])
    assert.equal(
      verifySlackSignature({ signingSecret: secret, timestamp, signature, body, now }),
      false,
    );
  assert.equal(
    verifySlackSignature({
      signingSecret: secret,
      timestamp: "abc",
      signature: sign("abc"),
      body,
      now,
    }),
    false,
  );
  assert.equal(
    verifySlackSignature({ signingSecret: "", timestamp, signature: sign(timestamp), body, now }),
    false,
  );
});
