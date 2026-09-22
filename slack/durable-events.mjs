/** Reads Ursula's record-envelope SSE representation. Authentication and
 * reconnect policy belong to the caller; this generator owns bounded parsing,
 * exact replay coordinates and cancellation of its HTTP body on exit. */
export async function* durableEvents(response, { nextRecord = 0, maxFrameBytes = 65_536 } = {}) {
  if (!Number.isSafeInteger(nextRecord) || nextRecord < 0)
    throw new RangeError("Invalid next record.");
  if (!Number.isSafeInteger(maxFrameBytes) || maxFrameBytes < 1)
    throw new RangeError("Invalid frame limit.");
  if (response.status === 410)
    throw new Error("Progress retention gap; resynchronization required.");
  if (!response.ok || !response.body) throw new Error(`Progress stream HTTP ${response.status}.`);
  if (response.headers.get("content-type")?.split(";")[0] !== "text/event-stream")
    throw new Error("Expected a progress SSE response.");
  if (response.headers.get("stream-sse-data-encoding"))
    throw new Error("Expected unencoded record envelopes.");
  const reader = response.body.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: true });
  const encoder = new TextEncoder();
  let buffer = "";
  let eventType = "message";
  let data = [];
  let frameBytes = 0;
  let pending = null;
  let expected = nextRecord;
  try {
    for (;;) {
      const { value, done } = await reader.read();
      buffer += done ? decoder.decode() : decoder.decode(value, { stream: true });
      let newline;
      while ((newline = buffer.indexOf("\n")) !== -1) {
        const line = buffer.slice(0, newline).replace(/\r$/, "");
        buffer = buffer.slice(newline + 1);
        frameBytes += encoder.encode(line).length + 1;
        if (frameBytes > maxFrameBytes) throw new Error("Progress frame exceeds its limit.");
        if (line) {
          if (line.startsWith("event:")) eventType = line.slice(6).trimStart();
          if (line.startsWith("data:")) data.push(line.slice(5).replace(/^ /, ""));
          continue;
        }
        const type = eventType;
        const payload = data.join("\n");
        eventType = "message";
        data = [];
        frameBytes = 0;
        if (!payload) continue;
        if (type === "error") throw new Error("Progress stream reported an upstream error.");
        if (type === "data") {
          if (pending) throw new Error("Progress record omitted its control cursor.");
          const envelope = JSON.parse(payload);
          if (!Number.isSafeInteger(envelope.record) || envelope.record !== expected)
            throw new Error("Progress record gap or unexpected replay coordinate.");
          if (!Object.hasOwn(envelope, "value")) throw new Error("Progress record omitted value.");
          pending = envelope;
        } else if (type === "control") {
          const control = JSON.parse(payload);
          const after = expected + (pending ? 1 : 0);
          if (control.streamNextRecord !== after || !Number.isSafeInteger(after))
            throw new Error("Progress control cursor disagrees with its record.");
          if (control.streamFirstRecord > expected)
            throw new Error("Progress retention gap; resynchronization required.");
          if (pending) {
            const record = pending;
            pending = null;
            expected = after;
            yield { record: record.record, nextRecord: expected, value: record.value };
          }
          if (control.streamClosed === true) return;
        } else {
          throw new Error("Unknown progress stream event.");
        }
      }
      if (encoder.encode(buffer).length + frameBytes > maxFrameBytes)
        throw new Error("Progress frame exceeds its limit.");
      if (done) {
        if (pending || buffer || data.length) throw new Error("Progress stream ended mid-record.");
        return;
      }
    }
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}
