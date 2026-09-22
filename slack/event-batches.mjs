/** Bounded, pull-driven transformation. The durable feed owns unread events;
 * slow consumers never require an unbounded in-memory queue. Flush at each
 * available page, preserving low latency without waiting for a full answer.
 * A saved partial batch fixes its end sequence across restart/page changes. */
export async function* eventBatches(events, { pending = null, maxChars }) {
  if (!Number.isSafeInteger(maxChars) || maxChars < 1)
    throw new RangeError("maxChars must be a positive integer.");
  let text = "";
  let sequence = null;
  let chars = 0;
  for await (const event of events) {
    const next = Number(event.sequence);
    if (event.event_type !== "assistant.delta" || typeof event.payload?.text !== "string") {
      if (pending) throw new Error("Checkpoint batch crossed an unexpected control event.");
      if (sequence !== null) yield { sequence, text };
      text = "";
      chars = 0;
      sequence = null;
      yield { sequence: next, text: null };
      continue;
    }
    const size = [...event.payload.text].length;
    if (!pending && sequence !== null && chars + size > maxChars) {
      yield { sequence, text };
      text = "";
      chars = 0;
    }
    text += event.payload.text;
    chars += size;
    sequence = next;
    if (pending && next === pending.sequence) {
      if (text !== pending.text)
        throw new Error("Checkpoint batch differs from the authoritative feed.");
      yield { sequence, text };
      text = "";
      chars = 0;
      sequence = null;
      pending = null;
    }
  }
  if (pending) throw new Error("Checkpoint batch is incomplete in the authoritative feed page.");
  if (sequence !== null) yield { sequence, text };
}
