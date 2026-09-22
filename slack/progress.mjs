/** Transport-independent projection of redacted execution facts. Replaying a
 * stream reconstructs this view; it grants no execution or cancellation rights.
 * Silence is freshness information, never evidence of model reasoning. */
export function initialProgress(startedAt) {
  return {
    sequence: 0,
    phase: "waiting",
    startedAt,
    changedAt: startedAt,
    observedAt: startedAt,
    tools: {},
    toolsCompleted: 0,
    toolsFailed: 0,
    terminal: false,
  };
}

const terminalPhases = new Set(["completed", "failed", "cancelled"]);
const modelPhases = new Set(["waiting", "thinking", "answering"]);

/** The envelope timestamp belongs to the producer. Duplicate/reordered events
 * do not reset elapsed time, resurrect terminal work or count tools twice. */
export function reduceProgress(previous, event) {
  const sequence = Number(event.sequence);
  if (!Number.isSafeInteger(sequence) || sequence <= previous.sequence || previous.terminal)
    return previous;
  const timestamp = Date.parse(event.created_at);
  if (!Number.isFinite(timestamp)) return previous;
  const state = { ...previous, sequence, observedAt: Math.max(previous.observedAt, timestamp) };
  const payload = event.payload ?? {};
  let phase = previous.phase;
  switch (event.event_type) {
    case "runtime.resuming":
      phase = "resuming";
      break;
    case "runtime.ready":
    case "run.started":
      phase = "waiting";
      break;
    case "model.phase":
      if (!modelPhases.has(payload.phase)) return { ...previous, sequence };
      phase = payload.phase;
      break;
    case "assistant.delta":
      if (typeof payload.text === "string" && payload.text.length) phase = "answering";
      break;
    case "tool.started": {
      const id = payload.tool_call_id;
      if (typeof id !== "string" || !id || id.length > 256 || Object.hasOwn(state.tools, id))
        return { ...previous, sequence };
      // The bounded producer projection supplies identity only. Never retain
      // arguments, outputs, paths, commands or the private model message.
      const name =
        typeof payload.tool_name === "string"
          ? [...payload.tool_name].slice(0, 64).join("")
          : "tool";
      state.tools = { ...state.tools, [id]: name };
      phase = "tools";
      break;
    }
    case "tool.completed": {
      const id = payload.tool_call_id;
      if (typeof id !== "string" || !Object.hasOwn(state.tools, id))
        return { ...previous, sequence };
      state.tools = { ...state.tools };
      delete state.tools[id];
      state.toolsCompleted += 1;
      if (payload.is_error !== false) state.toolsFailed += 1;
      phase = Object.keys(state.tools).length ? "tools" : "waiting";
      break;
    }
    case "run.completed":
      phase = "completed";
      break;
    case "run.failed":
      phase = "failed";
      break;
    case "run.cancelled":
      phase = "cancelled";
      break;
    // assistant.completed only closes the harness output. The authoritative
    // Turn must still settle before the presentation says work completed.
    default:
      return { ...previous, sequence };
  }
  if (phase !== previous.phase) state.changedAt = state.observedAt;
  state.phase = phase;
  state.terminal = terminalPhases.has(phase);
  return state;
}

export function progressView(state, now, staleAfterMs = 30_000) {
  return {
    phase: state.phase,
    elapsedSeconds: Math.max(0, Math.floor((now - state.startedAt) / 1000)),
    phaseSeconds: Math.max(0, Math.floor((now - state.changedAt) / 1000)),
    quietSeconds: Math.max(0, Math.floor((now - state.observedAt) / 1000)),
    stale: !state.terminal && now - state.observedAt >= staleAfterMs,
    activeTools: Object.values(state.tools),
    toolsCompleted: state.toolsCompleted,
    toolsFailed: state.toolsFailed,
    terminal: state.terminal,
  };
}
