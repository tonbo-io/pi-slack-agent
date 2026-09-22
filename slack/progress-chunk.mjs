const titles = {
  resuming: "Restoring execution environment",
  waiting: "Waiting for the next response",
  thinking: "Reasoning",
  tools: "Running tools",
  answering: "Writing the response",
  completed: "Completed",
  failed: "Could not finish",
  cancelled: "Stopped",
};

/** Fixed, application-owned labels cannot leak arbitrary tool names or args.
 * A stable id updates the same task rather than adding a card per tool/token. */
export function progressChunk(view) {
  const details = view.stale
    ? `${view.elapsedSeconds}s elapsed · No new progress for ${view.quietSeconds}s`
    : `${view.elapsedSeconds}s elapsed · ${view.toolsCompleted} tools finished`;
  return {
    type: "task_update",
    id: "execution",
    title: titles[view.phase] ?? titles.waiting,
    status:
      view.phase === "failed" || view.phase === "cancelled"
        ? "error"
        : view.terminal
          ? "complete"
          : "in_progress",
    details,
  };
}
