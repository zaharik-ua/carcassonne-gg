export function didRankedDuelTransitionToDone(previousStatus, nextStatus, ranking) {
  const wasDone = String(previousStatus || "").trim().toLowerCase() === "done";
  const isDone = String(nextStatus || "").trim().toLowerCase() === "done";
  const isRanked = Number(ranking) === 1;
  return !wasDone && isDone && isRanked;
}

export function isCompletedRankedDuel(status, ranking) {
  return String(status || "").trim().toLowerCase() === "done"
    && Number(ranking) === 1;
}
