export function resolveTournamentTextPatch(payload, currentTournament, fieldName) {
  if (!Object.prototype.hasOwnProperty.call(payload || {}, fieldName)) {
    return currentTournament?.[fieldName] ?? null;
  }

  return String(payload?.[fieldName] || "").trim() || null;
}
