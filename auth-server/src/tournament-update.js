export function resolveTournamentTextPatch(payload, currentTournament, fieldName) {
  if (!Object.prototype.hasOwnProperty.call(payload || {}, fieldName)) {
    return currentTournament?.[fieldName] ?? null;
  }

  return String(payload?.[fieldName] || "").trim() || null;
}

export function normalizeTournamentLineupType(tournamentType, lineupType) {
  const supportsLineup = ["teams", "clubs"].includes(String(tournamentType || "").trim().toLowerCase());
  return supportsLineup && String(lineupType || "").trim().toLowerCase() === "blind"
    ? "Blind"
    : "Open";
}
