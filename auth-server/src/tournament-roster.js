export function getTournamentRosterUpdateError({
  registrationEndsAt,
  isAdmin,
  existingPlayerIds,
  playerIds,
  now = Date.now(),
}) {
  const deadline = Date.parse(String(registrationEndsAt || ""));
  if (isAdmin === true || !Number.isFinite(deadline) || deadline > now) return null;

  const selectedIds = new Set(playerIds.map((id) => String(id).trim()));
  const removesRegisteredPlayer = existingPlayerIds.some((id) => !selectedIds.has(String(id).trim()));
  return removesRegisteredPlayer
    ? "Player replacements are closed after registration ends."
    : null;
}
