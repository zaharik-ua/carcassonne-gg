import { hasTournamentAdminAccess } from "./tournament-admin-access.js";

// Players of a CCL match may correct its result for this long after the match start;
// tournament admins may correct it at any time.
export const CCL_RESULT_EDIT_HOURS = 24;

const text = (value) => String(value ?? "").trim();

export function isCclResultEditableStatus(status) {
  return ["done", "error"].includes(text(status).toLowerCase());
}

// ISO end of the players' editing window, or null when the match has no start time.
export function getCclResultEditUntil(timeUtc) {
  const startTs = Date.parse(text(timeUtc));
  if (!Number.isFinite(startTs)) return null;
  return new Date(startTs + CCL_RESULT_EDIT_HOURS * 60 * 60 * 1000).toISOString();
}

// { allowed, expired, until, message } for one CCL duel row (status, time_utc, player ids).
export function getCclResultEditAccess({ duel, playerId, isAdmin, now = new Date() }) {
  const until = getCclResultEditUntil(duel?.time_utc);
  if (!isCclResultEditableStatus(duel?.status)) {
    return { allowed: false, expired: false, until, message: "Match results can be edited only for Done or Error matches" };
  }
  if (isAdmin) return { allowed: true, expired: false, until };
  const id = text(playerId);
  const isParticipant = !!id && [duel?.player_1_id, duel?.player_2_id].map(text).includes(id);
  if (!isParticipant) return { allowed: false, expired: false, until, message: "Forbidden" };
  const expired = !!until && Date.parse(until) <= now.getTime();
  return {
    allowed: !expired,
    expired,
    until,
    message: expired ? `Match result editing is available for ${CCL_RESULT_EDIT_HOURS} hours after match start.` : undefined,
  };
}

// Access check for the generic /duels/:id/games routes; `duel` needs tournament_id,
// status (or duel_status), time_utc and both player ids.
export async function checkCclResultEditAccess(db, user, duel, now = new Date()) {
  const isAdmin = await hasTournamentAdminAccess(db, user, duel?.tournament_id);
  return getCclResultEditAccess({
    duel: { ...duel, status: duel?.duel_status ?? duel?.status },
    playerId: user?.player_id ?? user?.bga_id,
    isAdmin,
    now,
  });
}
