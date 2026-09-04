import { TOURNAMENT_ENTITY_TYPES } from "./schema.js";

function dbGet(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (error, row) => {
      if (error) reject(error);
      else resolve(row || null);
    });
  });
}

function normalizeTournamentId(value) {
  return String(value || "").trim();
}

function normalizeUserId(value) {
  const userId = Number(value);
  return Number.isInteger(userId) && userId > 0 ? userId : null;
}

export async function hasInPersonTournamentAdminAccess(db, user, tournamentId) {
  if (!user) return false;
  if (Number(user.admin) === 1) return true;

  const userId = normalizeUserId(user.id);
  const normalizedTournamentId = normalizeTournamentId(tournamentId);
  if (!userId || !normalizedTournamentId) return false;

  const row = await dbGet(
    db,
    `
      SELECT 1 AS allowed
      FROM tournament_access_users
      WHERE tournament_entity_type = ?
        AND upper(trim(tournament_id)) = upper(trim(?))
        AND user_id = ?
        AND lower(trim(role)) = 'admin'
      LIMIT 1
    `,
    [TOURNAMENT_ENTITY_TYPES.IN_PERSON_TOURNAMENT, normalizedTournamentId, userId]
  );
  return Number(row?.allowed) === 1;
}

function defaultTournamentId(req) {
  return req?.params?.tournamentId
    ?? req?.params?.id
    ?? req?.body?.tournament_id
    ?? req?.query?.tournament_id;
}

export function createRequireInPersonTournamentAdmin({
  db,
  getTournamentId = defaultTournamentId,
} = {}) {
  if (!db) throw new Error("db is required");

  return async function requireInPersonTournamentAdmin(req, res, next) {
    if (!req?.user) {
      res.status(401).json({ ok: false, message: "Unauthorized" });
      return;
    }

    const tournamentId = normalizeTournamentId(getTournamentId(req));
    if (!tournamentId) {
      res.status(400).json({ ok: false, message: "Invalid in-person tournament id" });
      return;
    }

    try {
      const allowed = await hasInPersonTournamentAdminAccess(db, req.user, tournamentId);
      if (!allowed) {
        res.status(403).json({ ok: false, message: "Forbidden" });
        return;
      }
      req.inPersonTournamentId = tournamentId;
      next();
    } catch (error) {
      next(error);
    }
  };
}
