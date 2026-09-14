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

export function canManageTournamentAccessUsers(user) {
  return Number(user?.admin) === 1;
}

export async function hasTournamentAdminAccess(db, user, tournamentId) {
  if (!user) return false;
  if (canManageTournamentAccessUsers(user)) return true;

  const userId = normalizeUserId(user.id);
  const normalizedTournamentId = normalizeTournamentId(tournamentId);
  if (!userId || !normalizedTournamentId) return false;

  const row = await dbGet(
    db,
    `
      SELECT 1 AS allowed
      FROM tournament_access_users
      WHERE tournament_entity_type = 'tournament'
        AND upper(trim(tournament_id)) = upper(trim(?))
        AND user_id = ?
        AND lower(trim(role)) = 'admin'
      LIMIT 1
    `,
    [normalizedTournamentId, userId]
  );
  return Number(row?.allowed) === 1;
}

function defaultTournamentId(req) {
  return req?.params?.tournamentId
    ?? req?.params?.id
    ?? req?.body?.tournament_id
    ?? req?.query?.tournament_id;
}

export function createRequireTournamentAdmin({
  db,
  getTournamentId = defaultTournamentId,
} = {}) {
  if (!db) throw new Error("db is required");

  return async function requireTournamentAdmin(req, res, next) {
    if (!req?.user) {
      res.status(401).json({ ok: false, message: "Unauthorized" });
      return;
    }

    const tournamentId = normalizeTournamentId(getTournamentId(req));
    if (!tournamentId && !canManageTournamentAccessUsers(req.user)) {
      res.status(400).json({ ok: false, message: "Invalid tournament id" });
      return;
    }

    try {
      const allowed = await hasTournamentAdminAccess(db, req.user, tournamentId);
      if (!allowed) {
        res.status(403).json({ ok: false, message: "Forbidden" });
        return;
      }
      req.managedTournamentId = tournamentId;
      next();
    } catch (error) {
      next(error);
    }
  };
}
