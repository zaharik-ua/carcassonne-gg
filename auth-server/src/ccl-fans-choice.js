import { createHash } from "node:crypto";

import { loadCclTournament, resolveCclAssociation } from "./ccl-routes.js";

// Fans Choice: players vote for an association; a weighted draw later gives 3 extra Q3 spots.
// Verified players (signed in and linked to a BGA profile) are confirmed at once,
// other votes wait for the admins. One active vote per BGA nickname, per player and per site account;
// a cancelled vote (e.g. the association got a 4th spot) frees the voter to vote again.
const FANS_CHOICE_VOTES_SQL = `
  CREATE TABLE IF NOT EXISTS ccl_fans_choice_votes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tournament_id TEXT NOT NULL COLLATE NOCASE,
    association_id TEXT NOT NULL COLLATE NOCASE,
    bga_nickname TEXT NOT NULL CHECK (trim(bga_nickname) <> ''),
    -- Set when the voter is a verified player.
    player_id TEXT,
    user_id INTEGER,
    voter_association_id TEXT COLLATE NOCASE,
    -- pending: to check, confirmed, rejected, cancelled: voided, the voter may vote again.
    status TEXT NOT NULL CHECK (status IN ('pending', 'confirmed', 'rejected', 'cancelled')),
    -- Lets admins spot many votes from one network without keeping the address itself.
    ip_hash TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (tournament_id) REFERENCES tournaments(id) ON DELETE CASCADE
  );
  CREATE UNIQUE INDEX IF NOT EXISTS idx_ccl_fans_choice_votes_active_nickname
    ON ccl_fans_choice_votes(tournament_id COLLATE NOCASE, bga_nickname COLLATE NOCASE)
    WHERE status <> 'cancelled';
  CREATE UNIQUE INDEX IF NOT EXISTS idx_ccl_fans_choice_votes_active_player
    ON ccl_fans_choice_votes(tournament_id COLLATE NOCASE, player_id)
    WHERE status <> 'cancelled' AND player_id IS NOT NULL;
  CREATE INDEX IF NOT EXISTS idx_ccl_fans_choice_votes_association
    ON ccl_fans_choice_votes(tournament_id COLLATE NOCASE, association_id);
`;

// Per CCL season. Associations that already have 4–5 spots cannot be voted for.
export const FANS_CHOICE_SEASONS = {
  "CCL-2027": {
    opensAt: "2026-11-04T00:00:00Z",
    closesAt: "2026-11-09T00:00:00Z",
    excludedAssociations: ["ARG", "CHN", "HRV", "DEU", "JPN", "RCP", "UKR"],
    excludedReason: "Already has 4–5 spots in CCL 2027",
  },
};

// Same canonical association id as resolveAssociationId in server.js.
const ASSOCIATION_ID_SQL = "COALESCE(NULLIF(trim(a.code), ''), printf('ASSOCIATION_%s', a.rowid))";
const NICKNAME_MAX = 64;
const VOTE_FIELDS = "id, association_id, bga_nickname, status, created_at, updated_at";

const text = (value) => String(value ?? "").trim().replace(/\s+/g, " ");
const idKey = (value) => text(value).toUpperCase();

function dbGet(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (error, row) => (error ? reject(error) : resolve(row || null)));
  });
}

function dbAll(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || [])));
  });
}

function dbRun(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function onRun(error) {
      if (error) reject(error);
      else resolve(this);
    });
  });
}

function dbExec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (error) => (error ? reject(error) : resolve()));
  });
}

async function inTransaction(db, work) {
  await dbRun(db, "BEGIN IMMEDIATE TRANSACTION");
  try {
    const result = await work();
    await dbRun(db, "COMMIT");
    return result;
  } catch (error) {
    await dbRun(db, "ROLLBACK").catch(() => {});
    throw error;
  }
}

function requestError(status, message, extra = {}) {
  return Object.assign(new Error(message), { status, extra });
}

const isUniqueViolation = (error) => String(error?.code) === "SQLITE_CONSTRAINT" && /UNIQUE/i.test(String(error?.message));

export async function ensureCclFansChoiceSchema(db) {
  await dbExec(db, FANS_CHOICE_VOTES_SQL);
}

export function getFansChoiceVotingStatus(season, now) {
  const time = now.getTime();
  if (time < Date.parse(season.opensAt)) return "upcoming";
  if (time >= Date.parse(season.closesAt)) return "closed";
  return "open";
}

// A signed-in user linked to a BGA profile.
function verifiedPlayerId(user) {
  return text(user?.player_id) || null;
}

function userId(user) {
  const id = Number(user?.id);
  return user?.id != null && Number.isInteger(id) ? id : null;
}

async function loadAssociations(db, season) {
  const excluded = new Set(season.excludedAssociations.map(idKey));
  const rows = await dbAll(db, `
    SELECT ${ASSOCIATION_ID_SQL} AS id, a.name, a.flag
    FROM associations a
    WHERE trim(COALESCE(a.name, '')) <> ''
    ORDER BY a.name COLLATE NOCASE ASC
  `);
  return rows.map((row) => {
    const eligible = !excluded.has(idKey(row.id));
    return {
      id: text(row.id),
      name: row.name,
      flag: row.flag || null,
      eligible,
      reason: eligible ? null : season.excludedReason,
    };
  });
}

// The viewer's latest vote: the active one, else the last cancelled one so the page can say so.
async function loadViewerVote(db, tournamentId, user) {
  const playerId = verifiedPlayerId(user);
  const accountId = userId(user);
  if (!playerId && accountId === null) return null;
  const [column, value] = playerId ? ["player_id", playerId] : ["user_id", accountId];
  return dbGet(
    db,
    `
      SELECT ${VOTE_FIELDS}
      FROM ccl_fans_choice_votes
      WHERE upper(trim(tournament_id)) = upper(trim(?)) AND ${column} = ?
      ORDER BY (status <> 'cancelled') DESC, id DESC
      LIMIT 1
    `,
    [tournamentId, value]
  );
}

function hashIp(tournamentId, ip) {
  const address = text(ip);
  return address ? createHash("sha256").update(`${idKey(tournamentId)}:${address}`).digest("hex").slice(0, 32) : null;
}

export function registerCclFansChoiceRoutes(app, {
  db,
  requireTrustedJsonRequest = (_req, _res, next) => next(),
  getAuditActor = () => ({}),
  logAuditEvent = null,
  logger = console,
  now = () => new Date(),
}) {
  if (!app || !db) throw new Error("app and db are required");

  const handle = (failureMessage, handler) => async (req, res) => {
    try {
      return await handler(req, res);
    } catch (error) {
      const status = Number(error?.status) || 500;
      if (status >= 500) logger.error?.(failureMessage, error);
      return res.status(status).json({
        ok: false,
        message: status >= 500 ? failureMessage : error.message,
        ...(error?.extra || {}),
      });
    }
  };

  const audit = (req, entry) => {
    if (typeof logAuditEvent === "function") logAuditEvent({ ...getAuditActor(req.user), ...entry }, () => {});
  };

  const loadSeason = async (tournamentId) => {
    const tournament = await loadCclTournament(db, tournamentId);
    const season = tournament
      ? Object.entries(FANS_CHOICE_SEASONS).find(([id]) => idKey(id) === idKey(tournament.id))?.[1]
      : null;
    if (!season) throw requestError(404, "Fans Choice is not available for this tournament");
    return { tournament, season };
  };

  // Public: voting window, associations with eligibility, the viewer's vote.
  // ?vote_id= returns the state of a vote this browser remembers (guests have no account to look it up by).
  app.get("/public/ccl/:tournamentId/fans-choice", handle("Failed to load Fans Choice", async (req, res) => {
    const { tournament, season } = await loadSeason(req.params.tournamentId);
    const voteId = Number(req.query?.vote_id);
    const [associations, viewerAssociation, viewerVote, rememberedVote] = await Promise.all([
      loadAssociations(db, season),
      verifiedPlayerId(req.user) ? resolveCclAssociation(db, req.user?.association) : null,
      loadViewerVote(db, tournament.id, req.user),
      Number.isInteger(voteId) && voteId > 0
        ? dbGet(
          db,
          `SELECT ${VOTE_FIELDS} FROM ccl_fans_choice_votes WHERE id = ? AND upper(trim(tournament_id)) = upper(trim(?))`,
          [voteId, tournament.id]
        )
        : null,
    ]);
    return res.json({
      ok: true,
      tournament: { id: tournament.id, name: tournament.name },
      voting: {
        opens_at: season.opensAt,
        closes_at: season.closesAt,
        status: getFansChoiceVotingStatus(season, now()),
      },
      associations,
      viewer: {
        authenticated: Boolean(req.user),
        verified: Boolean(verifiedPlayerId(req.user)),
        player_id: verifiedPlayerId(req.user),
        bga_nickname: verifiedPlayerId(req.user) ? text(req.user?.bga_nickname) || null : null,
        association_id: viewerAssociation?.id || null,
        vote: viewerVote,
      },
      remembered_vote: rememberedVote,
    });
  }));

  // Public: casts a vote; a verified player's new vote replaces the previous one.
  app.post(
    "/public/ccl/:tournamentId/fans-choice/votes",
    requireTrustedJsonRequest,
    handle("Failed to save the vote", async (req, res) => {
      const { tournament, season } = await loadSeason(req.params.tournamentId);
      const votingStatus = getFansChoiceVotingStatus(season, now());
      if (votingStatus === "upcoming") throw requestError(409, "Voting has not started yet");
      if (votingStatus === "closed") throw requestError(409, "Voting is closed");

      const associationId = text(req.body?.association_id);
      if (!associationId) throw requestError(400, "Choose an association");
      const association = (await loadAssociations(db, season)).find((row) => idKey(row.id) === idKey(associationId));
      if (!association) throw requestError(400, `Association not found: ${associationId}`);
      if (!association.eligible) throw requestError(400, `${association.name}: ${association.reason}`);

      const playerId = verifiedPlayerId(req.user);
      const accountId = userId(req.user);
      const ipHash = hashIp(tournament.id, req.ip);
      const result = playerId
        ? await saveVerifiedVote({ req, tournament, association, playerId, accountId, ipHash })
        : await saveGuestVote({ req, tournament, association, accountId, ipHash });
      return res.status(result.created ? 201 : 200).json({
        ok: true,
        vote: { ...result.vote, association_name: association.name },
      });
    })
  );

  async function saveVerifiedVote({ req, tournament, association, playerId, accountId, ipHash }) {
    const nickname = text(req.user?.bga_nickname) || playerId;
    const voterAssociation = await resolveCclAssociation(db, req.user?.association);
    const outcome = await inTransaction(db, async () => {
      const activeVote = (column, value) => dbGet(
        db,
        `
          SELECT * FROM ccl_fans_choice_votes
          WHERE upper(trim(tournament_id)) = upper(trim(?)) AND ${column} = ? AND status <> 'cancelled'
        `,
        [tournament.id, value]
      );
      const own = await activeVote("player_id", playerId);
      // A guest vote typed with this player's nickname becomes the player's vote.
      const byNickname = await dbGet(
        db,
        `
          SELECT * FROM ccl_fans_choice_votes
          WHERE upper(trim(tournament_id)) = upper(trim(?)) AND bga_nickname = ? COLLATE NOCASE
            AND status <> 'cancelled'
        `,
        [tournament.id, nickname]
      );
      if (own && byNickname && own.id !== byNickname.id) {
        await dbRun(
          db,
          "UPDATE ccl_fans_choice_votes SET status = 'cancelled', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
          [byNickname.id]
        );
      }
      const previous = own || byNickname;
      const values = [association.id, nickname, playerId, accountId, voterAssociation?.id || null, ipHash];
      if (previous) {
        await dbRun(
          db,
          `
            UPDATE ccl_fans_choice_votes
            SET association_id = ?, bga_nickname = ?, player_id = ?, user_id = ?, voter_association_id = ?,
                ip_hash = ?, status = 'confirmed', updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
          `,
          [...values, previous.id]
        );
        return { id: previous.id, previous };
      }
      const inserted = await dbRun(
        db,
        `
          INSERT INTO ccl_fans_choice_votes (
            association_id, bga_nickname, player_id, user_id, voter_association_id, ip_hash, tournament_id, status
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, 'confirmed')
        `,
        [...values, tournament.id]
      );
      return { id: inserted.lastID, previous: null };
    });

    const vote = await dbGet(db, `SELECT ${VOTE_FIELDS} FROM ccl_fans_choice_votes WHERE id = ?`, [outcome.id]);
    audit(req, {
      event_type: outcome.previous ? "ccl_fans_choice_vote.updated" : "ccl_fans_choice_vote.created",
      entity_type: "ccl_fans_choice_vote",
      action: outcome.previous ? "update" : "create",
      record_id: String(outcome.id),
      changes: {
        association_id: { old: outcome.previous?.association_id ?? null, new: association.id },
        status: { old: outcome.previous?.status ?? null, new: "confirmed" },
      },
      metadata: { tournament_id: tournament.id, player_id: playerId },
    });
    return { vote, created: !outcome.previous };
  }

  async function saveGuestVote({ req, tournament, association, accountId, ipHash }) {
    const nickname = text(req.body?.bga_nickname);
    if (!nickname) throw requestError(400, "Enter your BGA nickname");
    if (nickname.length > NICKNAME_MAX) throw requestError(400, `BGA nickname must be at most ${NICKNAME_MAX} characters`);

    // A signed-in but unverified account still votes only once.
    if (accountId !== null) {
      const accountVote = await dbGet(
        db,
        `
          SELECT id FROM ccl_fans_choice_votes
          WHERE upper(trim(tournament_id)) = upper(trim(?)) AND user_id = ? AND status <> 'cancelled'
        `,
        [tournament.id, accountId]
      );
      if (accountVote) throw requestError(409, "You have already voted");
    }

    let inserted;
    try {
      inserted = await dbRun(
        db,
        `
          INSERT INTO ccl_fans_choice_votes (tournament_id, association_id, bga_nickname, user_id, ip_hash, status)
          VALUES (?, ?, ?, ?, ?, 'pending')
        `,
        [tournament.id, association.id, nickname, accountId, ipHash]
      );
    } catch (error) {
      if (isUniqueViolation(error)) throw requestError(409, "This BGA nickname has already voted");
      throw error;
    }

    const vote = await dbGet(db, `SELECT ${VOTE_FIELDS} FROM ccl_fans_choice_votes WHERE id = ?`, [inserted.lastID]);
    audit(req, {
      event_type: "ccl_fans_choice_vote.created",
      entity_type: "ccl_fans_choice_vote",
      action: "create",
      record_id: String(inserted.lastID),
      changes: {
        association_id: { old: null, new: association.id },
        bga_nickname: { old: null, new: nickname },
        status: { old: null, new: "pending" },
      },
      metadata: { tournament_id: tournament.id },
    });
    return { vote, created: true };
  }
}
