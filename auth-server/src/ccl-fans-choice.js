import { createHash } from "node:crypto";

import { loadCclTournament, resolveCclAssociation } from "./ccl-routes.js";
import { hasTournamentAdminAccess } from "./tournament-admin-access.js";

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
    -- Review of guest votes: the BGA id found by nickname, why a vote was rejected, who changed the status.
    bga_player_id TEXT,
    reject_reason TEXT,
    reviewed_at TEXT,
    reviewed_by TEXT,
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
const REJECT_REASON_MAX = 300;
const VOTE_STATUSES = ["pending", "confirmed", "rejected", "cancelled"];
const VOTE_FIELDS = "id, association_id, bga_nickname, status, created_at, updated_at";
// Columns added after the first version of the table.
const LATER_COLUMNS = ["bga_player_id", "reject_reason", "reviewed_at", "reviewed_by"];

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

// Repeatable: creates the table or adds the later columns to an older version of it.
export async function ensureCclFansChoiceSchema(db) {
  await dbExec(db, FANS_CHOICE_VOTES_SQL);
  const columns = new Set((await dbAll(db, "PRAGMA table_info(ccl_fans_choice_votes)")).map((column) => column.name));
  for (const column of LATER_COLUMNS.filter((name) => !columns.has(name))) {
    await dbExec(db, `ALTER TABLE ccl_fans_choice_votes ADD COLUMN ${column} TEXT`);
  }
}

// Admin review of a guest vote: a status, and a reason when it is rejected.
export function normalizeVoteReview(body) {
  const status = text(body?.status).toLowerCase();
  if (!VOTE_STATUSES.includes(status)) return { error: `status must be one of ${VOTE_STATUSES.join(", ")}` };
  const rejectReason = status === "rejected" ? text(body?.reject_reason) : "";
  if (status === "rejected" && !rejectReason) return { error: "Enter the reason for rejecting the vote" };
  if (rejectReason.length > REJECT_REASON_MAX) {
    return { error: `reject_reason must be at most ${REJECT_REASON_MAX} characters` };
  }
  return { value: { status, reject_reason: rejectReason || null } };
}

function actorLabel(user) {
  return text(user?.email)
    || text(user?.player_id ?? user?.bga_id)
    || (userId(user) !== null ? `user:${userId(user)}` : null);
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
  requireAuthenticated = (req, res, next) => (
    req.user ? next() : res.status(401).json({ ok: false, message: "Unauthorized" })
  ),
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

  const requireAdmin = async (user, tournamentId) => {
    const { tournament, season } = await loadSeason(tournamentId);
    if (!(await hasTournamentAdminAccess(db, user, tournament.id))) throw requestError(403, "Forbidden");
    return { tournament, season };
  };

  // Tournament admins: guest votes to review, newest first. Verified votes are confirmed automatically.
  // duplicate_count: other active votes of the same player (nickname or BGA id), verified ones included;
  // ip_duplicate_count: other active votes from the same network.
  app.get("/ccl/:tournamentId/fans-choice/votes", requireAuthenticated, handle("Failed to load Fans Choice votes", async (req, res) => {
    const { tournament } = await requireAdmin(req.user, req.params.tournamentId);
    const rows = await dbAll(
      db,
      `
        SELECT v.id, v.association_id, a.name AS association_name, a.flag AS association_flag,
               v.bga_nickname, v.bga_player_id, v.user_id, v.status, v.reject_reason, v.ip_hash,
               v.created_at, v.updated_at, v.reviewed_at, v.reviewed_by,
               (
                 SELECT COUNT(*) FROM ccl_fans_choice_votes o
                 WHERE o.tournament_id = v.tournament_id AND o.id <> v.id AND o.status <> 'cancelled'
                   AND (
                     lower(o.bga_nickname) = lower(v.bga_nickname)
                     OR (trim(COALESCE(v.bga_player_id, '')) <> '' AND COALESCE(o.player_id, o.bga_player_id) = v.bga_player_id)
                   )
               ) AS duplicate_count,
               (
                 SELECT COUNT(*) FROM ccl_fans_choice_votes o
                 WHERE o.tournament_id = v.tournament_id AND o.id <> v.id AND o.status <> 'cancelled'
                   AND v.ip_hash IS NOT NULL AND o.ip_hash = v.ip_hash
               ) AS ip_duplicate_count
        FROM ccl_fans_choice_votes v
        LEFT JOIN associations a ON upper(${ASSOCIATION_ID_SQL}) = upper(trim(v.association_id))
        WHERE upper(trim(v.tournament_id)) = upper(trim(?))
          AND v.player_id IS NULL
        ORDER BY v.created_at DESC, v.id DESC
      `,
      [tournament.id]
    );
    return res.json({
      ok: true,
      votes: rows.map((row) => ({ ...row, association_flag: row.association_flag || null })),
    });
  }));

  // Tournament admins: status of a guest vote; rejecting needs a reason.
  app.patch("/ccl/fans-choice/votes/:id", requireAuthenticated, handle("Failed to update the vote", async (req, res) => {
    const id = Number(req.params.id);
    const vote = Number.isInteger(id) && id > 0
      ? await dbGet(db, "SELECT * FROM ccl_fans_choice_votes WHERE id = ?", [id])
      : null;
    if (!vote) throw requestError(404, "Vote not found");
    const { tournament } = await requireAdmin(req.user, vote.tournament_id);
    if (vote.player_id) throw requestError(400, "Votes of verified players are confirmed automatically");
    const input = normalizeVoteReview(req.body);
    if (input.error) throw requestError(400, input.error);

    const reviewer = actorLabel(req.user);
    try {
      await dbRun(
        db,
        `
          UPDATE ccl_fans_choice_votes
          SET status = ?, reject_reason = ?, reviewed_at = CURRENT_TIMESTAMP, reviewed_by = ?, updated_at = CURRENT_TIMESTAMP
          WHERE id = ?
        `,
        [input.value.status, input.value.reject_reason, reviewer, id]
      );
    } catch (error) {
      // Restoring a cancelled vote whose nickname has voted again.
      if (isUniqueViolation(error)) throw requestError(409, "This BGA nickname already has another active vote");
      throw error;
    }
    audit(req, {
      event_type: "ccl_fans_choice_vote.reviewed",
      entity_type: "ccl_fans_choice_vote",
      action: "update",
      record_id: String(id),
      changes: Object.fromEntries(["status", "reject_reason"]
        .filter((field) => (vote[field] ?? null) !== input.value[field])
        .map((field) => [field, { old: vote[field] ?? null, new: input.value[field] }])),
      metadata: { tournament_id: tournament.id, bga_nickname: vote.bga_nickname },
    });
    const updated = await dbGet(
      db,
      `SELECT ${VOTE_FIELDS}, reject_reason, reviewed_at, reviewed_by FROM ccl_fans_choice_votes WHERE id = ?`,
      [id]
    );
    return res.json({ ok: true, vote: updated });
  }));

  // Tournament admins: BGA ids of guest votes from site profiles with exactly the same nickname,
  // as "Get players data" of in-person tournaments. A nickname of several profiles stays without an id.
  app.post(
    "/ccl/:tournamentId/fans-choice/votes/fill-bga-ids",
    requireAuthenticated,
    requireTrustedJsonRequest,
    handle("Failed to get players data", async (req, res) => {
      const { tournament } = await requireAdmin(req.user, req.params.tournamentId);
      const matches = await dbAll(
        db,
        `
          SELECT v.id AS vote_id, trim(g.id) AS bga_id
          FROM ccl_fans_choice_votes v
          JOIN profiles g ON g.bga_nickname COLLATE BINARY = v.bga_nickname COLLATE BINARY
            AND g.deleted_at IS NULL
          WHERE upper(trim(v.tournament_id)) = upper(trim(?))
            AND v.player_id IS NULL
            AND trim(COALESCE(v.bga_player_id, '')) = ''
        `,
        [tournament.id]
      );
      const idsByVote = new Map();
      matches.forEach((match) => idsByVote.set(match.vote_id, [...(idsByVote.get(match.vote_id) || []), match.bga_id]));
      let updatedCount = 0;
      for (const [voteId, ids] of idsByVote) {
        if (ids.length !== 1 || !/^[0-9]*[1-9][0-9]*$/.test(ids[0])) continue;
        const result = await dbRun(
          db,
          "UPDATE ccl_fans_choice_votes SET bga_player_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
          [ids[0], voteId]
        );
        updatedCount += result.changes;
      }
      return res.json({ ok: true, updated_count: updatedCount });
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
                ip_hash = ?, status = 'confirmed', reject_reason = NULL, updated_at = CURRENT_TIMESTAMP
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
