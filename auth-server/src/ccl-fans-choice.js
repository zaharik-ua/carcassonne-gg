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
    -- When BGA did not know the nickname: Get players data does not look it up again.
    bga_lookup_failed_at TEXT,
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
    opensAt: "2026-11-04T17:00:00Z",
    // The last second of 23:59, so the pages show "November 9 at 23:59 UTC".
    closesAt: "2026-11-09T23:59:59Z",
    excludedAssociations: ["ARG", "CHN", "HRV", "DEU", "JPN", "RCP", "UKR"],
    excludedReason: "Already has 4–5 spots in CCL 2027",
  },
};

// Same canonical association id as resolveAssociationId in server.js.
const ASSOCIATION_ID_SQL = "COALESCE(NULLIF(trim(a.code), ''), printf('ASSOCIATION_%s', a.rowid))";
const NICKNAME_MAX = 64;
const REJECT_REASON_MAX = 300;
const VERIFIED_REPLACED_REASON = "Replaced by the vote of the verified player";
const VOTE_STATUSES = ["pending", "confirmed", "rejected", "cancelled"];
const BGA_ID_PATTERN = /^[0-9]*[1-9][0-9]*$/;
const VOTE_FIELDS = "id, association_id, bga_nickname, status, created_at, updated_at";
// Columns added after the first version of the table.
const LATER_COLUMNS = ["bga_player_id", "reject_reason", "reviewed_at", "reviewed_by", "bga_lookup_failed_at"];

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

// Statuses that keep a reason: required for rejected, optional for cancelled.
const STATUSES_WITH_REASON = ["rejected", "cancelled"];

// Admin review of a guest vote: a status, and the reason for a rejected or cancelled vote.
export function normalizeVoteReview(body) {
  const status = text(body?.status).toLowerCase();
  if (!VOTE_STATUSES.includes(status)) return { error: `status must be one of ${VOTE_STATUSES.join(", ")}` };
  const rejectReason = STATUSES_WITH_REASON.includes(status) ? text(body?.reject_reason) : "";
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
  // (nickname) => { ok, player_id } | { ok: false, not_found: true } | { ok: false, message }.
  lookupBgaPlayer = null,
  bgaLookupIntervalMs = 1000,
  sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
}) {
  if (!app || !db) throw new Error("app and db are required");
  // BGA lookups run in the background: a click would otherwise outlast the proxy timeout.
  // One run at a time for all admins (BGA allows about one request a second); the last run of each
  // tournament is kept here (not in the DB) so the admin list can show its progress.
  const bgaLookups = new Map();
  let bgaLookupRunning = false;

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

  // Public: the vote count per association and every vote (nickname, BGA id, date, status, reason).
  // Counts leave out rejected and cancelled votes. No IP hashes, accounts or reviewers.
  // voted_at: verified players may change the vote, so their last change; guests vote once.
  app.get("/public/ccl/:tournamentId/fans-choice/results", handle("Failed to load Fans Choice results", async (req, res) => {
    const { tournament, season } = await loadSeason(req.params.tournamentId);
    const [associations, votes] = await Promise.all([
      loadAssociations(db, season),
      dbAll(
        db,
        `
          SELECT id, association_id, bga_nickname, COALESCE(player_id, bga_player_id) AS bga_player_id,
                 player_id IS NOT NULL AS verified, status,
                 CASE WHEN status IN ('rejected', 'cancelled') THEN reject_reason END AS reason,
                 CASE WHEN player_id IS NOT NULL THEN updated_at ELSE created_at END AS voted_at
          FROM ccl_fans_choice_votes
          WHERE upper(trim(tournament_id)) = upper(trim(?))
          ORDER BY voted_at ASC, id ASC
        `,
        [tournament.id]
      ),
    ]);
    const byAssociation = new Map(associations.map((association) => [idKey(association.id), {
      id: association.id,
      name: association.name,
      flag: association.flag,
      confirmed: 0,
      pending: 0,
      votes: [],
    }]));
    for (const vote of votes) {
      const association = byAssociation.get(idKey(vote.association_id));
      if (!association) continue;
      if (vote.status === "confirmed") association.confirmed += 1;
      if (vote.status === "pending") association.pending += 1;
      association.votes.push({
        id: vote.id,
        bga_nickname: vote.bga_nickname,
        bga_player_id: text(vote.bga_player_id) || null,
        verified: Number(vote.verified) === 1,
        status: vote.status,
        reason: vote.reason || null,
        voted_at: vote.voted_at,
      });
    }
    const results = [...byAssociation.values()]
      .filter((association) => association.votes.length)
      .sort((left, right) => String(left.name).localeCompare(String(right.name)));
    return res.json({
      ok: true,
      tournament: { id: tournament.id, name: tournament.name },
      voting: {
        opens_at: season.opensAt,
        closes_at: season.closesAt,
        status: getFansChoiceVotingStatus(season, now()),
      },
      totals: {
        confirmed: results.reduce((sum, association) => sum + association.confirmed, 0),
        pending: results.reduce((sum, association) => sum + association.pending, 0),
      },
      associations: results,
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
  // ip_duplicate_count: other active votes from the same network;
  // has_verified_account: the player has a site account linked to the profile (by BGA id, else exact nickname).
  app.get("/ccl/:tournamentId/fans-choice/votes", requireAuthenticated, handle("Failed to load Fans Choice votes", async (req, res) => {
    const { tournament } = await requireAdmin(req.user, req.params.tournamentId);
    const rows = await dbAll(
      db,
      `
        SELECT v.id, v.association_id, a.name AS association_name, a.flag AS association_flag,
               v.bga_nickname, v.bga_player_id, v.user_id, v.status, v.reject_reason, v.ip_hash,
               v.created_at, v.updated_at, v.reviewed_at, v.reviewed_by, v.bga_lookup_failed_at,
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
               ) AS ip_duplicate_count,
               EXISTS (
                 SELECT 1 FROM users u
                 JOIN profiles p ON p.id = u.bga_id AND p.deleted_at IS NULL
                 WHERE CASE
                   WHEN trim(COALESCE(v.bga_player_id, '')) <> '' THEN p.id = v.bga_player_id
                   ELSE p.bga_nickname COLLATE BINARY = v.bga_nickname COLLATE BINARY
                 END
               ) AS has_verified_account
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
      bga_lookup: bgaLookups.get(idKey(tournament.id)) || null,
      votes: rows.map((row) => ({
        ...row,
        association_flag: row.association_flag || null,
        has_verified_account: Number(row.has_verified_account) === 1,
      })),
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

  // Tournament admins: BGA ids of guest votes. First the site profiles: the same nickname in any case
  // (voters often type "Pandeyuk" for "pandeyuk"); a profile with exactly the same nickname wins over
  // the others; a nickname of several profiles stays without an id. NOCASE folds only Latin letters.
  // Then BGA for the nicknames still without an id, in the background, one lookup a second;
  // a nickname BGA does not know is not looked up again (bga_lookup_failed_at).
  // bga_lookup: the background run (see runBgaLookup), null when no nickname needs BGA.
  app.post(
    "/ccl/:tournamentId/fans-choice/votes/fill-bga-ids",
    requireAuthenticated,
    requireTrustedJsonRequest,
    handle("Failed to get players data", async (req, res) => {
      const { tournament } = await requireAdmin(req.user, req.params.tournamentId);
      const current = bgaLookups.get(idKey(tournament.id));
      if (bgaLookupRunning && !current?.running) {
        throw requestError(409, "BGA lookup is running for another tournament. Try again in a few minutes.");
      }
      const profilesCount = await fillBgaIdsFromProfiles(tournament.id);
      const bgaLookup = current?.running ? current : await startBgaLookup(tournament.id);
      return res.json({ ok: true, updated_count: profilesCount, profiles_count: profilesCount, bga_lookup: bgaLookup });
    })
  );

  async function fillBgaIdsFromProfiles(tournamentId) {
    const matches = await dbAll(
      db,
      `
        SELECT v.id AS vote_id, trim(g.id) AS bga_id,
               g.bga_nickname COLLATE BINARY = v.bga_nickname COLLATE BINARY AS exact
        FROM ccl_fans_choice_votes v
        JOIN profiles g ON g.bga_nickname COLLATE NOCASE = v.bga_nickname COLLATE NOCASE
          AND g.deleted_at IS NULL
        WHERE upper(trim(v.tournament_id)) = upper(trim(?))
          AND v.player_id IS NULL
          AND trim(COALESCE(v.bga_player_id, '')) = ''
      `,
      [tournamentId]
    );
    const matchesByVote = new Map();
    matches.forEach((match) => matchesByVote.set(match.vote_id, [...(matchesByVote.get(match.vote_id) || []), match]));
    let updatedCount = 0;
    for (const [voteId, voteMatches] of matchesByVote) {
      const exact = voteMatches.filter((match) => Number(match.exact) === 1);
      const ids = [...new Set((exact.length ? exact : voteMatches).map((match) => match.bga_id))];
      if (ids.length !== 1 || !BGA_ID_PATTERN.test(ids[0])) continue;
      const result = await dbRun(
        db,
        "UPDATE ccl_fans_choice_votes SET bga_player_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        [ids[0], voteId]
      );
      updatedCount += result.changes;
    }
    return updatedCount;
  }

  async function startBgaLookup(tournamentId) {
    if (typeof lookupBgaPlayer !== "function") return null;
    // Cancelled votes do not count, so they are not worth a BGA request.
    const votes = await dbAll(
      db,
      `
        SELECT id, bga_nickname FROM ccl_fans_choice_votes
        WHERE upper(trim(tournament_id)) = upper(trim(?))
          AND player_id IS NULL
          AND trim(COALESCE(bga_player_id, '')) = ''
          AND bga_lookup_failed_at IS NULL
          AND status <> 'cancelled'
        ORDER BY id
      `,
      [tournamentId]
    );
    // Another click may have started a run while the votes were loading.
    if (bgaLookupRunning) {
      const running = bgaLookups.get(idKey(tournamentId));
      if (running?.running) return running;
      throw requestError(409, "BGA lookup is running for another tournament. Try again in a few minutes.");
    }
    const votesByNickname = new Map();
    votes.forEach((vote) => {
      const nicknameKey = text(vote.bga_nickname).toLowerCase();
      votesByNickname.set(nicknameKey, [...(votesByNickname.get(nicknameKey) || []), vote]);
    });
    const queue = [...votesByNickname.values()];
    if (!queue.length) {
      bgaLookups.delete(idKey(tournamentId));
      return null;
    }
    // total/checked: nicknames; found/not_found: votes updated; error: BGA failed, the run stopped.
    const state = {
      running: true, started_at: now().toISOString(), finished_at: null,
      total: queue.length, checked: 0, found: 0, not_found: 0, error: null,
    };
    bgaLookups.set(idKey(tournamentId), state);
    bgaLookupRunning = true;
    runBgaLookup(state, queue)
      .catch((error) => {
        logger.error?.("BGA player lookup failed", error);
        state.error = "BGA lookup failed. Try again later.";
      })
      .finally(() => {
        state.running = false;
        state.finished_at = now().toISOString();
        bgaLookupRunning = false;
      });
    return state;
  }

  async function runBgaLookup(state, queue) {
    for (const nicknameVotes of queue) {
      if (state.checked) await sleep(bgaLookupIntervalMs);
      const ids = nicknameVotes.map((vote) => vote.id);
      const placeholders = ids.map(() => "?").join(", ");
      let lookup;
      try {
        lookup = await lookupBgaPlayer(text(nicknameVotes[0].bga_nickname));
      } catch (error) {
        lookup = { ok: false, message: error?.message };
      }
      if (lookup?.ok && BGA_ID_PATTERN.test(String(lookup.player_id || ""))) {
        const updated = await dbRun(
          db,
          `UPDATE ccl_fans_choice_votes SET bga_player_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id IN (${placeholders})`,
          [String(lookup.player_id), ...ids]
        );
        state.found += updated.changes;
      } else if (lookup?.not_found) {
        const updated = await dbRun(
          db,
          `UPDATE ccl_fans_choice_votes SET bga_lookup_failed_at = CURRENT_TIMESTAMP WHERE id IN (${placeholders})`,
          ids
        );
        state.not_found += updated.changes;
      } else {
        logger.error?.("BGA player lookup failed", lookup?.message);
        state.error = "BGA lookup failed. Try again later.";
        return;
      }
      state.checked += 1;
    }
  }

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
          `
            UPDATE ccl_fans_choice_votes
            SET status = 'cancelled', reject_reason = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
          `,
          [VERIFIED_REPLACED_REASON, byNickname.id]
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
