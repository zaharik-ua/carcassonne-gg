// Player Hub dashboard: GET /dashboard returns the blocks the current user may see.
// - Global admins (users.admin = 1): news proposals in Draft, new users with a linked BGA profile.
// - Tournament admins: only tournaments where the user has role = admin in tournament_access_users
//   (global admins too): unplanned duels with a structured deadline (tournament_stages, CCL for now)
//   and duels in Error.
// - Players with a linked profile: their own active duels.
// Blocks only list records with a link; actions on them come later.

export const DASHBOARD_BLOCK_LIMIT = 10;
export const DEFAULT_DAYS_BEFORE_DEADLINE = 3;
export const MAX_DAYS_BEFORE_DEADLINE = 60;
export const NEW_USERS_DAYS = 30;

const DAY_MS = 24 * 60 * 60 * 1000;
const OPEN_DUEL_STATUSES = ["draft", "planned", "in progress", "requested new time"];
const PLAYER_DUEL_STATUSES = [...OPEN_DUEL_STATUSES, "error"];

const text = (value) => String(value ?? "").trim();
const sqlList = (values) => values.map((value) => `'${value}'`).join(", ");

function dbAll(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || [])));
  });
}

export function parseDaysBeforeDeadline(value) {
  const raw = text(value);
  if (!raw) return DEFAULT_DAYS_BEFORE_DEADLINE;
  const days = Number(raw);
  if (!Number.isInteger(days) || days < 0) return DEFAULT_DAYS_BEFORE_DEADLINE;
  return Math.min(days, MAX_DAYS_BEFORE_DEADLINE);
}

function toIso(value) {
  const raw = text(value);
  if (!raw) return null;
  const withZone = /[zZ]|[+-]\d{2}:?\d{2}$/.test(raw) ? raw : `${raw.replace(" ", "T")}Z`;
  const ts = Date.parse(withZone);
  return Number.isFinite(ts) ? new Date(ts).toISOString() : null;
}

function daysBetween(fromIso, toIsoValue) {
  const from = Date.parse(fromIso || "");
  const to = Date.parse(toIsoValue || "");
  if (!Number.isFinite(from) || !Number.isFinite(to)) return null;
  return Math.floor((to - from) / DAY_MS);
}

function playerOf(row, prefix) {
  const id = text(row[`${prefix}_id`]);
  if (!id) return null;
  return {
    id,
    name: text(row[`${prefix}_nickname`]) || text(row[`${prefix}_name`]) || id,
    association: text(row[`${prefix}_association`]) || null,
  };
}

function contactsOf(row, prefix) {
  const contacts = {};
  for (const field of ["telegram", "whatsapp", "discord", "contact_email"]) {
    const value = text(row[`${prefix}_${field}`]);
    if (value) contacts[field] = value;
  }
  return contacts;
}

function tournamentOf(row) {
  const id = text(row.tournament_id);
  return {
    id,
    name: text(row.tournament_short_title) || text(row.tournament_name) || id,
  };
}

// Where an admin or a player opens the duel in Player Hub.
export function buildDuelLink(row) {
  const tournamentId = text(row.tournament_id);
  if (text(row.challenge_period_id)) return "/player-hub/challenges";
  if (!tournamentId) return null;
  const params = new URLSearchParams({ id: tournamentId });
  const matchId = text(row.match_id);
  if (matchId) {
    params.set("edit", "1");
    params.set("match_id", matchId);
  }
  return `/player-hub/tournaments?${params.toString()}`;
}

// Planning state of an open duel that has a deadline, in the order of the dashboard table.
export function getPlanningStatus(row, nowIso) {
  const deadline = toIso(row.deadline_utc);
  if (deadline && deadline < nowIso) return "deadline_passed";
  const proposal = text(row.proposed_time_status).toLowerCase();
  if (proposal === "pending") return "time_proposed";
  if (proposal === "change_pending") return "change_pending";
  if (!text(row.time_utc)) return "no_proposal";
  return null;
}

const PLAYER_COLUMNS = (alias, prefix, { contacts = false } = {}) => [
  `${alias}.id AS ${prefix}_id`,
  `${alias}.bga_nickname AS ${prefix}_nickname`,
  `${alias}.name AS ${prefix}_name`,
  `${alias}.association AS ${prefix}_association`,
  ...(contacts ? ["telegram", "whatsapp", "discord", "contact_email"].map((field) => `${alias}.${field} AS ${prefix}_${field}`) : []),
].join(", ");

const DUEL_BASE_SQL = (options = {}) => `
  SELECT
    d.id, d.tournament_id, d.match_id, d.challenge_period_id, d.status, d.time_utc, d.deadline_utc,
    d.proposed_time_status, d.proposed_time_utc, d.proposed_time_by_player_id,
    d.round_name, d.group_name, d.stage, d.duel_format, d.results_last_error, d.results_checked_at,
    t.name AS tournament_name, t.short_title AS tournament_short_title, m.time_utc AS match_time_utc,
    ${PLAYER_COLUMNS("p1", "player_1", options)},
    ${PLAYER_COLUMNS("p2", "player_2", options)}
  FROM duels d
  LEFT JOIN tournaments t ON upper(trim(t.id)) = upper(trim(d.tournament_id))
  LEFT JOIN matches m ON m.id = d.match_id AND m.deleted_at IS NULL
  LEFT JOIN profiles p1 ON p1.id = d.player_1_id AND p1.deleted_at IS NULL
  LEFT JOIN profiles p2 ON p2.id = d.player_2_id AND p2.deleted_at IS NULL
`;

const LIVE_DUEL_SQL = `
  d.deleted_at IS NULL
  AND COALESCE(d.is_test, 0) = 0
  AND COALESCE(t.is_test, 0) = 0
`;

function duelSummary(row) {
  return {
    id: row.id,
    tournament: tournamentOf(row),
    stage: text(row.stage) || null,
    group: text(row.group_name) || null,
    round: text(row.round_name) || null,
    status: text(row.status) || null,
    time_utc: toIso(row.time_utc) || toIso(row.match_time_utc),
    deadline_utc: toIso(row.deadline_utc),
    player_1: playerOf(row, "player_1"),
    player_2: playerOf(row, "player_2"),
    link: buildDuelLink(row),
  };
}

export async function loadAdminTournaments(db, userId) {
  if (!userId) return [];
  const rows = await dbAll(
    db,
    `
      SELECT DISTINCT trim(tau.tournament_id) AS id, t.name, t.short_title
      FROM tournament_access_users tau
      LEFT JOIN tournaments t ON upper(trim(t.id)) = upper(trim(tau.tournament_id))
      WHERE tau.tournament_entity_type = 'tournament'
        AND tau.user_id = ?
        AND lower(trim(tau.role)) = 'admin'
        AND trim(COALESCE(tau.tournament_id, '')) <> ''
      ORDER BY COALESCE(t.short_title, t.name, tau.tournament_id) COLLATE NOCASE
    `,
    [userId]
  );
  return rows.map((row) => ({ id: row.id, name: text(row.short_title) || text(row.name) || row.id }));
}

const placeholders = (values) => values.map(() => "?").join(", ");

export async function loadUnplannedDuels(db, { tournamentIds, nowIso, days }) {
  if (!tournamentIds.length) return { count: 0, items: [] };
  const limitIso = new Date(Date.parse(nowIso) + days * DAY_MS).toISOString();
  const rows = await dbAll(
    db,
    `
      ${DUEL_BASE_SQL({ contacts: true })}
      WHERE ${LIVE_DUEL_SQL}
        AND upper(trim(d.tournament_id)) IN (${placeholders(tournamentIds)})
        AND d.group_id IS NOT NULL
        AND trim(COALESCE(d.deadline_utc, '')) <> ''
        AND julianday(d.deadline_utc) <= julianday(?)
        AND trim(COALESCE(d.player_1_id, '')) <> ''
        AND trim(COALESCE(d.player_2_id, '')) <> ''
        AND lower(trim(COALESCE(d.status, ''))) IN (${sqlList(OPEN_DUEL_STATUSES)})
      ORDER BY julianday(d.deadline_utc) ASC, d.tournament_id, d.group_name, d.bracket_no
    `,
    [...tournamentIds.map((id) => id.toUpperCase()), limitIso]
  );
  const items = [];
  for (const row of rows) {
    const planningStatus = getPlanningStatus(row, nowIso);
    if (!planningStatus) continue;
    const deadline = toIso(row.deadline_utc);
    const proposedBy = text(row.proposed_time_by_player_id);
    items.push({
      ...duelSummary(row),
      planning_status: planningStatus,
      days_to_deadline: daysBetween(nowIso, deadline),
      proposed_time_utc: toIso(row.proposed_time_utc),
      proposed_by_player_id: proposedBy || null,
      player_1_contacts: contactsOf(row, "player_1"),
      player_2_contacts: contactsOf(row, "player_2"),
    });
  }
  return { count: items.length, items: items.slice(0, DASHBOARD_BLOCK_LIMIT) };
}

export async function loadErrorDuels(db, { tournamentIds }) {
  if (!tournamentIds.length) return { count: 0, items: [] };
  const rows = await dbAll(
    db,
    `
      ${DUEL_BASE_SQL()}
      WHERE ${LIVE_DUEL_SQL}
        AND upper(trim(d.tournament_id)) IN (${placeholders(tournamentIds)})
        AND lower(trim(COALESCE(d.status, ''))) = 'error'
      ORDER BY julianday(COALESCE(d.results_checked_at, d.updated_at)) DESC, d.id
    `,
    tournamentIds.map((id) => id.toUpperCase())
  );
  return {
    count: rows.length,
    items: rows.slice(0, DASHBOARD_BLOCK_LIMIT).map((row) => ({
      ...duelSummary(row),
      error: text(row.results_last_error) || null,
      checked_at: toIso(row.results_checked_at),
    })),
  };
}

export async function loadNewsDrafts(db, { nowIso }) {
  const rows = await dbAll(
    db,
    `
      SELECT n.id, n.title, n.created_at, n.tournament_id, n.created_by,
        p.bga_nickname AS author_nickname, p.name AS author_name,
        t.name AS tournament_name, t.short_title AS tournament_short_title
      FROM news n
      LEFT JOIN profiles p ON p.id = n.created_by AND p.deleted_at IS NULL
      LEFT JOIN tournaments t ON upper(trim(t.id)) = upper(trim(n.tournament_id))
      WHERE n.deleted_at IS NULL
        AND COALESCE(n.is_proposal, 0) = 1
        AND lower(trim(COALESCE(n.status, ''))) = 'draft'
      ORDER BY julianday(n.created_at) ASC, n.id ASC
    `
  );
  return {
    count: rows.length,
    items: rows.slice(0, DASHBOARD_BLOCK_LIMIT).map((row) => {
      const createdAt = toIso(row.created_at);
      return {
        id: row.id,
        title: text(row.title),
        author: text(row.created_by)
          ? { id: text(row.created_by), name: text(row.author_nickname) || text(row.author_name) || text(row.created_by) }
          : null,
        tournament: text(row.tournament_id) ? tournamentOf(row) : null,
        created_at: createdAt,
        days_waiting: daysBetween(createdAt, nowIso),
        link: `/player-hub/news?edit=1&news_id=${encodeURIComponent(row.id)}`,
      };
    }),
  };
}

export async function loadNewUsers(db, { nowIso }) {
  const sinceIso = new Date(Date.parse(nowIso) - NEW_USERS_DAYS * DAY_MS).toISOString();
  const rows = await dbAll(
    db,
    `
      SELECT u.id AS user_id, u.name AS user_name, u.created_at,
        p.id AS profile_id, p.bga_nickname, p.name AS profile_name, p.association, p.bga_elo
      FROM users u
      JOIN profiles p ON p.id = u.bga_id AND p.deleted_at IS NULL
      WHERE julianday(u.created_at) >= julianday(?)
      ORDER BY julianday(u.created_at) DESC, u.id DESC
    `,
    [sinceIso]
  );
  return {
    count: rows.length,
    days: NEW_USERS_DAYS,
    items: rows.slice(0, DASHBOARD_BLOCK_LIMIT).map((row) => ({
      user_id: row.user_id,
      name: text(row.profile_name) || text(row.user_name) || null,
      bga_nickname: text(row.bga_nickname) || null,
      association: text(row.association) || null,
      bga_elo: row.bga_elo ?? null,
      created_at: toIso(row.created_at),
      link: `https://carcassonne.gg/player/?id=${encodeURIComponent(row.profile_id)}`,
    })),
  };
}

// What the player has to do about one of their duels.
export function getPlayerDuelAction(row, playerId, nowIso) {
  if (text(row.status).toLowerCase() === "error") return "result_error";
  // Team match duels take the match time, which the captains agree on.
  if (text(row.match_id)) return text(row.time_utc) || text(row.match_time_utc) ? "planned" : "time_not_set";
  const proposal = text(row.proposed_time_status).toLowerCase();
  if (proposal === "pending" || proposal === "change_pending") {
    return text(row.proposed_time_by_player_id) === playerId ? "waiting_opponent" : "accept_time";
  }
  const deadline = toIso(row.deadline_utc);
  if (!text(row.time_utc)) {
    if (deadline && deadline < nowIso) return "deadline_passed";
    return "propose_time";
  }
  return "planned";
}

const PLAYER_ACTION_ORDER = [
  "accept_time", "propose_time", "deadline_passed", "result_error", "waiting_opponent", "time_not_set", "planned",
];

export async function loadMyDuels(db, { playerId, nowIso }) {
  if (!playerId) return { count: 0, action_count: 0, items: [] };
  const rows = await dbAll(
    db,
    `
      ${DUEL_BASE_SQL()}
      WHERE ${LIVE_DUEL_SQL}
        AND (d.player_1_id = ? OR d.player_2_id = ?)
        AND trim(COALESCE(d.player_1_id, '')) <> ''
        AND trim(COALESCE(d.player_2_id, '')) <> ''
        AND lower(trim(COALESCE(d.status, ''))) IN (${sqlList(PLAYER_DUEL_STATUSES)})
    `,
    [playerId, playerId]
  );
  const items = rows.map((row) => {
    const opponentPrefix = text(row.player_1_id) === playerId ? "player_2" : "player_1";
    const summary = duelSummary(row);
    return {
      ...summary,
      opponent: playerOf(row, opponentPrefix),
      action: getPlayerDuelAction(row, playerId, nowIso),
      proposed_time_utc: toIso(row.proposed_time_utc),
    };
  });
  const sortKey = (item) => item.time_utc || item.deadline_utc || "9999";
  items.sort((a, b) => (
    PLAYER_ACTION_ORDER.indexOf(a.action) - PLAYER_ACTION_ORDER.indexOf(b.action)
    || sortKey(a).localeCompare(sortKey(b))
  ));
  const needsAction = new Set(["accept_time", "propose_time", "deadline_passed", "result_error"]);
  return {
    count: items.length,
    action_count: items.filter((item) => needsAction.has(item.action)).length,
    items: items.slice(0, DASHBOARD_BLOCK_LIMIT),
  };
}

export async function buildDashboard(db, user, { query = {}, now = new Date() } = {}) {
  const nowIso = now.toISOString();
  const isGlobalAdmin = Number(user?.admin) === 1;
  const userId = Number.isInteger(Number(user?.id)) ? Number(user.id) : null;
  const playerId = text(user?.player_id);
  const days = parseDaysBeforeDeadline(query.days_before_deadline);

  const adminTournaments = await loadAdminTournaments(db, userId);
  const requestedTournamentId = text(query.tournament_id);
  const selected = requestedTournamentId
    ? adminTournaments.filter((tournament) => tournament.id.toUpperCase() === requestedTournamentId.toUpperCase())
    : adminTournaments;
  const tournamentIds = selected.map((tournament) => tournament.id);

  const blocks = {};
  if (isGlobalAdmin) {
    blocks.news_drafts = await loadNewsDrafts(db, { nowIso });
    blocks.new_users = await loadNewUsers(db, { nowIso });
  }
  if (adminTournaments.length) {
    blocks.unplanned_duels = await loadUnplannedDuels(db, { tournamentIds, nowIso, days });
    blocks.error_duels = await loadErrorDuels(db, { tournamentIds });
  }
  if (playerId) {
    blocks.my_duels = await loadMyDuels(db, { playerId, nowIso });
  }

  return {
    ok: true,
    generated_at: nowIso,
    roles: {
      global_admin: isGlobalAdmin,
      tournament_admin: adminTournaments.length > 0,
      player: !!playerId,
    },
    filters: {
      tournament_id: requestedTournamentId ? (selected[0]?.id || null) : null,
      days_before_deadline: days,
    },
    tournaments: adminTournaments,
    blocks,
  };
}

export function registerDashboardRoutes(app, { db, requireAuthenticated, logger = console, now = () => new Date() }) {
  if (!app || !db || !requireAuthenticated) throw new Error("app, db and requireAuthenticated are required");
  app.get("/dashboard", requireAuthenticated, async (req, res) => {
    try {
      res.set?.("Cache-Control", "no-store");
      return res.json(await buildDashboard(db, req.user, { query: req.query || {}, now: now() }));
    } catch (error) {
      logger.error?.("Failed to build dashboard", error);
      return res.status(500).json({ ok: false, message: "Failed to load dashboard" });
    }
  });
}
