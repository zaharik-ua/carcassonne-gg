// Player Hub dashboard: GET /dashboard returns the blocks the current user may see.
// - Global admins (users.admin = 1): open cases of all tournaments, news proposals in Draft,
//   new users with a linked BGA profile.
// - Tournament admins: only tournaments where the user has role = admin in tournament_access_users
//   (global admins too): open cases of those tournaments (global admins see all), unplanned duels
//   with a structured deadline (tournament_stages, CCL for now) and duels in Error.
// - Players with a linked profile: their own active duels and the cases they submitted (all tournaments).
// Blocks only list records with a link; actions on them come later.

export const DASHBOARD_BLOCK_LIMIT = 10;
export const DEFAULT_DAYS_BEFORE_DEADLINE = 3;
export const MAX_DAYS_BEFORE_DEADLINE = 60;
export const NEW_USERS_DAYS = 7;
// The unplanned duels block is paged; the other blocks show their first DASHBOARD_BLOCK_LIMIT records.
export const UNPLANNED_PAGE_SIZES = [10, 20, 50];
export const PLANNING_STATUSES = ["deadline_passed", "no_proposal", "time_proposed", "change_pending"];
// Unplanned duels with an active time proposal, by when the proposed time is: within N hours, or already passed.
export const PROPOSED_TIME_WINDOWS = { "4h": 4, "12h": 12, "1d": 24, passed: 0 };

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

export function parsePlanningStatusFilter(value) {
  const raw = text(value).toLowerCase();
  return PLANNING_STATUSES.includes(raw) ? raw : null;
}

export function parseUnplannedPageSize(value) {
  const size = Number(text(value));
  return UNPLANNED_PAGE_SIZES.includes(size) ? size : UNPLANNED_PAGE_SIZES[0];
}

export function parsePage(value) {
  const page = Number(text(value));
  return Number.isInteger(page) && page > 0 ? page : 1;
}

export function parseProposedTimeFilter(value) {
  const raw = text(value).toLowerCase();
  return Object.prototype.hasOwnProperty.call(PROPOSED_TIME_WINDOWS, raw) ? raw : null;
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

// `flagOf(row, playerId, profileAssociation)`: the association flag of the player, when the block shows flags.
function playerOf(row, prefix, flagOf = null) {
  const id = text(row[`${prefix}_id`]);
  if (!id) return null;
  const association = text(row[`${prefix}_association`]) || null;
  return {
    id,
    name: text(row[`${prefix}_nickname`]) || text(row[`${prefix}_name`]) || id,
    association,
    ...(flagOf ? { association_flag: flagOf(row, id, association) } : {}),
  };
}

// Association id as the CCL slots store it (ccl-routes.js ASSOCIATION_ID_SQL): the code, else ASSOCIATION_<rowid>.
const ASSOCIATION_ID_SQL = "COALESCE(NULLIF(trim(a.code), ''), printf('ASSOCIATION_%s', a.rowid))";

// Flags of the unplanned duels' players, as the tournament page shows them: the association of the
// player's slot in that tournament (tournament_players), else the association of the profile.
async function loadFlagResolver(db, tournamentIds) {
  const [associations, slots] = await Promise.all([
    dbAll(db, `SELECT ${ASSOCIATION_ID_SQL} AS id, a.flag FROM associations a`),
    dbAll(
      db,
      `
        SELECT upper(trim(tournament_id)) AS tournament_id, trim(player_id) AS player_id, association_id
        FROM tournament_players
        WHERE upper(trim(tournament_id)) IN (${tournamentIds.map(() => "?").join(", ")})
          AND trim(COALESCE(player_id, '')) <> ''
          AND trim(COALESCE(association_id, '')) <> ''
      `,
      tournamentIds.map((id) => id.toUpperCase())
    ),
  ]);
  const flags = new Map(associations.filter((row) => text(row.flag)).map((row) => [text(row.id).toUpperCase(), text(row.flag)]));
  const slotAssociations = new Map(slots.map((row) => [`${row.tournament_id}|${row.player_id}`, text(row.association_id)]));
  return (row, playerId, profileAssociation) => {
    const slotAssociation = slotAssociations.get(`${text(row.tournament_id).toUpperCase()}|${playerId}`);
    return flags.get(text(slotAssociation || profileAssociation).toUpperCase()) || null;
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

// Where an admin or a player opens the duel in Player Hub. `adminPage`: a duel of a structured
// tournament (CCL) opens its own match page, which only tournament admins have.
export function buildDuelLink(row, { adminPage = false } = {}) {
  const tournamentId = text(row.tournament_id);
  if (text(row.challenge_period_id)) return "/player-hub/challenges";
  if (!tournamentId) return null;
  const params = new URLSearchParams({ id: tournamentId });
  const matchId = text(row.match_id) || (adminPage && text(row.group_id) ? text(row.id) : "");
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
    d.id, d.tournament_id, d.match_id, d.challenge_period_id, d.group_id, d.status, d.time_utc, d.deadline_utc,
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

function duelSummary(row, flagOf = null) {
  return {
    id: row.id,
    tournament: tournamentOf(row),
    stage: text(row.stage) || null,
    group: text(row.group_name) || null,
    round: text(row.round_name) || null,
    status: text(row.status) || null,
    time_utc: toIso(row.time_utc) || toIso(row.match_time_utc),
    deadline_utc: toIso(row.deadline_utc),
    player_1: playerOf(row, "player_1", flagOf),
    player_2: playerOf(row, "player_2", flagOf),
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

// The proposed time of an unplanned duel with an active proposal is within the window (or already passed).
export function matchesProposedTimeWindow(item, window, nowIso) {
  if (!window) return true;
  const hasProposal = item.proposed_time_status === "pending" || item.proposed_time_status === "change_pending";
  const proposedTs = Date.parse(item.proposed_time_utc || "");
  if (!hasProposal || !Number.isFinite(proposedTs)) return false;
  const nowTs = Date.parse(nowIso);
  if (window === "passed") return proposedTs < nowTs;
  return proposedTs >= nowTs && proposedTs <= nowTs + PROPOSED_TIME_WINDOWS[window] * 60 * 60 * 1000;
}

const emptyStatusCounts = () => Object.fromEntries(PLANNING_STATUSES.map((status) => [status, 0]));

export async function loadUnplannedDuels(db, {
  tournamentIds, nowIso, days, planningStatus = null, proposedTime = null, page = 1, pageSize = UNPLANNED_PAGE_SIZES[0],
}) {
  if (!tournamentIds.length) {
    return { count: 0, items: [], status_counts: emptyStatusCounts(), page: 1, page_size: pageSize, total_pages: 1 };
  }
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
  const flagOf = rows.length ? await loadFlagResolver(db, tournamentIds) : null;
  const items = [];
  // Counts by status follow the proposed time filter, so each status option shows what it would list.
  const statusCounts = emptyStatusCounts();
  for (const row of rows) {
    const status = getPlanningStatus(row, nowIso);
    if (!status) continue;
    const deadline = toIso(row.deadline_utc);
    const proposedBy = text(row.proposed_time_by_player_id);
    const item = {
      ...duelSummary(row, flagOf),
      link: buildDuelLink(row, { adminPage: true }),
      duel_format: text(row.duel_format) || null,
      planning_status: status,
      days_to_deadline: daysBetween(nowIso, deadline),
      proposed_time_status: text(row.proposed_time_status).toLowerCase() || null,
      proposed_time_utc: toIso(row.proposed_time_utc),
      proposed_by_player_id: proposedBy || null,
      player_1_contacts: contactsOf(row, "player_1"),
      player_2_contacts: contactsOf(row, "player_2"),
    };
    if (!matchesProposedTimeWindow(item, proposedTime, nowIso)) continue;
    statusCounts[status] += 1;
    if (planningStatus && status !== planningStatus) continue;
    items.push(item);
  }
  // A page past the end (e.g. after a filter narrowed the list) shows the last page.
  const totalPages = Math.max(1, Math.ceil(items.length / pageSize));
  const currentPage = Math.min(page, totalPages);
  return {
    count: items.length,
    items: items.slice((currentPage - 1) * pageSize, currentPage * pageSize),
    status_counts: statusCounts,
    page: currentPage,
    page_size: pageSize,
    total_pages: totalPages,
  };
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

const OPEN_CASE_STATUSES = ["open", "in_progress"];
const CASE_PRIORITY_ORDER = ["urgent", "high", "normal", "low"];

// Where an admin opens the record a case is about; cases have no page of their own yet.
export function buildCaseLink(row) {
  if (text(row.challenge_period_id)) return "/player-hub/challenges";
  const tournamentId = text(row.tournament_id);
  if (!tournamentId) return null;
  const params = new URLSearchParams({ id: tournamentId });
  const matchId = text(row.match_id) || text(row.duel_match_id);
  if (matchId) {
    params.set("edit", "1");
    params.set("match_id", matchId);
  }
  return `/player-hub/tournaments?${params.toString()}`;
}

// Open and in-progress cases, most urgent and oldest first: of every tournament (global admins,
// `tournamentIds` = null), or only of `tournamentIds` (tournament admins); `tournamentId` narrows to one.
export async function loadOpenCases(db, { nowIso, tournamentId = "", tournamentIds = null }) {
  if (Array.isArray(tournamentIds) && !tournamentIds.length) return { count: 0, items: [] };
  const filters = [];
  const params = [];
  if (tournamentId) {
    filters.push("AND upper(trim(COALESCE(c.tournament_id, ''))) = upper(trim(?))");
    params.push(tournamentId);
  }
  if (Array.isArray(tournamentIds)) {
    filters.push(`AND upper(trim(COALESCE(c.tournament_id, ''))) IN (${placeholders(tournamentIds)})`);
    params.push(...tournamentIds.map((id) => text(id).toUpperCase()));
  }
  const rows = await dbAll(
    db,
    `
      SELECT c.id, c.case_type, c.category, c.status, c.priority, c.details, c.created_at,
        c.tournament_id, c.match_id, c.duel_id, c.challenge_period_id,
        c.submitted_by_player_id, sp.bga_nickname AS submitted_nickname, sp.name AS submitted_name,
        ru.name AS responsible_name, d.match_id AS duel_match_id,
        t.name AS tournament_name, t.short_title AS tournament_short_title
      FROM tournament_cases c
      LEFT JOIN profiles sp ON sp.id = c.submitted_by_player_id
      LEFT JOIN users ru ON ru.id = c.responsible_user_id
      LEFT JOIN duels d ON d.id = c.duel_id
      LEFT JOIN tournaments t ON upper(trim(t.id)) = upper(trim(c.tournament_id))
      WHERE c.deleted_at IS NULL
        AND c.status IN (${sqlList(OPEN_CASE_STATUSES)})
        ${filters.join("\n        ")}
      ORDER BY CASE c.priority ${CASE_PRIORITY_ORDER.map((value, index) => `WHEN '${value}' THEN ${index}`).join(" ")} ELSE 9 END,
        julianday(c.created_at) ASC, c.id ASC
    `,
    params
  );
  const person = (row, idField, prefix) => {
    const id = text(row[idField]);
    if (!id) return null;
    return { id, name: text(row[`${prefix}_nickname`]) || text(row[`${prefix}_name`]) || id };
  };
  return {
    count: rows.length,
    items: rows.slice(0, DASHBOARD_BLOCK_LIMIT).map((row) => {
      const createdAt = toIso(row.created_at);
      return {
        id: row.id,
        type: text(row.case_type) || null,
        category: text(row.category) || null,
        status: text(row.status),
        priority: text(row.priority) || "normal",
        details: text(row.details),
        submitted_by: person(row, "submitted_by_player_id", "submitted"),
        responsible: text(row.responsible_name) || null,
        tournament: text(row.tournament_id) ? tournamentOf(row) : null,
        duel_id: text(row.duel_id) || null,
        created_at: createdAt,
        days_open: daysBetween(createdAt, nowIso),
        link: buildCaseLink(row),
      };
    }),
  };
}

const ALL_CASE_STATUSES = ["open", "in_progress", "resolved", "closed"];

// Where a player opens the record their case is about (read-only views, no admin edit mode).
export function buildPlayerCaseLink(row) {
  if (text(row.challenge_period_id)) return "/player-hub/challenges";
  const tournamentId = text(row.tournament_id);
  if (!tournamentId) return null;
  return `/player-hub/tournaments?${new URLSearchParams({ id: tournamentId }).toString()}`;
}

// Cases the player submitted, in every tournament: active ones first (most urgent, oldest),
// then resolved and closed ones, newest first.
export async function loadMyCases(db, { playerId, userId = null, nowIso }) {
  if (!playerId && !userId) return { count: 0, active_count: 0, items: [] };
  const rows = await dbAll(
    db,
    `
      SELECT c.id, c.case_type, c.category, c.status, c.priority, c.details, c.created_at,
        c.resolution, c.resolved_at, c.tournament_id, c.match_id, c.duel_id, c.challenge_period_id,
        ru.name AS responsible_name,
        t.name AS tournament_name, t.short_title AS tournament_short_title
      FROM tournament_cases c
      LEFT JOIN users ru ON ru.id = c.responsible_user_id
      LEFT JOIN tournaments t ON upper(trim(t.id)) = upper(trim(c.tournament_id))
      WHERE c.deleted_at IS NULL
        AND (c.submitted_by_player_id = ? OR (? IS NOT NULL AND c.submitted_by_user_id = ?))
      ORDER BY CASE WHEN c.status IN (${sqlList(OPEN_CASE_STATUSES)}) THEN 0 ELSE 1 END,
        CASE WHEN c.status IN (${sqlList(OPEN_CASE_STATUSES)})
          THEN CASE c.priority ${CASE_PRIORITY_ORDER.map((value, index) => `WHEN '${value}' THEN ${index}`).join(" ")} ELSE 9 END
          ELSE 0 END,
        CASE WHEN c.status IN (${sqlList(OPEN_CASE_STATUSES)}) THEN julianday(c.created_at) ELSE -julianday(COALESCE(c.resolved_at, c.created_at)) END ASC,
        c.id ASC
    `,
    [playerId || "", userId, userId]
  );
  const activeCount = rows.filter((row) => OPEN_CASE_STATUSES.includes(text(row.status))).length;
  return {
    count: rows.length,
    active_count: activeCount,
    items: rows.slice(0, DASHBOARD_BLOCK_LIMIT).map((row) => {
      const createdAt = toIso(row.created_at);
      return {
        id: row.id,
        type: text(row.case_type) || null,
        category: text(row.category) || null,
        status: ALL_CASE_STATUSES.includes(text(row.status)) ? text(row.status) : "open",
        priority: text(row.priority) || "normal",
        details: text(row.details),
        responsible: text(row.responsible_name) || null,
        resolution: text(row.resolution) || null,
        resolved_at: toIso(row.resolved_at),
        tournament: text(row.tournament_id) ? tournamentOf(row) : null,
        created_at: createdAt,
        days_open: daysBetween(createdAt, nowIso),
        link: buildPlayerCaseLink(row),
      };
    }),
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
  const planningStatus = parsePlanningStatusFilter(query.planning_status);
  const proposedTime = parseProposedTimeFilter(query.proposed_time);
  const unplannedPage = parsePage(query.unplanned_page);
  const unplannedPageSize = parseUnplannedPageSize(query.unplanned_page_size);

  const adminTournaments = await loadAdminTournaments(db, userId);
  const requestedTournamentId = text(query.tournament_id);
  const selected = requestedTournamentId
    ? adminTournaments.filter((tournament) => tournament.id.toUpperCase() === requestedTournamentId.toUpperCase())
    : adminTournaments;
  const tournamentIds = selected.map((tournament) => tournament.id);

  const blocks = {};
  if (isGlobalAdmin) {
    blocks.cases = await loadOpenCases(db, { nowIso, tournamentId: requestedTournamentId });
  } else if (adminTournaments.length) {
    blocks.cases = await loadOpenCases(db, { nowIso, tournamentIds });
  }
  if (isGlobalAdmin) {
    blocks.news_drafts = await loadNewsDrafts(db, { nowIso });
    blocks.new_users = await loadNewUsers(db, { nowIso });
  }
  if (adminTournaments.length) {
    blocks.unplanned_duels = await loadUnplannedDuels(db, {
      tournamentIds, nowIso, days, planningStatus, proposedTime, page: unplannedPage, pageSize: unplannedPageSize,
    });
    blocks.error_duels = await loadErrorDuels(db, { tournamentIds });
  }
  if (playerId) {
    blocks.my_duels = await loadMyDuels(db, { playerId, nowIso });
    blocks.my_cases = await loadMyCases(db, { playerId, userId, nowIso });
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
      planning_status: planningStatus,
      proposed_time: proposedTime,
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
