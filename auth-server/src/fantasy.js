import { loadAssociationsById, loadCclTournament } from "./ccl-routes.js";

// Fantasy leagues: a signed-in user picks a team of CCL players within a budget; players score points
// for the teams that picked them. Design: "CCL 2027 Fantasy League — технічне рішення".
//
// - A league is a row of fantasy_leagues, linked to a CCL tournament and some of its stages
//   (the qualifiers trial and the main stage are two leagues of one tournament).
// - Access without tournament roles: global site admins (users.admin = 1) see every league, change its
//   settings and its admins; a league admin (fantasy_league_admins) sees the teams of their league only.
// - Prices hang on CCL slots (tournament_players with entry_stage) and are kept as a history:
//   a new price is a new row with its own effective_from, the current price is the latest one that has started.
// - One team per site account and league. A team with fewer players than the team size is a draft.
// - Until the main deadline a team can be changed freely; a complete team then scores every match of the stages.
// - Until the late deadline a user without a locked team saves a draft and commits it: Commit cannot be undone,
//   it locks the team and only matches that start after it count (scoring_from).
// - Points are counted from the CCL matches (duels) on every request, never stored: 2:0 → 3, 2:1 → 2, 1:2 → 1,
//   0:2 → 0 per player; a team scores the sum of its players. From the main deadline the leaderboard and the
//   locked teams are public.
//
// No foreign keys to tournament_players: the CCL module rebuilds that table, and a deleted slot must not
// take a saved team with it.
const FANTASY_SQL = `
  CREATE TABLE IF NOT EXISTS fantasy_leagues (
    id TEXT PRIMARY KEY COLLATE NOCASE CHECK (trim(id) <> ''),
    tournament_id TEXT NOT NULL COLLATE NOCASE,
    name TEXT NOT NULL CHECK (trim(name) <> ''),
    -- Comma-separated CCL entry stages whose players can be picked, e.g. "Q1,Q2,Q3".
    stages TEXT NOT NULL CHECK (trim(stages) <> ''),
    team_size INTEGER NOT NULL CHECK (team_size BETWEEN 1 AND 20),
    budget REAL NOT NULL CHECK (budget > 0 AND budget * 2 = CAST(budget * 2 AS INTEGER)),
    -- ISO times in UTC (Date.toISOString), so they also compare as text.
    opens_at TEXT NOT NULL,
    main_deadline TEXT NOT NULL,
    late_deadline TEXT NOT NULL,
    updated_by TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (opens_at < main_deadline AND main_deadline <= late_deadline),
    FOREIGN KEY (tournament_id) REFERENCES tournaments(id)
  );
  CREATE TABLE IF NOT EXISTS fantasy_league_admins (
    league_id TEXT NOT NULL COLLATE NOCASE,
    user_id INTEGER NOT NULL,
    added_by TEXT,
    added_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (league_id, user_id),
    FOREIGN KEY (league_id) REFERENCES fantasy_leagues(id) ON DELETE CASCADE
  );
  CREATE TABLE IF NOT EXISTS fantasy_prices (
    league_id TEXT NOT NULL COLLATE NOCASE,
    slot_id TEXT NOT NULL COLLATE NOCASE,
    stage TEXT NOT NULL,
    -- Credits, in steps of 0.5.
    price REAL NOT NULL CHECK (price > 0 AND price * 2 = CAST(price * 2 AS INTEGER)),
    -- The price applies from this moment until the next row of the slot.
    effective_from TEXT NOT NULL,
    -- Record ID of the row in the organiser's spreadsheet.
    source_record_id TEXT,
    imported_by TEXT,
    imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (league_id, slot_id, effective_from),
    FOREIGN KEY (league_id) REFERENCES fantasy_leagues(id) ON DELETE CASCADE
  );
  CREATE TABLE IF NOT EXISTS fantasy_teams (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    league_id TEXT NOT NULL COLLATE NOCASE,
    user_id INTEGER NOT NULL,
    team_name TEXT NOT NULL CHECK (length(trim(team_name)) BETWEEN 1 AND 40),
    -- main: built before the main deadline; late: saved after it.
    entry_type TEXT NOT NULL CHECK (entry_type IN ('main', 'late')),
    -- Set by Commit of a late team: from then on the team cannot change.
    locked_at TEXT,
    -- Matches that start before this time do not count; NULL = every match of the stages.
    scoring_from TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (league_id, user_id),
    CHECK (locked_at IS NULL OR entry_type = 'late'),
    CHECK ((locked_at IS NULL) = (scoring_from IS NULL)),
    FOREIGN KEY (league_id) REFERENCES fantasy_leagues(id) ON DELETE CASCADE
  );
  CREATE TABLE IF NOT EXISTS fantasy_team_players (
    team_id INTEGER NOT NULL,
    slot_id TEXT NOT NULL COLLATE NOCASE,
    -- The price when the player joined the team: later price changes do not push a saved team over budget.
    price_at_pick REAL NOT NULL CHECK (price_at_pick > 0),
    picked_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (team_id, slot_id),
    FOREIGN KEY (team_id) REFERENCES fantasy_teams(id) ON DELETE CASCADE
  );
  CREATE INDEX IF NOT EXISTS idx_fantasy_team_players_slot ON fantasy_team_players(slot_id);
  CREATE INDEX IF NOT EXISTS idx_fantasy_league_admins_user ON fantasy_league_admins(user_id);
`;

// Leagues the server creates when their tournament exists; later changes go through the settings route.
// Deadlines are the last second of 23:59, so pages can show "November 27 at 23:59 UTC";
// a deadline is passed at that second.
export const SEED_LEAGUES = [
  {
    id: "CCL-2027-Q",
    tournament_id: "CCL-2027",
    name: "Fantasy CCL 2027 — Qualifiers (trial)",
    stages: "Q1,Q2,Q3",
    team_size: 5,
    budget: 50,
    opens_at: "2026-11-24T00:00:00.000Z",
    main_deadline: "2026-11-27T23:59:59.000Z",
    late_deadline: "2026-12-04T23:59:59.000Z",
  },
];

export const FANTASY_STAGES = ["Q1", "Q2", "Q3", "Main"];
export const TEAM_NAME_MAX = 40;
const LEAGUE_NAME_MAX = 80;

const text = (value) => String(value ?? "").trim().replace(/\s+/g, " ");
const idKey = (value) => text(value).toUpperCase();
// Prices are in steps of 0.5: sums are counted in halves, so no float error decides a budget.
const halves = (price) => Math.round(Number(price) * 2);

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

export async function ensureFantasySchema(db) {
  await dbExec(db, FANTASY_SQL);
  for (const league of SEED_LEAGUES) {
    await dbRun(
      db,
      `
        INSERT OR IGNORE INTO fantasy_leagues (
          id, tournament_id, name, stages, team_size, budget, opens_at, main_deadline, late_deadline, updated_by
        )
        SELECT ?, t.id, ?, ?, ?, ?, ?, ?, ?, 'seed'
        FROM tournaments t
        WHERE upper(trim(t.id)) = upper(trim(?))
      `,
      [
        league.id, league.name, league.stages, league.team_size, league.budget, league.opens_at,
        league.main_deadline, league.late_deadline, league.tournament_id,
      ]
    );
  }
}

// A league row with its stages as a list, or null.
export async function loadLeague(db, leagueId) {
  const id = text(leagueId);
  if (!id) return null;
  const row = await dbGet(db, "SELECT * FROM fantasy_leagues WHERE upper(trim(id)) = upper(trim(?))", [id]);
  if (!row) return null;
  return { ...row, budget: Number(row.budget), team_size: Number(row.team_size), stages: parseStages(row.stages) };
}

const parseStages = (value) => String(value || "").split(",").map(text).filter(Boolean);

// upcoming → open (main teams) → late (late teams) → closed.
export function getFantasyStatus(league, now) {
  const time = now.getTime();
  if (time < Date.parse(league.opens_at)) return "upcoming";
  if (time < Date.parse(league.main_deadline)) return "open";
  if (time < Date.parse(league.late_deadline)) return "late";
  return "closed";
}

/**
 * Where a team stands at `status` (the league status now). playerCount = players in the team.
 * - complete: full main team before the main deadline (still editable);
 * - locked: full main team after the main deadline, scores every match;
 * - committed: late team after Commit, scores from scoring_from;
 * - draft: can still be changed (and, in the late window, committed once full);
 * - not_entered: the league is closed and the team was never complete or committed.
 */
export function describeTeamState(team, playerCount, league, status) {
  const complete = playerCount === league.team_size;
  const afterMain = status === "late" || status === "closed";
  let state;
  if (team.entry_type === "late" && team.locked_at) state = "committed";
  else if (team.entry_type === "main" && complete && afterMain) state = "locked";
  else if (status === "closed") state = "not_entered";
  else if (status === "open" && complete) state = "complete";
  else state = "draft";
  const locked = state === "committed" || state === "locked";
  return {
    state,
    locked,
    can_edit: !locked && (status === "open" || status === "late"),
    can_commit: !locked && status === "late" && complete,
  };
}

// Body of a save or commit: { team_name, slot_ids }. Returns { value } or { error }.
export function normalizeTeamInput(body, league) {
  const teamName = text(body?.team_name);
  if (!teamName) return { error: "Enter a team name" };
  if (teamName.length > TEAM_NAME_MAX) return { error: `Team name must be at most ${TEAM_NAME_MAX} characters` };
  const raw = body?.slot_ids;
  if (!Array.isArray(raw)) return { error: "slot_ids must be a list" };
  const slotIds = raw.map(text);
  if (slotIds.some((id) => !id)) return { error: "slot_ids must not contain empty values" };
  if (new Set(slotIds.map(idKey)).size !== slotIds.length) return { error: "A player can be picked only once" };
  if (slotIds.length > league.team_size) return { error: `A team has at most ${league.team_size} players` };
  return { value: { team_name: teamName, slot_ids: slotIds } };
}

function parseIsoTime(value, field) {
  const raw = text(value);
  const time = Date.parse(raw);
  // A full date and time with a zone, so "2026-11-27" is not read as midnight by accident.
  if (!raw || Number.isNaN(time) || !/T\d{2}:\d{2}/.test(raw) || !/(Z|[+-]\d{2}:?\d{2})$/i.test(raw)) {
    return { error: `${field} must be a date and time in UTC, e.g. 2026-11-27T23:59:59Z` };
  }
  return { value: new Date(time).toISOString() };
}

// Settings a global admin can change: name, budget, dates. Stages and team size stay as created,
// since saved teams depend on them. Returns { value } (only the fields sent) or { error }.
export function normalizeLeagueSettings(body, current) {
  const input = body && typeof body === "object" ? body : {};
  const value = {};
  if (Object.prototype.hasOwnProperty.call(input, "name")) {
    const name = text(input.name);
    if (!name) return { error: "Enter the league name" };
    if (name.length > LEAGUE_NAME_MAX) return { error: `name must be at most ${LEAGUE_NAME_MAX} characters` };
    value.name = name;
  }
  if (Object.prototype.hasOwnProperty.call(input, "budget")) {
    const budget = Number(input.budget);
    if (!Number.isFinite(budget) || budget <= 0 || !Number.isInteger(budget * 2)) {
      return { error: "budget must be above 0 in steps of 0.5" };
    }
    value.budget = budget;
  }
  for (const field of ["opens_at", "main_deadline", "late_deadline"]) {
    if (!Object.prototype.hasOwnProperty.call(input, field)) continue;
    const parsed = parseIsoTime(input[field], field);
    if (parsed.error) return parsed;
    value[field] = parsed.value;
  }
  const next = { ...current, ...value };
  if (!(Date.parse(next.opens_at) < Date.parse(next.main_deadline) && Date.parse(next.main_deadline) <= Date.parse(next.late_deadline))) {
    return { error: "Dates must go in order: opens, main deadline, late deadline" };
  }
  if (!Object.keys(value).length) return { error: "Nothing to change" };
  return { value };
}

// The price of every slot of the league at `at` (ISO): the latest row that has started.
export async function loadCurrentPrices(db, leagueId, at) {
  const rows = await dbAll(
    db,
    `
      SELECT fp.slot_id, fp.stage, fp.price, fp.source_record_id
      FROM fantasy_prices fp
      WHERE upper(trim(fp.league_id)) = upper(trim(?))
        AND fp.effective_from = (
          SELECT MAX(x.effective_from) FROM fantasy_prices x
          WHERE upper(trim(x.league_id)) = upper(trim(fp.league_id))
            AND upper(trim(x.slot_id)) = upper(trim(fp.slot_id))
            AND x.effective_from <= ?
        )
    `,
    [leagueId, at]
  );
  return new Map(rows.map((row) => [idKey(row.slot_id), row]));
}

// "Q1|<player id>" → group key, from the CCL matches (duels.stage + group_name).
export async function loadPlayerGroups(db, tournamentId) {
  const rows = await dbAll(
    db,
    `
      SELECT stage, group_name, trim(player_1_id) AS p1, trim(player_2_id) AS p2
      FROM duels
      WHERE upper(trim(tournament_id)) = upper(trim(?))
        AND source_type = 'ccl'
        AND deleted_at IS NULL
        AND trim(COALESCE(group_name, '')) <> ''
    `,
    [tournamentId]
  );
  const groups = new Map();
  for (const row of rows) {
    for (const playerId of [row.p1, row.p2]) {
      const key = `${idKey(row.stage)}|${playerId}`;
      if (playerId && !groups.has(key)) groups.set(key, text(row.group_name));
    }
  }
  return groups;
}

// Filled slots of the league stages (entry_stage), with the player's nickname.
export async function loadLeagueSlots(db, league) {
  return dbAll(
    db,
    `
      SELECT tp.id AS slot_id, trim(tp.player_id) AS player_id, tp.entry_stage AS stage, tp.association_id,
             tp.league_status, p.bga_nickname, p.name AS profile_name, e.seed,
             p.avatar, p.bga_elo, p.gg_elo, p.gg_rating_position
      FROM tournament_players tp
      LEFT JOIN profiles p ON trim(p.id) = trim(tp.player_id)
      LEFT JOIN ccl_stage_entries e ON upper(trim(e.slot_id)) = upper(trim(tp.id)) AND e.stage = tp.entry_stage
      WHERE upper(trim(tp.tournament_id)) = upper(trim(?))
        AND tp.entry_stage IN (${league.stages.map(() => "?").join(", ")})
        AND trim(COALESCE(tp.player_id, '')) <> ''
      ORDER BY tp.entry_stage, tp.id
    `,
    [league.tournament_id, ...league.stages]
  );
}

// Players a team can pick: filled slots of the league stages that have a current price.
async function loadFantasyPlayers(db, league, at) {
  const [slots, prices, associations, groups] = await Promise.all([
    loadLeagueSlots(db, league),
    loadCurrentPrices(db, league.id, at),
    loadAssociationsById(db),
    loadPlayerGroups(db, league.tournament_id),
  ]);
  return slots
    .filter((slot) => prices.has(idKey(slot.slot_id)))
    .map((slot) => {
      const association = associations.get(idKey(slot.association_id)) || null;
      return {
        slot_id: slot.slot_id,
        player_id: slot.player_id,
        nickname: slot.bga_nickname || slot.profile_name || slot.player_id,
        stage: slot.stage,
        group: groups.get(`${idKey(slot.stage)}|${slot.player_id}`) || null,
        seed: slot.seed || null,
        association: association ? { id: association.id, name: association.name, flag: association.flag } : null,
        price: Number(prices.get(idKey(slot.slot_id)).price),
        eliminated: slot.league_status === "eliminated",
      };
    })
    .sort((left, right) => right.price - left.price || String(left.nickname).localeCompare(String(right.nickname)));
}

async function loadTeam(db, leagueId, userId) {
  return dbGet(
    db,
    "SELECT * FROM fantasy_teams WHERE upper(trim(league_id)) = upper(trim(?)) AND user_id = ?",
    [leagueId, userId]
  );
}

async function loadTeamPlayers(db, teamId) {
  return dbAll(
    db,
    "SELECT slot_id, price_at_pick FROM fantasy_team_players WHERE team_id = ? ORDER BY price_at_pick DESC, slot_id",
    [teamId]
  );
}

// ---- Scoring (Phase 2). Points are not stored: they are counted from duels on every request,
// so a corrected match result changes the leaderboard at once.

// Points of a player for one Bo3 match: the winner gets 3 minus the games the opponent won (2:0 → 3, 2:1 → 2),
// the loser gets the games they won (1:2 → 1, 0:2 → 0).
export const matchPoints = (own, opp) => (own > opp ? Math.max(0, 3 - opp) : own);

const timeOf = (value) => {
  const time = Date.parse(text(value));
  return Number.isFinite(time) ? time : null;
};

// Finished CCL matches of the league stages: Done, a decided score, not deleted, not a test. Oldest first.
export async function loadScoredMatches(db, league) {
  return dbAll(
    db,
    `
      SELECT id, stage, group_name, round_name, time_utc, duel_format, status,
             trim(player_1_id) AS player_1_id, trim(player_2_id) AS player_2_id, dw1, dw2
      FROM duels
      WHERE source_type = 'ccl'
        AND upper(trim(tournament_id)) = upper(trim(?))
        AND stage IN (${league.stages.map(() => "?").join(", ")})
        AND lower(trim(COALESCE(status, ''))) = 'done'
        AND deleted_at IS NULL
        AND COALESCE(is_test, 0) = 0
        AND dw1 IS NOT NULL AND dw2 IS NOT NULL AND dw1 <> dw2
      ORDER BY time_utc IS NULL, time_utc, id
    `,
    [league.tournament_id, ...league.stages]
  );
}

// Matches still to play in the league stages (Planned or In progress, both players known), soonest first:
// a match without a time goes after the timed ones, by its deadline.
export async function loadUpcomingMatches(db, league) {
  return dbAll(
    db,
    `
      SELECT id, stage, group_name, round_name, time_utc, deadline_utc, duel_format,
             COALESCE(NULLIF(trim(status), ''), 'Planned') AS status,
             trim(player_1_id) AS player_1_id, trim(player_2_id) AS player_2_id
      FROM duels
      WHERE source_type = 'ccl'
        AND upper(trim(tournament_id)) = upper(trim(?))
        AND stage IN (${league.stages.map(() => "?").join(", ")})
        AND lower(COALESCE(NULLIF(trim(status), ''), 'planned')) IN ('planned', 'in progress')
        AND deleted_at IS NULL
        AND COALESCE(is_test, 0) = 0
        AND trim(COALESCE(player_1_id, '')) <> ''
        AND trim(COALESCE(player_2_id, '')) <> ''
      ORDER BY time_utc IS NULL, time_utc, deadline_utc IS NULL, deadline_utc, id
    `,
    [league.tournament_id, ...league.stages]
  );
}

// What the match details popup shows of a player (the fields of the duel popup on matches.html).
function matchPlayerView(playerId, slot, associations) {
  const association = slot ? associations.get(idKey(slot.association_id)) || null : null;
  const number = (value) => (value === null || value === undefined || value === "" ? null : Number(value));
  return {
    id: text(playerId),
    nickname: slot ? slot.bga_nickname || slot.profile_name || slot.player_id : text(playerId),
    name: text(slot?.profile_name) || null,
    avatar: text(slot?.avatar) || null,
    flag: association?.flag || null,
    country: association?.name || null,
    elo: number(slot?.gg_elo),
    rank: number(slot?.gg_rating_position),
    bgaElo: number(slot?.bga_elo),
  };
}

// slot key → the player's next match (or nothing), with both players for the popup.
export function nextMatchesBySlot(slots, matches, associations) {
  const slotByPlayer = new Map(slots.map((slot) => [text(slot.player_id), slot]));
  const next = new Map();
  for (const match of matches) {
    for (const [playerId, opponentId] of [[match.player_1_id, match.player_2_id], [match.player_2_id, match.player_1_id]]) {
      const slot = slotByPlayer.get(text(playerId));
      if (!slot || next.has(idKey(slot.slot_id))) continue;
      next.set(idKey(slot.slot_id), {
        duel_id: match.id,
        player_1_id: match.player_1_id,
        player_2_id: match.player_2_id,
        time_utc: match.time_utc || null,
        deadline_utc: match.deadline_utc || null,
        format: text(match.duel_format) || null,
        status: match.status,
        stage: match.stage || null,
        group: text(match.group_name) || null,
        round_name: text(match.round_name) || null,
        player: matchPlayerView(playerId, slot, associations),
        opponent: matchPlayerView(opponentId, slotByPlayer.get(text(opponentId)) || null, associations),
      });
    }
  }
  return next;
}

// player id → the player's results, oldest first, each from the player's side.
export function resultsByPlayer(matches) {
  const results = new Map();
  const add = (playerId, opponentId, own, opp, match) => {
    if (!playerId) return;
    if (!results.has(playerId)) results.set(playerId, []);
    results.get(playerId).push({
      duel_id: match.id,
      own,
      opp,
      points: matchPoints(own, opp),
      opponent_id: opponentId || null,
      time_utc: match.time_utc || null,
      stage: match.stage || null,
      group: text(match.group_name) || null,
      round_name: text(match.round_name) || null,
      // The duel as stored, for the match details popup (player 1 on the left).
      player_1_id: match.player_1_id || null,
      player_2_id: match.player_2_id || null,
      dw1: Number(match.dw1),
      dw2: Number(match.dw2),
      format: text(match.duel_format) || null,
      status: text(match.status) || "Done",
    });
  };
  for (const match of matches) {
    const dw1 = Number(match.dw1);
    const dw2 = Number(match.dw2);
    add(match.player_1_id, match.player_2_id, dw1, dw2, match);
    add(match.player_2_id, match.player_1_id, dw2, dw1, match);
  }
  return results;
}

// Whether a result counts for a team: every match for a main team (scoring_from NULL); for a late team,
// a match whose time is at or after its Commit. A match without a time counts for main teams only.
export function resultCounts(result, scoringFrom) {
  if (!scoringFrom) return true;
  const at = timeOf(result.time_utc);
  return at !== null && at >= timeOf(scoringFrom);
}

// Most points first; equal points share a place (1, 2, 2, 4), then by team name.
export function rankTeams(rows) {
  const sorted = [...rows].sort((left, right) => (
    right.points - left.points || String(left.team_name).localeCompare(String(right.team_name))
  ));
  let rank = 0;
  return sorted.map((row, index) => {
    if (index === 0 || row.points !== sorted[index - 1].points) rank = index + 1;
    return { rank, ...row };
  });
}

const playerStatus = (slot) => {
  if (slot?.league_status === "eliminated") return "eliminated";
  if (slot?.league_status === "qualified") return "qualified";
  return "playing";
};

// Every team of the league with its players and points. Only locked teams score (a full main team after the
// main deadline, a late team after Commit); drafts get points = null.
async function loadLeagueScores(db, league, current) {
  const status = getFantasyStatus(league, current);
  const [slots, matches, teams, picks] = await Promise.all([
    loadLeagueSlots(db, league),
    loadScoredMatches(db, league),
    dbAll(
      db,
      `
        SELECT t.*, p.bga_nickname
        FROM fantasy_teams t
        LEFT JOIN users u ON u.id = t.user_id
        LEFT JOIN profiles p ON p.id = u.bga_id AND p.deleted_at IS NULL
        WHERE upper(trim(t.league_id)) = upper(trim(?))
        ORDER BY t.id
      `,
      [league.id]
    ),
    dbAll(
      db,
      `
        SELECT tp.team_id, tp.slot_id, tp.price_at_pick
        FROM fantasy_team_players tp
        JOIN fantasy_teams t ON t.id = tp.team_id
        WHERE upper(trim(t.league_id)) = upper(trim(?))
        ORDER BY tp.team_id, tp.price_at_pick DESC, tp.slot_id
      `,
      [league.id]
    ),
  ]);
  const slotByKey = new Map(slots.map((slot) => [idKey(slot.slot_id), slot]));
  const results = resultsByPlayer(matches);
  const resultsOf = (slot) => (slot ? results.get(text(slot.player_id)) || [] : []);
  const playerPoints = new Map(slots.map((slot) => [
    idKey(slot.slot_id),
    resultsOf(slot).reduce((sum, result) => sum + result.points, 0),
  ]));
  const picksByTeam = new Map();
  for (const pick of picks) {
    if (!picksByTeam.has(pick.team_id)) picksByTeam.set(pick.team_id, []);
    picksByTeam.get(pick.team_id).push(pick);
  }
  const scoredTeams = teams.map((team) => {
    const teamPicks = picksByTeam.get(team.id) || [];
    const state = describeTeamState(team, teamPicks.length, league, status);
    const scoring = state.locked;
    const players = teamPicks.map((pick) => {
      const slot = slotByKey.get(idKey(pick.slot_id)) || null;
      const playerResults = resultsOf(slot).map((result) => ({
        ...result,
        counted: scoring && resultCounts(result, team.scoring_from),
      }));
      return {
        slot_id: pick.slot_id,
        price_at_pick: Number(pick.price_at_pick),
        slot,
        results: playerResults,
        points: scoring ? playerResults.reduce((sum, result) => sum + (result.counted ? result.points : 0), 0) : null,
      };
    });
    return {
      team,
      state,
      scoring,
      players,
      points: scoring ? players.reduce((sum, player) => sum + player.points, 0) : null,
    };
  });
  const lastResultAt = matches.reduce((latest, match) => {
    const at = timeOf(match.time_utc);
    return at !== null && (latest === null || at > latest) ? at : latest;
  }, null);
  return {
    status,
    slots,
    playerPoints,
    teams: scoredTeams,
    lastResultAt: lastResultAt === null ? null : new Date(lastResultAt).toISOString(),
  };
}

function userId(user) {
  const id = Number(user?.id);
  return user?.id != null && Number.isInteger(id) ? id : null;
}

const isGlobalAdmin = (user) => Number(user?.admin) === 1;

export async function hasFantasyLeagueAccess(db, user, leagueId) {
  if (!user) return false;
  if (isGlobalAdmin(user)) return true;
  const accountId = userId(user);
  if (accountId === null) return false;
  const row = await dbGet(
    db,
    "SELECT 1 AS allowed FROM fantasy_league_admins WHERE upper(trim(league_id)) = upper(trim(?)) AND user_id = ?",
    [leagueId, accountId]
  );
  return Number(row?.allowed) === 1;
}

function actorLabel(user) {
  return text(user?.email) || (userId(user) !== null ? `user:${userId(user)}` : null);
}

// The name shown for a team owner: the linked BGA nickname, else the account name.
function ownerName(user) {
  return text(user?.player_id ? user?.bga_nickname : "") || text(user?.name) || null;
}

function leagueView(league, status) {
  return {
    id: league.id,
    tournament_id: league.tournament_id,
    name: league.name,
    opens_at: league.opens_at,
    main_deadline: league.main_deadline,
    late_deadline: league.late_deadline,
    stages: league.stages,
    team_size: league.team_size,
    budget: league.budget,
    status,
  };
}

// currentPrices: slot key → today's price. A late draft is priced at today's prices (see saveTeam),
// so a price rise can push it over the budget; it then cannot be committed until players are replaced.
function teamView(team, players, league, status, currentPrices = new Map()) {
  const teamState = describeTeamState(team, players.length, league, status);
  const repriced = status === "late" && !teamState.locked;
  const priced = players.map((player) => {
    const paid = Number(player.price_at_pick);
    const today = currentPrices.get(idKey(player.slot_id));
    return { slot_id: player.slot_id, price_at_pick: paid, price: repriced && today != null ? Number(today) : paid };
  });
  const costHalves = priced.reduce((sum, player) => sum + halves(player.price), 0);
  const overBudget = costHalves > halves(league.budget);
  return {
    id: team.id,
    team_name: team.team_name,
    entry_type: team.entry_type,
    locked_at: team.locked_at || null,
    scoring_from: team.scoring_from || null,
    updated_at: team.updated_at,
    cost: costHalves / 2,
    over_budget: overBudget,
    players: priced,
    ...teamState,
    can_commit: teamState.can_commit && !overBudget,
  };
}

const priceMap = (prices) => new Map([...prices].map(([key, row]) => [key, Number(row.price)]));

export function registerFantasyRoutes(app, {
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

  // The league and its CCL tournament; 404 when either is missing.
  const requireLeague = async (leagueId) => {
    const league = await loadLeague(db, leagueId);
    const tournament = league ? await loadCclTournament(db, league.tournament_id) : null;
    if (!league || !tournament) throw requestError(404, "Fantasy league not found");
    return { league, tournament };
  };

  const requireLeagueAdmin = async (user, leagueId) => {
    const found = await requireLeague(leagueId);
    if (!(await hasFantasyLeagueAccess(db, user, found.league.id))) throw requestError(403, "Forbidden");
    return found;
  };

  const requireGlobalAdmin = async (user, leagueId) => {
    const found = await requireLeague(leagueId);
    if (!isGlobalAdmin(user)) throw requestError(403, "Only site admins can do this");
    return found;
  };

  // Public: league, players with prices; for a signed-in user also their own team.
  // Other users' teams are never returned here.
  app.get("/public/fantasy/:leagueId", handle("Failed to load Fantasy", async (req, res) => {
    const { league, tournament } = await requireLeague(req.params.leagueId);
    const current = now();
    const status = getFantasyStatus(league, current);
    const accountId = userId(req.user);
    const [pool, team, scores, upcoming, associations] = await Promise.all([
      loadFantasyPlayers(db, league, current.toISOString()),
      accountId === null ? null : loadTeam(db, league.id, accountId),
      loadLeagueScores(db, league, current),
      loadUpcomingMatches(db, league),
      loadAssociationsById(db),
    ]);
    const nextMatches = nextMatchesBySlot(scores.slots, upcoming, associations);
    const teamPlayers = team ? await loadTeamPlayers(db, team.id) : [];
    // How many teams picked a player: only once teams are locked, so nobody sees others' picks before the deadline.
    const afterMain = status === "late" || status === "closed";
    const pickedBy = new Map();
    if (afterMain) {
      for (const entry of scores.teams.filter((item) => item.scoring)) {
        for (const player of entry.players) pickedBy.set(idKey(player.slot_id), (pickedBy.get(idKey(player.slot_id)) || 0) + 1);
      }
    }
    const players = pool.map((player) => ({
      ...player,
      points: scores.playerPoints.get(idKey(player.slot_id)) || 0,
      next_match: nextMatches.get(idKey(player.slot_id)) || null,
      ...(afterMain ? { picked_by: pickedBy.get(idKey(player.slot_id)) || 0 } : {}),
    }));
    return res.json({
      ok: true,
      league: leagueView(league, status),
      tournament: { id: tournament.id, name: tournament.name },
      players,
      viewer: {
        authenticated: Boolean(req.user),
        name: req.user ? ownerName(req.user) : null,
        team: team ? teamView(team, teamPlayers, league, status, new Map(players.map((player) => [idKey(player.slot_id), player.price]))) : null,
      },
    });
  }));

  // The owner shown in public: the linked BGA nickname only; without it the page shows "Pending verification".
  // Account names, emails and user ids never go into public answers.
  const publicOwner = (team) => text(team.bga_nickname) || null;
  const committedAt = (team) => (team.entry_type === "late" ? team.locked_at || null : null);

  // Public, from the main deadline: locked teams ranked by points. Before it: { available: false }.
  app.get("/public/fantasy/:leagueId/leaderboard", handle("Failed to load the leaderboard", async (req, res) => {
    const { league } = await requireLeague(req.params.leagueId);
    const current = now();
    const status = getFantasyStatus(league, current);
    if (status === "upcoming" || status === "open") {
      return res.json({ ok: true, league: leagueView(league, status), available: false, opens_at: league.main_deadline, teams: [] });
    }
    const scores = await loadLeagueScores(db, league, current);
    const accountId = userId(req.user);
    const scoring = scores.teams.filter((entry) => entry.scoring);
    const mine = accountId === null ? null : scoring.find((entry) => Number(entry.team.user_id) === accountId);
    return res.json({
      ok: true,
      league: leagueView(league, status),
      available: true,
      last_result_at: scores.lastResultAt,
      my_team_id: mine ? mine.team.id : null,
      teams: rankTeams(scoring.map((entry) => ({
        team_id: entry.team.id,
        team_name: entry.team.team_name,
        owner: publicOwner(entry.team),
        entry_type: entry.team.entry_type,
        committed_at: committedAt(entry.team),
        points: entry.points,
      }))),
    });
  }));

  // Public: one locked team with its players and their matches, for the row opened in the leaderboard.
  // Drafts, uncommitted late teams and main teams before the deadline answer 404, so nobody learns they exist.
  app.get("/public/fantasy/:leagueId/teams/:teamId", handle("Failed to load the team", async (req, res) => {
    const { league } = await requireLeague(req.params.leagueId);
    const teamId = Number(req.params.teamId);
    const scores = Number.isInteger(teamId) ? await loadLeagueScores(db, league, now()) : null;
    const entry = scores?.teams.find((item) => item.scoring && Number(item.team.id) === teamId);
    if (!entry) throw requestError(404, "Team not found");
    const [associations, groups] = await Promise.all([loadAssociationsById(db), loadPlayerGroups(db, league.tournament_id)]);
    const nameOf = new Map(scores.slots.map((slot) => [text(slot.player_id), slot.bga_nickname || slot.profile_name || slot.player_id]));
    return res.json({
      ok: true,
      team: {
        id: entry.team.id,
        team_name: entry.team.team_name,
        owner: publicOwner(entry.team),
        entry_type: entry.team.entry_type,
        committed_at: committedAt(entry.team),
        points: entry.points,
        spent: entry.players.reduce((sum, player) => sum + halves(player.price_at_pick), 0) / 2,
        players: entry.players.map((player) => {
          const slot = player.slot;
          const association = slot ? associations.get(idKey(slot.association_id)) || null : null;
          return {
            slot_id: player.slot_id,
            player_id: slot ? text(slot.player_id) : null,
            nickname: slot ? slot.bga_nickname || slot.profile_name || slot.player_id : player.slot_id,
            stage: slot?.stage || null,
            group: slot ? groups.get(`${idKey(slot.stage)}|${text(slot.player_id)}`) || null : null,
            association: association ? { id: association.id, name: association.name, flag: association.flag } : null,
            price_at_pick: player.price_at_pick,
            status: playerStatus(slot),
            points: player.points,
            matches: player.results.map((result) => ({
              duel_id: result.duel_id,
              score: `${result.own}-${result.opp}`,
              points: result.points,
              counted: result.counted,
              opponent: result.opponent_id ? nameOf.get(result.opponent_id) || null : null,
              time_utc: result.time_utc,
              stage: result.stage,
              group: result.group,
              round_name: result.round_name,
              player_1_id: result.player_1_id,
              player_2_id: result.player_2_id,
              dw1: result.dw1,
              dw2: result.dw2,
              format: result.format,
              status: result.status,
            })),
          };
        }),
      },
    });
  }));

  // Saves the whole team in one transaction; with commit = true also locks a late team.
  const saveTeam = async (req, { commit }) => {
    const { league } = await requireLeague(req.params.leagueId);
    const accountId = userId(req.user);
    if (accountId === null) throw requestError(401, "Unauthorized");
    const current = now();
    const status = getFantasyStatus(league, current);
    if (status === "upcoming") throw requestError(409, "Team building has not started yet");
    if (status === "closed") throw requestError(409, "Team building is closed");
    if (commit && status !== "late") throw requestError(409, "Commit is only for teams entered after the main deadline");
    const input = normalizeTeamInput(req.body, league);
    if (input.error) throw requestError(400, input.error);
    const { team_name: teamName, slot_ids: slotIds } = input.value;
    if (commit && slotIds.length !== league.team_size) {
      throw requestError(400, `Pick ${league.team_size} players before you commit the team`);
    }
    const at = current.toISOString();

    const result = await inTransaction(db, async () => {
      const existing = await loadTeam(db, league.id, accountId);
      const existingPlayers = existing ? await loadTeamPlayers(db, existing.id) : [];
      if (existing && describeTeamState(existing, existingPlayers.length, league, status).locked) {
        throw requestError(409, "Your team is locked and cannot be changed");
      }

      // Before the main deadline players kept in the team keep their price; new ones are bought at today's price.
      // A late draft is not a team yet: every player is priced at today's price, so a player who got more
      // expensive since the draft was saved can push it over the budget, and then neither Save nor Commit
      // goes through until players are replaced.
      const kept = status === "open"
        ? new Map(existingPlayers.map((player) => [idKey(player.slot_id), Number(player.price_at_pick)]))
        : new Map();
      const prices = await loadCurrentPrices(db, league.id, at);
      const slots = slotIds.length
        ? await dbAll(
          db,
          `
            SELECT id FROM tournament_players
            WHERE upper(trim(tournament_id)) = upper(trim(?))
              AND upper(trim(id)) IN (${slotIds.map(() => "upper(trim(?))").join(", ")})
              AND entry_stage IN (${league.stages.map(() => "?").join(", ")})
              AND trim(COALESCE(player_id, '')) <> ''
          `,
          [league.tournament_id, ...slotIds, ...league.stages]
        )
        : [];
      const slotById = new Map(slots.map((slot) => [idKey(slot.id), slot]));
      const picks = slotIds.map((slotId) => {
        const slot = slotById.get(idKey(slotId));
        const price = kept.get(idKey(slotId)) ?? (prices.has(idKey(slotId)) ? Number(prices.get(idKey(slotId)).price) : null);
        if (!slot || price === null) throw requestError(400, `Player not available: ${slotId}`);
        return { slot_id: slot.id, price };
      });
      const costHalves = picks.reduce((sum, pick) => sum + halves(pick.price), 0);
      if (costHalves > halves(league.budget)) {
        const repriced = picks.some((pick) => {
          const paid = existingPlayers.find((player) => idKey(player.slot_id) === idKey(pick.slot_id));
          return paid && halves(paid.price_at_pick) < halves(pick.price);
        });
        throw requestError(
          400,
          `The team costs ${costHalves / 2}, over the budget of ${league.budget}`
            + (repriced ? ". Prices have gone up since you saved it: replace players to bring the team within budget" : "")
        );
      }

      // Before the main deadline a team is a main entry; after it, saving makes it a late one.
      const entryType = status === "open" ? "main" : "late";
      const lockedAt = commit ? at : null;
      let teamId = existing?.id;
      if (existing) {
        await dbRun(
          db,
          "UPDATE fantasy_teams SET team_name = ?, entry_type = ?, locked_at = ?, scoring_from = ?, updated_at = ? WHERE id = ?",
          [teamName, entryType, lockedAt, lockedAt, at, existing.id]
        );
        await dbRun(db, "DELETE FROM fantasy_team_players WHERE team_id = ?", [existing.id]);
      } else {
        try {
          const inserted = await dbRun(
            db,
            `
              INSERT INTO fantasy_teams (league_id, user_id, team_name, entry_type, locked_at, scoring_from, created_at, updated_at)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            `,
            [league.id, accountId, teamName, entryType, lockedAt, lockedAt, at, at]
          );
          teamId = inserted.lastID;
        } catch (error) {
          if (isUniqueViolation(error)) throw requestError(409, "You already have a team; reload the page");
          throw error;
        }
      }
      for (const pick of picks) {
        await dbRun(
          db,
          "INSERT INTO fantasy_team_players (team_id, slot_id, price_at_pick, picked_at) VALUES (?, ?, ?, ?)",
          [teamId, pick.slot_id, pick.price, at]
        );
      }
      const team = await dbGet(db, "SELECT * FROM fantasy_teams WHERE id = ?", [teamId]);
      return { team, players: await loadTeamPlayers(db, teamId), created: !existing, prices: priceMap(prices) };
    });

    audit(req, {
      event_type: commit ? "fantasy_team.committed" : "fantasy_team.saved",
      entity_type: "fantasy_team",
      action: result.created ? "create" : "update",
      record_id: String(result.team.id),
      changes: { slot_ids: { new: result.players.map((player) => player.slot_id) }, team_name: { new: result.team.team_name } },
      metadata: { league_id: league.id, entry_type: result.team.entry_type, locked_at: result.team.locked_at },
    });
    return { ...result, league, status };
  };

  // Signed-in user: saves the team (a draft while it is not full).
  app.put(
    "/fantasy/:leagueId/team",
    requireAuthenticated,
    requireTrustedJsonRequest,
    handle("Failed to save the team", async (req, res) => {
      const { team, players, created, league, status, prices } = await saveTeam(req, { commit: false });
      return res.status(created ? 201 : 200).json({ ok: true, team: teamView(team, players, league, status, prices) });
    })
  );

  // Signed-in user, after the main deadline: saves the full team and locks it. Cannot be undone.
  app.post(
    "/fantasy/:leagueId/team/commit",
    requireAuthenticated,
    requireTrustedJsonRequest,
    handle("Failed to commit the team", async (req, res) => {
      const { team, players, league, status, prices } = await saveTeam(req, { commit: true });
      return res.json({ ok: true, team: teamView(team, players, league, status, prices) });
    })
  );

  // Leagues the viewer administers (all of them for site admins), for the Player Hub menu and page.
  app.get("/fantasy/leagues/managed", requireAuthenticated, handle("Failed to load Fantasy leagues", async (req, res) => {
    const globalAdmin = isGlobalAdmin(req.user);
    const accountId = userId(req.user);
    const rows = globalAdmin
      ? await dbAll(db, "SELECT id FROM fantasy_leagues ORDER BY opens_at DESC, id")
      : await dbAll(
        db,
        `
          SELECT l.id FROM fantasy_leagues l
          JOIN fantasy_league_admins a ON upper(trim(a.league_id)) = upper(trim(l.id))
          WHERE a.user_id = ?
          ORDER BY l.opens_at DESC, l.id
        `,
        [accountId ?? -1]
      );
    const current = now();
    const leagues = [];
    for (const row of rows) {
      const league = await loadLeague(db, row.id);
      leagues.push(leagueView(league, getFantasyStatus(league, current)));
    }
    return res.json({ ok: true, can_manage_settings: globalAdmin, leagues });
  }));

  // Site admins: changes the name, budget or dates of a league.
  app.patch(
    "/fantasy/:leagueId",
    requireAuthenticated,
    requireTrustedJsonRequest,
    handle("Failed to save the league", async (req, res) => {
      const { league } = await requireGlobalAdmin(req.user, req.params.leagueId);
      const settings = normalizeLeagueSettings(req.body, league);
      if (settings.error) throw requestError(400, settings.error);
      const fields = Object.keys(settings.value);
      await dbRun(
        db,
        `UPDATE fantasy_leagues SET ${fields.map((field) => `${field} = ?`).join(", ")}, updated_by = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
        [...fields.map((field) => settings.value[field]), actorLabel(req.user), league.id]
      );
      audit(req, {
        event_type: "fantasy_league.updated",
        entity_type: "fantasy_league",
        action: "update",
        record_id: league.id,
        changes: Object.fromEntries(fields.map((field) => [field, { old: league[field], new: settings.value[field] }])),
        metadata: { league_id: league.id },
      });
      const updated = await loadLeague(db, league.id);
      return res.json({ ok: true, league: leagueView(updated, getFantasyStatus(updated, now())) });
    })
  );

  // League admins: every team with its players, for the counters and the CSV export.
  app.get("/fantasy/:leagueId/teams", requireAuthenticated, handle("Failed to load Fantasy teams", async (req, res) => {
    const { league } = await requireLeagueAdmin(req.user, req.params.leagueId);
    const current = now();
    const status = getFantasyStatus(league, current);
    const [teams, picks, scores] = await Promise.all([
      dbAll(
        db,
        `
          SELECT t.*, u.name AS account_name, p.bga_nickname
          FROM fantasy_teams t
          LEFT JOIN users u ON u.id = t.user_id
          LEFT JOIN profiles p ON p.id = u.bga_id AND p.deleted_at IS NULL
          WHERE upper(trim(t.league_id)) = upper(trim(?))
          ORDER BY t.id
        `,
        [league.id]
      ),
      dbAll(
        db,
        `
          SELECT tp.team_id, tp.slot_id, tp.price_at_pick, s.entry_stage AS stage,
                 COALESCE(pr.bga_nickname, pr.name, trim(s.player_id)) AS player,
                 (
                   SELECT x.source_record_id FROM fantasy_prices x
                   WHERE upper(trim(x.league_id)) = upper(trim(t.league_id))
                     AND upper(trim(x.slot_id)) = upper(trim(tp.slot_id))
                   ORDER BY x.effective_from DESC LIMIT 1
                 ) AS record_id
          FROM fantasy_team_players tp
          JOIN fantasy_teams t ON t.id = tp.team_id
          LEFT JOIN tournament_players s ON upper(trim(s.id)) = upper(trim(tp.slot_id))
          LEFT JOIN profiles pr ON trim(pr.id) = trim(s.player_id)
          WHERE upper(trim(t.league_id)) = upper(trim(?))
          ORDER BY tp.team_id, tp.price_at_pick DESC, tp.slot_id
        `,
        [league.id]
      ),
      loadLeagueScores(db, league, current),
    ]);
    // Points only for teams that score (locked); null for drafts.
    const scoreByTeam = new Map(scores.teams.map((entry) => [entry.team.id, entry]));
    const picksByTeam = new Map();
    for (const pick of picks) {
      if (!picksByTeam.has(pick.team_id)) picksByTeam.set(pick.team_id, []);
      const scored = scoreByTeam.get(pick.team_id)?.players.find((player) => idKey(player.slot_id) === idKey(pick.slot_id));
      picksByTeam.get(pick.team_id).push({
        slot_id: pick.slot_id,
        player: pick.player || null,
        stage: pick.stage || null,
        record_id: pick.record_id || null,
        price_at_pick: Number(pick.price_at_pick),
        points: scored ? scored.points : null,
      });
    }
    const counts = { total: teams.length, complete: 0, drafts: 0, late: 0, committed: 0 };
    const rows = teams.map((team) => {
      const players = picksByTeam.get(team.id) || [];
      const state = describeTeamState(team, players.length, league, status);
      if (players.length === league.team_size) counts.complete += 1;
      else counts.drafts += 1;
      if (team.entry_type === "late") counts.late += 1;
      if (state.state === "committed") counts.committed += 1;
      return {
        team_id: team.id,
        team_name: team.team_name,
        user_id: team.user_id,
        owner: text(team.bga_nickname) || text(team.account_name) || null,
        bga_nickname: team.bga_nickname || null,
        entry_type: team.entry_type,
        state: state.state,
        saved_at: team.updated_at,
        // A full main team is locked by the main deadline itself.
        locked_at: team.locked_at || (state.state === "locked" ? league.main_deadline : null),
        scoring_from: team.scoring_from || null,
        cost: players.reduce((sum, player) => sum + halves(player.price_at_pick), 0) / 2,
        points: scoreByTeam.get(team.id)?.points ?? null,
        players,
      };
    });
    return res.json({ ok: true, league: leagueView(league, status), counts, teams: rows });
  }));

  // Site admins: who administers a league.
  app.get("/fantasy/:leagueId/admins", requireAuthenticated, handle("Failed to load league admins", async (req, res) => {
    const { league } = await requireGlobalAdmin(req.user, req.params.leagueId);
    const admins = await dbAll(
      db,
      `
        SELECT a.user_id, u.email, u.name, p.bga_nickname, a.added_by, a.added_at
        FROM fantasy_league_admins a
        LEFT JOIN users u ON u.id = a.user_id
        LEFT JOIN profiles p ON p.id = u.bga_id AND p.deleted_at IS NULL
        WHERE upper(trim(a.league_id)) = upper(trim(?))
        ORDER BY a.added_at, a.rowid
      `,
      [league.id]
    );
    return res.json({ ok: true, admins });
  }));

  // Site admins: adds a league admin by the email of their site account (they must have signed in once).
  app.post(
    "/fantasy/:leagueId/admins",
    requireAuthenticated,
    requireTrustedJsonRequest,
    handle("Failed to add the league admin", async (req, res) => {
      const { league } = await requireGlobalAdmin(req.user, req.params.leagueId);
      const email = text(req.body?.email).toLowerCase();
      if (!email) throw requestError(400, "Enter the email of the site account");
      const accounts = await dbAll(db, "SELECT id FROM users WHERE lower(trim(email)) = ?", [email]);
      if (!accounts.length) throw requestError(404, "No site account has this email; the person must sign in to carcassonne.gg first");
      if (accounts.length > 1) throw requestError(409, "Several site accounts have this email");
      const result = await dbRun(
        db,
        "INSERT OR IGNORE INTO fantasy_league_admins (league_id, user_id, added_by) VALUES (?, ?, ?)",
        [league.id, accounts[0].id, actorLabel(req.user)]
      );
      if (result.changes) {
        audit(req, {
          event_type: "fantasy_league_admin.added",
          entity_type: "fantasy_league",
          action: "update",
          record_id: league.id,
          changes: { admin_user_id: { new: accounts[0].id } },
          metadata: { league_id: league.id },
        });
      }
      return res.status(result.changes ? 201 : 200).json({ ok: true, user_id: accounts[0].id, added: Boolean(result.changes) });
    })
  );

  // Site admins: removes a league admin.
  app.delete(
    "/fantasy/:leagueId/admins/:userId",
    requireAuthenticated,
    handle("Failed to remove the league admin", async (req, res) => {
      const { league } = await requireGlobalAdmin(req.user, req.params.leagueId);
      const adminUserId = Number(req.params.userId);
      if (!Number.isInteger(adminUserId)) throw requestError(400, "Invalid user id");
      const result = await dbRun(
        db,
        "DELETE FROM fantasy_league_admins WHERE upper(trim(league_id)) = upper(trim(?)) AND user_id = ?",
        [league.id, adminUserId]
      );
      if (!result.changes) throw requestError(404, "This user is not an admin of the league");
      audit(req, {
        event_type: "fantasy_league_admin.removed",
        entity_type: "fantasy_league",
        action: "update",
        record_id: league.id,
        changes: { admin_user_id: { old: adminUserId } },
        metadata: { league_id: league.id },
      });
      return res.json({ ok: true });
    })
  );
}
