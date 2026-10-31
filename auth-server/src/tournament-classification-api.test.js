import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";
import sqlite3 from "sqlite3";
import * as classification from "./tournament-classification.js";
import { ensureCategoriesSchema } from "./categories.js";
import { ensureCclSchema } from "./ccl-schema.js";
import { CCL_DEADLINE_FIELDS, isCclTournament } from "./ccl-slots.js";
import { canManageTournamentAccessUsers, createRequireTournamentAdmin } from "./tournament-admin-access.js";
import { normalizeTournamentLineupType, resolveTournamentTextPatch } from "./tournament-update.js";
import { normalizeTournamentRankColors, resolveTournamentRankColorsPatch } from "./tournament-rank-colors.js";

const source = readFileSync(new URL("./server.js", import.meta.url), "utf8");
function serverFunction(name) {
  const start = source.search(new RegExp(`(?:async )?function ${name}\\(`));
  assert.ok(start >= 0, `Missing ${name}`);
  return source.slice(start, source.indexOf("\n}", start) + 2);
}
const globalAdmin = { id: 1, admin: 1, player_id: "global", association: "UA" };
const tournamentAdmin = { id: 2, admin: 0, player_id: "organizer", association: "FR" };
const captain = { id: 3, admin: 0, player_id: "captain", team_captain: 1, association: "UA" };
const otherCaptain = { id: 4, admin: 0, player_id: "other", team_captain: 1, association: "DE" };

async function api(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve) => db.close(resolve)));
  const run = (sql, params = []) => new Promise((resolve, reject) => db.run(sql, params, function done(error) {
    if (error) reject(error); else resolve(this);
  }));
  const all = (sql, params = []) => new Promise((resolve, reject) => db.all(sql, params, (error, rows) => error ? reject(error) : resolve(rows)));
  await run(source.match(/function ensureTournamentsSchema\(\) \{\s*db.run\(`([\s\S]*?)`/)[1]);
  for (const sql of [
    "CREATE TABLE users (id INTEGER PRIMARY KEY)",
    "INSERT INTO users VALUES (1), (2), (3), (4)",
    `CREATE TABLE tournament_access_users (tournament_entity_type TEXT DEFAULT 'tournament', tournament_id TEXT,
      user_id INTEGER, role TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP, updated_at TEXT DEFAULT CURRENT_TIMESTAMP)`,
    "CREATE TABLE tournament_teams (tournament_id TEXT, team_id TEXT, captain_id TEXT)",
    "CREATE TABLE associations (code TEXT, name TEXT, flag TEXT)",
    `CREATE TABLE challenge_periods (id TEXT, rivals_tournament_id TEXT, name TEXT, logo TEXT, status TEXT,
      play_starts_at TEXT, play_ends_at TEXT)`,
  ]) await run(sql);
  await ensureCategoriesSchema(db);
  await ensureCclSchema(db);
  const handlers = new Map();
  const context = vm.createContext({
    ...classification, db, CCL_DEADLINE_FIELDS, isCclTournament,
    app: Object.fromEntries(["get", "post", "patch"].map((method) => [method, (path, ...callbacks) => handlers.set(`${method} ${path}`, callbacks)])),
    canManageTournamentAccessUsers, requireTournamentAdmin: createRequireTournamentAdmin({ db }),
    normalizeTournamentLineupType, resolveTournamentTextPatch,
    normalizeTournamentRankColors, resolveTournamentRankColorsPatch,
  });
  for (const name of ["TOURNAMENT_ACCESS_TYPES", "TOURNAMENT_TYPES", "STANDINGS_SCORING", "TEAM_TYPES",
    "TOURNAMENT_ACCESS_ROLES", "TOURNAMENT_PLAYER_HUB_VISIBILITY", "TOURNAMENT_LINEUP_SIZE_TYPES",
    "DEFAULT_TOURNAMENT_TEAM_SIZE", "TOURNAMENT_FORMATS", "TOURNAMENT_STAGE_FORMATS"]) {
    const declaration = source.match(new RegExp(`const ${name} = [\\s\\S]*?;\\n`));
    assert.ok(declaration, `Missing ${name}`);
    vm.runInContext(declaration[0], context);
  }
  for (const name of [
    "dbRunAsync", "dbAllAsync", "dbGetAsync", "normalizeNullableText", "normalizeEntityId", "normalizeUtcTimestamp", "normalizeBooleanInt",
    "normalizePositiveInteger", "normalizeCategoryName", "resolveTournamentCategoryName", "normalizeIntegerOrNull",
    "bountyTprNumber", "requireAdmin", "normalizeTournamentAccessType", "normalizeTournamentType", "normalizeTeamType", "isValidTeamType",
    "normalizeStandingsScoring", "normalizeTournamentPlayerHubVisibility", "normalizeTournamentCategory", "resolveTournamentCategory",
    "normalizeTournamentAccessRole", "normalizeTournamentAccessUsers", "normalizeTournamentLineupSizeType", "normalizeTournamentLineupSize",
    "isValidTournamentTeamSize", "normalizeTournamentTeamSize", "normalizeTournamentFormat", "normalizeTournamentStageFormat",
    "normalizeTournamentStageSettings", "getTournamentLookupVariants", "buildTournamentLookupWhereClause", "getTournamentAccessUserId",
    "getTournamentAccessProfileId", "normalizeTournamentCaptainTeamIds", "validateTournamentAccessUserIds", "replaceTournamentAccessUsers",
    "loadTournamentAccessForUser", "loadTournamentAccessForUserWithCcl", "resolveCclCaptainAssociationId",
    "resolveAssociationId", "cclCaptainSlotSql", "cclTournamentSql", "cclParticipantSql", "supportsTournamentAccessUsers", "pickCclDeadlines", "resolveCclDeadlinesInput", "loadTournamentRowById", "canClosedTournamentCaptainAccessMatch", "resolveChallengePeriodRivalsTournamentId",
  ]) vm.runInContext(serverFunction(name), context);
  const start = source.indexOf('app.get("/tournaments",');
  vm.runInContext(source.slice(start, source.indexOf('app.get("/tournament-teams",', start)), context);
  await run(source.match(/CREATE TABLE matches \([\s\S]*?\n        \);/)[0]);
  const matchesStart = source.indexOf('app.get("/matches",');
  vm.runInContext(source.slice(matchesStart, source.indexOf("function publicMainPageMatchesHandler(", matchesStart)), context);

  const request = (method, { id, body = {}, user = globalAdmin, query = {} } = {}) => new Promise((resolve, reject) => {
    const callbacks = handlers.get(`${method} /tournaments${id === undefined ? "" : "/:id"}`);
    let index = 0;
    let status = 200;
    const req = { user, body, params: { id }, query };
    const res = { status(code) { status = code; return this; }, json(data) { resolve({ status, data: JSON.parse(JSON.stringify(data)) }); } };
    const next = (error) => {
      if (error) return reject(error);
      try { Promise.resolve(callbacks[index++](req, res, next)).catch(reject); } catch (error) { reject(error); }
    };
    next();
  });
  const access = (id, user) => new Promise((resolve, reject) => context.loadTournamentAccessForUser(id, user, (error, row) => error ? reject(error) : resolve(row)));
  const matches = (user, query = {}) => new Promise((resolve, reject) => {
    const [handler] = handlers.get("get /matches");
    try {
      Promise.resolve(handler({ user, query }, { json: (data) => resolve(JSON.parse(JSON.stringify(data))) }, reject)).catch(reject);
    } catch (error) { reject(error); }
  });
  return { db, run, all, context, request, access, matches };
}

test("new tournament API writes only canonical pairs and derives compatible response fields", async (t) => {
  const { request, all } = await api(t);
  for (const category of classification.TOURNAMENT_CATEGORIES) {
    const result = await request("post", { body: { id: category.name, name: category.name, type: category.type, category: category.name } });
    assert.equal(result.status, 200, JSON.stringify(result));
    assert.equal(result.data.tournament.type, category.type);
    assert.equal(result.data.tournament.category, category.name);
    const [stored] = await all("SELECT * FROM tournaments WHERE id = ?", [category.name]);
    assert.equal(Object.hasOwn(stored, "tournament_type"), false);
    assert.equal(stored.type, category.type);
    assert.equal(Object.hasOwn(stored, "subtype"), false);
    assert.equal(result.data.tournament.access_type, category.mode === "official" ? "Official" : "Friendly");
  }
  for (const body of [
    { type: "Individuals", category: "WTCOC" }, { type: "Teams", category: "Other" },
    { type: "Teams", category: "WTCOC", subtype: "Friendly" },
  ]) assert.equal((await request("post", { body: { id: "invalid", name: "Invalid", ...body } })).status, 400);
  assert.equal((await all("SELECT id FROM tournaments WHERE id = 'invalid'")).length, 0);
});

test("existing forms and legacy National remain compatible with the new API", async (t) => {
  const { request } = await api(t);
  for (const [id, body, type, category] of [
    ["cup", { tournament_type: "Teams", team_type: "National", category: "ETCOC", subtype: "Official" }, "Teams", "ETCOC"],
    ["Friendly-Matches", { tournament_type: "Teams", team_type: "National", category: "", subtype: "Friendly" }, "Teams", "Friendly"],
    ["national", { tournament_type: "National", category: null }, "Individuals", "National"],
    ["king", { tournament_type: "Individuals", category: "KoC", subtype: null }, "Individuals", "King of Carcassonne"],
  ]) {
    const result = await request("post", { body: { id, name: id, ...body } });
    assert.equal(result.status, 200, JSON.stringify(result));
    assert.equal(result.data.tournament.type, type);
    assert.equal(result.data.tournament.category, category);
  }
});

test("all ten migrated tournaments expose their classification and visibility without rewriting rows", async (t) => {
  const { request, run, all } = await api(t);
  const fixtures = [
    ["Asian-Cup-2026", "Teams", "Asian Cup", "Official", "Hidden"],
    ["CCL-2027-Q", "Individuals", "CCL", null, "Visible"],
    ["Copa-America-2026", "Teams", "Copa America", "Official", "Hidden"],
    ["ETCOC-2026", "Teams", "ETCOC", "Official", "Visible"],
    ["Friendly-Matches", "Teams", "Friendly", "Friendly", "Visible"],
    ["Rivals-S1", "Individuals", "Rivals", null, "Hidden"],
    ["KoC", "Individuals", "King of Carcassonne", null, "Hidden"],
    ["MSO-2026", "Individuals", "MSO", null, "Hidden"],
    ["WTCOC-2025", "Teams", "WTCOC", "Official", "Hidden"],
    ["WTCOC-2026", "Teams", "WTCOC", "Official", "Hidden"],
  ];
  // IDs other than Friendly-Matches are fixture IDs, to be checked against the live DB in stage 4.
  for (const [id, type, category, subtype, visibility] of fixtures) {
    await run(`INSERT INTO tournaments (id, name, type, category, player_hub_visibility)
      VALUES (?, ?, ?, ?, ?)`, [id, id, type, category, visibility]);
  }
  const before = await all("SELECT * FROM tournaments ORDER BY id");
  const result = await request("get");
  assert.equal(result.data.tournaments.length, 10);
  for (const [id, type, category, , visibility] of fixtures) {
    const row = result.data.tournaments.find((row) => row.id === id);
    assert.equal(row.type, type);
    assert.equal(row.category, category || "Friendly");
    assert.equal(row.player_hub_visibility, visibility);
  }
  assert.deepEqual(await all("SELECT * FROM tournaments ORDER BY id"), before);
});

test("access assignments survive API responses and old-form saves for hyphenated and uppercase IDs", async (t) => {
  const { request, all } = await api(t);
  for (const id of ["ETCOC-2026", "WTCOC"]) {
    const created = await request("post", { body: { id, name: id, type: "Teams", category: "ETCOC",
      access_users: [{ user_id: 2, role: "admin" }] } });
    assert.equal(created.status, 200, JSON.stringify(created));
    assert.deepEqual(created.data.tournament.access_user_ids, [2]);
    const updated = await request("patch", { id, body: created.data.tournament });
    assert.equal(updated.status, 200, JSON.stringify(updated));
    assert.deepEqual(updated.data.tournament.access_user_ids, [2]);
    assert.equal((await all("SELECT user_id FROM tournament_access_users WHERE tournament_id = ?", [id]))[0].user_id, 2);
  }
});

test("partial updates preserve access assignments and every omitted setting", async (t) => {
  const { request, run, all } = await api(t);
  const created = await request("post", { body: {
    id: "cup", name: "Cup", type: "Teams", category: "ETCOC", short_title: "Short", logo: "logo", link: "link",
    player_hub_visibility: "Hidden", lineup_type: "Blind", lineup_size_type: 1, lineup_size: 5, team_size: 12,
    about: "About", rules: "Rules", is_test: true, ranking: false,
    access_users: [{ user_id: 2, role: "admin" }, { user_id: 3, role: "captain" }],
  } });
  assert.equal(created.status, 200, JSON.stringify(created));
  await run("INSERT INTO tournament_teams VALUES ('cup', 'UA', 'captain')");
  const [before] = await all("SELECT * FROM tournaments WHERE id = 'cup'");
  const roles = await all("SELECT * FROM tournament_access_users ORDER BY user_id");
  for (const user of [globalAdmin, tournamentAdmin]) {
    const result = await request("patch", { id: "cup", user, body: { name: "Renamed" } });
    assert.equal(result.status, 200, JSON.stringify(result));
    const [after] = await all("SELECT * FROM tournaments WHERE id = 'cup'");
    assert.deepEqual(after, { ...before, name: "Renamed", updated_at: after.updated_at });
    assert.deepEqual(await all("SELECT * FROM tournament_access_users ORDER BY user_id"), roles);
  }
  assert.equal((await request("patch", { id: "cup", user: captain, body: { name: "Forbidden" } })).status, 403);
  assert.equal((await request("patch", { id: "cup", user: tournamentAdmin, body: { access_users: [] } })).status, 403);
  assert.equal((await request("patch", { id: "cup", body: { type: "Individuals", category: "ETCOC" } })).status, 400);
  const cleared = await request("patch", { id: "cup", body: { access_users: [] } });
  assert.equal(cleared.status, 200);
  assert.equal((await all("SELECT * FROM tournament_access_users")).length, 0);
});

test("official access, assigned captains, Friendly access and hidden visibility use the same classification", async (t) => {
  const { request, access, run, context } = await api(t);
  for (const [id, category, visibility] of [["cup", "ETCOC", "Visible"], ["hidden", "WTCOC", "Hidden"], ["Friendly-Matches", "Friendly", "Visible"]]) {
    await request("post", { body: { id, name: id, type: "Teams", category, player_hub_visibility: visibility,
      access_users: category === "Friendly" ? [] : [{ user_id: 2, role: "admin" }] } });
    if (category !== "Friendly") await run("INSERT INTO tournament_teams VALUES (?, 'UA', 'captain')", [id]);
  }
  // During rollout the old columns may still exist, but must not control access.
  await run("ALTER TABLE tournaments ADD COLUMN subtype TEXT");
  await run("ALTER TABLE tournaments ADD COLUMN access_type TEXT");
  await run("UPDATE tournaments SET subtype = 'Friendly', access_type = 'Friendly' WHERE id = 'cup'");
  assert.equal((await access("cup", globalAdmin)).access_role, "admin");
  assert.equal((await access("cup", tournamentAdmin)).access_role, "admin");
  const own = await access("cup", captain);
  assert.equal(own.has_access, true);
  assert.equal(own.access_role, "captain");
  assert.deepEqual(Array.from(own.captain_team_ids), ["UA"]);
  assert.equal(context.canClosedTournamentCaptainAccessMatch(own, "UA", "UA", "FR"), true);
  assert.equal(context.canClosedTournamentCaptainAccessMatch(own, "UA", "DE", "FR"), false);
  assert.equal((await access("cup", otherCaptain)).has_access, false);
  assert.equal((await access("Friendly-Matches", otherCaptain)).has_access, true);
  const visible = (await request("get", { user: otherCaptain })).data.tournaments;
  assert.deepEqual(visible.map((row) => row.id), ["Friendly-Matches"]);
  const ownVisible = (await request("get", { user: captain })).data.tournaments;
  assert.deepEqual(ownVisible.map((row) => row.id), ["cup", "Friendly-Matches"]);
  assert.equal((await access("hidden", captain)).has_access, true);
});

test("CCL captains get captain access through a slot of their association", async (t) => {
  const { request, access, run } = await api(t);
  const created = await request("post", { body: {
    id: "CCL-2027", name: "CCL 2027", type: "Individuals", category: "CCL",
    ccl_q12_registration_ends_at: "2026-11-11T23:59:00Z", ccl_main_registration_ends_at: "",
  } });
  assert.equal(created.status, 200, JSON.stringify(created));
  assert.equal(created.data.tournament.ccl_q12_registration_ends_at, "2026-11-11T23:59:00.000Z");
  const patched = await request("patch", { id: "CCL-2027", body: { ccl_q3_registration_ends_at: "2026-11-20T23:59:00Z" } });
  assert.equal(patched.data.tournament.ccl_q12_registration_ends_at, "2026-11-11T23:59:00.000Z");
  assert.equal(patched.data.tournament.ccl_q3_registration_ends_at, "2026-11-20T23:59:00.000Z");
  assert.equal((await request("patch", { id: "CCL-2027", body: { ccl_q3_registration_ends_at: "soon" } })).status, 400);

  // This harness keeps a simplified tournament_teams table; foreign keys stay off as on the server.
  await run("PRAGMA foreign_keys = OFF");
  await run("INSERT INTO associations VALUES ('UA', 'Ukraine', NULL), ('DE', 'Germany', NULL)");
  await run("INSERT INTO tournament_players (id, tournament_id, association_id, entry_stage) VALUES ('slot-1', 'CCL-2027', 'UA', 'Q1')");
  const own = await access("ccl-2027", captain);
  assert.equal(own.access_role, "captain");
  assert.deepEqual(Array.from(own.captain_team_ids), ["UA"]);
  assert.equal(own.ccl_q3_registration_ends_at, "2026-11-20T23:59:00.000Z");
  assert.equal((await access("CCL-2027", otherCaptain)).access_role, null);
  assert.equal((await access("CCL-2027", { ...captain, team_captain: 0 })).access_role, null);

  const mine = (await request("get", { user: captain, query: { scope: "my-tournaments" } })).data.tournaments;
  assert.deepEqual(mine.map((row) => [row.id, row.access_role, row.captain_team_ids]), [["CCL-2027", "captain", ["UA"]]]);
  assert.deepEqual((await request("get", { user: otherCaptain, query: { scope: "my-tournaments" } })).data.tournaments, []);

  // Player Hub Tournaments: CCL is open to admins and to players holding a slot, not to everyone.
  assert.deepEqual((await request("get", { user: captain })).data.tournaments, []);
  assert.equal((await access("CCL-2027", otherCaptain)).has_access, false);
  await run("INSERT INTO tournament_players (id, tournament_id, association_id, entry_stage, player_id) VALUES ('slot-2', 'CCL-2027', 'DE', 'Q2', 'other')");
  const participant = await access("CCL-2027", otherCaptain);
  assert.equal(participant.has_access, true);
  assert.equal(participant.ccl_participant, true);
  const listed = (await request("get", { user: otherCaptain })).data.tournaments;
  assert.deepEqual(listed.map((row) => [row.id, row.ccl_participant]), [["CCL-2027", true]]);
  assert.deepEqual(
    (await request("get", { user: tournamentAdmin, query: { scope: "my-tournaments" } })).data.tournaments.map((row) => row.id),
    []
  );
  await request("patch", { id: "CCL-2027", body: { access_users: [{ user_id: 2, role: "admin" }] } });
  assert.deepEqual((await request("get", { user: tournamentAdmin })).data.tournaments.map((row) => row.id), ["CCL-2027"]);
});

test("Rivals patches retain TPR settings and linked Challenges after legacy columns are removed", async (t) => {
  const { request, run, all, context } = await api(t);
  await request("post", { body: { id: "rivals", name: "Rivals", type: "Individuals", category: "Rivals",
    standings_scoring: "bounty_tpr", tpr_target_games: 17, tpr_smoothing: 0.8, tpr_benchmark_percentile: 0.9 } });
  await run("INSERT INTO challenge_periods (id, name, rivals_tournament_id) VALUES ('period', 'Period', 'rivals')");
  const [before] = await all("SELECT * FROM tournaments WHERE id = 'rivals'");
  const updated = await request("patch", { id: "rivals", body: { about: "Updated description" } });
  assert.equal(updated.status, 200, JSON.stringify(updated));
  const [after] = await all("SELECT * FROM tournaments WHERE id = 'rivals'");
  assert.deepEqual(after, { ...before, about: "Updated description", updated_at: after.updated_at });
  assert.equal((await context.resolveChallengePeriodRivalsTournamentId("rivals")).id, "rivals");
  const list = (await request("get")).data.tournaments;
  assert.equal(list.find((row) => row.id === "rivals").challenge_periods[0].id, "period");
});

test("official captains can open View for the same matches returned by the paginated list", async (t) => {
  const { request, run, matches } = await api(t);
  await request("post", { body: { id: "ETCOC-2026", name: "ETCOC", type: "Teams", category: "ETCOC" } });
  // Assignment can differ from the captain's profile association.
  await run("INSERT INTO tournament_teams VALUES ('ETCOC-2026', 'ROU', 'captain')");
  await run(`INSERT INTO matches (id, tournament_id, team_1, team_2, status, time_utc) VALUES
    ('20261011ROUHRV', 'ETCOC-2026', 'ROU', 'HRV', 'Done', '2026-10-11T12:00:00Z'),
    ('other-match', 'ETCOC-2026', 'FR', 'DE', 'Done', '2026-10-11T13:00:00Z')`);
  const query = { tournament_id: "ETCOC-2026", section: "finished", today_start: "2026-10-16T00:00:00Z" };
  const listed = await matches(captain, query);
  assert.deepEqual(listed.matches.map((row) => row.id), ["20261011ROUHRV"]);
  const opened = await matches(captain);
  assert.deepEqual(opened.matches.map((row) => row.id), listed.matches.map((row) => row.id));
  assert.equal((await matches(otherCaptain)).matches.length, 0);
  assert.equal((await matches(otherCaptain, query)).matches.length, 0);
  assert.equal((await matches(globalAdmin)).matches.length, 2);
});

test("View retains legacy access-user captain restrictions and tournament-admin access", async (t) => {
  const { request, run, matches } = await api(t);
  await request("post", { body: { id: "ETCOC-2026", name: "ETCOC", type: "Teams", category: "ETCOC",
    access_users: [{ user_id: 2, role: "admin" }, { user_id: 3, role: "captain" }] } });
  await run(`INSERT INTO matches (id, tournament_id, team_1, team_2, status, time_utc) VALUES
    ('own', 'ETCOC-2026', 'UA', 'FR', 'Done', '2026-10-11T12:00:00Z'),
    ('other', 'ETCOC-2026', 'ROU', 'HRV', 'Done', '2026-10-11T13:00:00Z')`);
  const query = { tournament_id: "ETCOC-2026", section: "finished", today_start: "2026-10-16T00:00:00Z" };
  for (const user of [captain, tournamentAdmin, otherCaptain, globalAdmin]) {
    const opened = (await matches(user)).matches.map((row) => row.id).sort();
    const listed = (await matches(user, query)).matches.map((row) => row.id).sort();
    assert.deepEqual(opened, listed);
    assert.deepEqual(opened, user === captain ? ["own"] : user === otherCaptain ? [] : ["other", "own"]);
  }
});
