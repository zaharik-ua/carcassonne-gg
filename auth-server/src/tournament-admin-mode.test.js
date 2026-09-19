import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";
import sqlite3 from "sqlite3";

const html = readFileSync(new URL("../../gg-html/player-hub/tournaments.html", import.meta.url), "utf8");
const server = readFileSync(new URL("./server.js", import.meta.url), "utf8");
function pageFunction(name) {
  const start = html.indexOf(`  function ${name}(`);
  assert.ok(start >= 0);
  return html.slice(start, html.indexOf("\n  }", start) + 4);
}
const context = vm.createContext({
  normalizeCode: (value) => String(value || "").trim().toUpperCase(),
});
for (const name of ["normalizeTournamentAccessUsers", "normalizeTournamentCaptainTeamIds", "getTournamentAccessState", "getTournamentAdminMode"]) {
  vm.runInContext(pageFunction(name), context);
}
const official = { id: "cup", subtype: "official", access_role: "admin", captain_team_ids: [] };
function mode(tournament, auth = {}, preference) {
  return context.getTournamentAdminMode(context.getTournamentAccessState(tournament, auth), auth, preference);
}

test("tournament and global admins who captain a team default to captain mode", () => {
  for (const isAdminUser of [false, true]) {
    const tournament = { ...official, captain_team_ids: ["TEAM-A"] };
    const auth = { isAdminUser };
    assert.equal(mode(tournament, auth).available, true);
    assert.equal(mode(tournament, auth).enabled, false);
    assert.equal(mode(tournament, auth).locked, false);
    assert.equal(mode(tournament, auth, true).enabled, true);
    assert.equal(mode(tournament, auth, false).enabled, false);
  }
});

test("admins without a captain role cannot disable Admin mode", () => {
  for (const isAdminUser of [false, true]) {
    const state = mode(official, { isAdminUser, captainAssociationCode: "UA" }, false);
    assert.equal(state.enabled, true);
    assert.equal(state.locked, true);
  }
  // Being an association captain does not assign an official tournament team.
  assert.equal(mode(official, { isAdminUser: true, isCaptainUser: true, captainAssociationCode: "UA" }).locked, true);
});

test("captains without admin access have no switch, including a forged preference", () => {
  const state = mode({ ...official, access_role: "captain", captain_team_ids: ["UA"] }, {}, true);
  assert.equal(state.available, false);
  assert.equal(state.enabled, false);
});

test("legacy association captains and friendly tournament captains can switch", () => {
  const auth = { isAdminUser: true, isCaptainUser: true, captainAssociationCode: "UA" };
  assert.equal(mode({ ...official, access_via_access_users: true }, auth).enabled, false);
  assert.equal(mode({ ...official, subtype: "friendly" }, auth).enabled, false);
});

test("switching mode preserves preference and resets editor and pagination", () => {
  const classes = new Set();
  const preferences = new Map();
  const pagination = { calendar: { currentPage: 4 }, finished: { currentPage: 2 } };
  let resetView;
  const ui = vm.createContext({
    adminModeControlEl: { classList: { toggle: (key, hide) => hide ? classes.add(key) : classes.delete(key) } },
    adminModeEl: {}, adminModePreferences: preferences,
    MATCH_SECTION_KEYS: Object.keys(pagination), matchesPaginationState: pagination,
    CAPTAIN_VIEWS: { MATCHES: "matches" }, resetCaptainSubview: (view) => { resetView = view; },
  });
  vm.runInContext(pageFunction("renderTournamentAdminMode"), ui);
  ui.renderTournamentAdminMode({ available: true, enabled: false, locked: false }, "user/cup");
  assert.equal(ui.adminModeEl.checked, false);
  ui.adminModeEl.checked = true;
  ui.adminModeEl.onchange();
  assert.equal(preferences.get("user/cup"), true);
  assert.equal(preferences.has("user/another-cup"), false);
  assert.equal(pagination.calendar.currentPage, 1);
  assert.equal(pagination.finished.currentPage, 1);
  assert.equal(resetView, "matches");
  ui.renderTournamentAdminMode({ available: true, enabled: true, locked: true }, "user/other");
  assert.equal(ui.adminModeEl.disabled, true);
  ui.adminModeEl.onchange();
  assert.equal(preferences.has("user/other"), false);
});

test("team filter applies before counting and paging, including multiple captain teams", async (t) => {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve) => db.close(resolve)));
  await new Promise((resolve, reject) => db.exec(`
    CREATE TABLE matches (id TEXT, tournament_id TEXT, team_1 TEXT, team_2 TEXT, status TEXT, deleted_at TEXT);
    INSERT INTO matches VALUES
      ('a', 'cup', 'UA', 'FR', 'Planned', NULL),
      ('b', 'cup', 'DE', 'UA', 'Planned', NULL),
      ('c', 'cup', 'PL', 'FR', 'Planned', NULL),
      ('d', 'cup', 'DE', 'FR', 'Planned', NULL),
      ('e', 'other', 'UA', 'FR', 'Planned', NULL);
  `, (error) => error ? reject(error) : resolve()));
  const start = server.indexOf('  const baseWhereClauses = ["m.deleted_at IS NULL"];');
  const end = server.indexOf("  const sectionVisibilityParams", start);
  assert.ok(start > 0 && end > start);
  for (const [requestedTeamIds, expected] of [[["UA", "PL"], 3], [["UA"], 2], [[], 0], [null, 4]]) {
    const sqlContext = vm.createContext({ requestedTeamIds, requestedTournamentId: "cup", requestedAssociation: "", requestedStatus: "" });
    const { where, params } = vm.runInContext(`${server.slice(start, end)}\n({where: baseWhereClauses.join(" AND "), params: baseWhereParams})`, sqlContext);
    const all = (sql, values) => new Promise((resolve, reject) => db.all(sql, values, (error, rows) => error ? reject(error) : resolve(rows)));
    const count = await all(`SELECT COUNT(*) AS total FROM matches m WHERE ${where}`, params);
    assert.equal(count[0].total, expected);
    const page = await all(`SELECT id FROM matches m WHERE ${where} ORDER BY id LIMIT 1 OFFSET 1`, params);
    assert.equal(page.length, expected > 1 ? 1 : 0);
    if (requestedTeamIds?.length) assert.equal(page[0].id, "b");
  }
});

test("page initialization switches matches, rosters and editing actions together", async () => {
  const start = html.indexOf("  async function initCaptainPage()");
  const initSource = html.slice(start, html.indexOf('\n  window.addEventListener("gg-auth-state-change"', start));
  for (const globalAdmin of [false, true]) {
    for (const enabled of [false, true]) {
      const tournament = { ...official, captain_team_ids: ["UA"] };
      const rows = [
        { id: "own", tournament_id: "cup", team_1: "UA", team_2: "FR" },
        { id: "other", tournament_id: "cup", team_1: "PL", team_2: "DE" },
      ];
      const payloads = {
        me: { authenticated: true, user: { id: "user", isAdmin: globalAdmin, profile: { id: "player", association: "UA" } } },
        tournaments: { tournaments: [tournament] },
        teams: { teams: [] }, profiles: { profiles: [] },
        players: { can_access_all_teams: true, tournament_teams: [
          { team_id: "UA", captain_id: "player" }, { team_id: "PL", captain_id: "another" },
        ] },
      };
      const hidden = new Set();
      let rendered;
      const requests = [];
      const ui = vm.createContext({
        ...context,
        captainPageState: { view: "matches", tournamentId: "cup", mode: "list", associationOverride: "" },
        isCaptainPageInitializing: false, captainPageRefreshQueued: false,
        CAPTAIN_VIEWS: { MATCHES: "matches" },
        ME_URL: "me", TEAMS_URL: "teams", TOURNAMENTS_URL: "tournaments", PROFILES_PUBLIC_URL: "profiles", TOURNAMENT_PLAYERS_URL: "players",
        fetch: async (url) => ({ ok: true, json: async () => payloads[String(url).split("?")[0]] }),
        getCurrentUserAuthId: (user) => user.id, getCurrentUserProfileId: (user) => user.profile.id,
        setMatchesLoading() {}, renderCaptainLoadingState() {}, setPageHeading() {},
        getImmediatePageHeading: () => "Cup", renderTournamentAdminMode() {},
        adminModePreferences: new Map([[JSON.stringify(["user", "cup"]), enabled]]),
        MATCH_SECTION_KEYS: ["calendar"], matchesPaginationState: { calendar: { currentPage: 1, pageSize: 10 } },
        ADMIN_MATCH_FILTER_ERROR: "__ERROR__",
        fetchMatchesSection: async (_section, options) => {
          requests.push(options);
          return options.teamIds ? rows.filter((row) => options.teamIds.includes(row.team_1)) : rows;
        },
        readMatchTeam1: (match) => match.team_1, readMatchTeam2: (match) => match.team_2,
        addButtonEl: { classList: { add: (name) => hidden.add(name), remove: (name) => hidden.delete(name) } },
        hintEl: {}, countEl: {}, contentEl: {},
        loadLineupsByMatchIds: async () => new Map(),
        renderMatches: (matches, _teams, _lineups, _profiles, options) => { rendered = { matches, options }; },
      });
      vm.runInContext(pageFunction("canOfficialCaptainAccessMatch"), ui);
      vm.runInContext(initSource, ui);
      await ui.initCaptainPage();
      assert.ok(rendered, `page should render: ${ui.contentEl.textContent || ""}`);
      assert.deepEqual(Array.from(rendered.matches, (match) => match.id), enabled ? ["own", "other"] : ["own"]);
      assert.deepEqual(Array.from(rendered.options.tournamentTeams, (team) => team.team_id), enabled ? ["UA", "PL"] : ["UA"]);
      assert.equal(rendered.options.lineupsOnly, !enabled);
      assert.equal(rendered.options.canEditRegisteredPlayers, enabled);
      assert.equal(hidden.has("cp-hidden"), !enabled);
      assert.equal(Array.isArray(requests[0].teamIds), !enabled);
    }
  }
});
