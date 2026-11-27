import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";

import {
  buildCaseLink,
  buildDashboard,
  buildDuelLink,
  buildPlayerCaseLink,
  getPlanningStatus,
  getPlayerDuelAction,
  loadUnplannedDuels,
  matchesProposedTimeWindow,
  parseDaysBeforeDeadline,
  parsePage,
  parsePlanningStatusFilter,
  parsePageSize,
  parseProposedTimeFilter,
  registerDashboardRoutes,
} from "./dashboard.js";

const NOW = new Date("2026-11-20T12:00:00.000Z");
const USERS = {
  globalAdmin: { id: 1, admin: 1, player_id: "p-admin" },
  tournamentAdmin: { id: 2, admin: 0, player_id: "" },
  player: { id: 3, admin: 0, player_id: "p1" },
  nobody: { id: 4, admin: 0, player_id: "" },
};

const exec = (db, sql) => new Promise((resolve, reject) => db.exec(sql, (error) => (error ? reject(error) : resolve())));
const run = (db, sql, params = []) => new Promise((resolve, reject) => db.run(sql, params, (error) => (error ? reject(error) : resolve())));

async function createDatabase(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve) => db.close(resolve)));
  await exec(db, `
    CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, bga_id TEXT, admin INTEGER, created_at TEXT);
    CREATE TABLE profiles (id TEXT PRIMARY KEY, bga_nickname TEXT, name TEXT, association TEXT, bga_elo INTEGER,
      telegram TEXT, whatsapp TEXT, discord TEXT, contact_email TEXT, deleted_at TEXT);
    CREATE TABLE associations (code TEXT, name TEXT, flag TEXT);
    INSERT INTO associations VALUES ('UA', 'Ukraine', 'ua.png'), ('PL', 'Poland', NULL), ('DE', 'Germany', 'de.png'), ('FR', 'France', 'fr.png');
    CREATE TABLE tournament_players (id INTEGER PRIMARY KEY, tournament_id TEXT, player_id TEXT, association_id TEXT);
    INSERT INTO tournament_players (tournament_id, player_id, association_id) VALUES ('CCL-2027', 'p3', 'FR');
    CREATE TABLE tournaments (id TEXT PRIMARY KEY, name TEXT, short_title TEXT, is_test INTEGER DEFAULT 0, logo TEXT);
    CREATE TABLE tournament_access_users (tournament_entity_type TEXT, tournament_id TEXT, user_id INTEGER, role TEXT);
    CREATE TABLE matches (id TEXT PRIMARY KEY, time_utc TEXT, deleted_at TEXT);
    CREATE TABLE tournament_cases (id INTEGER PRIMARY KEY, case_type TEXT, category TEXT, status TEXT, priority TEXT,
      details TEXT, submitted_by_user_id INTEGER, submitted_by_player_id TEXT, resolution TEXT, resolved_at TEXT,
      responsible_user_id INTEGER, reported_player_id TEXT, match_id TEXT,
      duel_id TEXT, tournament_id TEXT, challenge_period_id TEXT, created_at TEXT, deleted_at TEXT);
    INSERT INTO tournament_cases (id, case_type, category, status, priority, details, submitted_by_player_id,
      responsible_user_id, duel_id, tournament_id, challenge_period_id, created_at, deleted_at) VALUES
      (1, 'complaint', 'no_show', 'open', 'normal', 'Player two did not show up', 'p1', NULL, 'error', 'WTCOC', NULL, '2026-11-10 10:00:00', NULL),
      (2, 'problem', NULL, 'in_progress', 'urgent', 'Wrong result', 'p2', 1, NULL, 'OTHER', NULL, '2026-11-18 10:00:00', NULL),
      (3, 'complaint', 'no_show', 'open', 'normal', 'Challenge no-show', 'p3', NULL, NULL, NULL, 'cp1', '2026-11-12 10:00:00', NULL),
      (4, 'request', NULL, 'resolved', 'high', 'Resolved', 'p1', NULL, NULL, 'WTCOC', NULL, '2026-11-01 10:00:00', NULL),
      (5, 'other', NULL, 'open', 'high', 'Deleted', 'p1', NULL, NULL, 'WTCOC', NULL, '2026-11-01 10:00:00', '2026-11-02');
    UPDATE tournament_cases SET resolution = 'Fixed', resolved_at = '2026-11-03 10:00:00' WHERE id = 4;
    UPDATE tournament_cases SET reported_player_id = 'p2' WHERE id = 1;
    CREATE TABLE news (id INTEGER PRIMARY KEY, title TEXT, status TEXT, is_proposal INTEGER, tournament_id TEXT,
      created_by TEXT, created_at TEXT, deleted_at TEXT);
    CREATE TABLE duels (id TEXT PRIMARY KEY, tournament_id TEXT, match_id TEXT, challenge_period_id TEXT, group_id TEXT,
      bracket_no INTEGER, stage TEXT, group_name TEXT, round_name TEXT, duel_format TEXT, status TEXT, time_utc TEXT,
      deadline_utc TEXT, proposed_time_status TEXT, proposed_time_utc TEXT, proposed_time_by_player_id TEXT,
      player_1_id TEXT, player_2_id TEXT, results_last_error TEXT, results_checked_at TEXT, is_test INTEGER DEFAULT 0,
      deleted_at TEXT, updated_at TEXT);

    INSERT INTO users VALUES
      (1, 'Admin', 'p-admin', 1, '2026-01-01 10:00:00'),
      (2, 'TA', NULL, 0, '2026-11-01 10:00:00'),
      (3, 'Player One', 'p1', 0, '2026-11-15 10:00:00'),
      (4, 'No profile', NULL, 0, '2026-11-18 10:00:00'),
      (5, 'Old', 'p2', 0, '2026-09-01 10:00:00'),
      (6, 'Two weeks ago', 'p3', 0, '2026-11-06 10:00:00');
    INSERT INTO profiles (id, bga_nickname, name, association, bga_elo, telegram) VALUES
      ('p-admin', 'admin', 'Admin', 'UA', 300, NULL),
      ('p1', 'one', 'Player One', 'UA', 250, '@one'),
      ('p2', 'two', 'Player Two', 'PL', 260, NULL),
      ('p3', 'three', 'Player Three', 'DE', 240, NULL);
    INSERT INTO tournaments (id, name, short_title, is_test, logo) VALUES
      ('CCL-2027', 'CCL 2027', 'CCL', 0, 'ccl.png'), ('WTCOC', 'WTCOC 2026', NULL, 0, NULL), ('OTHER', 'Other', NULL, 0, NULL);
    INSERT INTO tournament_access_users VALUES
      ('tournament', 'ccl-2027', 2, 'admin'),
      ('tournament', 'WTCOC', 2, 'admin'),
      ('tournament', 'OTHER', 2, 'captain'),
      ('tournament', 'CCL-2027', 1, 'admin');
    INSERT INTO matches VALUES ('M1', NULL, NULL), ('M2', '2026-11-25T18:00:00.000Z', NULL);
    INSERT INTO news (id, title, status, is_proposal, created_by, created_at, deleted_at) VALUES
      (1, 'Proposal', 'Draft', 1, 'p1', '2026-11-10 10:00:00', NULL),
      (2, 'Editor draft', 'Draft', 0, 'p-admin', '2026-11-10 10:00:00', NULL),
      (3, 'Published proposal', 'Published', 1, 'p1', '2026-11-10 10:00:00', NULL),
      (4, 'Deleted proposal', 'Draft', 1, 'p1', '2026-11-10 10:00:00', '2026-11-11');
  `);
  const duel = (values) => run(
    db,
    `INSERT INTO duels (id, tournament_id, match_id, group_id, bracket_no, round_name, status, time_utc, deadline_utc,
      proposed_time_status, proposed_time_utc, proposed_time_by_player_id, player_1_id, player_2_id, results_last_error,
      is_test, deleted_at)
     VALUES ($id, $tournament_id, $match_id, $group_id, $bracket_no, $round_name, $status, $time_utc, $deadline_utc,
      $proposed_time_status, $proposed_time_utc, $proposed_by, $p1, $p2, $error, $is_test, $deleted_at)`,
    {
      $id: values.id, $tournament_id: values.tournament_id ?? "CCL-2027", $match_id: values.match_id ?? null,
      $group_id: values.group_id === undefined ? "CCL-2027|Q3|X" : values.group_id, $bracket_no: values.bracket_no ?? 1,
      $round_name: values.round_name ?? "Round 1", $status: values.status ?? "Draft", $time_utc: values.time_utc ?? null,
      $deadline_utc: values.deadline_utc === undefined ? "2026-11-22T23:59:00.000Z" : values.deadline_utc,
      $proposed_time_status: values.proposed_time_status ?? null, $proposed_time_utc: values.proposed_time_utc ?? null,
      $proposed_by: values.proposed_by ?? null,
      $p1: values.p1 === undefined ? "p1" : values.p1, $p2: values.p2 === undefined ? "p2" : values.p2,
      $error: values.error ?? null, $is_test: values.is_test ?? 0, $deleted_at: values.deleted_at ?? null,
    }
  );
  await duel({ id: "no-proposal", bracket_no: 1 });
  await duel({ id: "pending", bracket_no: 2, proposed_time_status: "pending", proposed_time_utc: "2026-11-20T15:00:00.000Z", proposed_by: "p3", p2: "p3" });
  await duel({ id: "change", bracket_no: 3, time_utc: "2026-11-21T18:00:00.000Z", proposed_time_status: "change_pending",
    proposed_time_utc: "2026-11-20T10:00:00.000Z", proposed_by: "p1" });
  await duel({ id: "passed", deadline_utc: "2026-11-15T23:59:00.000Z" });
  await duel({ id: "planned", time_utc: "2026-11-21T18:00:00.000Z", status: "Planned", proposed_time_status: "accepted" });
  await duel({ id: "far", deadline_utc: "2026-12-06T23:59:00.000Z" });
  await duel({ id: "no-opponent", p2: null });
  await duel({ id: "done", status: "Done" });
  await duel({ id: "test", is_test: 1 });
  await duel({ id: "deleted", deleted_at: "2026-11-19" });
  await duel({ id: "team-duel", tournament_id: "WTCOC", match_id: "M1", group_id: null, deadline_utc: null, status: "Planned" });
  await duel({ id: "team-duel-timed", tournament_id: "WTCOC", match_id: "M2", group_id: null, deadline_utc: null, status: "Planned", p1: "p3" });
  await duel({ id: "error", tournament_id: "WTCOC", match_id: "M1", group_id: null, deadline_utc: null, status: "Error", error: "BGA table not found", p1: "p3", p2: "p2" });
  await duel({ id: "error-other", tournament_id: "OTHER", group_id: null, deadline_utc: null, status: "Error", p1: "p3", p2: "p2" });
  return db;
}

test("days before deadline defaults to 3 and is clamped", () => {
  assert.equal(parseDaysBeforeDeadline(undefined), 5);
  assert.equal(parseDaysBeforeDeadline("7"), 7);
  assert.equal(parseDaysBeforeDeadline("-1"), 5);
  assert.equal(parseDaysBeforeDeadline("abc"), 5);
  assert.equal(parseDaysBeforeDeadline("1000"), 60);
});

test("planning status of an open duel", () => {
  const now = NOW.toISOString();
  assert.equal(getPlanningStatus({ deadline_utc: "2026-11-19T23:59:00.000Z" }, now), "deadline_passed");
  assert.equal(getPlanningStatus({ deadline_utc: "2026-11-22T23:59:00.000Z", proposed_time_status: "pending" }, now), "time_proposed");
  assert.equal(getPlanningStatus({ deadline_utc: "2026-11-22T23:59:00.000Z", proposed_time_status: "change_pending", time_utc: "x" }, now), "change_pending");
  assert.equal(getPlanningStatus({ deadline_utc: "2026-11-22T23:59:00.000Z" }, now), "no_proposal");
  assert.equal(getPlanningStatus({ deadline_utc: "2026-11-22T23:59:00.000Z", time_utc: "2026-11-21T18:00:00Z" }, now), null);
});

test("player action of a duel", () => {
  const now = NOW.toISOString();
  assert.equal(getPlayerDuelAction({ proposed_time_status: "pending", proposed_time_by_player_id: "p2" }, "p1", now), "accept_time");
  assert.equal(getPlayerDuelAction({ proposed_time_status: "pending", proposed_time_by_player_id: "p1" }, "p1", now), "waiting_opponent");
  assert.equal(getPlayerDuelAction({}, "p1", now), "propose_time");
  assert.equal(getPlayerDuelAction({ deadline_utc: "2026-11-01T00:00:00Z" }, "p1", now), "deadline_passed");
  assert.equal(getPlayerDuelAction({ status: "Error" }, "p1", now), "result_error");
  assert.equal(getPlayerDuelAction({ match_id: "M1" }, "p1", now), "time_not_set");
  assert.equal(getPlayerDuelAction({ match_id: "M2", match_time_utc: "2026-11-25T18:00:00Z" }, "p1", now), "planned");
});

test("duel links open the place where the duel is managed", () => {
  assert.equal(buildDuelLink({ tournament_id: "CCL-2027" }), "/player-hub/tournaments?id=CCL-2027");
  assert.equal(buildDuelLink({ id: "D1", tournament_id: "CCL-2027", group_id: "G1" }), "/player-hub/tournaments?id=CCL-2027");
  assert.equal(buildDuelLink({ id: "D1", tournament_id: "CCL-2027", group_id: "G1" }, { adminPage: true }), "/player-hub/tournaments?id=CCL-2027&edit=1&match_id=D1");
  assert.equal(buildDuelLink({ tournament_id: "WTCOC", match_id: "M1" }), "/player-hub/tournaments?id=WTCOC&edit=1&match_id=M1");
  assert.equal(buildDuelLink({ tournament_id: "RIVALS", challenge_period_id: "cp1" }), "/player-hub/challenges");
  assert.equal(buildDuelLink({}), null);
});

test("global admin sees global blocks and only the tournaments they were added to", async (t) => {
  const db = await createDatabase(t);
  const dashboard = await buildDashboard(db, USERS.globalAdmin, { now: NOW });
  assert.deepEqual(dashboard.roles, { global_admin: true, tournament_admin: true, player: true });
  assert.deepEqual(dashboard.tournaments.map((item) => item.id), ["CCL-2027"]);
  assert.deepEqual(dashboard.blocks.news_drafts.items.map((item) => item.title), ["Proposal"]);
  assert.equal(dashboard.blocks.news_drafts.items[0].author.name, "one");
  assert.equal(dashboard.blocks.news_drafts.items[0].days_waiting, 10);
  assert.equal(dashboard.blocks.news_drafts.items[0].link, "/player-hub/news?edit=1&news_id=1");
  // Users with a linked profile created in the last 7 days, newest first.
  assert.deepEqual(dashboard.blocks.new_users.items.map((item) => item.bga_nickname), ["one"]);
  assert.equal(dashboard.blocks.new_users.days, 7);
  assert.equal(dashboard.blocks.new_users.items[0].association_flag, "ua.png");
  assert.equal(dashboard.blocks.new_users.page, 1);
  assert.equal(dashboard.blocks.new_users.total_pages, 1);
  // Open cases of every tournament, not only the ones the admin was added to: most urgent, then oldest.
  const cases = dashboard.blocks.cases;
  assert.deepEqual(cases.items.map((item) => item.id), [2, 1, 3]);
  assert.equal(cases.count, 3);
  assert.equal(cases.items[0].responsible, "Admin");
  assert.equal(cases.items[1].submitted_by.name, "one");
  // The player who did not show up, for admins.
  assert.equal(cases.items[1].reported_player.id, "p2");
  assert.equal(cases.items[0].reported_player, null);
  assert.equal(cases.items[1].details, "Player two did not show up");
  assert.equal(cases.items[1].days_open, 10);
  assert.equal(cases.items[1].link, "/player-hub/tournaments?id=WTCOC&edit=1&match_id=M1");
  assert.equal(cases.items[2].link, "/player-hub/challenges");
  assert.deepEqual(dashboard.blocks.error_duels.items, []);
});

test("global admin cases follow the tournament filter, even for tournaments they do not administer", async (t) => {
  const db = await createDatabase(t);
  const other = await buildDashboard(db, USERS.globalAdmin, { now: NOW, query: { tournament_id: "other" } });
  assert.deepEqual(other.blocks.cases.items.map((item) => item.id), [2]);
});

test("case links", () => {
  assert.equal(buildCaseLink({ tournament_id: "WTCOC", match_id: "M1" }), "/player-hub/tournaments?id=WTCOC&edit=1&match_id=M1");
  assert.equal(buildCaseLink({ tournament_id: "CCL-2027" }), "/player-hub/tournaments?id=CCL-2027");
  assert.equal(buildCaseLink({ challenge_period_id: "cp1", tournament_id: "RIVALS" }), "/player-hub/challenges");
  assert.equal(buildCaseLink({}), null);
});

test("tournament admin: unplanned duels with a deadline and error duels of their tournaments", async (t) => {
  const db = await createDatabase(t);
  const dashboard = await buildDashboard(db, USERS.tournamentAdmin, { now: NOW });
  assert.deepEqual(dashboard.roles, { global_admin: false, tournament_admin: true, player: false });
  assert.equal(dashboard.blocks.news_drafts, undefined);
  // Open cases of their tournaments only (WTCOC, CCL-2027): not OTHER, not resolved or deleted ones.
  assert.deepEqual(dashboard.blocks.cases.items.map((item) => item.id), [1]);
  assert.equal(dashboard.blocks.my_duels, undefined);
  assert.deepEqual(dashboard.tournaments.map((item) => item.id).sort(), ["WTCOC", "ccl-2027"]);

  const unplanned = dashboard.blocks.unplanned_duels;
  assert.deepEqual(
    unplanned.items.map((item) => [item.id, item.planning_status]),
    [["passed", "deadline_passed"], ["no-proposal", "no_proposal"], ["pending", "time_proposed"], ["change", "change_pending"]]
  );
  assert.equal(unplanned.count, 4);
  const pending = unplanned.items.find((item) => item.id === "pending");
  assert.equal(pending.proposed_by_player_id, "p3");
  assert.equal(pending.days_to_deadline, 2);
  assert.equal(pending.tournament.name, "CCL");
  assert.equal(pending.tournament.logo, "ccl.png");
  assert.deepEqual(pending.player_1_contacts, { telegram: "@one" });
  assert.equal(pending.link, "/player-hub/tournaments?id=CCL-2027&edit=1&match_id=pending");
  assert.equal(pending.player_1.association_flag, "ua.png");
  assert.equal(pending.player_1.has_user, true);
  // The slot association (FR) wins over the profile association (DE).
  assert.equal(pending.player_2.association_flag, "fr.png");
  assert.equal(unplanned.items.find((item) => item.id === "no-proposal").player_2.association_flag, null);
  assert.deepEqual(unplanned.status_counts, { deadline_passed: 1, no_proposal: 1, time_proposed: 1, change_pending: 1 });
  // "change" has a proposed time (10:00) before NOW (12:00).
  assert.deepEqual(unplanned.metrics, { total: 4, deadline_passed: 1, no_proposal: 1, time_proposed: 1, proposed_time_passed: 1 });
  // p2 without a site user.
  await run(db, "UPDATE users SET bga_id = NULL WHERE bga_id = 'p2'");
  const withoutUser = await buildDashboard(db, USERS.tournamentAdmin, { now: NOW });
  const noProposal = withoutUser.blocks.unplanned_duels.items.find((item) => item.id === "no-proposal");
  assert.equal(noProposal.player_1.has_user, true);
  assert.equal(noProposal.player_2.has_user, false);

  assert.deepEqual(dashboard.blocks.error_duels.items.map((item) => item.id), ["error"]);
  assert.equal(dashboard.blocks.error_duels.items[0].error, "BGA table not found");
  assert.equal(dashboard.blocks.error_duels.items[0].link, "/player-hub/tournaments?id=WTCOC&edit=1&match_id=M1");
});

test("unplanned duel filters: planning status and proposed time", () => {
  assert.equal(parsePlanningStatusFilter("time_proposed"), "time_proposed");
  assert.equal(parsePlanningStatusFilter("planned"), null);
  assert.equal(parseProposedTimeFilter("12H"), "12h");
  assert.equal(parseProposedTimeFilter("2h"), null);
  const now = NOW.toISOString();
  const proposal = (time) => ({ proposed_time_status: "pending", proposed_time_utc: time });
  assert.equal(matchesProposedTimeWindow({}, null, now), true);
  assert.equal(matchesProposedTimeWindow({}, "4h", now), false);
  assert.equal(matchesProposedTimeWindow(proposal("2026-11-20T15:00:00.000Z"), "4h", now), true);
  assert.equal(matchesProposedTimeWindow(proposal("2026-11-20T17:00:00.000Z"), "4h", now), false);
  assert.equal(matchesProposedTimeWindow(proposal("2026-11-20T17:00:00.000Z"), "12h", now), true);
  assert.equal(matchesProposedTimeWindow(proposal("2026-11-21T11:00:00.000Z"), "1d", now), true);
  assert.equal(matchesProposedTimeWindow(proposal("2026-11-20T10:00:00.000Z"), "passed", now), true);
  assert.equal(matchesProposedTimeWindow(proposal("2026-11-20T10:00:00.000Z"), "1d", now), false);
  assert.equal(matchesProposedTimeWindow({ proposed_time_status: "accepted", proposed_time_utc: "2026-11-20T10:00:00.000Z" }, "passed", now), false);
});

test("unplanned duels filtered by planning status and proposed time", async (t) => {
  const db = await createDatabase(t);
  const byStatus = await buildDashboard(db, USERS.tournamentAdmin, { now: NOW, query: { planning_status: "no_proposal" } });
  assert.deepEqual(byStatus.blocks.unplanned_duels.items.map((item) => item.id), ["no-proposal"]);
  assert.equal(byStatus.blocks.unplanned_duels.count, 1);
  assert.equal(byStatus.blocks.unplanned_duels.status_counts.time_proposed, 1);
  assert.equal(byStatus.filters.planning_status, "no_proposal");

  const soon = await buildDashboard(db, USERS.tournamentAdmin, { now: NOW, query: { proposed_time: "4h" } });
  assert.deepEqual(soon.blocks.unplanned_duels.items.map((item) => item.id), ["pending"]);
  assert.deepEqual(soon.blocks.unplanned_duels.status_counts, { deadline_passed: 0, no_proposal: 0, time_proposed: 1, change_pending: 0 });
  assert.equal(soon.filters.proposed_time, "4h");

  const passed = await buildDashboard(db, USERS.tournamentAdmin, { now: NOW, query: { proposed_time: "passed", planning_status: "time_proposed" } });
  assert.equal(passed.blocks.unplanned_duels.count, 0);
  assert.equal(passed.blocks.unplanned_duels.status_counts.change_pending, 1);
});

test("unplanned duels are paged", async (t) => {
  assert.equal(parsePageSize("20"), 20);
  assert.equal(parsePageSize("15"), 10);
  assert.equal(parsePage("3"), 3);
  assert.equal(parsePage("0"), 1);
  assert.equal(parsePage("x"), 1);

  const db = await createDatabase(t);
  const first = await buildDashboard(db, USERS.tournamentAdmin, { now: NOW });
  assert.equal(first.blocks.unplanned_duels.page, 1);
  assert.equal(first.blocks.unplanned_duels.page_size, 10);
  assert.equal(first.blocks.unplanned_duels.total_pages, 1);

  const second = await loadUnplannedDuels(db, { tournamentIds: ["CCL-2027"], nowIso: NOW.toISOString(), days: 3, page: 2, pageSize: 2 });
  assert.deepEqual(second.items.map((item) => item.id), ["pending", "change"]);
  assert.equal(second.count, 4);
  assert.equal(second.total_pages, 2);

  // Page 9 of a one-page list shows that page.
  const past = await buildDashboard(db, USERS.tournamentAdmin, { now: NOW, query: { unplanned_page: "9", unplanned_page_size: "50" } });
  assert.equal(past.blocks.unplanned_duels.page, 1);
  assert.equal(past.blocks.unplanned_duels.page_size, 50);
  assert.equal(past.blocks.unplanned_duels.items.length, 4);
});

test("days and tournament filters", async (t) => {
  const db = await createDatabase(t);
  const wide = await buildDashboard(db, USERS.tournamentAdmin, { now: NOW, query: { days_before_deadline: "30" } });
  assert.ok(wide.blocks.unplanned_duels.items.some((item) => item.id === "far"));
  assert.equal(wide.filters.days_before_deadline, 30);

  const onlyWtcoc = await buildDashboard(db, USERS.tournamentAdmin, { now: NOW, query: { tournament_id: "wtcoc" } });
  assert.equal(onlyWtcoc.filters.tournament_id, "WTCOC");
  assert.equal(onlyWtcoc.blocks.unplanned_duels.count, 0);
  assert.equal(onlyWtcoc.blocks.error_duels.count, 1);

  // A tournament the user does not administer shows nothing.
  const foreign = await buildDashboard(db, USERS.tournamentAdmin, { now: NOW, query: { tournament_id: "OTHER" } });
  assert.equal(foreign.blocks.error_duels.count, 0);
  assert.equal(foreign.blocks.cases.count, 0);
  assert.equal(foreign.filters.tournament_id, null);
});

test("player sees their own active duels, actions first", async (t) => {
  const db = await createDatabase(t);
  const dashboard = await buildDashboard(db, USERS.player, { now: NOW });
  assert.deepEqual(dashboard.roles, { global_admin: false, tournament_admin: false, player: true });
  const myDuels = dashboard.blocks.my_duels;
  assert.deepEqual(
    myDuels.items.map((item) => [item.id, item.action]),
    [
      ["pending", "accept_time"],
      ["no-proposal", "propose_time"],
      ["far", "propose_time"],
      ["passed", "deadline_passed"],
      ["change", "waiting_opponent"],
      ["team-duel", "time_not_set"],
      ["planned", "planned"],
    ]
  );
  assert.equal(myDuels.action_count, 4);
  assert.equal(myDuels.items[0].opponent.name, "three");
});

test("player sees the cases they submitted in every tournament, active first", async (t) => {
  const db = await createDatabase(t);
  const dashboard = await buildDashboard(db, USERS.player, { now: NOW });
  const myCases = dashboard.blocks.my_cases;
  // Case 3 was filed by p3, case 5 is deleted.
  assert.deepEqual(myCases.items.map((item) => [item.id, item.status]), [[1, "open"], [4, "resolved"]]);
  assert.equal(myCases.count, 2);
  assert.equal(myCases.active_count, 1);
  assert.equal(myCases.items[0].details, "Player two did not show up");
  assert.equal(myCases.items[0].link, "/player-hub/tournaments?id=WTCOC");
  assert.equal(myCases.items[1].resolution, "Fixed");
  assert.equal(myCases.items[1].resolved_at, "2026-11-03T10:00:00.000Z");
});

test("player case links never open the admin edit mode", () => {
  assert.equal(buildPlayerCaseLink({ tournament_id: "WTCOC", match_id: "M1" }), "/player-hub/tournaments?id=WTCOC");
  assert.equal(buildPlayerCaseLink({ challenge_period_id: "cp1" }), "/player-hub/challenges");
  assert.equal(buildPlayerCaseLink({}), null);
});

test("player without a profile and without admin rights gets no blocks", async (t) => {
  const db = await createDatabase(t);
  const dashboard = await buildDashboard(db, USERS.nobody, { now: NOW });
  assert.deepEqual(dashboard.blocks, {});
});

test("GET /dashboard is registered behind requireAuthenticated", async (t) => {
  const db = await createDatabase(t);
  const routes = new Map();
  const requireAuthenticated = () => {};
  registerDashboardRoutes(
    { get: (path, ...handlers) => routes.set(path, handlers) },
    { db, requireAuthenticated, now: () => NOW, logger: { error() {} } }
  );
  const handlers = routes.get("/dashboard");
  assert.equal(handlers[0], requireAuthenticated);
  const result = {};
  await handlers.at(-1)(
    { user: USERS.player, query: {} },
    { set() {}, status(code) { result.status = code; return this; }, json(body) { result.body = body; return this; } }
  );
  assert.equal(result.status, undefined);
  assert.equal(result.body.ok, true);
  assert.ok(result.body.blocks.my_duels);
});
