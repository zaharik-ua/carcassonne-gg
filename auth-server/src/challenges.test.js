import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";
import {
  CHALLENGE_PLAYER_PERIOD_STATUSES,
  CHALLENGE_RIVALS_PAIR_DUEL_STATUSES,
  DEFAULT_MAX_PENDING_REQUESTS_PER_PLAYER,
  buildChallengeMatchCapacity,
  ensureChallengePeriodConfigurationSchema,
  ensureChallengePeriodPlayersSchema,
  getChallengeFormatDurationMinutes,
  isChallengeMatchSlotStatus,
  isChallengePendingRequestLimitReached,
  isChallengePlayerRequestEligibleStatus,
  isChallengeRivalsPairDuelStatus,
  resolveMaxMatchesPerPlayer,
  resolveMaxPendingRequestsPerPlayer,
  shouldCloseChallengeRequestsForPlayerStatus,
} from "./challenges.js";

function exec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (error) => (error ? reject(error) : resolve()));
  });
}

function run(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function onRun(error) {
      if (error) reject(error);
      else resolve(this);
    });
  });
}

function all(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || [])));
  });
}

async function createDatabase(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve) => db.close(resolve)));
  await exec(db, "CREATE TABLE challenge_periods (id TEXT PRIMARY KEY)");
  return db;
}

test("normalizes Challenge period limits with stable defaults", () => {
  assert.equal(resolveMaxMatchesPerPlayer(undefined), 1);
  assert.equal(resolveMaxMatchesPerPlayer("2"), 2);
  assert.equal(resolveMaxMatchesPerPlayer(0), null);
  assert.equal(resolveMaxPendingRequestsPerPlayer(undefined), DEFAULT_MAX_PENDING_REQUESTS_PER_PLAYER);
  assert.equal(resolveMaxPendingRequestsPerPlayer("5"), 5);
  assert.equal(resolveMaxPendingRequestsPerPlayer(""), null);
});

test("checks pending-request limits using the configured period value", () => {
  assert.equal(isChallengePendingRequestLimitReached(2, 3), false);
  assert.equal(isChallengePendingRequestLimitReached(3, 3), true);
  assert.equal(isChallengePendingRequestLimitReached(4, 5), false);
  assert.equal(isChallengePendingRequestLimitReached(5, 5), true);
});

test("defines N-match slot statuses and format durations", () => {
  ["Planned", "In progress", "Done", "Error"].forEach((status) => {
    assert.equal(isChallengeMatchSlotStatus(status), true, `${status} must occupy a slot`);
  });
  ["Draft", "Requested new time", "Cancelled"].forEach((status) => {
    assert.equal(isChallengeMatchSlotStatus(status), false, `${status} must not occupy a slot`);
  });
  assert.equal(getChallengeFormatDurationMinutes("Bo3"), 90);
  assert.equal(getChallengeFormatDurationMinutes("Bo5"), 150);
  assert.equal(getChallengeFormatDurationMinutes("Bo7"), null);
});

test("defines Rivals-wide pair blocking statuses", () => {
  assert.deepEqual(
    [...CHALLENGE_RIVALS_PAIR_DUEL_STATUSES],
    ["Draft", "Requested new time", "Planned", "In progress", "Done", "Error"]
  );
  ["Draft", "Requested new time", "Planned", "In progress", "Done", "Error"].forEach((status) => {
    assert.equal(isChallengeRivalsPairDuelStatus(status), true, `${status} must block the pair`);
  });
  ["Cancelled", "", null].forEach((status) => {
    assert.equal(isChallengeRivalsPairDuelStatus(status), false, `${status} must not block the pair`);
  });
});

test("limits player period status to manual participation choices", () => {
  assert.deepEqual(
    [...CHALLENGE_PLAYER_PERIOD_STATUSES],
    ["not_selected", "available", "unavailable"]
  );
  assert.equal(shouldCloseChallengeRequestsForPlayerStatus("available"), false);
  assert.equal(shouldCloseChallengeRequestsForPlayerStatus("not_selected"), false);
  assert.equal(shouldCloseChallengeRequestsForPlayerStatus("unavailable"), true);
  assert.equal(isChallengePlayerRequestEligibleStatus("available"), true);
  assert.equal(isChallengePlayerRequestEligibleStatus("not_selected"), true);
  assert.equal(isChallengePlayerRequestEligibleStatus("unavailable"), false);
});

test("builds derived match capacity without storing counters", () => {
  assert.deepEqual(buildChallengeMatchCapacity(1, 2), {
    matches_count: 1,
    matches_limit: 2,
    matches_remaining: 1,
    is_match_limit_reached: false,
  });
  assert.deepEqual(buildChallengeMatchCapacity(3, 2), {
    matches_count: 3,
    matches_limit: 2,
    matches_remaining: 0,
    is_match_limit_reached: true,
  });
  assert.deepEqual(buildChallengeMatchCapacity(null, null), {
    matches_count: 0,
    matches_limit: 1,
    matches_remaining: 1,
    is_match_limit_reached: false,
  });
});

test("adds the pending-request limit to a legacy period table idempotently", async (t) => {
  const db = await createDatabase(t);
  await ensureChallengePeriodConfigurationSchema(db);
  await ensureChallengePeriodConfigurationSchema(db);

  const columns = await all(db, "PRAGMA table_info(challenge_periods)");
  const limitColumn = columns.find((column) => column.name === "max_pending_requests_per_player");
  assert.ok(limitColumn);
  assert.equal(String(limitColumn.dflt_value).replaceAll("'", ""), "3");
  assert.equal(limitColumn.notnull, 1);

  await run(db, "INSERT INTO challenge_periods (id) VALUES ('default-period')");
  await run(
    db,
    "INSERT INTO challenge_periods (id, max_pending_requests_per_player) VALUES ('custom-period', 5)"
  );
  const rows = await all(
    db,
    "SELECT id, max_pending_requests_per_player FROM challenge_periods ORDER BY id"
  );
  assert.deepEqual(rows, [
    { id: "custom-period", max_pending_requests_per_player: 5 },
    { id: "default-period", max_pending_requests_per_player: 3 },
  ]);
  await assert.rejects(
    run(
      db,
      "INSERT INTO challenge_periods (id, max_pending_requests_per_player) VALUES ('invalid-period', 0)"
    ),
    /CHECK constraint failed/
  );
});

test("migrates legacy player match statuses to manual availability", async (t) => {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve) => db.close(resolve)));
  await exec(
    db,
    `
      CREATE TABLE challenge_periods (id TEXT PRIMARY KEY);
      CREATE TABLE profiles (id TEXT PRIMARY KEY);
      CREATE TABLE duels (id TEXT PRIMARY KEY, status TEXT);
      CREATE TABLE challenge_period_players (
        period_id TEXT NOT NULL,
        player_id TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'not_selected',
        challenge_duel_id TEXT,
        availability_start_1_utc TEXT,
        availability_end_1_utc TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (period_id, player_id),
        CHECK (status IN ('not_selected', 'available', 'unavailable', 'match_scheduled', 'played'))
      );
      INSERT INTO challenge_periods (id) VALUES ('period-1');
      INSERT INTO profiles (id) VALUES ('p1'), ('p2'), ('p3'), ('p4');
      INSERT INTO duels (id, status) VALUES ('duel-1', 'Planned'), ('duel-2', 'Done');
      INSERT INTO challenge_period_players (
        period_id, player_id, status, challenge_duel_id, availability_start_1_utc, availability_end_1_utc
      ) VALUES
        ('period-1', 'p1', 'match_scheduled', 'duel-1', '2026-08-20T10:00:00.000Z', '2026-08-20T12:00:00.000Z'),
        ('period-1', 'p2', 'played', 'duel-2', NULL, NULL),
        ('period-1', 'p3', 'unavailable', NULL, NULL, NULL);
    `
  );

  await ensureChallengePeriodPlayersSchema(db);
  await ensureChallengePeriodPlayersSchema(db);

  const columns = await all(db, "PRAGMA table_info(challenge_period_players)");
  assert.equal(columns.some((column) => column.name === "challenge_duel_id"), false);
  assert.equal(columns.some((column) => column.name === "status_updated_at"), true);
  const players = await all(
    db,
    `
      SELECT player_id, status, availability_start_1_utc, availability_end_1_utc
      FROM challenge_period_players
      ORDER BY player_id
    `
  );
  assert.deepEqual(players, [
    {
      player_id: "p1",
      status: "available",
      availability_start_1_utc: "2026-08-20T10:00:00.000Z",
      availability_end_1_utc: "2026-08-20T12:00:00.000Z",
    },
    {
      player_id: "p2",
      status: "available",
      availability_start_1_utc: null,
      availability_end_1_utc: null,
    },
    {
      player_id: "p3",
      status: "unavailable",
      availability_start_1_utc: null,
      availability_end_1_utc: null,
    },
  ]);
  assert.deepEqual(await all(db, "SELECT id, status FROM duels ORDER BY id"), [
    { id: "duel-1", status: "Planned" },
    { id: "duel-2", status: "Done" },
  ]);
  assert.deepEqual(
    await all(
      db,
      `
        SELECT name
        FROM sqlite_master
        WHERE type = 'index'
          AND name LIKE 'idx_challenge_period_players_%'
        ORDER BY name
      `
    ),
    [
      { name: "idx_challenge_period_players_status" },
      { name: "idx_challenge_period_players_status_updated" },
    ]
  );
  await assert.rejects(
    run(
      db,
      "INSERT INTO challenge_period_players (period_id, player_id, status) VALUES ('period-1', 'p4', 'played')"
    ),
    /CHECK constraint failed/
  );
});
