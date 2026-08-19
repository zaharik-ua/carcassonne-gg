import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";
import {
  DEFAULT_MAX_PENDING_REQUESTS_PER_PLAYER,
  ensureChallengePeriodConfigurationSchema,
  getChallengeFormatDurationMinutes,
  isChallengeMatchSlotStatus,
  isChallengePendingRequestLimitReached,
  resolveMaxMatchesPerPlayer,
  resolveMaxPendingRequestsPerPlayer,
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

test("defines the future N-match slot statuses and format durations", () => {
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
