import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import sqlite3 from "sqlite3";
import {
  CHALLENGE_PLAYER_PERIOD_STATUSES,
  CHALLENGE_RIVALS_PAIR_DUEL_STATUSES,
  DEFAULT_MAX_PENDING_REQUESTS_PER_PLAYER,
  buildChallengeMatchCapacity,
  closeChallengePendingRequestsAfterAccept,
  ensureChallengePeriodConfigurationSchema,
  ensureChallengePeriodPlayersSchema,
  getChallengeFormatDurationMinutes,
  isChallengeMatchSlotStatus,
  isChallengePendingRequestLimitReached,
  isChallengePlayerRequestEligibleStatus,
  isChallengeRivalsPairDuelStatus,
  loadChallengeBlockingRivalsPairDuel,
  loadChallengeMatchCapacities,
  loadChallengeScheduleConflict,
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

function get(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (error, row) => (error ? reject(error) : resolve(row || null)));
  });
}

function close(db) {
  return new Promise((resolve, reject) => {
    db.close((error) => (error ? reject(error) : resolve()));
  });
}

async function ensureChallengeAcceptanceTestSchema(db) {
  await exec(
    db,
    `
      CREATE TABLE challenge_periods (
        id TEXT PRIMARY KEY,
        rivals_tournament_id TEXT,
        max_matches_per_player INTEGER NOT NULL
      );
      CREATE TABLE challenge_requests (
        id TEXT PRIMARY KEY,
        period_id TEXT NOT NULL,
        player_1_id TEXT NOT NULL,
        player_2_id TEXT NOT NULL,
        status TEXT NOT NULL,
        updated_at TEXT
      );
      CREATE TABLE duels (
        id TEXT PRIMARY KEY,
        challenge_period_id TEXT NOT NULL,
        challenge_request_id TEXT,
        source_type TEXT,
        player_1_id TEXT NOT NULL,
        player_2_id TEXT NOT NULL,
        status TEXT NOT NULL,
        time_utc TEXT,
        duel_format TEXT,
        deleted_at TEXT,
        cancelled_by_player_id TEXT,
        cancellation_reason TEXT,
        cancelled_at TEXT,
        updated_by TEXT,
        updated_at TEXT
      );
    `
  );
}

async function createChallengeAcceptanceDatabase(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => close(db));
  await ensureChallengeAcceptanceTestSchema(db);
  return db;
}

async function createConcurrentChallengeAcceptanceDatabase(t) {
  const directory = await mkdtemp(join(tmpdir(), "challenge-acceptance-"));
  const databasePath = join(directory, "acceptance.sqlite");
  const firstDb = new sqlite3.Database(databasePath);
  firstDb.configure("busyTimeout", 3000);
  await ensureChallengeAcceptanceTestSchema(firstDb);
  const secondDb = new sqlite3.Database(databasePath);
  secondDb.configure("busyTimeout", 3000);
  t.after(async () => {
    await Promise.all([close(firstDb), close(secondDb)]);
    await rm(directory, { recursive: true, force: true });
  });
  return [firstDb, secondDb];
}

async function acceptChallengeMatchWithGuards(db, options) {
  let transactionStarted = false;
  try {
    await run(db, "BEGIN IMMEDIATE TRANSACTION");
    transactionStarted = true;
    const capacities = await loadChallengeMatchCapacities(db, {
      periodId: options.periodId,
      playerIds: [options.player1Id, options.player2Id],
      maxMatchesPerPlayer: options.maxMatchesPerPlayer,
    });
    if (
      capacities[options.player1Id]?.is_match_limit_reached
      || capacities[options.player2Id]?.is_match_limit_reached
    ) {
      const error = new Error("match_limit_reached");
      error.code = "match_limit_reached";
      throw error;
    }
    const blockingPairDuel = await loadChallengeBlockingRivalsPairDuel(db, {
      periodId: options.periodId,
      rivalsTournamentId: options.rivalsTournamentId,
      player1Id: options.player1Id,
      player2Id: options.player2Id,
    });
    if (blockingPairDuel) {
      const error = new Error("rivals_pair_used");
      error.code = "rivals_pair_used";
      throw error;
    }
    if (options.timeUtc && options.format) {
      const scheduleConflict = await loadChallengeScheduleConflict(db, {
        periodId: options.periodId,
        playerIds: [options.player1Id, options.player2Id],
        timeUtc: options.timeUtc,
        format: options.format,
        excludeDuelId: options.excludeDuelId,
      });
      if (scheduleConflict) {
        const error = new Error("schedule_conflict");
        error.code = "schedule_conflict";
        error.conflict = scheduleConflict;
        throw error;
      }
    }
    await run(
      db,
      `
        INSERT INTO duels (
          id,
          challenge_period_id,
          challenge_request_id,
          source_type,
          player_1_id,
          player_2_id,
          status,
          time_utc,
          duel_format
        )
        VALUES (?, ?, ?, 'challenge', ?, ?, 'Planned', ?, ?)
      `,
      [
        options.duelId,
        options.periodId,
        options.requestId,
        options.player1Id,
        options.player2Id,
        options.timeUtc || null,
        options.format || null,
      ]
    );
    await run(db, "COMMIT");
    transactionStarted = false;
    return options.duelId;
  } catch (error) {
    if (transactionStarted) await run(db, "ROLLBACK").catch(() => {});
    throw error;
  }
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
  assert.equal(getChallengeFormatDurationMinutes("bo5"), 150);
  assert.equal(getChallengeFormatDurationMinutes("Bo7"), null);
});

test("uses the larger format duration on both sides of a mixed-format match", async (t) => {
  const db = await createChallengeAcceptanceDatabase(t);
  await exec(
    db,
    `
      INSERT INTO challenge_periods (id, rivals_tournament_id, max_matches_per_player)
      VALUES ('period-1', 'RIVALS-1', 3);
      INSERT INTO duels (
        id, challenge_period_id, source_type, player_1_id, player_2_id,
        status, time_utc, duel_format
      ) VALUES (
        'existing-bo5', 'period-1', 'challenge', 'B', 'X',
        'Planned', '2026-08-20 10:00:00.000', 'Bo5'
      );
    `
  );

  const afterConflict = await loadChallengeScheduleConflict(db, {
    periodId: "period-1",
    playerIds: ["A", "B"],
    timeUtc: "2026-08-20T12:29:00.000Z",
    format: "Bo3",
  });
  assert.equal(afterConflict?.duel?.id, "existing-bo5");
  assert.deepEqual(afterConflict?.conflicting_player_ids, ["B"]);
  assert.equal(afterConflict?.required_gap_minutes, 150);
  assert.equal(afterConflict?.start_difference_minutes, 149);

  assert.equal(await loadChallengeScheduleConflict(db, {
    periodId: "period-1",
    playerIds: ["A", "B"],
    timeUtc: "2026-08-20T12:30:00.000Z",
    format: "Bo3",
  }), null);
  assert.equal((await loadChallengeScheduleConflict(db, {
    periodId: "period-1",
    playerIds: ["A", "B"],
    timeUtc: "2026-08-20T07:31:00.000Z",
    format: "Bo3",
  }))?.duel?.id, "existing-bo5");
  assert.equal(await loadChallengeScheduleConflict(db, {
    periodId: "period-1",
    playerIds: ["A", "B"],
    timeUtc: "2026-08-20T07:30:00.000Z",
    format: "Bo3",
  }), null);
});

test("checks only active-slot Challenge duels in the same period with valid times", async (t) => {
  const db = await createChallengeAcceptanceDatabase(t);
  await exec(
    db,
    `
      INSERT INTO challenge_periods (id, rivals_tournament_id, max_matches_per_player) VALUES
        ('period-1', 'RIVALS-1', 3),
        ('period-2', 'RIVALS-1', 3);
      INSERT INTO duels (
        id, challenge_period_id, source_type, player_1_id, player_2_id,
        status, time_utc, duel_format, deleted_at
      ) VALUES
        ('other-period', 'period-2', 'challenge', 'A', 'X', 'Planned', '2026-08-20T10:00:00.000Z', 'Bo3', NULL),
        ('draft', 'period-1', 'challenge', 'A', 'X', 'Draft', '2026-08-20T10:00:00.000Z', 'Bo3', NULL),
        ('cancelled', 'period-1', 'challenge', 'B', 'X', 'Cancelled', '2026-08-20T10:00:00.000Z', 'Bo5', NULL),
        ('deleted', 'period-1', 'challenge', 'A', 'X', 'Done', '2026-08-20T10:00:00.000Z', 'Bo3', '2026-08-20T09:00:00.000Z'),
        ('invalid-time', 'period-1', 'challenge', 'B', 'X', 'Error', 'not-a-time', 'Bo5', NULL),
        ('unrelated', 'period-1', 'challenge', 'C', 'X', 'In progress', '2026-08-20T10:00:00.000Z', 'Bo5', NULL);
    `
  );

  const candidate = {
    periodId: "period-1",
    playerIds: ["A", "B"],
    timeUtc: "2026-08-20T10:30:00.000Z",
    format: "Bo3",
  };
  assert.equal(await loadChallengeScheduleConflict(db, candidate), null);

  await run(db, "UPDATE duels SET status = 'Error' WHERE id = 'draft'");
  assert.equal((await loadChallengeScheduleConflict(db, candidate))?.duel?.id, "draft");
});

test("rejects a conflicting accept atomically and allows the exact boundary", async (t) => {
  const db = await createChallengeAcceptanceDatabase(t);
  await exec(
    db,
    `
      INSERT INTO challenge_periods (id, rivals_tournament_id, max_matches_per_player)
      VALUES ('period-1', 'RIVALS-1', 2);
      INSERT INTO duels (
        id, challenge_period_id, source_type, player_1_id, player_2_id,
        status, time_utc, duel_format
      ) VALUES (
        'existing', 'period-1', 'challenge', 'A', 'X',
        'Done', '2026-08-20T10:00:00.000Z', 'Bo5'
      );
    `
  );

  await assert.rejects(
    acceptChallengeMatchWithGuards(db, {
      duelId: "conflicting",
      requestId: "request-conflicting",
      periodId: "period-1",
      rivalsTournamentId: "RIVALS-1",
      player1Id: "A",
      player2Id: "B",
      maxMatchesPerPlayer: 2,
      timeUtc: "2026-08-20T12:29:00.000Z",
      format: "Bo3",
    }),
    (error) => error?.code === "schedule_conflict"
      && error?.conflict?.duel?.id === "existing"
  );
  assert.equal(Number((await get(db, "SELECT COUNT(*) AS count FROM duels"))?.count), 1);

  await acceptChallengeMatchWithGuards(db, {
    duelId: "boundary",
    requestId: "request-boundary",
    periodId: "period-1",
    rivalsTournamentId: "RIVALS-1",
    player1Id: "A",
    player2Id: "B",
    maxMatchesPerPlayer: 2,
    timeUtc: "2026-08-20T12:30:00.000Z",
    format: "Bo3",
  });
  assert.deepEqual(await get(
    db,
    "SELECT id, time_utc, duel_format FROM duels WHERE id = 'boundary'"
  ), {
    id: "boundary",
    time_utc: "2026-08-20T12:30:00.000Z",
    duel_format: "Bo3",
  });
});

test("can exclude the rescheduled duel from its own schedule check", async (t) => {
  const db = await createChallengeAcceptanceDatabase(t);
  await exec(
    db,
    `
      INSERT INTO challenge_periods (id, rivals_tournament_id, max_matches_per_player)
      VALUES ('period-1', 'RIVALS-1', 2);
      INSERT INTO duels (
        id, challenge_period_id, challenge_request_id, source_type,
        player_1_id, player_2_id, status, time_utc, duel_format
      ) VALUES (
        'rescheduled', 'period-1', 'request-1', 'challenge',
        'A', 'B', 'Planned', '2026-08-20T10:00:00.000Z', 'Bo3'
      );
    `
  );

  assert.equal((await loadChallengeScheduleConflict(db, {
    periodId: "period-1",
    playerIds: ["A", "B"],
    timeUtc: "2026-08-20T10:00:00.000Z",
    format: "Bo3",
  }))?.duel?.id, "rescheduled");
  assert.equal(await loadChallengeScheduleConflict(db, {
    periodId: "period-1",
    playerIds: ["A", "B"],
    timeUtc: "2026-08-20T10:00:00.000Z",
    format: "Bo3",
    excludeDuelId: "rescheduled",
  }), null);
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

test("auto-cancels Rivals pair duplicates and only saturated players' other requests", async (t) => {
  const db = await createChallengeAcceptanceDatabase(t);
  await exec(
    db,
    `
      INSERT INTO challenge_periods (id, rivals_tournament_id, max_matches_per_player) VALUES
        ('period-1', 'RIVALS-1', 2),
        ('period-2', 'RIVALS-1', 2),
        ('period-3', 'RIVALS-2', 2);
      INSERT INTO challenge_requests (id, period_id, player_1_id, player_2_id, status) VALUES
        ('accepted', 'period-1', 'A', 'B', 'accepted'),
        ('limit-a', 'period-1', 'A', 'C', 'pending'),
        ('free-b', 'period-1', 'B', 'D', 'pending'),
        ('unrelated', 'period-1', 'C', 'D', 'pending'),
        ('pair-rivals', 'period-2', 'B', 'A', 'pending'),
        ('pair-other-rivals', 'period-3', 'A', 'B', 'pending');
      INSERT INTO duels (
        id,
        challenge_period_id,
        challenge_request_id,
        source_type,
        player_1_id,
        player_2_id,
        status
      ) VALUES
        ('duel-limit-a', 'period-1', 'limit-a', 'challenge', 'A', 'C', 'Requested new time'),
        ('duel-free-b', 'period-1', 'free-b', 'challenge', 'B', 'D', 'Draft'),
        ('duel-unrelated', 'period-1', 'unrelated', 'challenge', 'C', 'D', 'Draft'),
        ('duel-pair-rivals', 'period-2', 'pair-rivals', 'challenge', 'B', 'A', 'Draft'),
        ('duel-pair-other-rivals', 'period-3', 'pair-other-rivals', 'challenge', 'A', 'B', 'Draft');
    `
  );

  const result = await closeChallengePendingRequestsAfterAccept(db, {
    periodId: "period-1",
    rivalsTournamentId: "RIVALS-1",
    acceptedRequestId: "accepted",
    player1Id: "A",
    player2Id: "B",
    saturatedPlayerIds: ["A"],
    actorPlayerId: "B",
  });

  assert.deepEqual(result, {
    auto_cancelled_request_ids: ["limit-a", "pair-rivals"],
    auto_cancelled_request_count: 2,
    cancelled_duel_count: 2,
  });
  assert.deepEqual(
    await all(db, "SELECT id, status FROM challenge_requests ORDER BY id"),
    [
      { id: "accepted", status: "accepted" },
      { id: "free-b", status: "pending" },
      { id: "limit-a", status: "auto_cancelled" },
      { id: "pair-other-rivals", status: "pending" },
      { id: "pair-rivals", status: "auto_cancelled" },
      { id: "unrelated", status: "pending" },
    ]
  );
  assert.deepEqual(
    await all(db, "SELECT id, status FROM duels ORDER BY id"),
    [
      { id: "duel-free-b", status: "Draft" },
      { id: "duel-limit-a", status: "Cancelled" },
      { id: "duel-pair-other-rivals", status: "Draft" },
      { id: "duel-pair-rivals", status: "Cancelled" },
      { id: "duel-unrelated", status: "Draft" },
    ]
  );
});

test("keeps other pending requests open below the limit while cancelling the Rivals pair", async (t) => {
  const db = await createChallengeAcceptanceDatabase(t);
  await exec(
    db,
    `
      INSERT INTO challenge_periods (id, rivals_tournament_id, max_matches_per_player) VALUES
        ('period-1', 'RIVALS-1', 2),
        ('period-2', 'RIVALS-1', 2);
      INSERT INTO challenge_requests (id, period_id, player_1_id, player_2_id, status) VALUES
        ('accepted', 'period-1', 'A', 'B', 'accepted'),
        ('other-a', 'period-1', 'A', 'C', 'pending'),
        ('other-b', 'period-1', 'B', 'D', 'pending'),
        ('pair-rivals', 'period-2', 'A', 'B', 'pending');
    `
  );

  const result = await closeChallengePendingRequestsAfterAccept(db, {
    periodId: "period-1",
    rivalsTournamentId: "RIVALS-1",
    acceptedRequestId: "accepted",
    player1Id: "A",
    player2Id: "B",
    saturatedPlayerIds: [],
    actorPlayerId: "B",
  });

  assert.deepEqual(result.auto_cancelled_request_ids, ["pair-rivals"]);
  assert.deepEqual(
    await all(db, "SELECT id, status FROM challenge_requests ORDER BY id"),
    [
      { id: "accepted", status: "accepted" },
      { id: "other-a", status: "pending" },
      { id: "other-b", status: "pending" },
      { id: "pair-rivals", status: "auto_cancelled" },
    ]
  );
});

test("serializes simultaneous accepts at the N=1 match limit", async (t) => {
  const [firstDb, secondDb] = await createConcurrentChallengeAcceptanceDatabase(t);
  await run(
    firstDb,
    "INSERT INTO challenge_periods (id, rivals_tournament_id, max_matches_per_player) VALUES ('period-1', 'RIVALS-1', 1)"
  );

  const results = await Promise.allSettled([
    acceptChallengeMatchWithGuards(firstDb, {
      duelId: "duel-ab",
      requestId: "request-ab",
      periodId: "period-1",
      rivalsTournamentId: "RIVALS-1",
      player1Id: "A",
      player2Id: "B",
      maxMatchesPerPlayer: 1,
    }),
    acceptChallengeMatchWithGuards(secondDb, {
      duelId: "duel-ac",
      requestId: "request-ac",
      periodId: "period-1",
      rivalsTournamentId: "RIVALS-1",
      player1Id: "A",
      player2Id: "C",
      maxMatchesPerPlayer: 1,
    }),
  ]);

  assert.equal(results.filter((result) => result.status === "fulfilled").length, 1);
  assert.equal(results.filter((result) => result.status === "rejected").length, 1);
  assert.equal(results.find((result) => result.status === "rejected")?.reason?.code, "match_limit_reached");
  assert.equal(
    Number((await get(
      firstDb,
      "SELECT COUNT(*) AS count FROM duels WHERE player_1_id = 'A' OR player_2_id = 'A'"
    ))?.count),
    1
  );
});

test("allows two simultaneous accepts at N=2 when both slots are free", async (t) => {
  const [firstDb, secondDb] = await createConcurrentChallengeAcceptanceDatabase(t);
  await run(
    firstDb,
    "INSERT INTO challenge_periods (id, rivals_tournament_id, max_matches_per_player) VALUES ('period-1', 'RIVALS-1', 2)"
  );

  const results = await Promise.allSettled([
    acceptChallengeMatchWithGuards(firstDb, {
      duelId: "duel-ab",
      requestId: "request-ab",
      periodId: "period-1",
      rivalsTournamentId: "RIVALS-1",
      player1Id: "A",
      player2Id: "B",
      maxMatchesPerPlayer: 2,
    }),
    acceptChallengeMatchWithGuards(secondDb, {
      duelId: "duel-ac",
      requestId: "request-ac",
      periodId: "period-1",
      rivalsTournamentId: "RIVALS-1",
      player1Id: "A",
      player2Id: "C",
      maxMatchesPerPlayer: 2,
    }),
  ]);

  assert.equal(results.filter((result) => result.status === "fulfilled").length, 2);
  assert.equal(
    Number((await get(
      firstDb,
      "SELECT COUNT(*) AS count FROM duels WHERE player_1_id = 'A' OR player_2_id = 'A'"
    ))?.count),
    2
  );
});

test("allows only one simultaneous accept when the selected times conflict", async (t) => {
  const [firstDb, secondDb] = await createConcurrentChallengeAcceptanceDatabase(t);
  await run(
    firstDb,
    "INSERT INTO challenge_periods (id, rivals_tournament_id, max_matches_per_player) VALUES ('period-1', 'RIVALS-1', 2)"
  );

  const results = await Promise.allSettled([
    acceptChallengeMatchWithGuards(firstDb, {
      duelId: "duel-ab",
      requestId: "request-ab",
      periodId: "period-1",
      rivalsTournamentId: "RIVALS-1",
      player1Id: "A",
      player2Id: "B",
      maxMatchesPerPlayer: 2,
      timeUtc: "2026-08-20T10:00:00.000Z",
      format: "Bo3",
    }),
    acceptChallengeMatchWithGuards(secondDb, {
      duelId: "duel-ac",
      requestId: "request-ac",
      periodId: "period-1",
      rivalsTournamentId: "RIVALS-1",
      player1Id: "A",
      player2Id: "C",
      maxMatchesPerPlayer: 2,
      timeUtc: "2026-08-20T11:00:00.000Z",
      format: "Bo3",
    }),
  ]);

  assert.equal(results.filter((result) => result.status === "fulfilled").length, 1);
  assert.equal(results.filter((result) => result.status === "rejected").length, 1);
  assert.equal(results.find((result) => result.status === "rejected")?.reason?.code, "schedule_conflict");
  assert.equal(Number((await get(firstDb, "SELECT COUNT(*) AS count FROM duels"))?.count), 1);
});

test("allows only one simultaneous accept when one of two slots remains", async (t) => {
  const [firstDb, secondDb] = await createConcurrentChallengeAcceptanceDatabase(t);
  await exec(
    firstDb,
    `
      INSERT INTO challenge_periods (id, rivals_tournament_id, max_matches_per_player)
      VALUES ('period-1', 'RIVALS-1', 2);
      INSERT INTO duels (
        id, challenge_period_id, source_type, player_1_id, player_2_id, status
      ) VALUES ('existing', 'period-1', 'challenge', 'A', 'X', 'Done');
    `
  );

  const results = await Promise.allSettled([
    acceptChallengeMatchWithGuards(firstDb, {
      duelId: "duel-ab",
      requestId: "request-ab",
      periodId: "period-1",
      rivalsTournamentId: "RIVALS-1",
      player1Id: "A",
      player2Id: "B",
      maxMatchesPerPlayer: 2,
    }),
    acceptChallengeMatchWithGuards(secondDb, {
      duelId: "duel-ac",
      requestId: "request-ac",
      periodId: "period-1",
      rivalsTournamentId: "RIVALS-1",
      player1Id: "A",
      player2Id: "C",
      maxMatchesPerPlayer: 2,
    }),
  ]);

  assert.equal(results.filter((result) => result.status === "fulfilled").length, 1);
  assert.equal(results.filter((result) => result.status === "rejected").length, 1);
  assert.equal(results.find((result) => result.status === "rejected")?.reason?.code, "match_limit_reached");
  assert.equal(
    Number((await get(
      firstDb,
      "SELECT COUNT(*) AS count FROM duels WHERE player_1_id = 'A' OR player_2_id = 'A'"
    ))?.count),
    2
  );
});

test("allows only one simultaneous accept for the same pair in different Rivals periods", async (t) => {
  const [firstDb, secondDb] = await createConcurrentChallengeAcceptanceDatabase(t);
  await exec(
    firstDb,
    `
      INSERT INTO challenge_periods (id, rivals_tournament_id, max_matches_per_player) VALUES
        ('period-1', 'RIVALS-1', 2),
        ('period-2', 'RIVALS-1', 2);
    `
  );

  const results = await Promise.allSettled([
    acceptChallengeMatchWithGuards(firstDb, {
      duelId: "duel-period-1",
      requestId: "request-period-1",
      periodId: "period-1",
      rivalsTournamentId: "RIVALS-1",
      player1Id: "A",
      player2Id: "B",
      maxMatchesPerPlayer: 2,
    }),
    acceptChallengeMatchWithGuards(secondDb, {
      duelId: "duel-period-2",
      requestId: "request-period-2",
      periodId: "period-2",
      rivalsTournamentId: "RIVALS-1",
      player1Id: "B",
      player2Id: "A",
      maxMatchesPerPlayer: 2,
    }),
  ]);

  assert.equal(results.filter((result) => result.status === "fulfilled").length, 1);
  assert.equal(results.filter((result) => result.status === "rejected").length, 1);
  assert.equal(results.find((result) => result.status === "rejected")?.reason?.code, "rivals_pair_used");
  assert.equal(Number((await get(firstDb, "SELECT COUNT(*) AS count FROM duels"))?.count), 1);
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
