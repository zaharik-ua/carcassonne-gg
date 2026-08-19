export const DEFAULT_MAX_MATCHES_PER_PLAYER = 1;
export const DEFAULT_MAX_PENDING_REQUESTS_PER_PLAYER = 3;

export const CHALLENGE_PERIOD_STATUSES = new Set([
  "draft",
  "planning_open",
  "active",
  "result_review",
  "archived",
  "cancelled",
]);

export const CHALLENGE_PERIOD_PENDING_EXPIRING_STATUSES = new Set([
  "result_review",
  "archived",
  "cancelled",
]);

export const CHALLENGE_PLAYER_PERIOD_STATUSES = new Set([
  "not_selected",
  "available",
  "unavailable",
]);

export const CHALLENGE_MATCH_SLOT_DUEL_STATUSES = new Set([
  "Planned",
  "In progress",
  "Done",
  "Error",
]);

export const CHALLENGE_NON_SLOT_DUEL_STATUSES = new Set([
  "Draft",
  "Requested new time",
  "Cancelled",
]);

export const CHALLENGE_RIVALS_PAIR_DUEL_STATUSES = new Set([
  "Draft",
  "Requested new time",
  "Planned",
  "In progress",
  "Done",
  "Error",
]);

export const CHALLENGE_FORMAT_DURATION_MINUTES = Object.freeze({
  Bo3: 90,
  Bo5: 150,
});

function resolvePositiveInteger(value, defaultValue) {
  if (value === undefined || value === null) return defaultValue;
  const normalized = String(value).trim();
  if (!normalized) return null;
  const parsed = Number(normalized);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

export function resolveMaxMatchesPerPlayer(value) {
  return resolvePositiveInteger(value, DEFAULT_MAX_MATCHES_PER_PLAYER);
}

export function resolveMaxPendingRequestsPerPlayer(value) {
  return resolvePositiveInteger(value, DEFAULT_MAX_PENDING_REQUESTS_PER_PLAYER);
}

export function isChallengePendingRequestLimitReached(pendingCount, configuredLimit) {
  const limit = resolveMaxPendingRequestsPerPlayer(configuredLimit);
  if (!limit) return false;
  const count = Number(pendingCount);
  return Number.isFinite(count) && count >= limit;
}

export function isChallengeMatchSlotStatus(status) {
  return CHALLENGE_MATCH_SLOT_DUEL_STATUSES.has(String(status || "").trim());
}

export function isChallengeRivalsPairDuelStatus(status) {
  return CHALLENGE_RIVALS_PAIR_DUEL_STATUSES.has(String(status || "").trim());
}

export function isChallengePlayerRequestEligibleStatus(status) {
  const normalized = String(status || "").trim().toLowerCase();
  return normalized === "available" || normalized === "not_selected";
}

export function getChallengeFormatDurationMinutes(format) {
  return CHALLENGE_FORMAT_DURATION_MINUTES[String(format || "").trim()] || null;
}

export function buildChallengeMatchCapacity(matchesCount, configuredLimit) {
  const parsedCount = Number(matchesCount);
  const count = Number.isFinite(parsedCount) && parsedCount > 0
    ? Math.floor(parsedCount)
    : 0;
  const limit = resolveMaxMatchesPerPlayer(configuredLimit) || DEFAULT_MAX_MATCHES_PER_PLAYER;
  return {
    matches_count: count,
    matches_limit: limit,
    matches_remaining: Math.max(0, limit - count),
    is_match_limit_reached: count >= limit,
  };
}

function normalizeChallengeIdentifier(value) {
  const normalized = String(value ?? "").trim();
  return normalized || null;
}

export async function loadChallengeMatchCapacities(db, options = {}) {
  const periodId = normalizeChallengeIdentifier(options.periodId);
  const excludedDuelId = normalizeChallengeIdentifier(options.excludeDuelId);
  const playerIds = Array.from(new Set(
    (Array.isArray(options.playerIds) ? options.playerIds : [])
      .map(normalizeChallengeIdentifier)
      .filter(Boolean)
  ));
  const capacities = {};
  if (!periodId || !playerIds.length) return capacities;

  const slotStatuses = Array.from(CHALLENGE_MATCH_SLOT_DUEL_STATUSES);
  await Promise.all(playerIds.map(async (playerId) => {
    const row = await dbGet(
      db,
      `
        SELECT COUNT(*) AS matches_count
        FROM duels
        WHERE challenge_period_id = ?
          AND source_type = 'challenge'
          AND deleted_at IS NULL
          AND status IN (${slotStatuses.map(() => "?").join(", ")})
          AND (player_1_id = ? OR player_2_id = ?)
          AND (? IS NULL OR id <> ?)
      `,
      [
        periodId,
        ...slotStatuses,
        playerId,
        playerId,
        excludedDuelId,
        excludedDuelId,
      ]
    );
    capacities[playerId] = buildChallengeMatchCapacity(
      row?.matches_count,
      options.maxMatchesPerPlayer
    );
  }));
  return capacities;
}

export async function loadChallengeBlockingRivalsPairDuel(db, options = {}) {
  const periodId = normalizeChallengeIdentifier(options.periodId);
  const rivalsTournamentId = normalizeChallengeIdentifier(options.rivalsTournamentId);
  const player1Id = normalizeChallengeIdentifier(options.player1Id);
  const player2Id = normalizeChallengeIdentifier(options.player2Id);
  const excludedDuelId = normalizeChallengeIdentifier(options.excludeDuelId);
  if (!periodId || !player1Id || !player2Id) return null;

  const pairStatuses = Array.from(CHALLENGE_RIVALS_PAIR_DUEL_STATUSES);
  const scopeSql = rivalsTournamentId
    ? "upper(trim(COALESCE(pair_period.rivals_tournament_id, ''))) = upper(trim(?))"
    : "pair_duel.challenge_period_id = ?";
  return dbGet(
    db,
    `
      SELECT
        pair_duel.id,
        pair_duel.status,
        pair_duel.challenge_period_id
      FROM duels pair_duel
      INNER JOIN challenge_periods pair_period
        ON pair_period.id = pair_duel.challenge_period_id
      WHERE pair_duel.source_type = 'challenge'
        AND pair_duel.deleted_at IS NULL
        AND pair_duel.status IN (${pairStatuses.map(() => "?").join(", ")})
        AND ${scopeSql}
        AND (
          (pair_duel.player_1_id = ? AND pair_duel.player_2_id = ?)
          OR (pair_duel.player_1_id = ? AND pair_duel.player_2_id = ?)
        )
        AND (? IS NULL OR pair_duel.id <> ?)
      LIMIT 1
    `,
    [
      ...pairStatuses,
      rivalsTournamentId || periodId,
      player1Id,
      player2Id,
      player2Id,
      player1Id,
      excludedDuelId,
      excludedDuelId,
    ]
  );
}

export async function closeChallengePendingRequestsAfterAccept(db, options = {}) {
  const periodId = normalizeChallengeIdentifier(options.periodId);
  const rivalsTournamentId = normalizeChallengeIdentifier(options.rivalsTournamentId);
  const acceptedRequestId = normalizeChallengeIdentifier(options.acceptedRequestId);
  const player1Id = normalizeChallengeIdentifier(options.player1Id);
  const player2Id = normalizeChallengeIdentifier(options.player2Id);
  const actorPlayerId = normalizeChallengeIdentifier(options.actorPlayerId) || player1Id;
  const saturatedPlayerIds = Array.from(new Set(
    (Array.isArray(options.saturatedPlayerIds) ? options.saturatedPlayerIds : [])
      .map(normalizeChallengeIdentifier)
      .filter((playerId) => playerId === player1Id || playerId === player2Id)
  ));
  if (!periodId || !acceptedRequestId || !player1Id || !player2Id) {
    return {
      auto_cancelled_request_ids: [],
      auto_cancelled_request_count: 0,
      cancelled_duel_count: 0,
    };
  }

  const targetConditions = [];
  const targetParams = [];
  if (rivalsTournamentId) {
    targetConditions.push(`
      (
        (
          (cr.player_1_id = ? AND cr.player_2_id = ?)
          OR (cr.player_1_id = ? AND cr.player_2_id = ?)
        )
        AND EXISTS (
          SELECT 1
          FROM challenge_periods pair_period
          WHERE pair_period.id = cr.period_id
            AND upper(trim(COALESCE(pair_period.rivals_tournament_id, ''))) = upper(trim(?))
        )
      )
    `);
    targetParams.push(player1Id, player2Id, player2Id, player1Id, rivalsTournamentId);
  } else {
    targetConditions.push(`
      (
        cr.period_id = ?
        AND (
          (cr.player_1_id = ? AND cr.player_2_id = ?)
          OR (cr.player_1_id = ? AND cr.player_2_id = ?)
        )
      )
    `);
    targetParams.push(periodId, player1Id, player2Id, player2Id, player1Id);
  }
  if (saturatedPlayerIds.length) {
    const playerPlaceholders = saturatedPlayerIds.map(() => "?").join(", ");
    targetConditions.push(`
      (
        cr.period_id = ?
        AND (
          cr.player_1_id IN (${playerPlaceholders})
          OR cr.player_2_id IN (${playerPlaceholders})
        )
      )
    `);
    targetParams.push(periodId, ...saturatedPlayerIds, ...saturatedPlayerIds);
  }

  const targetRequests = await dbAll(
    db,
    `
      SELECT cr.id, cr.period_id, cr.player_1_id, cr.player_2_id
      FROM challenge_requests cr
      WHERE cr.status = 'pending'
        AND cr.id <> ?
        AND (${targetConditions.join(" OR ")})
      ORDER BY cr.period_id ASC, cr.id ASC
    `,
    [acceptedRequestId, ...targetParams]
  );
  const targetRequestIds = targetRequests
    .map((request) => normalizeChallengeIdentifier(request?.id))
    .filter(Boolean);
  if (!targetRequestIds.length) {
    return {
      auto_cancelled_request_ids: [],
      auto_cancelled_request_count: 0,
      cancelled_duel_count: 0,
    };
  }

  const requestPlaceholders = targetRequestIds.map(() => "?").join(", ");
  const duelResult = await dbRun(
    db,
    `
      UPDATE duels
      SET
        status = 'Cancelled',
        cancelled_by_player_id = CASE
          WHEN player_1_id IN (?, ?)
            AND player_2_id NOT IN (?, ?)
            THEN player_1_id
          WHEN player_2_id IN (?, ?)
            AND player_1_id NOT IN (?, ?)
            THEN player_2_id
          ELSE ?
        END,
        cancellation_reason = 'another_match_accepted',
        cancelled_at = CURRENT_TIMESTAMP,
        updated_by = ?,
        updated_at = CURRENT_TIMESTAMP
      WHERE challenge_request_id IN (${requestPlaceholders})
        AND status IN ('Draft', 'Requested new time')
        AND source_type = 'challenge'
        AND deleted_at IS NULL
    `,
    [
      player1Id,
      player2Id,
      player1Id,
      player2Id,
      player1Id,
      player2Id,
      player1Id,
      player2Id,
      actorPlayerId,
      actorPlayerId,
      ...targetRequestIds,
    ]
  );
  const requestResult = await dbRun(
    db,
    `
      UPDATE challenge_requests
      SET status = 'auto_cancelled', updated_at = CURRENT_TIMESTAMP
      WHERE id IN (${requestPlaceholders})
        AND status = 'pending'
    `,
    targetRequestIds
  );

  return {
    auto_cancelled_request_ids: targetRequestIds,
    auto_cancelled_request_count: Number(requestResult?.changes) || 0,
    cancelled_duel_count: Number(duelResult?.changes) || 0,
  };
}

export function shouldCloseChallengeRequestsForPlayerStatus(status) {
  return String(status || "").trim().toLowerCase() === "unavailable";
}

function dbAll(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => {
      if (error) reject(error);
      else resolve(rows || []);
    });
  });
}

function dbGet(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (error, row) => {
      if (error) reject(error);
      else resolve(row || null);
    });
  });
}

function dbRun(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function onRun(error) {
      if (error) reject(error);
      else resolve({ changes: this?.changes || 0, lastID: this?.lastID ?? null });
    });
  });
}

function dbExec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (error) => {
      if (error) reject(error);
      else resolve();
    });
  });
}

export function ensureChallengePeriodConfigurationSchema(db) {
  return new Promise((resolve, reject) => {
    db.all("PRAGMA table_info(challenge_periods)", (pragmaError, columns) => {
      if (pragmaError) {
        reject(pragmaError);
        return;
      }
      const hasMaxPendingRequests = (columns || [])
        .some((column) => column.name === "max_pending_requests_per_player");
      if (hasMaxPendingRequests) {
        resolve();
        return;
      }
      db.run(
        `
          ALTER TABLE challenge_periods
          ADD COLUMN max_pending_requests_per_player INTEGER NOT NULL DEFAULT 3
          CHECK (max_pending_requests_per_player >= 1)
        `,
        (alterError) => {
          if (alterError) reject(alterError);
          else resolve();
        }
      );
    });
  });
}

export async function ensureChallengePeriodPlayersSchema(db) {
  const columns = await dbAll(db, "PRAGMA table_info(challenge_period_players)");
  if (!columns.length) return;

  const table = await dbGet(
    db,
    "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'challenge_period_players' LIMIT 1"
  );
  const columnNames = new Set(columns.map((column) => String(column?.name || "")));
  const requiredColumns = [
    "period_id",
    "player_id",
    "status",
    "availability_start_1_utc",
    "availability_end_1_utc",
    "availability_start_2_utc",
    "availability_end_2_utc",
    "availability_start_3_utc",
    "availability_end_3_utc",
    "created_at",
    "status_updated_at",
    "updated_at",
  ];
  const tableSql = String(table?.sql || "").toLowerCase();
  const needsRebuild = columnNames.has("challenge_duel_id")
    || tableSql.includes("match_scheduled")
    || tableSql.includes("'played'")
    || requiredColumns.some((columnName) => !columnNames.has(columnName));

  if (needsRebuild) {
    const sourceColumn = (columnName, fallbackSql) => (
      columnNames.has(columnName) ? columnName : fallbackSql
    );
    const createdAt = sourceColumn("created_at", "CURRENT_TIMESTAMP");
    const updatedAt = sourceColumn("updated_at", createdAt);
    const statusUpdatedAt = sourceColumn("status_updated_at", updatedAt);
    const availabilityColumns = [1, 2, 3]
      .flatMap((index) => [
        sourceColumn(`availability_start_${index}_utc`, "NULL"),
        sourceColumn(`availability_end_${index}_utc`, "NULL"),
      ]);

    try {
      await dbExec(
        db,
        `
          BEGIN IMMEDIATE TRANSACTION;
          DROP TABLE IF EXISTS challenge_period_players_v2;
          CREATE TABLE challenge_period_players_v2 (
            period_id TEXT NOT NULL,
            player_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'not_selected',
            availability_start_1_utc TEXT,
            availability_end_1_utc TEXT,
            availability_start_2_utc TEXT,
            availability_end_2_utc TEXT,
            availability_start_3_utc TEXT,
            availability_end_3_utc TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            status_updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (period_id, player_id),
            FOREIGN KEY (period_id) REFERENCES challenge_periods(id) ON DELETE CASCADE,
            FOREIGN KEY (player_id) REFERENCES profiles(id),
            CHECK (status IN ('not_selected', 'available', 'unavailable'))
          );
          INSERT INTO challenge_period_players_v2 (
            period_id,
            player_id,
            status,
            availability_start_1_utc,
            availability_end_1_utc,
            availability_start_2_utc,
            availability_end_2_utc,
            availability_start_3_utc,
            availability_end_3_utc,
            created_at,
            status_updated_at,
            updated_at
          )
          SELECT
            period_id,
            player_id,
            CASE
              WHEN status IN ('match_scheduled', 'played') THEN 'available'
              WHEN status IN ('not_selected', 'available', 'unavailable') THEN status
              ELSE 'not_selected'
            END,
            ${availabilityColumns.join(",\n            ")},
            COALESCE(${createdAt}, CURRENT_TIMESTAMP),
            COALESCE(${statusUpdatedAt}, ${updatedAt}, ${createdAt}, CURRENT_TIMESTAMP),
            COALESCE(${updatedAt}, ${createdAt}, CURRENT_TIMESTAMP)
          FROM challenge_period_players;
          DROP TABLE challenge_period_players;
          ALTER TABLE challenge_period_players_v2 RENAME TO challenge_period_players;
          COMMIT;
        `
      );
    } catch (error) {
      await dbExec(db, "ROLLBACK;").catch(() => {});
      throw error;
    }
  }

  await dbExec(
    db,
    `
      CREATE INDEX IF NOT EXISTS idx_challenge_period_players_status
      ON challenge_period_players(period_id, status);
      CREATE INDEX IF NOT EXISTS idx_challenge_period_players_status_updated
      ON challenge_period_players(period_id, status, status_updated_at);
    `
  );
}
