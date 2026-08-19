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
