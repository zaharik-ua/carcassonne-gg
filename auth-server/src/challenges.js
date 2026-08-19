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

// Legacy match-derived statuses remain supported until the Part 2 migration.
export const CHALLENGE_PLAYER_PERIOD_STATUSES = new Set([
  "not_selected",
  "available",
  "unavailable",
  "match_scheduled",
  "played",
]);

// Existing one-match blocking behavior remains unchanged during Part 1.
export const CHALLENGE_ACTIVE_DUEL_STATUSES = new Set([
  "Planned",
  "In progress",
  "Error",
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

export function getChallengeFormatDurationMinutes(format) {
  return CHALLENGE_FORMAT_DURATION_MINUTES[String(format || "").trim()] || null;
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
