#!/usr/bin/env bash
set -euo pipefail

DB_PATH="${1:-/home/carcassonne-gg/auth-server/data/auth.sqlite}"
TOURNAMENT_ID="${2:-WTCOC-2026}"

echo "DB: ${DB_PATH}"
echo "Tournament: ${TOURNAMENT_ID}"
echo
echo "Before:"
sqlite3 -header -column "${DB_PATH}" "
SELECT
  id,
  tournament_id,
  time_utc,
  lineup_type,
  lineup_deadline_h,
  lineup_deadline_utc,
  updated_at
FROM matches
WHERE upper(trim(COALESCE(tournament_id, ''))) = upper(trim('${TOURNAMENT_ID}'))
  AND deleted_at IS NULL
ORDER BY id ASC;
"

echo
echo "Applying update..."
sqlite3 "${DB_PATH}" "
BEGIN TRANSACTION;
UPDATE matches
SET
  lineup_type = 'Open',
  lineup_deadline_h = NULL,
  lineup_deadline_utc = NULL,
  updated_at = CURRENT_TIMESTAMP
WHERE upper(trim(COALESCE(tournament_id, ''))) = upper(trim('${TOURNAMENT_ID}'))
  AND deleted_at IS NULL;
COMMIT;
"

echo
echo "After:"
sqlite3 -header -column "${DB_PATH}" "
SELECT
  id,
  tournament_id,
  time_utc,
  lineup_type,
  lineup_deadline_h,
  lineup_deadline_utc,
  updated_at
FROM matches
WHERE upper(trim(COALESCE(tournament_id, ''))) = upper(trim('${TOURNAMENT_ID}'))
  AND deleted_at IS NULL
ORDER BY id ASC;
"
