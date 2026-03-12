#!/usr/bin/env bash
set -euo pipefail

DB_PATH="${1:-/home/carcassonne-gg/auth-server/data/auth.sqlite}"
CSV_PATH="${2:-/home/carcassonne-gg/auth-server/profiles_status.csv}"

if [[ ! -f "$DB_PATH" ]]; then
  echo "DB file not found: $DB_PATH" >&2
  exit 1
fi

if [[ ! -f "$CSV_PATH" ]]; then
  echo "CSV file not found: $CSV_PATH" >&2
  exit 1
fi

HAS_STATUS_COLUMN="$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM pragma_table_info('profiles') WHERE name = 'status';")"
if [[ "$HAS_STATUS_COLUMN" -eq 0 ]]; then
  sqlite3 "$DB_PATH" "ALTER TABLE profiles ADD COLUMN status TEXT NOT NULL DEFAULT 'Active';"
fi

sqlite3 "$DB_PATH" <<'SQL'
DROP TABLE IF EXISTS profiles_status_import;
CREATE TABLE profiles_status_import (
  player_id TEXT,
  status TEXT
);
SQL

sqlite3 "$DB_PATH" <<SQL
.mode csv
.import '$CSV_PATH' profiles_status_import
SQL

sqlite3 "$DB_PATH" <<'SQL'
BEGIN IMMEDIATE;

UPDATE profiles
SET status = COALESCE((
  SELECT NULLIF(trim(psi.status), '')
  FROM profiles_status_import psi
  WHERE trim(psi.player_id) = trim(profiles.player_id)
  LIMIT 1
), 'Active');

DROP TABLE profiles_status_import;

COMMIT;
SQL

echo "Status import done. First 20 rows:"
sqlite3 -header -column "$DB_PATH" \
"SELECT id, player_id, bga_nickname, status FROM profiles ORDER BY id LIMIT 20;"
