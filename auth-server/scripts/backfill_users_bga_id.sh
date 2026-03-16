#!/usr/bin/env bash
set -euo pipefail

DB_PATH="${1:-/home/carcassonne-gg/auth-server/data/auth.sqlite}"

if [[ ! -f "$DB_PATH" ]]; then
  echo "DB file not found: $DB_PATH" >&2
  exit 1
fi

ALTER_SQL="$(sqlite3 "$DB_PATH" <<'SQL'
SELECT CASE
  WHEN NOT EXISTS (
    SELECT 1
    FROM pragma_table_info('users')
    WHERE name = 'bga_id'
  )
  THEN 'ALTER TABLE users ADD COLUMN bga_id TEXT;'
  ELSE ''
END;
SQL
)"

if [[ -n "$ALTER_SQL" ]]; then
  sqlite3 "$DB_PATH" "$ALTER_SQL"
fi

sqlite3 "$DB_PATH" <<'SQL'
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_bga_id
ON users(bga_id)
WHERE bga_id IS NOT NULL AND trim(bga_id) <> '';
SQL

sqlite3 "$DB_PATH" <<'SQL'
BEGIN IMMEDIATE;

UPDATE users
SET bga_id = NULL
WHERE trim(COALESCE(bga_id, '')) = '';

WITH ranked_matches AS (
  SELECT
    u.id AS user_id,
    p.id AS profile_id,
    ROW_NUMBER() OVER (
      PARTITION BY p.id
      ORDER BY datetime(COALESCE(u.last_login, u.updated_at, u.created_at)) DESC, u.id ASC
    ) AS rn
  FROM users u
  JOIN profiles p
    ON lower(COALESCE(p.email, '')) = lower(COALESCE(u.email, ''))
   AND p.deleted_at IS NULL
  WHERE trim(COALESCE(u.bga_id, '')) = ''
    AND trim(COALESCE(u.email, '')) <> ''
    AND trim(COALESCE(p.id, '')) <> ''
)
UPDATE users
SET
  bga_id = (
    SELECT rm.profile_id
    FROM ranked_matches rm
    WHERE rm.user_id = users.id
      AND rm.rn = 1
  ),
  updated_at = CURRENT_TIMESTAMP
WHERE id IN (
  SELECT rm.user_id
  FROM ranked_matches rm
  WHERE rm.rn = 1
);

COMMIT;
SQL

echo "Backfill done. Preview:"
sqlite3 -header -column "$DB_PATH" \
"SELECT id, google_id, email, bga_id, last_login FROM users ORDER BY id LIMIT 20;"
