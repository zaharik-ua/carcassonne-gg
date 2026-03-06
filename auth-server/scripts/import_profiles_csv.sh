#!/usr/bin/env bash
set -euo pipefail

DB_PATH="${1:-/home/carcassonne-gg/auth-server/data/auth.sqlite}"
CSV_PATH="${2:-/home/carcassonne-gg/auth-server/profiles.csv}"

if [[ ! -f "$DB_PATH" ]]; then
  echo "DB file not found: $DB_PATH" >&2
  exit 1
fi

if [[ ! -f "$CSV_PATH" ]]; then
  echo "CSV file not found: $CSV_PATH" >&2
  exit 1
fi

sqlite3 "$DB_PATH" <<SQL
PRAGMA foreign_keys = OFF;

CREATE TABLE IF NOT EXISTS profiles (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT UNIQUE COLLATE NOCASE,
  bga_nickname TEXT,
  name TEXT,
  association TEXT,
  master_title INTEGER NOT NULL DEFAULT 0,
  master_title_date DATE,
  team_captain INTEGER NOT NULL DEFAULT 0,
  player_id TEXT,
  admin INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_profiles_player_id ON profiles(player_id);
DROP TABLE IF EXISTS profiles_import;
CREATE TABLE profiles_import (
  "ID" TEXT,
  "BGA nickname" TEXT,
  "Name" TEXT,
  "Association" TEXT,
  "Email" TEXT,
  "Master Title" TEXT,
  "Master Title Date" TEXT,
  "Team Captain" TEXT,
  "_extra" TEXT
);
SQL

sqlite3 "$DB_PATH" <<SQL
.mode csv
.import '$CSV_PATH' profiles_import
SQL

sqlite3 "$DB_PATH" <<'SQL'
BEGIN IMMEDIATE;

DELETE FROM profiles;

INSERT INTO profiles (
  player_id,
  bga_nickname,
  name,
  association,
  email,
  master_title,
  master_title_date,
  team_captain,
  admin,
  updated_at
)
SELECT
  NULLIF(trim("ID"), '') AS player_id,
  NULLIF(trim("BGA nickname"), '') AS bga_nickname,
  NULLIF(trim("Name"), '') AS name,
  NULLIF(trim("Association"), '') AS association,
  NULLIF(trim("Email"), '') AS email,
  CASE WHEN lower(trim("Master Title")) = 'master' THEN 1 ELSE 0 END AS master_title,
  CASE
    WHEN trim("Master Title Date") = '' THEN NULL
    ELSE substr(trim("Master Title Date"), 7, 4) || '-' ||
         substr(trim("Master Title Date"), 4, 2) || '-' ||
         substr(trim("Master Title Date"), 1, 2)
  END AS master_title_date,
  CASE WHEN lower(trim("Team Captain")) = 'captain' THEN 1 ELSE 0 END AS team_captain,
  0 AS admin,
  CURRENT_TIMESTAMP
FROM profiles_import
WHERE trim("ID") <> ''
ON CONFLICT(player_id) DO UPDATE SET
  bga_nickname = excluded.bga_nickname,
  name = excluded.name,
  association = excluded.association,
  email = excluded.email,
  master_title = excluded.master_title,
  master_title_date = excluded.master_title_date,
  team_captain = excluded.team_captain,
  updated_at = CURRENT_TIMESTAMP;

DROP TABLE profiles_import;

COMMIT;
SQL

echo "Import done. First 20 rows:"
sqlite3 -header -column "$DB_PATH" \
"SELECT id, player_id, bga_nickname, name, association, email, master_title, master_title_date, team_captain FROM profiles ORDER BY id LIMIT 20;"
