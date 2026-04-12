#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AUTH_SERVER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PYTHON_BIN="${AUTH_SERVER_DIR}/.venv/bin/python"

DB_PATH="${AUTH_SQLITE_PATH:-${AUTH_SERVER_DIR}/data/auth.sqlite}"
TOURNAMENT_ID="${WTCOC_TOURNAMENT_ID:-WTCOC-2026}"
ACTOR_ID="${WTCOC_SYNC_ACTOR_ID:-1}"

if [[ ! -x "${PYTHON_BIN}" ]]; then
  PYTHON_BIN="python3"
fi

cd "${AUTH_SERVER_DIR}"
"${PYTHON_BIN}" "${AUTH_SERVER_DIR}/run_sync_wtcoc_matches.py" \
  --db-path "${DB_PATH}" \
  --tournament-id "${TOURNAMENT_ID}" \
  --actor-id "${ACTOR_ID}" \
  --apply \
  "$@"
