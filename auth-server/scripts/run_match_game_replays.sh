#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AUTH_SERVER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_PYTHON="${AUTH_SERVER_DIR}/.venv/bin/python"
AUTH_DB_PATH="${AUTH_SQLITE_PATH:-${DB_PATH:-${AUTH_SERVER_DIR}/data/auth.sqlite}}"
LOCK_FILE="/tmp/carcassonne-update-duels.lock"

if [[ -x "${VENV_PYTHON}" ]]; then
  PYTHON_EXECUTABLE="${VENV_PYTHON}"
else
  PYTHON_EXECUTABLE="${PYTHON_BIN:-python3}"
fi

cd "${AUTH_SERVER_DIR}"

COMMAND=(
  "${PYTHON_EXECUTABLE}"
  -m update_matches.match_game_replay_cli
  --db-path "${AUTH_DB_PATH}"
  "$@"
)

if command -v flock >/dev/null 2>&1; then
  exec flock -n "${LOCK_FILE}" "${COMMAND[@]}"
fi

exec "${COMMAND[@]}"
