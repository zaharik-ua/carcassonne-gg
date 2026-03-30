#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AUTH_SERVER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOCK_FILE="/tmp/carcassonne-update-player-elo.lock"

cd "${AUTH_SERVER_DIR}"

/usr/bin/flock -n -E 0 "${LOCK_FILE}" bash -lc '
  set -euo pipefail
  cd "'"${AUTH_SERVER_DIR}"'"
  elo_output="$(python3 run_update_player_elo.py --selection-mode only_null "$@")"
  printf "%s\n" "${elo_output}"
  updated_count="$(printf "%s" "${elo_output}" | python3 -c '"'"'import json,sys; data=json.load(sys.stdin); print(int(data.get("updated", 0)))'"'"')"
  if [ "${updated_count}" -gt 0 ]; then
    python3 run_update_ratings.py --planned
  fi
' bash "$@"
