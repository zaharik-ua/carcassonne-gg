#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AUTH_SERVER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOCK_FILE="/tmp/carcassonne-update-profile-bga-data.lock"

cd "${AUTH_SERVER_DIR}"

/usr/bin/flock -n -E 0 "${LOCK_FILE}" bash -lc '
  set -euo pipefail
  cd "'"${AUTH_SERVER_DIR}"'"
  python3 run_update_profile_bga_data_batch.py "$@"
' bash "$@"
