#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AUTH_SERVER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${AUTH_SERVER_DIR}"
python3 run_update_player_elo.py "$@"
