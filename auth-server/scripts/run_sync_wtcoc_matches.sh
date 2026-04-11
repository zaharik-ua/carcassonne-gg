#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AUTH_SERVER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

python3 "${AUTH_SERVER_DIR}/run_sync_wtcoc_matches.py" "$@"
