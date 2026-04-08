#!/usr/bin/env bash
set -euo pipefail

TTL_MINUTES="${CHROME_TMP_CLEANUP_TTL_MINUTES:-360}"

find /tmp -maxdepth 1 -type d -name 'org.chromium.Chromium.scoped_dir.*' -mmin +"${TTL_MINUTES}" -exec rm -rf {} +
find /tmp -maxdepth 1 -type d -name 'carcassonne-chrome-profile-*' -mmin +"${TTL_MINUTES}" -exec rm -rf {} +
