#!/bin/bash

cd /home/carcassonne-gg/json-data/tournaments-done || exit 1
echo "=== $(date) ===" >> /home/carcassonne-gg/cron_backup.log


# List of tournament IDs
tournaments=("TW-2024-CCLQ" "UCOC-2025" "HR-2025-OC" "MX-2025-CCLQ")

for tournament_id in "${tournaments[@]}"; do
  curl -s "https://api.carcassonne.com.ua/tournaments_done?tournament_id=${tournament_id}" -o "${tournament_id}.json"
  git add "${tournament_id}.json"
done

git commit -m "Update per-tournament json files from server" || exit 0
git push origin main
