#!/bin/bash

cd /home/carcassonne-gg/json-data/tournaments-done || exit 1
echo "=== $(date) ===" >> /home/carcassonne-gg/cron_backup.log


# List of tournament IDs
tournaments=("BCPL-2026-WIN")

for tournament_id in "${tournaments[@]}"; do
  curl -s "https://api.carcassonne.com.ua/public/tournaments_done?tournament_id=${tournament_id}" -o "${tournament_id}.json"
  git add "${tournament_id}.json"
done

git commit -m "Update per-tournament json files from server" || exit 0
git push origin main
