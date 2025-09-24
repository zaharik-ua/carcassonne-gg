#!/bin/bash

cd /home/carcassonne-gg/json-data/tournaments-done || exit 1
echo "=== $(date) ===" >> /home/carcassonne-gg/cron_backup.log


# List of tournament IDs
# tournaments=("TWCOC-2024" "UCOC-2025" "HR-2025-OC")
# tournaments=("ACOC-2025" "USCC-2025")
 tournaments=("Asian-Cup-2025")
# tournaments=("Copa-America-2021" "BCPL-2025-Sum" "TECS-2025")

for tournament_id in "${tournaments[@]}"; do
  curl -s "https://api.carcassonne.com.ua/public/tournaments_done?tournament_id=${tournament_id}" -o "${tournament_id}.json"
  git add "${tournament_id}.json"
done

git commit -m "Update per-tournament json files from server" || exit 0
git push origin main
