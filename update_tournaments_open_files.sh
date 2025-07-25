#!/bin/bash

cd /home/carcassonne-gg/json-data || exit 1
echo "=== $(date) ===" >> /home/carcassonne-gg/cron_backup.log

# List of tournament IDs
tournaments=("Asian-Cup-2025" "TECS-2025" "UCOCup-2025" "BCPL-2025-Sum")

for tournament_id in "${tournaments[@]}"; do
  curl -s "https://api.carcassonne.com.ua/tournaments?tournament_id=${tournament_id}" -o "tournaments-open/${tournament_id}.json"
  git add "tournaments-open/${tournament_id}.json"
done

curl -s https://api.carcassonne.com.ua/tournaments_list -o tournaments-list.json
git add tournaments-list.json

git commit -m "Update json-files from server" || exit 0
git push origin main