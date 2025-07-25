#!/bin/bash

cd /home/carcassonne-gg/json-data || exit 1
echo "=== $(date '+%Y-%m-%d %H:%M:%S') ===" >> /home/carcassonne-gg/cron_update_open.log

changes_made=false

# List of tournament IDs
tournaments=("Asian-Cup-2025" "TECS-2025" "UCOCup-2025" "BCPL-2025-Sum")

for tournament_id in "${tournaments[@]}"; do
  temp_file=$(mktemp)
  curl -s "https://api.carcassonne.com.ua/tournaments?tournament_id=${tournament_id}" -o "$temp_file"
  if jq empty "$temp_file" > /dev/null 2>&1; then
    mv "$temp_file" "tournaments-open/${tournament_id}.json"
    if ! git diff --quiet "tournaments-open/${tournament_id}.json"; then
      git add "tournaments-open/${tournament_id}.json"
      echo "Updated: tournaments-open/${tournament_id}.json" >> /home/carcassonne-gg/cron_update_open.log
      changes_made=true
    fi
  else
    echo "❌ Failed to fetch or parse ${tournament_id}" >> /home/carcassonne-gg/cron_update_open.log
    rm "$temp_file"
  fi
done

temp_file=$(mktemp)
curl -s https://api.carcassonne.com.ua/tournaments_list -o "$temp_file"
if jq empty "$temp_file" > /dev/null 2>&1; then
  mv "$temp_file" tournaments-list.json
  if ! git diff --quiet tournaments-list.json; then
    git add tournaments-list.json
    echo "Updated: tournaments-list.json" >> /home/carcassonne-gg/cron_update_open.log
    changes_made=true
  fi
else
  echo "❌ Failed to fetch or parse tournaments-list.json" >> /home/carcassonne-gg/cron_update_open.log
  rm "$temp_file"
fi

if [ "$changes_made" = true ]; then
  git commit -m "Update json-files from server"
  git push origin main
else
  echo "No updates" >> /home/carcassonne-gg/cron_update_open.log
fi