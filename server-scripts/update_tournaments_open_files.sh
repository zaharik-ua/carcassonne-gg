	#!/bin/bash

cd /home/carcassonne-gg/json-data || exit 1
echo "=== $(date '+%Y-%m-%d %H:%M:%S') ===" >> /home/carcassonne-gg/cron_update_open.log

changes_made=false

# # List of tournament IDs
# tournaments=("Asian-Cup-2025" "TECS-2025" "UCOCup-2025" "BCPL-2025-Sum" "CZ-2025-COC" "HR-2025-OC-2" "UCOCup-2025" "AR-2025-LNE" "FI-2025-OC")

# for tournament_id in "${tournaments[@]}"; do
#   temp_file=$(mktemp)
#   curl -s "https://api.carcassonne.com.ua/public/tournaments?tournament_id=${tournament_id}" -o "$temp_file"
#   if grep -q '"status"[[:space:]]*:[[:space:]]*"success"' "$temp_file"; then
#     mv "$temp_file" "tournaments-open/${tournament_id}.json"
#     if ! git diff --quiet "tournaments-open/${tournament_id}.json"; then
#       git add "tournaments-open/${tournament_id}.json"
#       echo "Updated: tournaments-open/${tournament_id}.json" >> /home/carcassonne-gg/cron_update_open.log
#       changes_made=true
#     fi
#   else
#     echo "❌ Failed to fetch or parse ${tournament_id} (response: $(cat "$temp_file" | head -c 200))" >> /home/carcassonne-gg/cron_update_open.log
#     rm "$temp_file"
#   fi
# done

temp_file=$(mktemp)
curl -s https://api.carcassonne.com.ua/public/tournaments -o "$temp_file"
if grep -q '"status"[[:space:]]*:[[:space:]]*"success"' "$temp_file"; then
  mv "$temp_file" tournaments-open.json
  if ! git diff --quiet tournaments-open.json; then
    git add tournaments-open.json
    echo "Updated: tournaments-open.json" >> /home/carcassonne-gg/cron_update_open.log
    changes_made=true
  fi
else
  echo "❌ Failed to fetch or parse tournaments-open.json (response: $(cat "$temp_file" | head -c 200))" >> /home/carcassonne-gg/cron_update_open.log
  rm "$temp_file"
fi

temp_file=$(mktemp)
curl -s https://api.carcassonne.com.ua/public/tournaments_list -o "$temp_file"
if grep -q '"status"[[:space:]]*:[[:space:]]*"success"' "$temp_file"; then
  mv "$temp_file" tournaments-list.json
  if ! git diff --quiet tournaments-list.json; then
    git add tournaments-list.json
    echo "Updated: tournaments-list.json" >> /home/carcassonne-gg/cron_update_open.log
    changes_made=true
  fi
else
  echo "❌ Failed to fetch or parse tournaments-list.json (response: $(cat "$temp_file" | head -c 200))" >> /home/carcassonne-gg/cron_update_open.log
  rm "$temp_file"
fi

if [ "$changes_made" = true ]; then
  git commit -m "Update json-files from server"

  if ! git push origin main; then
    echo "🔁 Push failed. Attempting to rebase and retry..." >> /home/carcassonne-gg/cron_update_open.log

    # Save any remaining changes before rebase
    git add .
    git commit -m "Auto-commit before rebase" || echo "No changes to commit"

    git pull --rebase origin main

    git push origin main
    if [ $? -eq 0 ]; then
      echo "✅ Push successful after rebase" >> /home/carcassonne-gg/cron_update_open.log
    else
      echo "❌ Push still failed after rebase" >> /home/carcassonne-gg/cron_update_open.log
    fi
  else
    echo "✅ Push successful" >> /home/carcassonne-gg/cron_update_open.log
  fi
else
  echo "No updates" >> /home/carcassonne-gg/cron_update_open.log
fi
