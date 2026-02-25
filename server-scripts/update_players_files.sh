#!/bin/bash

cd /home/carcassonne-gg/json-data || exit 1
echo "=== $(date '+%Y-%m-%d %H:%M:%S') ===" >> /home/carcassonne-gg/cron_update_players.log

changes_made=false

temp_file=$(mktemp)
curl -s https://api.carcassonne.com.ua/public/players -o "$temp_file"
if grep -q '"status"[[:space:]]*:[[:space:]]*"success"' "$temp_file"; then
  mv "$temp_file" masters.json
  if ! git diff --quiet masters.json; then
    git add masters.json
    echo "Updated: masters.json" >> /home/carcassonne-gg/cron_update_players.log
    changes_made=true
  fi
else
  echo "❌ Failed to fetch or parse masters.json (response: $(cat "$temp_file" | head -c 200))" >> /home/carcassonne-gg/cron_update_players.log
  rm "$temp_file"
fi


temp_file=$(mktemp)
curl -s https://api.carcassonne.com.ua/public/achievements -o "$temp_file"
if grep -q '"status"[[:space:]]*:[[:space:]]*"success"' "$temp_file"; then
  mv "$temp_file" achievements.json
  if ! git diff --quiet achievements.json; then
    git add achievements.json
    echo "Updated: achievements.json" >> /home/carcassonne-gg/cron_update_players.log
    changes_made=true
  fi
else
  echo "❌ Failed to fetch or parse achievements.json (response: $(cat "$temp_file" | head -c 200))" >> /home/carcassonne-gg/cron_update_players.log
  rm "$temp_file"
fi

if git diff --cached --quiet; then
  echo "No updates" >> /home/carcassonne-gg/cron_update_players.log
else
  git commit -m "Update json-files from server"
  git pull --rebase origin main
  git push origin main
fi
