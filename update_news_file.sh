#!/bin/bash

cd /home/carcassonne-gg/json-data || exit 1
echo "=== $(date '+%Y-%m-%d %H:%M:%S') ===" >> /home/carcassonne-gg/cron_update_open.log

changes_made=false

temp_file=$(mktemp)
curl -s https://api.carcassonne.com.ua/news -o "$temp_file"
if grep -q '"status"[[:space:]]*:[[:space:]]*"success"' "$temp_file"; then
  mv "$temp_file" news.json
  if ! git diff --quiet news.json; then
    git add news.json
    echo "Updated: news.json" >> /home/carcassonne-gg/cron_update_open.log
    changes_made=true
  fi
else
  echo "❌ Failed to fetch or parse news.json (response: $(cat "$temp_file" | head -c 200))" >> /home/carcassonne-gg/cron_update_open.log
  rm "$temp_file"
fi

if git diff --cached --quiet; then
  echo "No updates" >> /home/carcassonne-gg/cron_update_open.log
else
  git commit -m "Update json-files from server"
  git pull --rebase origin main
  git push origin main
fi