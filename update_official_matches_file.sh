	#!/bin/bash

cd /home/carcassonne-gg/json-data || exit 1
echo "=== $(date '+%Y-%m-%d %H:%M:%S') ===" >> /home/carcassonne-gg/cron_update_official.log

changes_made=false

temp_file=$(mktemp)
curl -s https://api.carcassonne.com.ua/public/official_matches -o "$temp_file"
if grep -q '"status"[[:space:]]*:[[:space:]]*"success"' "$temp_file"; then
  mv "$temp_file" official_matches.json
  if ! git diff --quiet official_matches.json; then
    git add official_matches.json
    echo "Updated: official_matches.json" >> /home/carcassonne-gg/cron_update_official.log
    changes_made=true
  fi
else
  echo "❌ Failed to fetch or parse official_matches.json (response: $(cat "$temp_file" | head -c 200))" >> /home/carcassonne-gg/cron_update_official.log
  rm "$temp_file"
fi

if [ "$changes_made" = true ]; then
  git commit -m "Update json-files from server"

  if ! git push origin main; then
    echo "🔁 Push failed. Attempting to rebase and retry..." >> /home/carcassonne-gg/cron_update_official.log

    # Save any remaining changes before rebase
    git add .
    git commit -m "Auto-commit before rebase" || echo "No changes to commit"

    git pull --rebase origin main

    git push origin main
    if [ $? -eq 0 ]; then
      echo "✅ Push successful after rebase" >> /home/carcassonne-gg/cron_update_official.log
    else
      echo "❌ Push still failed after rebase" >> /home/carcassonne-gg/cron_update_official.log
    fi
  else
    echo "✅ Push successful" >> /home/carcassonne-gg/cron_update_official.log
  fi
else
  echo "No updates" >> /home/carcassonne-gg/cron_update_official.log
fi
