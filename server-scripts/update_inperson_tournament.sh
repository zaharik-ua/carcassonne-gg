#!/bin/bash

# Change this in one place to switch tournament, e.g. ua2025, wc2025, etc.
TOURNAMENT_ID="${1:-wc2025}"

cd /home/carcassonne-gg/json-data || exit 1
echo "=== $(date '+%Y-%m-%d %H:%M:%S') ===" >> /home/carcassonne-gg/cron_update_inperson.log

changes_made=false

temp_file=$(mktemp)
curl -s "https://api.carcassonne.com.ua/public/${TOURNAMENT_ID}" -o "$temp_file"
if grep -q '"status"[[:space:]]*:[[:space:]]*"success"' "$temp_file"; then
  mv "$temp_file" "${TOURNAMENT_ID}.json"
  if ! git diff --quiet "${TOURNAMENT_ID}.json"; then
    git add "${TOURNAMENT_ID}.json"
    echo "Updated: ${TOURNAMENT_ID}.json" >> /home/carcassonne-gg/cron_update_inperson.log
    changes_made=true
  fi
else
  echo "❌ Failed to fetch or parse ${TOURNAMENT_ID}.json (response: $(cat "$temp_file" | head -c 200))" >> /home/carcassonne-gg/cron_update_inperson.log
  rm "$temp_file"
fi

if [ "$changes_made" = true ]; then
  git commit -m "Update json-files from server"

  if ! git push origin main; then
    echo "🔁 Push failed. Attempting to rebase and retry..." >> /home/carcassonne-gg/cron_update_inperson.log

    # Save any remaining changes before rebase
    git add .
    git commit -m "Auto-commit before rebase" || echo "No changes to commit"

    git pull --rebase origin main

    git push origin main
    if [ $? -eq 0 ]; then
      echo "✅ Push successful after rebase" >> /home/carcassonne-gg/cron_update_inperson.log
    else
      echo "❌ Push still failed after rebase" >> /home/carcassonne-gg/cron_update_inperson.log
    fi
  else
    echo "✅ Push successful" >> /home/carcassonne-gg/cron_update_inperson.log
  fi
else
  echo "No updates" >> /home/carcassonne-gg/cron_update_inperson.log
fi
