#!/bin/bash

cd /home/carcassonne-gg/json-data || exit 1
echo "=== $(date) ===" >> /home/carcassonne-gg/cron_backup.log

curl -s https://api.carcassonne.com.ua/players -o masters.json
git add masters.json
curl -s https://api.carcassonne.com.ua/achievements -o achievements.json
git add achievements.json
git commit -m "Update json-files from server" || exit 0
git push origin main
