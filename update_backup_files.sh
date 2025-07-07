#!/bin/bash

cd /home/carcassonne-gg-backups/fallback-data || exit 1
echo "=== $(date) ===" >> /home/carcassonne-gg-backups/cron_backup.log

curl -s https://api.carcassonne.com.ua/matches -o matches.json
git add matches.json
curl -s https://api.carcassonne.com.ua/players -o masters.json
git add masters.json
curl -s https://api.carcassonne.com.ua/achievements -o achievements.json
git add achievements.json
curl -s https://api.carcassonne.com.ua/tournaments -o tournaments.json
git add tournaments.json
git commit -m "Update json-files from server" || exit 0
git push origin main
