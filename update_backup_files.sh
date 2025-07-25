#!/bin/bash

cd /home/carcassonne-gg/fallback-data || exit 1
echo "=== $(date) ===" >> /home/carcassonne-gg/cron_backup.log

curl -s https://api.carcassonne.com.ua/matches -o matches.json
git add matches.json
curl -s https://api.carcassonne.com.ua/players -o masters.json
git add masters.json
curl -s https://api.carcassonne.com.ua/achievements -o achievements.json
git add achievements.json
curl -s https://api.carcassonne.com.ua/tournaments -o tournaments.json
git add tournaments.json
curl -s https://api.carcassonne.com.ua/tournaments_list -o tournaments_list.json
git add tournaments_list.json
git commit -m "Update json-files from server" || exit 0
git push origin main
