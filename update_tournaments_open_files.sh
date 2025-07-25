#!/bin/bash

cd /home/carcassonne-gg/json-data || exit 1
echo "=== $(date) ===" >> /home/carcassonne-gg/cron_backup.log

curl -s https://api.carcassonne.com.ua/tournaments -o tournaments-open.json
git add tournaments-open.json
curl -s https://api.carcassonne.com.ua/tournaments_list -o tournaments-list.json
git add tournaments-list.json

git commit -m "Update json-files from server" || exit 0
git push origin main
