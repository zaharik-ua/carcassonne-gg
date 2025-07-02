#!/bin/bash

curl -s https://api.carcassonne.com.ua/matches -o matches.json
git add matches.json
curl -s https://api.carcassonne.com.ua/players -o masters.json
git add masters.json
git commit -m "Update json-files from server" || exit 0
git push origin main
