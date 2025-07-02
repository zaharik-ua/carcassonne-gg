#!/bin/bash

curl -s https://api.carcassonne.com.ua/matches -o matches.json
git add matches.json
git commit -m "Update matches.json from server" || exit 0
git push origin main
