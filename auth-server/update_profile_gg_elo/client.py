from __future__ import annotations

import json
from urllib.request import Request, urlopen


DEFAULT_PLAYERS_URL = (
    "https://zaharik-ua.github.io/carcassonne-gg/json-data/list_of_players.json"
)


class PlayersJsonClient:
    def __init__(self, *, url: str = DEFAULT_PLAYERS_URL, timeout_seconds: float = 30) -> None:
        self.url = str(url).strip()
        self.timeout_seconds = float(timeout_seconds)

    def fetch(self) -> dict:
        request = Request(
            self.url,
            headers={"User-Agent": "carcassonne-gg-elo-sync/1.0"},
        )
        with urlopen(request, timeout=self.timeout_seconds) as response:
            payload = json.load(response)
        if not isinstance(payload, dict):
            raise ValueError("Players JSON root must be an object")
        return payload
