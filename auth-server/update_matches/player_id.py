from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path

import requests

from .config import REQUEST_TIMEOUT_SECONDS

CACHE_DURATION = 60 * 60 * 24


def _cache_path() -> Path:
    raw_path = os.getenv("BGA_PLAYER_ID_CACHE_FILE", "update_matches/.cache/player_id_cache.json")
    path = Path(raw_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def load_cache() -> dict:
    path = _cache_path()
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_cache(cache: dict) -> None:
    path = _cache_path()
    with path.open("w", encoding="utf-8") as f:
        json.dump(cache, f)


def get_player_id(nickname: str) -> int:
    nickname = nickname.strip().lower()
    cache_key = f"playerId-{nickname}"
    current_time = time.time()
    cache = load_cache()

    if cache_key in cache:
        cached = cache[cache_key]
        if current_time - cached["timestamp"] < CACHE_DURATION:
            return int(cached["id"])

    response = requests.get(
        "https://boardgamearena.com/player/player/findplayer.html",
        params={"q": nickname, "start": 0, "count": 999999},
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    text = response.text.strip()
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"<body>(.*?)</body>", text, re.DOTALL)
        if not match:
            raise ValueError("Invalid API response from BGA")
        data = json.loads(match.group(1).strip())

    for user in data.get("items", []):
        if user.get("q", "").lower() == nickname:
            player_id = int(user["id"])
            cache[cache_key] = {"id": player_id, "timestamp": current_time}
            save_cache(cache)
            return player_id

    raise ValueError(f"Player not found: {nickname}")
