from __future__ import annotations

import os


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


BASE_URL = "https://boardgamearena.com"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

PLAYER_ELO_BATCH_SIZE = env_int("PLAYER_ELO_BATCH_SIZE", 80)
PLAYER_ELO_HTTP_TIMEOUT_SECONDS = env_int("PLAYER_ELO_HTTP_TIMEOUT_SECONDS", 20)
PLAYER_ELO_MIN_INTERVAL_MS = env_int("PLAYER_ELO_MIN_INTERVAL_MS", 250)
