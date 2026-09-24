import os


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


BASE_URL = "https://boardgamearena.com"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

DEBUG_LOG = env_bool("APP_DEBUG_LOG", False)
HTTP_WORKERS = env_int("BGA_HTTP_WORKERS", 6)
REQUEST_TIMEOUT_SECONDS = env_int("BGA_HTTP_TIMEOUT_SECONDS", 20)
TOKEN_TTL_SECONDS = env_int("BGA_TOKEN_TTL_SECONDS", 86400)
MIN_INTERVAL_MS = env_int("BGA_HTTP_MIN_INTERVAL_MS", 250)
MATCH_UPDATE_BATCH_SIZE = env_int("MATCH_UPDATE_BATCH_SIZE", 20)
BGA_REPLAY_TOTAL_LIMIT = env_int("BGA_REPLAY_TOTAL_LIMIT", 80)
BGA_REPLAY_FRESH_RESERVE = env_int("BGA_REPLAY_FRESH_RESERVE", 50)
BGA_REPLAY_HISTORICAL_LIMIT = env_int("BGA_REPLAY_HISTORICAL_LIMIT", 30)
BGA_REPLAY_MAX_TOTAL_LIMIT = env_int("BGA_REPLAY_MAX_TOTAL_LIMIT", 100)
BGA_REPLAY_COOLDOWN_HOURS = env_int("BGA_REPLAY_COOLDOWN_HOURS", 24)
