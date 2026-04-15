import os
import math


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
EMPTY_TABLES_ERROR_LIMIT = env_int("BGA_EMPTY_TABLES_ERROR_LIMIT", 3)
EMPTY_TABLES_BATCH_FAILURE_RATIO = float(os.getenv("BGA_EMPTY_TABLES_BATCH_FAILURE_RATIO", "0.75"))
EMPTY_TABLES_BATCH_FAILURE_MIN = env_int("BGA_EMPTY_TABLES_BATCH_FAILURE_MIN", 15)


def empty_tables_batch_failure_threshold(batch_size: int) -> int:
    if batch_size <= 0:
        return EMPTY_TABLES_BATCH_FAILURE_MIN
    ratio_threshold = max(1, int(math.ceil(batch_size * max(0.0, EMPTY_TABLES_BATCH_FAILURE_RATIO))))
    return min(batch_size, max(EMPTY_TABLES_BATCH_FAILURE_MIN, ratio_threshold))
