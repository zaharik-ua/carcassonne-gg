from __future__ import annotations

import re
import time

import requests

from .config import BASE_URL, PLAYER_ELO_HTTP_TIMEOUT_SECONDS, PLAYER_ELO_MIN_INTERVAL_MS, USER_AGENT
from .models import PlayerEloUpdateRequest, PlayerEloUpdateResult

ELO_PATTERNS = [
    re.compile(r"class=['\"]gamerank_value['\"]\s*>\s*([^<]+)\s*</span>", re.IGNORECASE),
    re.compile(r"class=['\"]gamerank_value['\"][^>]*>\s*([^<]+)\s*</", re.IGNORECASE),
]


class BgaEloClient:
    def __init__(self, timeout_seconds: int = PLAYER_ELO_HTTP_TIMEOUT_SECONDS) -> None:
        self.timeout_seconds = timeout_seconds
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )
        self._last_request_ts = 0.0

    def fetch_player_elo(self, player: PlayerEloUpdateRequest) -> PlayerEloUpdateResult:
        self._throttle()
        url = f"{BASE_URL}/playerstat?id={player.bga_player_id}&game=1"
        try:
            response = self._session.get(url, timeout=self.timeout_seconds)
            response.raise_for_status()
        except requests.RequestException as exc:
            return PlayerEloUpdateResult(
                status="error",
                source_url=url,
                message=f"HTTP error: {exc}",
            )

        raw_elo = extract_elo_from_html(response.text)
        if raw_elo is None:
            return PlayerEloUpdateResult(
                status="error",
                source_url=url,
                message="Elo value not found on playerstat page",
            )

        elo = normalize_elo(raw_elo)
        if elo is None:
            return PlayerEloUpdateResult(
                status="error",
                raw_elo=raw_elo,
                source_url=url,
                message=f"Unable to parse Elo value: {raw_elo}",
            )

        return PlayerEloUpdateResult(
            status="success",
            elo=elo,
            raw_elo=raw_elo,
            source_url=url,
        )

    def _throttle(self) -> None:
        if PLAYER_ELO_MIN_INTERVAL_MS <= 0:
            return
        now = time.monotonic()
        elapsed_ms = (now - self._last_request_ts) * 1000.0
        wait_ms = PLAYER_ELO_MIN_INTERVAL_MS - elapsed_ms
        if wait_ms > 0:
            time.sleep(wait_ms / 1000.0)
        self._last_request_ts = time.monotonic()


def extract_elo_from_html(html: str) -> str | None:
    for pattern in ELO_PATTERNS:
        match = pattern.search(html)
        if match:
            return match.group(1).strip()
    return None


def normalize_elo(raw_elo: str) -> int | None:
    digits = re.sub(r"[^\d]", "", raw_elo or "")
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None
