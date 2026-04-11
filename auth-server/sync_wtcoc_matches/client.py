from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen


DEFAULT_WTCOC_API_BASE_URL = "https://www.carcassonne.cat/wtcoc/api"
DEFAULT_WTCOC_API_TOKEN = "y8u93klm876wrfu45lrt"


@dataclass(frozen=True)
class WtcocApiResponse:
    source: str
    payload: dict[str, Any]


class WtcocApiClient:
    def __init__(self, *, token: str, base_url: str = DEFAULT_WTCOC_API_BASE_URL, timeout_seconds: int = 30) -> None:
        normalized_token = str(token or "").strip()
        if not normalized_token:
            raise ValueError("WTCOC API token is required")
        self.token = normalized_token
        self.base_url = str(base_url or DEFAULT_WTCOC_API_BASE_URL).rstrip("/")
        self.timeout_seconds = int(timeout_seconds)

    def fetch_calendar(self) -> WtcocApiResponse:
        return self._fetch_endpoint("calendar")

    def fetch_playoff(self) -> WtcocApiResponse:
        return self._fetch_endpoint("playoff")

    def _fetch_endpoint(self, source: str) -> WtcocApiResponse:
        query = urlencode({"token": self.token})
        url = f"{self.base_url}/{source}/index.php?{query}"
        try:
            with urlopen(url, timeout=self.timeout_seconds) as response:
                raw_text = response.read().decode("utf-8", errors="replace")
        except HTTPError as exc:  # pragma: no cover
            raise RuntimeError(f"WTCOC API HTTP error for {source}: {exc.code}") from exc
        except URLError as exc:  # pragma: no cover
            raise RuntimeError(f"WTCOC API network error for {source}: {exc.reason}") from exc

        sanitized_text = self._strip_php_warnings(raw_text)
        try:
            payload = json.loads(sanitized_text)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"WTCOC API returned invalid JSON for {source}: {exc}") from exc

        if not isinstance(payload, dict):
            raise RuntimeError(f"WTCOC API returned unexpected payload type for {source}")
        return WtcocApiResponse(source=source, payload=payload)

    @staticmethod
    def _strip_php_warnings(text: str) -> str:
        lines = []
        for line in str(text or "").splitlines():
            stripped = line.lstrip()
            if stripped.startswith("Warning: ") or stripped.startswith("Notice: "):
                continue
            lines.append(line)
        return "\n".join(lines).strip()
