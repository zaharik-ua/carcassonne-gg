from __future__ import annotations

import sys
import re
import threading
import time
from typing import Optional

import requests
from selenium.common.exceptions import InvalidSessionIdException, WebDriverException

from .bga_login import login_if_needed
from .config import BASE_URL, MIN_INTERVAL_MS, REQUEST_TIMEOUT_SECONDS, TOKEN_TTL_SECONDS, USER_AGENT
from .selenium_driver import BusyError, DriverStartupError, driver_manager

_lock = threading.Lock()
_session: requests.Session | None = None
_token: str | None = None
_last_refresh: float = 0.0
_last_request_ts: float = 0.0


def _extract_request_token(html: str) -> Optional[str]:
    if not html:
        return None
    patterns = [
        r'requestToken"\s*:\s*"([^"]+)"',
        r"requestToken'\s*:\s*'([^']+)'",
        r"requestToken\s*=\s*\"([^\"]+)\"",
        r"requestToken\s*=\s*'([^']+)'",
    ]
    for pattern in patterns:
        match = re.search(pattern, html)
        if match:
            return match.group(1)
    return None


def _cookies_to_session(cookies: list[dict]) -> requests.Session:
    sess = requests.Session()
    sess.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    for cookie in cookies:
        sess.cookies.set(
            cookie.get("name"),
            cookie.get("value"),
            domain=cookie.get("domain"),
            path=cookie.get("path", "/"),
        )
    return sess


def _is_expired() -> bool:
    if _last_refresh <= 0:
        return True
    return (time.time() - _last_refresh) > TOKEN_TTL_SECONDS


def refresh_http_session(reason: str = "startup") -> tuple[requests.Session, str]:
    global _session, _token, _last_refresh

    attempts = 3
    for attempt in range(1, attempts + 1):
        try:
            with driver_manager.use_driver(f"http_refresh_{int(time.time())}") as driver:
                driver.get(f"{BASE_URL}/gamestats")
                if "/account" in driver.current_url:
                    login_if_needed(driver)
                    driver.get(f"{BASE_URL}/gamestats")

                token = None
                try:
                    token = driver.execute_script(
                        "return window.bgaConfig && bgaConfig.requestToken ? bgaConfig.requestToken : null;"
                    )
                except Exception:
                    token = None

                if not token:
                    token = _extract_request_token(driver.page_source)
                if not token:
                    raise RuntimeError("Failed to extract bgaConfig.requestToken from /gamestats")

                sess = _cookies_to_session(driver.get_cookies())
                _session = sess
                _token = token
                _last_refresh = time.time()
                print(f"✅ BGA HTTP session refreshed ({reason})", file=sys.stderr, flush=True)
                return sess, token

        except BusyError as exc:
            if str(exc) == "warming_up" and attempt < attempts:
                time.sleep(2)
                continue
            raise
        except DriverStartupError:
            raise
        except (InvalidSessionIdException, WebDriverException):
            raise

    raise RuntimeError("Failed to refresh BGA HTTP session")


def get_http_session(force_refresh: bool = False) -> tuple[requests.Session, str]:
    global _session, _token
    with _lock:
        if _session is None or _token is None or force_refresh or _is_expired():
            refresh_http_session(reason="forced" if force_refresh else "expired")
        return _session, _token


def snapshot_session() -> tuple[dict, dict, str]:
    session, token = get_http_session()
    return session.cookies.copy(), dict(session.headers), token


def request_json(
    path: str,
    params: Optional[dict] = None,
    method: str = "GET",
    data: Optional[dict] = None,
    headers: Optional[dict] = None,
    retry_on_auth: bool = True,
) -> dict:
    _throttle()
    session, token = get_http_session()
    merged_headers = {
        "X-Requested-With": "XMLHttpRequest",
        "X-Request-Token": token,
    }
    if headers:
        merged_headers.update(headers)

    url = f"{BASE_URL}{path}"
    response = session.request(
        method,
        url,
        params=params,
        data=data,
        headers=merged_headers,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )

    if response.status_code in (401, 403) and retry_on_auth:
        refresh_http_session(reason=f"http_{response.status_code}")
        session, token = get_http_session()
        merged_headers["X-Request-Token"] = token
        _throttle()
        response = session.request(
            method,
            url,
            params=params,
            data=data,
            headers=merged_headers,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

    response.raise_for_status()
    return response.json()


def _throttle() -> None:
    global _last_request_ts
    if MIN_INTERVAL_MS <= 0:
        return
    with _lock:
        now = time.monotonic()
        elapsed_ms = (now - _last_request_ts) * 1000.0
        wait_ms = MIN_INTERVAL_MS - elapsed_ms
        if wait_ms > 0:
            time.sleep(wait_ms / 1000.0)
        _last_request_ts = time.monotonic()
