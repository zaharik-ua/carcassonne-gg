from __future__ import annotations

import sys
import re
import threading
import time
from typing import Optional

import requests
from selenium.common.exceptions import InvalidSessionIdException, WebDriverException

from .bga_login import BGACredential, get_bga_credentials, login_if_needed
from .config import BASE_URL, MIN_INTERVAL_MS, REQUEST_TIMEOUT_SECONDS, TOKEN_TTL_SECONDS, USER_AGENT
from .selenium_driver import BusyError, DriverStartupError, driver_manager

_lock = threading.Lock()
_session: requests.Session | None = None
_token: str | None = None
_last_refresh: float = 0.0
_last_request_ts: float = 0.0
_credential_index: int = 0
_credential_label: str | None = None


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


def _credential_cycle(credentials: list[BGACredential], *, rotate_account: bool) -> list[tuple[int, BGACredential]]:
    if not credentials:
        return []

    start_index = _credential_index % len(credentials)
    if rotate_account and len(credentials) > 1:
        start_index = (start_index + 1) % len(credentials)

    ordered: list[tuple[int, BGACredential]] = []
    for offset in range(len(credentials)):
        index = (start_index + offset) % len(credentials)
        ordered.append((index, credentials[index]))
    return ordered


def refresh_http_session(reason: str = "startup", *, rotate_account: bool = False) -> tuple[requests.Session, str]:
    global _session, _token, _last_refresh, _credential_index, _credential_label

    credentials = get_bga_credentials()
    if not credentials:
        raise RuntimeError("Missing BGA credentials. Configure BGA_EMAIL/BGA_PASSWORD and optional BGA_EMAIL_N/BGA_PASSWORD_N.")

    attempts = 3
    cycle = _credential_cycle(credentials, rotate_account=rotate_account)
    last_error: Exception | None = None

    for credential_index, credential in cycle:
        recovered_after_driver_recreate = False
        for attempt in range(1, attempts + 1):
            try:
                with driver_manager.use_driver(f"http_refresh_{int(time.time())}") as driver:
                    driver.get(f"{BASE_URL}/gamestats")
                    if "/account" in driver.current_url:
                        login_if_needed(driver, credential)
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
                    _credential_index = credential_index
                    _credential_label = credential.label
                    if recovered_after_driver_recreate:
                        print(
                            f"♻️ Selenium driver recreated successfully; BGA HTTP session recovered "
                            f"({reason}, account={credential.label})",
                            file=sys.stderr,
                            flush=True,
                        )
                    print(
                        f"✅ BGA HTTP session refreshed ({reason}, account={credential.label})",
                        file=sys.stderr,
                        flush=True,
                    )
                    return sess, token

            except BusyError as exc:
                last_error = exc
                if str(exc) == "warming_up" and attempt < attempts:
                    time.sleep(2)
                    continue
                raise
            except DriverStartupError as exc:
                last_error = exc
                raise
            except (InvalidSessionIdException, WebDriverException) as exc:
                last_error = exc
                try:
                    driver_manager.close_driver()
                except Exception:
                    pass
                _session = None
                _token = None
                _last_refresh = 0.0
                if attempt < attempts:
                    recovered_after_driver_recreate = True
                    print(
                        f"⚠️ Selenium session refresh failed ({reason}, account={credential.label}), "
                        f"recreating driver ({attempt}/{attempts}): {exc}",
                        file=sys.stderr,
                        flush=True,
                    )
                    time.sleep(min(5, attempt))
                    continue
                break
            except Exception as exc:
                last_error = exc
                _session = None
                _token = None
                _last_refresh = 0.0
                print(
                    f"⚠️ BGA HTTP session refresh failed ({reason}, account={credential.label}): {exc}",
                    file=sys.stderr,
                    flush=True,
                )
                break

        if len(credentials) > 1:
            print(
                f"🔁 Switching BGA account after refresh failure ({reason}, account={credential.label})",
                file=sys.stderr,
                flush=True,
            )

    if last_error is not None:
        raise RuntimeError(f"Failed to refresh BGA HTTP session: {last_error}") from last_error
    raise RuntimeError("Failed to refresh BGA HTTP session")


def get_http_session(force_refresh: bool = False) -> tuple[requests.Session, str]:
    global _session, _token
    with _lock:
        if _session is None or _token is None or force_refresh or _is_expired():
            refresh_http_session(reason="forced" if force_refresh else "expired")
        return _session, _token


def rotate_http_session(reason: str) -> tuple[requests.Session, str]:
    with _lock:
        return refresh_http_session(reason=reason, rotate_account=True)


def snapshot_session() -> tuple[dict, dict, str]:
    session, token = get_http_session()
    return session.cookies.copy(), dict(session.headers), token


def current_account_label() -> str | None:
    return _credential_label


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
