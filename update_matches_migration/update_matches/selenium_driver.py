from __future__ import annotations

import os
import random
import shutil
import subprocess
import time
from contextlib import contextmanager
from threading import Condition, Lock, Thread

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


IDLE_TIMEOUT = _env_int("DRIVER_IDLE_SECONDS", 180)
CHROME_STARTUP_TIMEOUT = _env_int("CHROME_STARTUP_TIMEOUT", 20)
DRIVER_ACQUIRE_TIMEOUT = _env_int("DRIVER_ACQUIRE_TIMEOUT", 5)
SELENIUM_MAX_QUEUE = _env_int("SELENIUM_MAX_QUEUE", 2)
PAGE_LOAD_TIMEOUT = _env_int("CHROME_PAGE_LOAD_TIMEOUT", 30)

last_activity_time = time.time()


def _first_existing_path(candidates: list[str]) -> str | None:
    for path in candidates:
        if path and os.path.exists(path):
            return path
    return None


class BusyError(Exception):
    def __init__(self, message: str, retry_after: int = 5):
        super().__init__(message)
        self.retry_after = retry_after


class DriverStartupError(Exception):
    pass


def _resolve_chromedriver_path() -> str:
    env_path = os.getenv("CHROMEDRIVER_PATH")
    if env_path:
        return env_path

    which_path = shutil.which("chromedriver")
    if which_path:
        return which_path

    detected = _first_existing_path([
        "/opt/homebrew/bin/chromedriver",
        "/usr/local/bin/chromedriver",
        "/usr/bin/chromedriver",
    ])
    if detected:
        return detected

    raise DriverStartupError("chromedriver not found; set CHROMEDRIVER_PATH")


def _resolve_chrome_binary_path() -> str:
    env_path = os.getenv("CHROME_BINARY_PATH")
    if env_path:
        return env_path

    detected = _first_existing_path([
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
        "/opt/google/chrome/chrome",
        "/usr/bin/google-chrome",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
    ])
    if detected:
        return detected

    raise DriverStartupError("Chrome/Chromium binary not found; set CHROME_BINARY_PATH")


class DriverManager:
    def __init__(self) -> None:
        self.driver = None
        self.driver_lock = Lock()
        self._create_lock = Lock()
        self._create_cond = Condition(self._create_lock)
        self._creating = False
        self._last_create_error = None
        self._waiters = 0

    def _alive(self) -> bool:
        try:
            _ = self.driver.current_url
            return True
        except Exception:
            return False

    def create_driver(self):
        service = Service(_resolve_chromedriver_path())
        chrome_binary = _resolve_chrome_binary_path()

        attempts = 3
        last_exc = None
        for attempt in range(attempts):
            chrome_options = Options()
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--no-first-run")
            chrome_options.add_argument("--disable-background-networking")
            chrome_options.add_argument("--metrics-recording-only")
            chrome_options.add_argument("--mute-audio")
            chrome_options.add_argument("--headless=new" if attempt == 0 else "--headless")
            chrome_options.binary_location = chrome_binary

            holder = {"driver": None, "error": None}

            def _start():
                try:
                    holder["driver"] = webdriver.Chrome(service=service, options=chrome_options)
                except Exception as exc:
                    holder["error"] = exc

            th = Thread(target=_start, daemon=True)
            th.start()
            th.join(timeout=CHROME_STARTUP_TIMEOUT)

            if th.is_alive():
                self._kill_stray_processes()
                last_exc = TimeoutError(
                    f"Chrome startup timed out after {CHROME_STARTUP_TIMEOUT}s"
                )
            elif holder["driver"] is not None:
                self.driver = holder["driver"]
                try:
                    self.driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
                    self.driver.set_script_timeout(PAGE_LOAD_TIMEOUT)
                except Exception:
                    pass
                return self.driver
            else:
                last_exc = holder["error"] or RuntimeError("Unknown Chrome startup failure")

            if attempt < attempts - 1:
                time.sleep(min(10, (2 ** attempt) + random.uniform(0, 1)))

        raise DriverStartupError(str(last_exc))

    def ensure_driver(self):
        with self._create_lock:
            if self.driver is not None and self._alive():
                return self.driver

            if self._creating:
                waited = self._create_cond.wait(timeout=DRIVER_ACQUIRE_TIMEOUT)
                if not waited:
                    raise BusyError("driver_creation_timeout", retry_after=5)
                if self.driver is not None and self._alive():
                    return self.driver
                if self._last_create_error:
                    raise DriverStartupError(str(self._last_create_error))

            self._creating = True
            self._last_create_error = None
            try:
                return self.create_driver()
            except Exception as exc:
                self._last_create_error = exc
                raise
            finally:
                self._creating = False
                self._create_cond.notify_all()

    @contextmanager
    def use_driver(self, req_id: str = "-"):
        if self.driver_lock.locked():
            if self._waiters >= SELENIUM_MAX_QUEUE:
                raise BusyError("selenium_busy", retry_after=5)
            self._waiters += 1

        acquired = self.driver_lock.acquire(timeout=DRIVER_ACQUIRE_TIMEOUT)
        if not acquired:
            if self._waiters > 0:
                self._waiters -= 1
            raise BusyError("acquire_timeout", retry_after=5)

        try:
            if self._waiters > 0:
                self._waiters -= 1
            driver = self.ensure_driver()
            update_last_activity_time()
            yield driver
        finally:
            self.driver_lock.release()

    def close_driver(self) -> None:
        if self.driver:
            self.driver.quit()
            self.driver = None

    @staticmethod
    def _kill_stray_processes() -> None:
        try:
            subprocess.run(["pkill", "-x", "chromedriver"], check=False)
            subprocess.run(["pkill", "-x", "chromium"], check=False)
        except Exception:
            pass


driver_manager = DriverManager()


def update_last_activity_time() -> None:
    global last_activity_time
    last_activity_time = time.time()


def monitor_idle_time() -> None:
    global last_activity_time
    while True:
        if driver_manager.driver is None:
            time.sleep(30)
            continue

        idle_time = time.time() - last_activity_time
        if idle_time > IDLE_TIMEOUT and not driver_manager.driver_lock.locked():
            driver_manager.close_driver()
        time.sleep(30)


Thread(target=monitor_idle_time, daemon=True).start()
