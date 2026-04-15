from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv() -> None:
        return None

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

load_dotenv()


@dataclass(frozen=True)
class BGACredential:
    email: str
    password: str
    label: str


def _mask_email(email: str) -> str:
    local, sep, domain = email.partition("@")
    if not sep:
        return email[:2] + "***" if len(email) > 2 else "***"
    visible_local = local[:2] if len(local) > 2 else local[:1]
    return f"{visible_local}***@{domain}"


def get_bga_credentials() -> list[BGACredential]:
    credentials: list[BGACredential] = []

    primary_email = (os.getenv("BGA_EMAIL") or "").strip()
    primary_password = os.getenv("BGA_PASSWORD") or ""
    if primary_email and primary_password:
        credentials.append(
            BGACredential(
                email=primary_email,
                password=primary_password,
                label=f"primary:{_mask_email(primary_email)}",
            )
        )

    indexed_emails: list[tuple[int, str]] = []
    for key, value in os.environ.items():
        match = re.fullmatch(r"BGA_EMAIL_(\d+)", key)
        if not match:
            continue
        email = str(value or "").strip()
        if not email:
            continue
        indexed_emails.append((int(match.group(1)), email))

    for index, email in sorted(indexed_emails):
        password = os.getenv(f"BGA_PASSWORD_{index}") or ""
        if not password:
            continue
        credentials.append(
            BGACredential(
                email=email,
                password=password,
                label=f"reserve{index}:{_mask_email(email)}",
            )
        )

    return credentials


def login_if_needed(driver, credential: BGACredential) -> None:
    if not credential.email or not credential.password:
        raise RuntimeError("Missing BGA credential email/password")

    driver.get("https://boardgamearena.com/account")

    try:
        WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, ".cc-window .cc-dismiss"))
        ).click()
    except Exception:
        pass

    wait = WebDriverWait(driver, 10)

    if "welcome" in driver.current_url:
        _click_stay_connected_if_present(driver)
        return

    email_inputs = driver.find_elements(By.CSS_SELECTOR, "input[name='email']")
    visible_inputs = [
        el for el in email_inputs
        if el.get_attribute("type") != "hidden" and el.is_displayed()
    ]
    if not visible_inputs:
        raise RuntimeError("No active email input found on BGA login page")

    visible_inputs[0].send_keys(credential.email)

    wait.until(lambda d: any(
        el.is_displayed() and el.is_enabled()
        for el in d.find_elements(By.CSS_SELECTOR, "a.bga-button.bga-button--blue")
    ))
    next_button = next(
        (btn for btn in driver.find_elements(By.CSS_SELECTOR, "a.bga-button.bga-button--blue")
         if btn.is_displayed() and btn.is_enabled()),
        None,
    )
    if not next_button:
        raise RuntimeError("BGA login step 'Next' button not found")
    next_button.click()

    password_input = wait.until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, "input[type='password']"))
    )
    password_input.send_keys(credential.password)

    wait.until(lambda d: any(
        el.is_displayed() and el.is_enabled()
        for el in d.find_elements(By.CSS_SELECTOR, "a.bga-button.bga-button--blue")
    ))
    login_button = next(
        (btn for btn in driver.find_elements(By.CSS_SELECTOR, "a.bga-button.bga-button--blue")
         if btn.is_displayed() and btn.is_enabled()),
        None,
    )
    if not login_button:
        raise RuntimeError("BGA login button not found")
    login_button.click()

    wait.until(EC.presence_of_element_located((By.ID, "topbar")))
    time.sleep(2)
    _click_stay_connected_if_present(driver)


def _click_stay_connected_if_present(driver) -> None:
    try:
        stay_connected = driver.find_element(By.ID, "stay_connected")
        if stay_connected.is_displayed():
            yes_button = stay_connected.find_element(By.ID, "stay_connected_yes")
            if yes_button.is_displayed() and yes_button.is_enabled():
                yes_button.click()
                time.sleep(1)
    except Exception:
        pass
