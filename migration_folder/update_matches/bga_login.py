from __future__ import annotations

import os
import time

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    def load_dotenv() -> None:
        return None
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

load_dotenv()

BGA_EMAIL = os.getenv("BGA_EMAIL")
BGA_PASSWORD = os.getenv("BGA_PASSWORD")


def login_if_needed(driver) -> None:
    if not BGA_EMAIL or not BGA_PASSWORD:
        raise RuntimeError("Missing BGA_EMAIL or BGA_PASSWORD")

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

    visible_inputs[0].send_keys(BGA_EMAIL)

    wait.until(lambda d: any(
        el.is_displayed() and el.is_enabled()
        for el in d.find_elements(By.CSS_SELECTOR, "a.bga-button.bga-button--blue")
    ))
    buttons = driver.find_elements(By.CSS_SELECTOR, "a.bga-button.bga-button--blue")
    next_button = next((btn for btn in buttons if btn.is_displayed() and btn.is_enabled()), None)
    if not next_button:
        raise RuntimeError("BGA login step 'Next' button not found")
    next_button.click()

    password_input = wait.until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, "input[type='password']"))
    )
    password_input.send_keys(BGA_PASSWORD)

    wait.until(lambda d: any(
        el.is_displayed() and el.is_enabled()
        for el in d.find_elements(By.CSS_SELECTOR, "a.bga-button.bga-button--blue")
    ))
    buttons = driver.find_elements(By.CSS_SELECTOR, "a.bga-button.bga-button--blue")
    login_button = next((btn for btn in buttons if btn.is_displayed() and btn.is_enabled()), None)
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
