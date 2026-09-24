from __future__ import annotations

import io
import unittest
from contextlib import contextmanager, redirect_stderr
from unittest.mock import patch

try:
    from . import http_session
    from .bga_login import BGACredential
except ModuleNotFoundError as exc:
    if exc.name in {"requests", "selenium"}:
        raise unittest.SkipTest(
            "HTTP session tests require requests and selenium"
        ) from exc
    raise


class FakeDriver:
    current_url = "https://boardgamearena.com/gamestats"
    page_source = "<html></html>"

    def __init__(self) -> None:
        self.visited: list[str] = []
        self.deleted_cookie_count = 0

    def get(self, url: str) -> None:
        self.visited.append(url)
        self.current_url = url

    def execute_script(self, _script: str):
        return None

    def delete_all_cookies(self) -> None:
        self.deleted_cookie_count += 1

    def get_cookies(self) -> list[dict]:
        return []


class FakeDriverManager:
    def __init__(self, driver: FakeDriver) -> None:
        self.driver = driver

    @contextmanager
    def use_driver(self, _reason: str):
        yield self.driver


class HttpSessionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.previous_state = (
            http_session._session,
            http_session._token,
            http_session._last_refresh,
            http_session._credential_index,
            http_session._credential_label,
        )
        http_session._session = None
        http_session._token = None
        http_session._last_refresh = 0.0
        http_session._credential_index = 0
        http_session._credential_label = None

    def tearDown(self) -> None:
        (
            http_session._session,
            http_session._token,
            http_session._last_refresh,
            http_session._credential_index,
            http_session._credential_label,
        ) = self.previous_state

    def test_waits_for_request_token_from_runtime_config(self) -> None:
        driver = FakeDriver()
        tokens = iter([None, None, "token-ready"])
        driver.execute_script = lambda _script: next(tokens)

        class ImmediateWait:
            def __init__(self, target, _timeout, poll_frequency):
                self.target = target
                self.poll_frequency = poll_frequency

            def until(self, condition):
                for _ in range(3):
                    value = condition(self.target)
                    if value:
                        return value
                raise AssertionError("token was not returned")

        with patch.object(http_session, "WebDriverWait", ImmediateWait):
            token = http_session._wait_for_request_token(driver)

        self.assertEqual(token, "token-ready")

    def test_automatic_rotation_excludes_replay_standby_account(self) -> None:
        credentials = [
            BGACredential("one@example.com", "secret", "primary:one"),
            BGACredential("two@example.com", "secret", "reserve2:two"),
            BGACredential(
                "four@example.com",
                "secret",
                "reserve4:four",
                replay_standby=True,
            ),
        ]

        automatic = http_session._credential_cycle(
            credentials,
            rotate_account=False,
        )
        explicitly_selected = http_session._credential_cycle(
            credentials,
            rotate_account=False,
            account_label="reserve4:four",
        )

        self.assertEqual([credential.label for _, credential in automatic], [
            "primary:one",
            "reserve2:two",
        ])
        self.assertEqual(
            [credential.label for _, credential in explicitly_selected],
            ["reserve4:four"],
        )

    def test_missing_request_token_retries_refresh_before_succeeding(self) -> None:
        credential = BGACredential(
            email="player@example.com",
            password="secret",
            label="primary:pl***@example.com",
        )
        driver = FakeDriver()

        with (
            patch.object(http_session, "get_bga_credentials", return_value=[credential]),
            patch.object(http_session, "driver_manager", FakeDriverManager(driver)),
            patch.object(http_session, "login_if_needed"),
            patch.object(
                http_session,
                "_wait_for_request_token",
                side_effect=[None, "token-ready"],
            ) as wait_for_token,
            patch.object(http_session.time, "sleep"),
            redirect_stderr(io.StringIO()),
        ):
            _session, token = http_session.refresh_http_session(
                reason="test",
                account_label=credential.label,
            )

        self.assertEqual(token, "token-ready")
        self.assertEqual(wait_for_token.call_count, 2)
        self.assertEqual(http_session._credential_label, credential.label)

    def test_missing_request_token_fails_only_after_all_refresh_attempts(self) -> None:
        credential = BGACredential(
            email="player@example.com",
            password="secret",
            label="primary:pl***@example.com",
        )
        driver = FakeDriver()

        with (
            patch.object(http_session, "get_bga_credentials", return_value=[credential]),
            patch.object(http_session, "driver_manager", FakeDriverManager(driver)),
            patch.object(http_session, "login_if_needed"),
            patch.object(
                http_session,
                "_wait_for_request_token",
                return_value=None,
            ) as wait_for_token,
            patch.object(http_session.time, "sleep"),
            redirect_stderr(io.StringIO()),
        ):
            with self.assertRaisesRegex(RuntimeError, "after waiting 10 seconds"):
                http_session.refresh_http_session(
                    reason="test",
                    account_label=credential.label,
                )

        self.assertEqual(
            wait_for_token.call_count,
            http_session.SESSION_REFRESH_ATTEMPTS,
        )


if __name__ == "__main__":
    unittest.main()
