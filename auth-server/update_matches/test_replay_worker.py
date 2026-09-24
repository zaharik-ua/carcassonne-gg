from __future__ import annotations

import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event

from .game_replay import (
    LOGS_PATH,
    ArchiveMissingError,
    ReplayAccessError,
    ReplayLimitError,
    TemporaryReplayError,
    ensure_game_replays_schema,
)
from .replay_worker import (
    ReplayWorkerAlreadyRunningError,
    enqueue_historical_game_replay,
    replay_worker_lock,
    run_replay_worker,
)
from .replay_budget import replace_replay_budget_overrides


class ReplayWorkerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "auth.sqlite"
        self.lock_path = Path(self.temp_dir.name) / "worker.lock"
        self.now = datetime(2026, 9, 24, 12, 0, tzinfo=timezone.utc)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE games (
                  id TEXT PRIMARY KEY,
                  bga_table_id TEXT,
                  deleted_at TEXT
                )
                """
            )
            ensure_game_replays_schema(conn)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_worker_does_not_fetch_before_next_attempt(self) -> None:
        self._add_game("future", "101")
        self._queue(
            "future",
            next_attempt_at=self.now + timedelta(seconds=1),
        )
        calls: list[str] = []

        summary = self._run(
            lambda _path, game_id, **_kwargs: calls.append(game_id)
        )

        self.assertEqual(calls, [])
        self.assertEqual(summary["due"], 0)
        self.assertEqual(summary["processed"], 0)

    def test_initial_fallback_schedules_one_color_refresh(self) -> None:
        self._add_game("game-1", "101")
        self._queue("game-1")
        calls: list[dict] = []

        summary = self._run(
            lambda _path, _game_id, **kwargs: (
                calls.append(kwargs)
                or {"color_source": "fallback", "account_label": "account-a"}
            )
        )
        row = self._replay("game-1")

        self.assertEqual(summary["ready_fallback_colors"], 1)
        self.assertEqual(summary["deferred"], 1)
        self.assertEqual(row["status"], "ready")
        self.assertEqual(row["retry_reason"], "colors")
        self.assertEqual(row["color_refresh_count"], 0)
        self.assertEqual(
            row["next_attempt_at"],
            "2026-09-24 12:15:00.000000",
        )
        self.assertFalse(calls[0]["force"])
        self.assertFalse(calls[0]["color_refresh"])

    def test_initial_bga_colors_finish_without_another_retry(self) -> None:
        self._add_game("game-1", "101")
        self._queue("game-1")

        summary = self._run(
            lambda *_args, **_kwargs: {
                "color_source": "bga",
                "account_label": "account-a",
            }
        )
        row = self._replay("game-1")

        self.assertEqual(summary["processed"], 1)
        self.assertEqual(summary["ready_bga_colors"], 1)
        self.assertEqual(summary["deferred"], 0)
        self.assertEqual(row["status"], "ready")
        self.assertEqual(row["color_source"], "bga")
        self.assertIsNone(row["retry_reason"])
        self.assertIsNone(row["next_attempt_at"])

    def test_color_refresh_with_bga_colors_finishes_queue(self) -> None:
        self._add_game("game-1", "101")
        self._queue(
            "game-1",
            status="ready",
            retry_reason="colors",
            color_source="fallback",
        )
        calls: list[dict] = []

        summary = self._run(
            lambda _path, _game_id, **kwargs: (
                calls.append(kwargs)
                or {"color_source": "bga", "account_label": "account-a"}
            )
        )
        row = self._replay("game-1")

        self.assertEqual(summary["ready_bga_colors"], 1)
        self.assertEqual(row["status"], "ready")
        self.assertEqual(row["color_source"], "bga")
        self.assertEqual(row["color_refresh_count"], 1)
        self.assertIsNone(row["retry_reason"])
        self.assertIsNone(row["next_attempt_at"])
        self.assertTrue(calls[0]["force"])
        self.assertTrue(calls[0]["color_refresh"])
        self.assertTrue(calls[0]["preserve_existing_fallback"])

    def test_second_fallback_is_final_and_is_not_rescheduled(self) -> None:
        self._add_game("game-1", "101")
        self._queue(
            "game-1",
            status="ready",
            retry_reason="colors",
            color_source="fallback",
        )

        summary = self._run(
            lambda *_args, **_kwargs: {
                "color_source": "fallback",
                "account_label": "account-a",
            }
        )
        row = self._replay("game-1")

        self.assertEqual(summary["ready_fallback_colors"], 1)
        self.assertEqual(summary["deferred"], 0)
        self.assertEqual(row["color_refresh_count"], 1)
        self.assertIsNone(row["retry_reason"])
        self.assertIsNone(row["next_attempt_at"])

    def test_archive_is_requested_once_then_becomes_manual(self) -> None:
        self._add_game("game-1", "101")
        self._queue("game-1")

        def missing_archive(*_args, **_kwargs):
            raise ArchiveMissingError(
                "Cannot find gamenotifs log file",
                endpoint=LOGS_PATH,
                archive_requested=True,
            )

        first = self._run(missing_archive)
        first_row = self._replay("game-1")
        self.assertEqual(first["archives_requested"], 1)
        self.assertEqual(first_row["status"], "pending")
        self.assertEqual(first_row["retry_reason"], "archive")
        self.assertEqual(
            first_row["archive_requested_at"],
            "2026-09-24 12:00:00.000000",
        )
        self.assertIn("archive_missing", first_row["last_error"])

        second = self._run(
            missing_archive,
            now=self.now + timedelta(minutes=15),
        )
        second_row = self._replay("game-1")
        self.assertEqual(second["manual_required"], 1)
        self.assertEqual(second_row["status"], "error")
        self.assertIsNone(second_row["retry_reason"])
        self.assertIsNone(second_row["next_attempt_at"])

    def test_temporary_error_is_deferred_but_access_error_is_manual(self) -> None:
        for game_id, table_id in (("temporary", "101"), ("access", "102")):
            self._add_game(game_id, table_id)
            self._queue(game_id)

        def fail_by_game(_path: str, game_id: str, **_kwargs):
            if game_id == "access":
                raise ReplayAccessError("forbidden", endpoint=LOGS_PATH)
            raise TemporaryReplayError("network", endpoint=LOGS_PATH)

        summary = self._run(fail_by_game, limit=2)

        self.assertEqual(summary["deferred"], 1)
        self.assertEqual(summary["manual_required"], 1)
        self.assertEqual(self._replay("temporary")["retry_reason"], "initial")
        self.assertEqual(self._replay("access")["status"], "error")
        self.assertIsNone(self._replay("access")["retry_reason"])

    def test_replay_limit_stops_and_leaves_job_due(self) -> None:
        for game_id, table_id in (("game-1", "101"), ("game-2", "102")):
            self._add_game(game_id, table_id)
            self._queue(game_id)
        calls: list[str] = []

        def replay_limit(_path: str, game_id: str, **_kwargs):
            calls.append(game_id)
            raise ReplayLimitError("limit (replay)", endpoint=LOGS_PATH)

        summary = self._run(replay_limit, limit=3)

        self.assertEqual(calls, ["game-1"])
        self.assertEqual(summary["status"], "stopped")
        self.assertEqual(summary["stop_reason"], "replay_limit")
        self.assertTrue(summary["stopped_by_bga_limit"])
        self.assertEqual(summary["remaining"], 2)
        self.assertEqual(self._replay("game-1")["status"], "pending")

    def test_historical_replay_limit_does_not_try_another_account(self) -> None:
        for game_id, table_id in (("historical-1", "101"), ("historical-2", "102")):
            self._add_game(game_id, table_id)
            self._queue(game_id, queue_class="historical")
        calls: list[tuple[str, list[str]]] = []

        def replay_limit(_path: str, game_id: str, **kwargs):
            calls.append((game_id, list(kwargs["account_labels"])))
            raise ReplayLimitError("limit (replay)", endpoint=LOGS_PATH)

        summary = self._run(
            replay_limit,
            queue_class="historical",
            limit=3,
            account_labels=["account-a", "account-b", "account-c"],
        )

        self.assertEqual(
            calls,
            [("historical-1", ["account-a", "account-b", "account-c"])],
        )
        self.assertEqual(summary["status"], "stopped")
        self.assertEqual(summary["stop_reason"], "replay_limit")
        self.assertEqual(summary["requests"], 1)
        self.assertEqual(summary["remaining"], 2)

    def test_stale_lease_can_be_reclaimed(self) -> None:
        self._add_game("game-1", "101")
        self._queue(
            "game-1",
            status="fetching",
            lease_owner="dead-worker",
            lease_until=self.now - timedelta(seconds=1),
        )

        summary = self._run(
            lambda *_args, **_kwargs: {
                "color_source": "bga",
                "account_label": "account-a",
            }
        )

        self.assertEqual(summary["processed"], 1)
        row = self._replay("game-1")
        self.assertEqual(row["status"], "ready")
        self.assertIsNone(row["lease_owner"])

    def test_active_lease_prevents_two_workers_claiming_same_game(self) -> None:
        self._add_game("game-1", "101")
        self._queue("game-1")
        request_started = Event()
        release_request = Event()
        calls: list[str] = []

        def blocking_fetch(_path: str, game_id: str, **_kwargs):
            calls.append(game_id)
            request_started.set()
            self.assertTrue(release_request.wait(timeout=5))
            return {"color_source": "bga", "account_label": "account-a"}

        with ThreadPoolExecutor(max_workers=1) as executor:
            first_worker = executor.submit(self._run, blocking_fetch)
            self.assertTrue(request_started.wait(timeout=5))
            second = self._run(
                lambda *_args, **_kwargs: calls.append("second-worker")
            )
            release_request.set()
            first = first_worker.result(timeout=5)

        self.assertEqual(calls, ["game-1"])
        self.assertEqual(first["processed"], 1)
        self.assertEqual(second["processed"], 0)

    def test_worker_never_exceeds_the_per_run_limit(self) -> None:
        for index in range(5):
            game_id = f"fresh-{index}"
            self._add_game(game_id, str(100 + index))
            self._queue(game_id)
        calls: list[str] = []

        summary = self._run(
            lambda _path, game_id, **_kwargs: (
                calls.append(game_id)
                or {"color_source": "bga", "account_label": "account-a"}
            ),
            limit=3,
        )

        self.assertEqual(calls, ["fresh-0", "fresh-1", "fresh-2"])
        self.assertEqual(summary["processed"], 3)
        self.assertEqual(summary["requests"], 3)
        self.assertEqual(summary["remaining"], 2)
        with self.assertRaisesRegex(ValueError, "between 1 and 3"):
            self._run(lambda *_args, **_kwargs: {}, limit=4)

    def test_historical_waits_for_fresh_and_uses_each_account_once(self) -> None:
        self._add_game("fresh", "100")
        self._queue("fresh", queue_class="fresh")
        for index in range(1, 5):
            game_id = f"historical-{index}"
            self._add_game(game_id, str(100 + index))
            self._queue(game_id, queue_class="historical")

        blocked = self._run(
            lambda *_args, **_kwargs: self.fail("historical request must wait"),
            queue_class="historical",
            limit=3,
            account_labels=["a", "b", "c"],
        )
        self.assertEqual(blocked["stop_reason"], "fresh_priority")
        self.assertEqual(blocked["processed"], 0)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                UPDATE game_replays
                SET status = 'ready', retry_reason = NULL, next_attempt_at = NULL,
                    color_source = 'bga'
                WHERE game_id = 'fresh'
                """
            )
        offered_accounts: list[list[str]] = []

        def historical_fetch(_path: str, _game_id: str, **kwargs):
            offered = list(kwargs["account_labels"])
            offered_accounts.append(offered)
            return {"color_source": "bga", "account_label": offered[0]}

        summary = self._run(
            historical_fetch,
            queue_class="historical",
            limit=3,
            account_labels=["a", "b", "c"],
        )

        self.assertEqual(offered_accounts, [["a", "b", "c"], ["b", "c"], ["c"]])
        self.assertEqual(summary["requests"], 3)
        self.assertEqual(summary["remaining"], 1)

    def test_historical_stops_if_fresh_work_appears_between_requests(self) -> None:
        for index in range(1, 3):
            game_id = f"historical-{index}"
            self._add_game(game_id, str(100 + index))
            self._queue(game_id, queue_class="historical")
        calls: list[str] = []

        def fetch_and_add_fresh(_path: str, game_id: str, **kwargs):
            calls.append(game_id)
            if len(calls) == 1:
                self._add_game("fresh-arrival", "200")
                self._queue("fresh-arrival", queue_class="fresh")
            return {
                "color_source": "bga",
                "account_label": kwargs["account_labels"][0],
            }

        summary = self._run(
            fetch_and_add_fresh,
            queue_class="historical",
            limit=3,
            account_labels=["a", "b", "c"],
        )

        self.assertEqual(calls, ["historical-1"])
        self.assertEqual(summary["status"], "stopped")
        self.assertEqual(summary["stop_reason"], "fresh_priority")
        self.assertEqual(summary["requests"], 1)

    def test_fresh_priority_is_unchanged_by_active_budget_overrides(self) -> None:
        replace_replay_budget_overrides(
            self.db_path,
            account_labels=["account-a", "account-b", "account-c"],
            extra_historical_limit=70,
            total_limit_override=100,
            starts_at=self.now,
            expires_at=self.now + timedelta(hours=2),
            created_by="admin",
            reason="priority regression test",
        )
        self._add_game("fresh", "100")
        self._queue("fresh", queue_class="fresh")
        self._add_game("historical", "101")
        self._queue("historical", queue_class="historical")
        calls: list[str] = []

        blocked = self._run(
            lambda *_args, **_kwargs: self.fail("historical must wait for fresh"),
            queue_class="historical",
            account_labels=["account-a", "account-b", "account-c"],
        )
        self.assertEqual(blocked["stop_reason"], "fresh_priority")

        def successful_fetch(_path: str, game_id: str, **kwargs):
            calls.append(game_id)
            return {
                "color_source": "bga",
                "account_label": kwargs["account_labels"][0],
            }

        fresh = self._run(
            successful_fetch,
            queue_class="fresh",
            limit=1,
            account_labels=["account-a", "account-b", "account-c"],
        )
        historical = self._run(
            successful_fetch,
            queue_class="historical",
            limit=1,
            account_labels=["account-a", "account-b", "account-c"],
        )

        self.assertEqual(calls, ["fresh", "historical"])
        self.assertEqual(fresh["ready_bga_colors"], 1)
        self.assertEqual(historical["ready_bga_colors"], 1)

    def test_historical_enqueue_is_idempotent_and_preserves_final_rows(self) -> None:
        for game_id, table_id in (
            ("new", "101"),
            ("bga", "102"),
            ("fallback", "103"),
        ):
            self._add_game(game_id, table_id)
        self._queue(
            "bga",
            status="ready",
            retry_reason=None,
            next_attempt_at=None,
            color_source="bga",
        )
        self._queue(
            "fallback",
            status="ready",
            retry_reason=None,
            next_attempt_at=None,
            color_source="fallback",
            color_refresh_count=1,
        )

        first = enqueue_historical_game_replay(
            self.db_path,
            "new",
            historical_batch_id="batch-1",
            now=self.now,
        )
        second = enqueue_historical_game_replay(
            self.db_path,
            "new",
            historical_batch_id="batch-1",
            now=self.now,
        )
        bga = enqueue_historical_game_replay(self.db_path, "bga", now=self.now)
        fallback = enqueue_historical_game_replay(
            self.db_path,
            "fallback",
            now=self.now,
        )

        self.assertEqual(first["action"], "queued")
        self.assertEqual(second["action"], "already_queued")
        self.assertEqual(bga["action"], "ready_bga")
        self.assertEqual(fallback["action"], "manual_required")
        with sqlite3.connect(self.db_path) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM game_replays WHERE game_id = 'new'"
            ).fetchone()[0]
        self.assertEqual(count, 1)

    def test_flock_rejects_a_second_worker(self) -> None:
        with replay_worker_lock(self.lock_path):
            with self.assertRaises(ReplayWorkerAlreadyRunningError):
                with replay_worker_lock(self.lock_path):
                    pass

    def _run(
        self,
        replay_fetcher,
        *,
        queue_class: str = "fresh",
        limit: int = 3,
        account_labels: list[str] | None = None,
        now: datetime | None = None,
    ):
        return run_replay_worker(
            self.db_path,
            queue_class=queue_class,
            limit=limit,
            lease_owner=f"test-{queue_class}",
            account_labels=account_labels or ["account-a"],
            replay_fetcher=replay_fetcher,
            now=now or self.now,
        )

    def _add_game(self, game_id: str, table_id: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO games (id, bga_table_id) VALUES (?, ?)",
                (game_id, table_id),
            )

    def _queue(
        self,
        game_id: str,
        *,
        status: str = "pending",
        retry_reason: str | None = "initial",
        queue_class: str = "fresh",
        next_attempt_at: datetime | None | object = ...,
        color_source: str | None = None,
        color_refresh_count: int = 0,
        lease_owner: str | None = None,
        lease_until: datetime | None = None,
    ) -> None:
        if next_attempt_at is ...:
            next_attempt_at = self.now
        with sqlite3.connect(self.db_path) as conn:
            table_id = conn.execute(
                "SELECT bga_table_id FROM games WHERE id = ?",
                (game_id,),
            ).fetchone()[0]
            conn.execute(
                """
                INSERT INTO game_replays (
                  game_id, bga_table_id, status, retry_reason, queue_class,
                  queued_at, next_attempt_at, color_source, color_refresh_count,
                  lease_owner, lease_until
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    game_id,
                    table_id,
                    status,
                    retry_reason,
                    queue_class,
                    self._timestamp(self.now),
                    self._timestamp(next_attempt_at) if next_attempt_at else None,
                    color_source,
                    color_refresh_count,
                    lease_owner,
                    self._timestamp(lease_until) if lease_until else None,
                ),
            )

    def _replay(self, game_id: str) -> sqlite3.Row:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            return conn.execute(
                "SELECT * FROM game_replays WHERE game_id = ?",
                (game_id,),
            ).fetchone()

    @staticmethod
    def _timestamp(value: datetime) -> str:
        return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


if __name__ == "__main__":
    unittest.main()
