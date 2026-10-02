from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from .game_replay import ensure_game_replays_schema
from .replay_worker import ReplayWorkerAlreadyRunningError, replay_worker_lock, run_replay_worker
from .requeue_discarded_replay_tiles import requeue_discarded_replay_tiles


class RequeueDiscardedReplayTilesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.db_path = Path(self.directory.name) / "auth.sqlite"
        self.report_path = Path(self.directory.name) / "tile-count-mismatches.json"
        self.events = json.dumps([
            {"type": "pickTile", "tile_type": 4},
            {"type": "pickTile", "tile_type": 4},
        ])
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("CREATE TABLE games (id TEXT PRIMARY KEY, bga_table_id TEXT, deleted_at TEXT)")
            ensure_game_replays_schema(conn)

    def _add_game(self, game_id: str, table_id: str, **overrides) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("INSERT INTO games VALUES (?, ?, NULL)", (game_id, table_id))
            conn.execute(
                """
                INSERT INTO game_replays (
                    game_id, bga_table_id, status, events_json, carcassonne_lab_url,
                    queue_class, retry_reason, next_attempt_at, historical_batch_id,
                    history_request_count, color_refresh_count, color_source,
                    archive_requested_at, last_account_label, last_error
                ) VALUES (?, ?, 'ready', ?, 'old-url', 'historical', 'colors',
                          '2000-01-01', 'old-batch', 5, 2, 'bga',
                          '2000-01-01', 'old-account', 'old-error')
                """,
                (game_id, table_id, self.events),
            )
            for column, value in overrides.items():
                conn.execute(f"UPDATE game_replays SET {column} = ? WHERE game_id = ?", (value, game_id))

    def _write_report(self, *games: tuple[str, str]) -> None:
        self.report_path.write_text(json.dumps({
            "matches": [{"game_id": game_id, "bga_table_id": table_id} for game_id, table_id in games],
            "errors": [],
        }), encoding="utf-8")

    def _row(self, game_id: str) -> dict:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            return dict(conn.execute("SELECT * FROM game_replays WHERE game_id = ?", (game_id,)).fetchone())

    def test_preview_apply_preserves_payload_and_repeated_apply_skips_pending(self) -> None:
        self._add_game("bga", "101")
        self._add_game("fallback", "102", color_source="fallback")
        self._add_game("unreviewed", "103")
        self._write_report(("bga", "101"), ("fallback", "102"), ("bga", "101"))
        before = self.db_path.read_bytes()
        preview = requeue_discarded_replay_tiles(self.db_path, self.report_path)
        self.assertEqual(preview["eligible"], 2)
        self.assertEqual(preview["queued"], 0)
        self.assertEqual(self.db_path.read_bytes(), before)
        applied = requeue_discarded_replay_tiles(self.db_path, self.report_path, apply=True)
        self.assertEqual(applied["queued"], 2)
        for game_id in ("bga", "fallback"):
            row = self._row(game_id)
            self.assertEqual((row["status"], row["queue_class"], row["retry_reason"]), ("pending", "fresh", "initial"))
            self.assertEqual(row["events_json"], self.events)
            self.assertEqual(row["carcassonne_lab_url"], "old-url")
            self.assertEqual(row["next_attempt_at"], row["queued_at"])
            self.assertEqual(row["history_request_count"], 0)
            self.assertEqual(row["color_refresh_count"], 0)
            for column in ("color_source", "historical_batch_id", "archive_requested_at",
                           "last_account_label", "lease_owner", "lease_until", "last_error"):
                self.assertIsNone(row[column], column)
        self.assertEqual(self._row("unreviewed")["status"], "ready")
        repeated = requeue_discarded_replay_tiles(self.db_path, self.report_path, apply=True)
        self.assertEqual(repeated["queued"], 0)
        self.assertEqual(repeated["skipped"], 2)

    def test_fresh_worker_fetches_instead_of_treating_the_job_as_recovered(self) -> None:
        self._add_game("refresh", "101")
        self._write_report(("refresh", "101"))
        requeue_discarded_replay_tiles(self.db_path, self.report_path, apply=True)
        calls = []

        def fetch(path, game_id, **kwargs):
            calls.append(kwargs)
            self.assertEqual(self._row(game_id)["status"], "fetching")
            with sqlite3.connect(path) as conn:
                conn.execute(
                    "UPDATE game_replays SET status = 'ready', events_json = '[]', carcassonne_lab_url = 'new-url' WHERE game_id = ?",
                    (game_id,),
                )
            return {"color_source": "bga", "account_label": "test-account"}

        result = run_replay_worker(
            self.db_path, queue_class="fresh", limit=1,
            account_labels=["test-account"], replay_fetcher=fetch,
        )
        self.assertEqual(result["requests"], 1)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["request_class"], "fresh")
        self.assertFalse(calls[0]["color_refresh"])
        row = self._row("refresh")
        self.assertEqual(row["events_json"], "[]")
        self.assertEqual(row["carcassonne_lab_url"], "new-url")
        self.assertEqual(row["status"], "ready")
        self.assertIsNone(row["retry_reason"])

    def test_changed_deleted_active_invalid_and_non_ready_games_are_skipped(self) -> None:
        self._add_game("equal", "101", events_json="[]")
        self._add_game("pending", "102", status="pending")
        self._add_game("deleted", "103")
        self._add_game("changed", "104")
        self._add_game("active", "105", lease_owner="worker", lease_until="2999-01-01")
        self._add_game("invalid", "106", events_json="invalid")
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE games SET deleted_at = '2000-01-01' WHERE id = 'deleted'")
            conn.execute("UPDATE games SET bga_table_id = '999' WHERE id = 'changed'")
        self._write_report(("equal", "101"), ("pending", "102"), ("deleted", "103"),
                           ("changed", "104"), ("active", "105"), ("invalid", "106"), ("absent", "107"))
        before = self.db_path.read_bytes()
        result = requeue_discarded_replay_tiles(self.db_path, self.report_path, apply=True)
        self.assertEqual(result["queued"], 0)
        self.assertEqual(result["status"], "partial")
        self.assertEqual([item["reason"] for item in result["items"]], [
            "counts_already_equal", "not_ready", "deleted_game", "table_id_mismatch",
            "active_lease", "invalid_events_json", "not_found",
        ])
        self.assertEqual(self.db_path.read_bytes(), before)

    def test_failed_update_rolls_back_the_entire_batch(self) -> None:
        self._add_game("first", "101")
        self._add_game("second", "102")
        self._write_report(("first", "101"), ("second", "102"))
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TRIGGER fail_second BEFORE UPDATE ON game_replays
                WHEN NEW.game_id = 'second'
                BEGIN SELECT RAISE(ABORT, 'test failure'); END
            """)
        before = self.db_path.read_bytes()
        with self.assertRaises(sqlite3.IntegrityError):
            requeue_discarded_replay_tiles(self.db_path, self.report_path, apply=True)
        self.assertEqual(self.db_path.read_bytes(), before)

    def test_worker_lock_and_invalid_report_prevent_updates(self) -> None:
        self._add_game("refresh", "101")
        self._write_report(("refresh", "101"))
        before = self.db_path.read_bytes()
        with replay_worker_lock(self.db_path.with_name("auth.sqlite.bga-replay-worker.lock")):
            with self.assertRaises(ReplayWorkerAlreadyRunningError):
                requeue_discarded_replay_tiles(self.db_path, self.report_path, apply=True)
        self.assertEqual(self.db_path.read_bytes(), before)
        self._write_report(("refresh", "101"), ("refresh", "999"))
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            requeue_discarded_replay_tiles(self.db_path, self.report_path, apply=True)
        self.assertEqual(self.db_path.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
