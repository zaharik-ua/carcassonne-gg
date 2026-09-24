from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from . import duel_game_replay_cli
from .game_replay import ensure_game_replays_schema


class DuelGameReplayCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "auth.sqlite"
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE duels (
                  id TEXT PRIMARY KEY,
                  deleted_at TEXT
                );
                CREATE TABLE games (
                  id TEXT PRIMARY KEY,
                  duel_id TEXT,
                  bga_table_id TEXT,
                  game_number INTEGER,
                  deleted_at TEXT
                );
                INSERT INTO duels VALUES ('duel-1', NULL);
                INSERT INTO duels VALUES ('duel-2', NULL);
                INSERT INTO duels VALUES ('deleted-duel', '2026-01-01');
                INSERT INTO games VALUES ('game-2', 'duel-1', '102', 1, NULL);
                INSERT INTO games VALUES ('game-1', 'duel-1', '101', 2, NULL);
                INSERT INTO games VALUES ('other-game', 'duel-2', '201', 1, NULL);
                INSERT INTO games VALUES ('deleted-game', 'duel-1', '103', 3, '2026-01-01');
                """
            )
            ensure_game_replays_schema(conn)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_fetches_only_exact_duel_in_game_number_order(self) -> None:
        calls: list[tuple[str, dict]] = []

        def fetch_replay(_db_path: str, game_id: str, **kwargs):
            calls.append((game_id, kwargs))
            return {"status": "ready", "cached": False}

        summary = duel_game_replay_cli.fetch_and_store_duel_game_replays(
            self.db_path,
            "duel-1",
            include_pending=True,
            replay_fetcher=fetch_replay,
        )

        self.assertEqual(
            calls,
            [
                ("game-2", {"force": False, "request_class": "manual"}),
                ("game-1", {"force": False, "request_class": "manual"}),
            ],
        )
        self.assertEqual(summary["duel_id"], "duel-1")
        self.assertEqual(summary["games_found"], 2)
        self.assertEqual(summary["requests"], 2)
        self.assertEqual(summary["ready"], 2)

    def test_default_reuses_bga_ready_and_defers_pending(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO game_replays (
                  game_id, bga_table_id, status, color_source
                ) VALUES ('game-2', '102', 'ready', 'bga')
                """
            )
        calls: list[str] = []

        summary = duel_game_replay_cli.fetch_and_store_duel_game_replays(
            self.db_path,
            "duel-1",
            replay_fetcher=lambda _path, game_id, **_kwargs: calls.append(game_id),
        )

        self.assertEqual(calls, [])
        self.assertEqual(summary["status"], "partial")
        self.assertEqual(summary["processed"], 1)
        self.assertEqual(summary["cached"], 1)
        self.assertEqual(summary["deferred"], 1)

    def test_max_requests_is_a_hard_stop(self) -> None:
        calls: list[str] = []

        summary = duel_game_replay_cli.fetch_and_store_duel_game_replays(
            self.db_path,
            "duel-1",
            include_pending=True,
            max_requests=1,
            replay_fetcher=lambda _path, game_id, **_kwargs: (
                calls.append(game_id) or {"status": "ready", "cached": False}
            ),
        )

        self.assertEqual(calls, ["game-2"])
        self.assertEqual(summary["status"], "stopped")
        self.assertEqual(summary["stop_reason"], "max_requests")
        self.assertEqual(summary["remaining"], 1)

    def test_missing_or_deleted_duel_is_rejected(self) -> None:
        for duel_id in ("missing", "deleted-duel"):
            with self.subTest(duel_id=duel_id):
                with self.assertRaises(duel_game_replay_cli.DuelNotFoundError):
                    duel_game_replay_cli.fetch_and_store_duel_game_replays(
                        self.db_path,
                        duel_id,
                    )


if __name__ == "__main__":
    unittest.main()
