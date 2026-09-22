from __future__ import annotations

import io
import json
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from . import match_game_replay_cli


class MatchGameReplayCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "auth.sqlite"
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE matches (
                  id TEXT PRIMARY KEY,
                  deleted_at TEXT
                );
                CREATE TABLE duels (
                  id TEXT PRIMARY KEY,
                  match_id TEXT,
                  duel_number INTEGER,
                  deleted_at TEXT
                );
                CREATE TABLE games (
                  id TEXT PRIMARY KEY,
                  duel_id TEXT,
                  bga_table_id TEXT,
                  game_number INTEGER,
                  deleted_at TEXT
                );

                INSERT INTO matches VALUES ('match-1', NULL);
                INSERT INTO matches VALUES ('deleted-match', '2026-01-01 00:00:00');
                INSERT INTO duels VALUES ('duel-2', 'match-1', 2, NULL);
                INSERT INTO duels VALUES ('duel-1', 'match-1', 1, NULL);
                INSERT INTO duels VALUES ('deleted-duel', 'match-1', 3, '2026-01-01 00:00:00');
                INSERT INTO games VALUES ('game-2', 'duel-2', '222222222', 1, NULL);
                INSERT INTO games VALUES ('game-1', 'duel-1', '111111111', 1, NULL);
                INSERT INTO games VALUES ('game-no-table', 'duel-1', NULL, 2, NULL);
                INSERT INTO games VALUES ('deleted-game', 'duel-1', '333333333', 3, '2026-01-01 00:00:00');
                INSERT INTO games VALUES ('deleted-duel-game', 'deleted-duel', '444444444', 1, NULL);
                """
            )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_fetches_active_match_games_in_duel_and_game_order(self) -> None:
        calls = []

        def fetch_replay(db_path: str, game_id: str, **kwargs):
            calls.append((db_path, game_id, kwargs))
            return {"status": "ready", "cached": game_id == "game-2"}

        summary = match_game_replay_cli.fetch_and_store_match_game_replays(
            self.db_path,
            "match-1",
            force=True,
            poll_attempts=7,
            poll_delay=0.5,
            replay_fetcher=fetch_replay,
        )

        self.assertEqual([call[1] for call in calls], ["game-1", "game-2"])
        self.assertEqual(
            calls[0],
            (
                str(self.db_path),
                "game-1",
                {"force": True, "poll_attempts": 7, "poll_delay": 0.5},
            ),
        )
        self.assertEqual(
            summary,
            {
                "status": "partial",
                "match_id": "match-1",
                "games_found": 3,
                "processed": 2,
                "ready": 2,
                "cached": 1,
                "failed": 0,
                "skipped": 1,
                "errors": [
                    {
                        "game_id": "game-no-table",
                        "duel_id": "duel-1",
                        "error": "Game has no bga_table_id",
                    }
                ],
            },
        )

    def test_continues_after_replay_failure(self) -> None:
        def fetch_replay(_db_path: str, game_id: str, **_kwargs):
            if game_id == "game-1":
                raise RuntimeError("BGA failed")
            return {"status": "ready", "cached": False}

        summary = match_game_replay_cli.fetch_and_store_match_game_replays(
            self.db_path,
            "match-1",
            replay_fetcher=fetch_replay,
        )

        self.assertEqual(summary["processed"], 2)
        self.assertEqual(summary["ready"], 1)
        self.assertEqual(summary["failed"], 1)
        self.assertEqual(summary["skipped"], 1)
        self.assertEqual(summary["errors"][0]["game_id"], "game-1")
        self.assertEqual(summary["errors"][0]["error"], "BGA failed")

    def test_missing_or_deleted_match_is_rejected(self) -> None:
        for match_id in ("missing", "deleted-match"):
            with self.subTest(match_id=match_id):
                with self.assertRaises(match_game_replay_cli.MatchNotFoundError):
                    match_game_replay_cli.fetch_and_store_match_game_replays(
                        self.db_path,
                        match_id,
                        replay_fetcher=lambda *_args, **_kwargs: {},
                    )

    def test_main_returns_nonzero_for_partial_result(self) -> None:
        args = SimpleNamespace(
            db_path=str(self.db_path),
            match_id="match-1",
            force=False,
            poll_attempts=10,
            poll_delay=1.0,
        )
        with (
            patch.object(match_game_replay_cli, "parse_args", return_value=args),
            patch.object(
                match_game_replay_cli,
                "fetch_and_store_match_game_replays",
                return_value={"status": "partial", "failed": 1},
            ),
            redirect_stdout(io.StringIO()) as output,
        ):
            exit_code = match_game_replay_cli.main()

        self.assertEqual(exit_code, 1)
        self.assertEqual(json.loads(output.getvalue())["status"], "partial")


if __name__ == "__main__":
    unittest.main()
