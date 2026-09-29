from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from .game_replay import ensure_game_replays_schema
from .tournament_replay_queue import populate_etcoc_2026_replay_queue


class TournamentReplayQueueTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "auth.sqlite"
        self.now = datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc)
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE duels (
                  id TEXT PRIMARY KEY,
                  tournament_id TEXT,
                  deleted_at TEXT
                );
                CREATE TABLE games (
                  id TEXT PRIMARY KEY,
                  duel_id TEXT,
                  bga_table_id TEXT,
                  game_number INTEGER,
                  deleted_at TEXT
                );

                INSERT INTO duels VALUES ('target-duel', 'ETCOC-2026', NULL);
                INSERT INTO duels VALUES ('other-duel', 'OTHER-2026', NULL);
                INSERT INTO duels VALUES ('deleted-duel', 'ETCOC-2026', '2026-01-01');

                INSERT INTO games VALUES ('missing', 'target-duel', '101', 1, NULL);
                INSERT INTO games VALUES ('complete', 'target-duel', '102', 2, NULL);
                INSERT INTO games VALUES ('fallback', 'target-duel', '103', 3, NULL);
                INSERT INTO games VALUES ('pending-bga', 'target-duel', '104', 4, NULL);
                INSERT INTO games VALUES ('already-queued', 'target-duel', '105', 5, NULL);
                INSERT INTO games VALUES ('invalid-table', 'target-duel', '', 6, NULL);
                INSERT INTO games VALUES ('missing-meeple-color', 'target-duel', '108', 7, NULL);
                INSERT INTO games VALUES ('other', 'other-duel', '201', 1, NULL);
                INSERT INTO games VALUES ('deleted-game', 'target-duel', '106', 8, '2026-01-01');
                INSERT INTO games VALUES ('deleted-duel-game', 'deleted-duel', '107', 1, NULL);
                """
            )
            ensure_game_replays_schema(conn)
            complete_events = json.dumps(
                [
                    {"type": "playTile", "player_id": "1"},
                    {"type": "playTile", "player_id": "2"},
                ]
            )
            complete_players = json.dumps(
                [
                    {"player_id": "1", "color_hex": "ff0000", "meeple_color": "red"},
                    {"player_id": "2", "color_hex": "0000ff", "meeple_color": "blue"},
                ]
            )
            missing_color_players = json.dumps(
                [
                    {"player_id": "1", "color_hex": None, "meeple_color": None},
                    {"player_id": "2", "color_hex": None, "meeple_color": None},
                ]
            )
            missing_meeple_color_players = json.dumps(
                [
                    {"player_id": "1", "color_hex": "ff0000", "meeple_color": None},
                    {"player_id": "2", "color_hex": "0000ff", "meeple_color": "blue"},
                ]
            )
            conn.executemany(
                """
                INSERT INTO game_replays (
                  game_id, bga_table_id, status, events_json, players_json,
                  color_source, queue_class, retry_reason, next_attempt_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        "complete", "102", "ready", complete_events, complete_players,
                        "bga", "fresh", None, None,
                    ),
                    (
                        "fallback", "103", "ready", complete_events, missing_color_players,
                        "fallback", "fresh", None, None,
                    ),
                    (
                        "pending-bga", "104", "pending", complete_events, complete_players,
                        "bga", "fresh", "initial", "2026-09-29 13:00:00",
                    ),
                    (
                        "already-queued", "105", "ready", complete_events, missing_color_players,
                        "fallback", "historical", "colors", "2026-09-29 13:00:00",
                    ),
                    (
                        "missing-meeple-color", "108", "ready", complete_events,
                        missing_meeple_color_players, "bga", "fresh", None, None,
                    ),
                    (
                        "other", "201", "ready", complete_events, missing_color_players,
                        "fallback", "fresh", None, None,
                    ),
                ],
            )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_dry_run_reports_without_writing(self) -> None:
        summary = populate_etcoc_2026_replay_queue(
            self.db_path,
            now=self.now,
        )

        self.assertEqual(summary["mode"], "dry-run")
        self.assertEqual(summary["games_found"], 7)
        self.assertEqual(summary["complete_replays"], 1)
        self.assertEqual(summary["missing_replays"], 2)
        self.assertEqual(summary["incomplete_replays"], 4)
        self.assertEqual(summary["create_historical"], 1)
        self.assertEqual(summary["requeue_historical"], 3)
        self.assertEqual(summary["already_historical"], 1)
        self.assertEqual(summary["invalid_bga_table_id"], 1)
        self.assertEqual(summary["changed"], 0)
        with sqlite3.connect(self.db_path) as conn:
            self.assertIsNone(
                conn.execute(
                    "SELECT game_id FROM game_replays WHERE game_id = 'missing'"
                ).fetchone()
            )
            fallback = conn.execute(
                "SELECT queue_class FROM game_replays WHERE game_id = 'fallback'"
            ).fetchone()
        self.assertEqual(fallback[0], "fresh")

    def test_apply_creates_and_requeues_only_target_incomplete_replays(self) -> None:
        summary = populate_etcoc_2026_replay_queue(
            self.db_path,
            apply=True,
            batch_id="etcoc-test",
            now=self.now,
        )

        self.assertEqual(summary["status"], "partial")
        self.assertEqual(summary["changed"], 4)
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = {
                row["game_id"]: row
                for row in conn.execute(
                    """
                    SELECT game_id, status, queue_class, retry_reason,
                           color_source, next_attempt_at, historical_batch_id
                    FROM game_replays
                    """
                ).fetchall()
            }

        self.assertEqual(rows["missing"]["status"], "pending")
        self.assertEqual(rows["missing"]["retry_reason"], "initial")
        self.assertEqual(rows["fallback"]["status"], "ready")
        self.assertEqual(rows["fallback"]["retry_reason"], "colors")
        self.assertEqual(rows["fallback"]["color_source"], "fallback")
        self.assertEqual(rows["pending-bga"]["status"], "pending")
        self.assertEqual(rows["pending-bga"]["retry_reason"], "initial")
        self.assertIsNone(rows["pending-bga"]["color_source"])
        self.assertEqual(rows["missing-meeple-color"]["retry_reason"], "colors")
        self.assertEqual(rows["missing-meeple-color"]["color_source"], "fallback")
        for game_id in (
            "missing",
            "fallback",
            "pending-bga",
            "already-queued",
            "missing-meeple-color",
        ):
            self.assertEqual(rows[game_id]["queue_class"], "historical")
        self.assertEqual(rows["missing"]["historical_batch_id"], "etcoc-test")
        self.assertEqual(rows["other"]["queue_class"], "fresh")
        self.assertEqual(rows["complete"]["queue_class"], "fresh")

    def test_apply_is_idempotent_for_active_historical_rows(self) -> None:
        populate_etcoc_2026_replay_queue(
            self.db_path,
            apply=True,
            batch_id="first",
            now=self.now,
        )
        second = populate_etcoc_2026_replay_queue(
            self.db_path,
            apply=True,
            batch_id="second",
            now=self.now,
        )

        self.assertEqual(second["changed"], 0)
        self.assertEqual(second["already_historical"], 5)
        with sqlite3.connect(self.db_path) as conn:
            batch_id = conn.execute(
                "SELECT historical_batch_id FROM game_replays WHERE game_id = 'missing'"
            ).fetchone()[0]
        self.assertEqual(batch_id, "first")


if __name__ == "__main__":
    unittest.main()
