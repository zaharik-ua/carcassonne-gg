from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from .challenge_replay_queue import populate_done_challenge_replay_queue
from .game_replay import ensure_game_replays_schema


class ChallengeReplayQueueTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "auth.sqlite"
        self.now = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE duels (
                  id TEXT PRIMARY KEY,
                  source_type TEXT,
                  status TEXT,
                  deleted_at TEXT
                );
                CREATE TABLE games (
                  id TEXT PRIMARY KEY,
                  duel_id TEXT,
                  bga_table_id TEXT,
                  game_number INTEGER,
                  deleted_at TEXT
                );

                INSERT INTO duels VALUES ('done-challenge', ' Challenge ', ' done ', NULL);
                INSERT INTO duels VALUES ('planned-challenge', 'challenge', 'Planned', NULL);
                INSERT INTO duels VALUES ('done-other', 'tournament', 'Done', NULL);
                INSERT INTO duels VALUES ('deleted-challenge', 'challenge', 'Done', '2026-01-01');

                INSERT INTO games VALUES ('missing', 'done-challenge', '301', 1, NULL);
                INSERT INTO games VALUES ('fallback', 'done-challenge', '302', 2, NULL);
                INSERT INTO games VALUES ('complete', 'done-challenge', '303', 3, NULL);
                INSERT INTO games VALUES ('already-queued', 'done-challenge', '304', 4, NULL);
                INSERT INTO games VALUES ('planned', 'planned-challenge', '401', 1, NULL);
                INSERT INTO games VALUES ('other-source', 'done-other', '402', 1, NULL);
                INSERT INTO games VALUES ('deleted-duel-game', 'deleted-challenge', '403', 1, NULL);
                INSERT INTO games VALUES ('deleted-game', 'done-challenge', '404', 5, '2026-01-01');
                """
            )
            ensure_game_replays_schema(conn)
            events = json.dumps(
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
            fallback_players = json.dumps(
                [
                    {"player_id": "1", "color_hex": None, "meeple_color": None},
                    {"player_id": "2", "color_hex": None, "meeple_color": None},
                ]
            )
            conn.executemany(
                """
                INSERT INTO game_replays (
                  game_id, bga_table_id, status, events_json, players_json,
                  color_source, queue_class, retry_reason, next_attempt_at
                ) VALUES (?, ?, 'ready', ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        "fallback", "302", events, fallback_players,
                        "fallback", "fresh", None, None,
                    ),
                    (
                        "complete", "303", events, complete_players,
                        "bga", "fresh", None, None,
                    ),
                    (
                        "already-queued", "304", events, fallback_players,
                        "fallback", "historical", "colors", "2026-09-30 13:00:00",
                    ),
                    (
                        "planned", "401", events, fallback_players,
                        "fallback", "fresh", None, None,
                    ),
                    (
                        "other-source", "402", events, fallback_players,
                        "fallback", "fresh", None, None,
                    ),
                ],
            )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_dry_run_selects_only_done_challenge_duels(self) -> None:
        summary = populate_done_challenge_replay_queue(
            self.db_path,
            now=self.now,
        )

        self.assertEqual(summary["mode"], "dry-run")
        self.assertEqual(summary["duel_source_type"], "challenge")
        self.assertEqual(summary["duel_status"], "Done")
        self.assertEqual(summary["games_found"], 4)
        self.assertEqual(summary["complete_replays"], 1)
        self.assertEqual(summary["missing_replays"], 1)
        self.assertEqual(summary["incomplete_replays"], 2)
        self.assertEqual(summary["create_historical"], 1)
        self.assertEqual(summary["requeue_historical"], 1)
        self.assertEqual(summary["already_historical"], 1)
        self.assertEqual(summary["changed"], 0)
        self.assertNotIn("items", summary)
        self.assertNotIn("errors", summary)
        self.assertNotIn("items_truncated", summary)

        with sqlite3.connect(self.db_path) as conn:
            self.assertIsNone(
                conn.execute(
                    "SELECT game_id FROM game_replays WHERE game_id = 'missing'"
                ).fetchone()
            )

    def test_apply_creates_and_requeues_only_matching_games(self) -> None:
        summary = populate_done_challenge_replay_queue(
            self.db_path,
            apply=True,
            batch_id="challenge-test",
            now=self.now,
        )

        self.assertEqual(summary["status"], "ok")
        self.assertEqual(summary["changed"], 2)
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = {
                row["game_id"]: row
                for row in conn.execute(
                    """
                    SELECT game_id, queue_class, retry_reason, historical_batch_id
                    FROM game_replays
                    """
                ).fetchall()
            }

        self.assertEqual(rows["missing"]["queue_class"], "historical")
        self.assertEqual(rows["missing"]["retry_reason"], "initial")
        self.assertEqual(rows["missing"]["historical_batch_id"], "challenge-test")
        self.assertEqual(rows["fallback"]["queue_class"], "historical")
        self.assertEqual(rows["fallback"]["retry_reason"], "colors")
        self.assertEqual(rows["already-queued"]["queue_class"], "historical")
        self.assertEqual(rows["planned"]["queue_class"], "fresh")
        self.assertEqual(rows["other-source"]["queue_class"], "fresh")
        self.assertEqual(rows["complete"]["queue_class"], "fresh")

    def test_apply_is_idempotent(self) -> None:
        populate_done_challenge_replay_queue(
            self.db_path,
            apply=True,
            batch_id="first",
            now=self.now,
        )
        second = populate_done_challenge_replay_queue(
            self.db_path,
            apply=True,
            batch_id="second",
            now=self.now,
        )

        self.assertEqual(second["changed"], 0)
        self.assertEqual(second["already_historical"], 3)


if __name__ == "__main__":
    unittest.main()
