from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from .game_replay import ensure_game_replays_schema
from .team_tournaments_replay_queue import (
    TOURNAMENT_IDS,
    populate_2026_team_tournaments_replay_queue,
)


class TeamTournamentsReplayQueueTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "auth.sqlite"
        self.now = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE matches (
                  id TEXT PRIMARY KEY,
                  tournament_id TEXT,
                  deleted_at TEXT
                );
                CREATE TABLE duels (
                  id TEXT PRIMARY KEY,
                  match_id TEXT,
                  deleted_at TEXT
                );
                CREATE TABLE games (
                  id TEXT PRIMARY KEY,
                  duel_id TEXT,
                  bga_table_id TEXT,
                  game_number INTEGER,
                  deleted_at TEXT
                );

                INSERT INTO matches VALUES ('asian-match', ' asian-cup-2026 ', NULL);
                INSERT INTO matches VALUES ('copa-match', 'Copa-America-2026', NULL);
                INSERT INTO matches VALUES ('wtcoc-match', 'WTCOC-2026', NULL);
                INSERT INTO matches VALUES ('other-match', 'ETCOC-2026', NULL);
                INSERT INTO matches VALUES ('deleted-match', 'WTCOC-2026', '2026-01-01');

                INSERT INTO duels VALUES ('asian-duel', 'asian-match', NULL);
                INSERT INTO duels VALUES ('copa-duel', 'copa-match', NULL);
                INSERT INTO duels VALUES ('wtcoc-duel', 'wtcoc-match', NULL);
                INSERT INTO duels VALUES ('other-duel', 'other-match', NULL);
                INSERT INTO duels VALUES ('deleted-match-duel', 'deleted-match', NULL);
                INSERT INTO duels VALUES ('deleted-duel', 'wtcoc-match', '2026-01-01');

                INSERT INTO games VALUES ('asian-missing', 'asian-duel', '501', 1, NULL);
                INSERT INTO games VALUES ('copa-fallback', 'copa-duel', '502', 1, NULL);
                INSERT INTO games VALUES ('wtcoc-complete', 'wtcoc-duel', '503', 1, NULL);
                INSERT INTO games VALUES ('wtcoc-queued', 'wtcoc-duel', '504', 2, NULL);
                INSERT INTO games VALUES ('other-game', 'other-duel', '601', 1, NULL);
                INSERT INTO games VALUES ('deleted-match-game', 'deleted-match-duel', '602', 1, NULL);
                INSERT INTO games VALUES ('deleted-duel-game', 'deleted-duel', '603', 1, NULL);
                INSERT INTO games VALUES ('deleted-game', 'asian-duel', '604', 2, '2026-01-01');
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
                        "copa-fallback", "502", events, fallback_players,
                        "fallback", "fresh", None, None,
                    ),
                    (
                        "wtcoc-complete", "503", events, complete_players,
                        "bga", "fresh", None, None,
                    ),
                    (
                        "wtcoc-queued", "504", events, fallback_players,
                        "fallback", "historical", "colors", "2026-10-08 13:00:00",
                    ),
                    (
                        "other-game", "601", events, fallback_players,
                        "fallback", "fresh", None, None,
                    ),
                ],
            )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_dry_run_selects_all_three_tournaments_only(self) -> None:
        summary = populate_2026_team_tournaments_replay_queue(
            self.db_path,
            now=self.now,
        )

        self.assertEqual(summary["mode"], "dry-run")
        self.assertEqual(summary["tournament_ids"], list(TOURNAMENT_IDS))
        self.assertEqual(summary["games_found"], 4)
        self.assertEqual(summary["complete_replays"], 1)
        self.assertEqual(summary["missing_replays"], 1)
        self.assertEqual(summary["incomplete_replays"], 2)
        self.assertEqual(summary["create_historical"], 1)
        self.assertEqual(summary["requeue_historical"], 1)
        self.assertEqual(summary["already_historical"], 1)
        self.assertEqual(summary["changed"], 0)

        with sqlite3.connect(self.db_path) as conn:
            self.assertIsNone(
                conn.execute(
                    "SELECT game_id FROM game_replays WHERE game_id = 'asian-missing'"
                ).fetchone()
            )

    def test_apply_updates_only_selected_tournament_games(self) -> None:
        summary = populate_2026_team_tournaments_replay_queue(
            self.db_path,
            apply=True,
            batch_id="team-2026-test",
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

        self.assertEqual(rows["asian-missing"]["queue_class"], "historical")
        self.assertEqual(rows["asian-missing"]["retry_reason"], "initial")
        self.assertEqual(rows["asian-missing"]["historical_batch_id"], "team-2026-test")
        self.assertEqual(rows["copa-fallback"]["queue_class"], "historical")
        self.assertEqual(rows["copa-fallback"]["retry_reason"], "colors")
        self.assertEqual(rows["wtcoc-queued"]["queue_class"], "historical")
        self.assertEqual(rows["wtcoc-complete"]["queue_class"], "fresh")
        self.assertEqual(rows["other-game"]["queue_class"], "fresh")

    def test_apply_is_idempotent(self) -> None:
        populate_2026_team_tournaments_replay_queue(
            self.db_path,
            apply=True,
            batch_id="first",
            now=self.now,
        )
        second = populate_2026_team_tournaments_replay_queue(
            self.db_path,
            apply=True,
            batch_id="second",
            now=self.now,
        )

        self.assertEqual(second["changed"], 0)
        self.assertEqual(second["already_historical"], 3)


if __name__ == "__main__":
    unittest.main()
