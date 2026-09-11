from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from .game_replay import (
    ARCHIVE_REQUEST_PATH,
    LOGS_PATH,
    BgaReplayError,
    GameNotFoundError,
    build_carcassonne_lab_url,
    ensure_game_replays_schema,
    fetch_and_store_game_replay,
)


class GameReplayTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "auth.sqlite"
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE games (
                  id TEXT PRIMARY KEY,
                  bga_table_id TEXT,
                  deleted_at TEXT
                );
                INSERT INTO games (id, bga_table_id, deleted_at)
                VALUES ('game-row-1', '913515989', NULL);
                """
            )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_fetches_normalizes_and_stores_replay(self) -> None:
        auth_calls: list[str] = []

        def request(path, params=None, **_kwargs):
            self.assertEqual(path, LOGS_PATH)
            self.assertEqual(params, {"table": "913515989", "translated": "true"})
            return self._successful_payload()

        result = fetch_and_store_game_replay(
            self.db_path,
            "game-row-1",
            request=request,
            authenticate=lambda: auth_calls.append("authenticated"),
        )

        self.assertEqual(auth_calls, ["authenticated"])
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["tile_count"], 1)
        self.assertEqual(result["meeple_count"], 1)
        self.assertFalse(result["archive_requested"])
        self.assertEqual(result["players"][0]["meeple_color"], "red")
        self.assertEqual(
            result["carcassonne_lab_url"],
            "https://www.carcassonnelab.com/#/0/0/Fg/34L5?players=Alpha&colors=red",
        )

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            stored = conn.execute(
                "SELECT * FROM game_replays WHERE game_id = 'game-row-1'"
            ).fetchone()
            foreign_keys = conn.execute("PRAGMA foreign_key_list(game_replays)").fetchall()

        self.assertEqual(stored["bga_table_id"], "913515989")
        self.assertEqual(stored["status"], "ready")
        self.assertEqual(stored["carcassonne_lab_url"], result["carcassonne_lab_url"])
        events = json.loads(stored["events_json"])
        self.assertEqual(
            events[0],
            {
                "seq": 1,
                "type": "playTile",
                "player_id": "100",
                "player_name": "Alpha",
                "tile_type": 17,
                "x": -2,
                "y": 3,
                "orientation": 4,
                "rotation": 3,
                "color_hex": "ff0000",
                "meeple_color": "red",
            },
        )
        self.assertEqual(events[1]["position"], 5)
        self.assertEqual(events[1]["player_id"], "100")
        self.assertEqual(events[1]["tile_event_seq"], 1)
        self.assertEqual(events[1]["meeple_color"], "red")
        players = json.loads(stored["players_json"])
        self.assertEqual(
            players,
            [
                {
                    "player_id": "100",
                    "player_name": "Alpha",
                    "color_hex": "ff0000",
                    "meeple_color": "red",
                },
                {
                    "player_id": "200",
                    "player_name": "Beta",
                    "color_hex": "0000ff",
                    "meeple_color": "blue",
                },
            ],
        )
        self.assertTrue(any(row[2] == "games" and row[3] == "game_id" for row in foreign_keys))

    def test_lab_url_uses_first_move_player_order_and_matching_colors(self) -> None:
        events = [
            {
                "type": "playTile",
                "player_id": "200",
                "player_name": "Beta Player",
                "tile_type": 17,
                "x": 0,
                "y": 1,
                "rotation": 0,
            },
            {
                "type": "playTile",
                "player_id": "100",
                "player_name": "Alpha",
                "tile_type": 4,
                "x": -1,
                "y": 1,
                "rotation": 2,
            },
        ]
        players = [
            {"player_id": "100", "player_name": "Alpha", "meeple_color": "red"},
            {"player_id": "200", "player_name": "Beta Player", "meeple_color": "blue"},
        ]

        url = build_carcassonne_lab_url(events, players)

        self.assertIsNotNone(url)
        self.assertIn("?players=Beta%20Player,Alpha&colors=blue,red", url)

    def test_lab_url_is_not_created_when_a_player_color_is_missing(self) -> None:
        self.assertIsNone(build_carcassonne_lab_url(
            [
                {
                    "type": "playTile",
                    "player_id": "100",
                    "player_name": "Alpha",
                    "tile_type": 17,
                    "x": 0,
                    "y": 1,
                    "rotation": 0,
                }
            ],
            [{"player_id": "100", "player_name": "Alpha", "meeple_color": None}],
        ))

    def test_existing_replay_table_gets_lab_url_column(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE game_replays (
                  game_id TEXT PRIMARY KEY,
                  bga_table_id TEXT NOT NULL
                )
                """
            )
            ensure_game_replays_schema(conn)
            columns = {
                row[1]
                for row in conn.execute("PRAGMA table_info(game_replays)").fetchall()
            }

        self.assertIn("carcassonne_lab_url", columns)

    def test_requests_archive_and_polls_until_logs_are_ready(self) -> None:
        calls: list[str] = []
        log_responses = [
            {"status": 0, "error": "Cannot find gamenotifs log file"},
            {"status": 0, "error": "Cannot find gamenotifs log file"},
            self._successful_payload(),
        ]

        def request(path, params=None, **_kwargs):
            calls.append(path)
            if path == ARCHIVE_REQUEST_PATH:
                self.assertEqual(params, {"table": "913515989"})
                return {"status": 1, "data": {}}
            return log_responses.pop(0)

        delays: list[float] = []
        result = fetch_and_store_game_replay(
            self.db_path,
            "game-row-1",
            request=request,
            authenticate=lambda: None,
            poll_attempts=3,
            poll_delay=0.25,
            sleep=delays.append,
        )

        self.assertEqual(
            calls,
            [LOGS_PATH, ARCHIVE_REQUEST_PATH, LOGS_PATH, LOGS_PATH],
        )
        self.assertEqual(delays, [0.25])
        self.assertTrue(result["archive_requested"])

    def test_bga_error_is_persisted(self) -> None:
        def request(_path, params=None, **_kwargs):
            return {"status": 0, "error": "Replay access denied"}

        with self.assertRaisesRegex(BgaReplayError, "Replay access denied"):
            fetch_and_store_game_replay(
                self.db_path,
                "game-row-1",
                request=request,
                authenticate=lambda: None,
            )

        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT status, last_error FROM game_replays WHERE game_id = 'game-row-1'"
            ).fetchone()
        self.assertEqual(row, ("error", "Replay access denied"))

    def test_ready_replay_is_reused_without_authentication(self) -> None:
        fetch_and_store_game_replay(
            self.db_path,
            "game-row-1",
            request=lambda *_args, **_kwargs: self._successful_payload(),
            authenticate=lambda: None,
        )
        calls: list[str] = []

        result = fetch_and_store_game_replay(
            self.db_path,
            "game-row-1",
            request=lambda *_args, **_kwargs: calls.append("request"),
            authenticate=lambda: calls.append("authenticate"),
        )

        self.assertTrue(result["cached"])
        self.assertEqual(calls, [])

    def test_missing_game_is_rejected(self) -> None:
        with self.assertRaisesRegex(GameNotFoundError, "Game not found"):
            fetch_and_store_game_replay(
                self.db_path,
                "missing",
                request=lambda *_args, **_kwargs: self._successful_payload(),
                authenticate=lambda: None,
            )

    @staticmethod
    def _successful_payload():
        return {
            "status": 1,
            "data": {
                "players": {
                    "100": {"name": "Alpha"},
                    "200": {"name": "Beta"},
                },
                "logs": [
                    {
                        "data": [
                            {
                                "type": "gameStateChange",
                                "args": {
                                    "args": {
                                        "result": [
                                            {"player": "100", "color": "#ff0000"},
                                            {"id": "200", "color": "0000ff"},
                                        ]
                                    }
                                },
                            },
                            {
                                "type": "playTile",
                                "args": [
                                    {
                                        "piece": "tile",
                                        "player_id": "100",
                                        "player_name": "Alpha",
                                        "type": "17",
                                        "x": "-2",
                                        "y": "3",
                                        "ori": "4",
                                    }
                                ],
                            },
                            {
                                "type": "playPartisan",
                                "args": [{"piece": "partisan", "pos": "5"}],
                            },
                        ]
                    }
                ],
            },
        }


if __name__ == "__main__":
    unittest.main()
