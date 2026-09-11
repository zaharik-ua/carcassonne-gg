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
    build_board_stats,
    build_carcassonne_lab_url,
    build_meeple_stats,
    build_player_time_stats,
    build_scoring_stats,
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
        self.assertEqual(result["board_stats"]["final_bounds"]["width"], 3)
        self.assertEqual(result["board_stats"]["final_bounds"]["height"], 4)
        self.assertEqual(result["meeple_stats"]["total_placements"], 1)
        self.assertEqual(result["scoring"]["totals"]["cities"], 4)
        self.assertEqual(result["player_time"]["players"][0]["duration_seconds"], 20)
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
            events[1],
            {
                "seq": 2,
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
        self.assertEqual(events[0]["type"], "pickTile")
        self.assertEqual(events[0]["tile_id"], 7)
        self.assertEqual(events[0]["tile_type"], 17)
        self.assertEqual(events[0]["player_id"], "100")
        self.assertEqual(events[2]["position"], 5)
        self.assertEqual(events[2]["player_id"], "100")
        self.assertEqual(events[2]["tile_event_seq"], 2)
        self.assertEqual(events[2]["meeple_color"], "red")
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
        self.assertEqual(
            json.loads(stored["board_stats_json"])["final_bounds"],
            {
                "min_x": -2,
                "max_x": 0,
                "min_y": 0,
                "max_y": 3,
                "width": 3,
                "height": 4,
            },
        )
        self.assertEqual(json.loads(stored["meeple_stats_json"])["total_placements"], 1)
        self.assertEqual(json.loads(stored["scoring_json"])["totals"]["cities"], 4)
        self.assertEqual(
            json.loads(stored["player_time_json"])["source"],
            "newActivePlayer.time",
        )
        self.assertTrue(any(row[2] == "games" and row[3] == "game_id" for row in foreign_keys))

    def test_builds_board_expansion_history(self) -> None:
        board = build_board_stats(
            [
                {"seq": 1, "type": "playTile", "x": 1, "y": 0},
                {"seq": 2, "type": "playTile", "x": 1, "y": -2},
                {"seq": 3, "type": "playTile", "x": 0, "y": -1},
            ]
        )

        self.assertEqual(board["placed_tile_count"], 3)
        self.assertEqual(board["tile_count_including_start"], 4)
        self.assertEqual(
            board["final_bounds"],
            {
                "min_x": 0,
                "max_x": 1,
                "min_y": -2,
                "max_y": 0,
                "width": 2,
                "height": 3,
            },
        )
        self.assertEqual([event["move_number"] for event in board["expansion_events"]], [1, 2])

    def test_groups_feature_scores_and_player_turn_time(self) -> None:
        players = [
            {"player_id": "100", "player_name": "Alpha"},
            {"player_id": "200", "player_name": "Beta"},
        ]
        logs = [
            {
                "data": [
                    {"type": "newActivePlayer", "time": 1_700_000_000, "args": {"player_id": "100"}},
                    {
                        "type": "scoreRoad",
                        "time": 1_700_000_004,
                        "args": {"player_id": "100", "points": "5"},
                    },
                    {"type": "newActivePlayer", "time": 1_700_000_010, "args": {"player_id": "200"}},
                    {
                        "type": "scoreFeature",
                        "time": 1_700_000_014,
                        "args": {"player_id": "200", "feature": "cloister", "score": "9"},
                    },
                    {"type": "replay_has_ended", "time": 1_700_000_025, "args": {}},
                ]
            }
        ]

        scoring = build_scoring_stats(logs, players)
        timing = build_player_time_stats(logs, players)

        self.assertEqual(scoring["totals"]["roads"], 5)
        self.assertEqual(scoring["totals"]["monasteries"], 9)
        self.assertEqual(scoring["players"][0]["total_points"], 5)
        self.assertEqual(scoring["players"][1]["total_points"], 9)
        self.assertEqual(timing["game_duration_seconds"], 25)
        self.assertEqual(timing["players"][0]["duration_seconds"], 10)
        self.assertEqual(timing["players"][1]["duration_seconds"], 15)

    def test_parses_real_bga_realizations_returns_and_active_player_time(self) -> None:
        players = [
            {"player_id": "100", "player_name": "Alpha"},
            {"player_id": "200", "player_name": "Beta"},
        ]
        logs = [
            {
                "time": "1700000000",
                "data": [{
                    "type": "gameStateChange",
                    "args": {"type": "activeplayer", "active_player": "100"},
                }],
            },
            {
                "time": "1700000010",
                "data": [{
                    "type": "gameStateChange",
                    "args": {"type": "activeplayer", "active_player": "200"},
                }],
            },
            {
                "time": "1700000025",
                "data": [
                    {
                        "type": "realizationAchieved",
                        "log": "${player_name} achieved a ${real_type}",
                        "args": {
                            "real_id": "city-1",
                            "real_type": "city",
                            "tile_to_value": {"1": 2, "2": 2},
                            "winners": [100],
                            "part_to_recover": {"100": {"normal": 1}},
                        },
                    },
                    {
                        "type": "winPoints",
                        "log": "${player_name} wins ${points} points",
                        "args": {"player_id": 100, "points": 4, "score": 4},
                    },
                    {
                        "type": "gameStateChange",
                        "args": {"type": "activeplayer", "active_player": "100"},
                    },
                ],
            },
            {
                "time": "1700000030",
                "data": [{
                    "type": "realizationAchieved",
                    "log": "This fields is feeding ${city_nbr} cities and worth ${points} points",
                    "args": {
                        "real_id": "field-1",
                        "real_type": "field",
                        "points": 3,
                        "tile_to_value": {"7": 3},
                        "winners": [200],
                    },
                }],
            },
        ]
        placements = [
            {"type": "playPartisan", "player_id": "100"},
            {"type": "playPartisan", "player_id": "100"},
            {"type": "playPartisan", "player_id": "200"},
        ]

        scoring = build_scoring_stats(logs, players)
        meeples = build_meeple_stats(placements, players, logs)
        timing = build_player_time_stats(logs, players)

        self.assertEqual(scoring["totals"]["cities"], 4)
        self.assertEqual(scoring["totals"]["fields"], 3)
        self.assertEqual(scoring["players"][0]["total_points"], 4)
        self.assertEqual(scoring["players"][1]["total_points"], 3)
        self.assertEqual(
            [event["phase"] for event in scoring["events"]],
            ["completed", "end_game"],
        )
        self.assertEqual(meeples["total_placements"], 3)
        self.assertEqual(meeples["total_returns"], 1)
        self.assertEqual(meeples["remaining_on_board_at_end"], 2)
        self.assertEqual(timing["source"], "gameStateChange.active_player/time")
        self.assertEqual(timing["players"][0]["duration_seconds"], 15)
        self.assertEqual(timing["players"][1]["duration_seconds"], 15)

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

    def test_existing_replay_table_gets_derived_history_columns(self) -> None:
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

        self.assertTrue(
            {
                "carcassonne_lab_url",
                "board_stats_json",
                "meeple_stats_json",
                "scoring_json",
                "player_time_json",
            }.issubset(columns)
        )

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

    def test_ready_replay_backfills_derived_history_without_bga_request(self) -> None:
        fetch_and_store_game_replay(
            self.db_path,
            "game-row-1",
            request=lambda *_args, **_kwargs: self._successful_payload(),
            authenticate=lambda: None,
        )
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                UPDATE game_replays
                SET board_stats_json = NULL,
                    meeple_stats_json = NULL,
                    scoring_json = NULL,
                    player_time_json = NULL
                WHERE game_id = 'game-row-1'
                """
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
        self.assertEqual(result["board_stats"]["final_bounds"]["width"], 3)
        self.assertEqual(result["scoring"]["totals"]["cities"], 4)

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
                                "time": 1_700_000_000,
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
                                "type": "newActivePlayer",
                                "time": 1_700_000_001,
                                "args": {"player_id": "100"},
                            },
                            {
                                "type": "pickTile",
                                "time": 1_700_000_002,
                                "args": {"id": 7, "type": "17"},
                            },
                            {
                                "type": "playTile",
                                "time": 1_700_000_011,
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
                                "time": 1_700_000_012,
                                "args": [{"piece": "partisan", "pos": "5"}],
                            },
                            {
                                "type": "scoreFeature",
                                "time": 1_700_000_013,
                                "args": {
                                    "player_id": "100",
                                    "player_name": "Alpha",
                                    "feature": "city",
                                    "points": "4",
                                },
                            },
                            {
                                "type": "replay_has_ended",
                                "time": 1_700_000_021,
                                "args": {},
                            },
                        ]
                    }
                ],
            },
        }


if __name__ == "__main__":
    unittest.main()
