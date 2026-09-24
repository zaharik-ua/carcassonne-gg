from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import game_replay as game_replay_module
from .game_replay import (
    ARCHIVE_REQUEST_PATH,
    LEGACY_GAME_REPLAY_COLUMNS,
    LOGS_PATH,
    REPLAY_OUTCOME_ACCESS_ERROR,
    REPLAY_OUTCOME_ARCHIVE_MISSING,
    REPLAY_OUTCOME_LIMIT,
    REPLAY_OUTCOME_READY,
    REPLAY_OUTCOME_TEMPORARY_ERROR,
    ArchiveMissingError,
    BgaReplayError,
    GameNotFoundError,
    ReplayAccessError,
    ReplayBudgetExceededError,
    ReplayLimitError,
    TemporaryReplayError,
    apply_replay_player_colors,
    build_board_stats,
    build_carcassonne_lab_url,
    build_meeple_stats,
    build_player_time_stats,
    build_scoring_stats,
    ensure_game_replays_schema,
    fetch_and_store_game_replay,
    fetch_bga_replay,
)
from .replay_budget import ReplayBudgetLimits, replace_replay_budget_overrides


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
        self.assertEqual(result["color_source"], "bga")
        self.assertEqual(result["board_stats"], {"width": 3, "height": 4})
        self.assertEqual(result["meeple_stats"]["total_placements"], 1)
        self.assertEqual(result["scoring"]["totals"]["cities"], 4)
        self.assertEqual(result["player_time"]["players"][0]["duration_seconds"], 20)
        self.assertEqual(result["players"][0]["meeple_color"], "red")
        self.assertEqual(
            result["carcassonne_lab_url"],
            "https://www.carcassonnelab.com/#/0/0/CED/34L523H0"
            "?players=Alpha,Beta&colors=red,blue",
        )

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            stored = conn.execute(
                "SELECT * FROM game_replays WHERE game_id = 'game-row-1'"
            ).fetchone()
            foreign_keys = conn.execute("PRAGMA foreign_key_list(game_replays)").fetchall()
            requests = conn.execute(
                """
                SELECT account_label, endpoint, request_class, outcome, error
                FROM bga_replay_requests
                ORDER BY id
                """
            ).fetchall()

        self.assertEqual(stored["bga_table_id"], "913515989")
        self.assertEqual(stored["status"], "ready")
        self.assertEqual(stored["color_source"], "bga")
        self.assertEqual(stored["last_account_label"], "injected")
        self.assertEqual(stored["history_request_count"], 1)
        self.assertEqual(
            [tuple(row) for row in requests],
            [("injected", LOGS_PATH, "manual", REPLAY_OUTCOME_READY, None)],
        )
        self.assertNotIn("logs_json", stored.keys())
        self.assertNotIn("event_count", stored.keys())
        self.assertNotIn("tile_count", stored.keys())
        self.assertNotIn("meeple_count", stored.keys())
        self.assertNotIn("archive_requested", stored.keys())
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
            json.loads(stored["board_stats_json"]),
            {"width": 3, "height": 4},
        )
        self.assertEqual(json.loads(stored["meeple_stats_json"])["total_placements"], 1)
        self.assertEqual(json.loads(stored["scoring_json"])["totals"]["cities"], 4)
        self.assertEqual(
            json.loads(stored["player_time_json"])["source"],
            "newActivePlayer.time",
        )
        self.assertTrue(any(row[2] == "games" and row[3] == "game_id" for row in foreign_keys))

    def test_incomplete_colors_use_one_fallback_palette_everywhere(self) -> None:
        result = fetch_and_store_game_replay(
            self.db_path,
            "game-row-1",
            request=lambda *_args, **_kwargs: self._two_player_color_payload(
                beta_color="#0000ff",
                alpha_color=None,
            ),
            authenticate=lambda: None,
        )

        self.assertEqual(result["color_source"], "fallback")
        colors_by_player = {
            player["player_id"]: (player["color_hex"], player["meeple_color"])
            for player in result["players"]
        }
        self.assertEqual(
            colors_by_player,
            {
                "200": ("ff0000", "red"),
                "100": ("008000", "green"),
            },
        )
        colored_events = [
            event
            for event in json.loads(
                self._stored_replay_value("events_json")
            )
            if event["type"] in {"playTile", "playPartisan"}
        ]
        self.assertEqual(
            [event["meeple_color"] for event in colored_events],
            ["red", "red", "green"],
        )
        meeple_players = {
            player["player_id"]: player["meeple_color"]
            for player in result["meeple_stats"]["players"]
        }
        self.assertEqual(meeple_players, {"200": "red", "100": "green"})
        self.assertTrue(result["carcassonne_lab_url"].endswith(
            "?players=Beta,Alpha&colors=red,green"
        ))

    def test_force_refresh_atomically_replaces_fallback_with_bga_colors(self) -> None:
        fetch_and_store_game_replay(
            self.db_path,
            "game-row-1",
            request=lambda *_args, **_kwargs: self._two_player_color_payload(
                beta_color="#0000ff",
                alpha_color=None,
            ),
            authenticate=lambda: None,
        )

        observed_during_request: list[tuple[str, str]] = []

        def request_complete_colors(*_args, **_kwargs):
            with sqlite3.connect(self.db_path) as conn:
                observed_during_request.append(
                    conn.execute(
                        """
                        SELECT status, color_source
                        FROM game_replays
                        WHERE game_id = 'game-row-1'
                        """
                    ).fetchone()
                )
            return self._two_player_color_payload(
                beta_color="#0000ff",
                alpha_color="#ffa500",
            )

        result = fetch_and_store_game_replay(
            self.db_path,
            "game-row-1",
            force=True,
            request=request_complete_colors,
            authenticate=lambda: None,
        )

        self.assertEqual(observed_during_request, [("ready", "fallback")])
        self.assertEqual(result["color_source"], "bga")
        self.assertTrue(result["carcassonne_lab_url"].endswith(
            "?players=Beta,Alpha&colors=blue,yellow"
        ))
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            stored = conn.execute(
                "SELECT * FROM game_replays WHERE game_id = 'game-row-1'"
            ).fetchone()
        self.assertEqual(stored["status"], "ready")
        self.assertEqual(stored["color_source"], "bga")
        self.assertIsNone(stored["last_error"])
        players = {
            player["player_id"]: player
            for player in json.loads(stored["players_json"])
        }
        self.assertEqual(players["200"]["meeple_color"], "blue")
        self.assertEqual(players["100"]["meeple_color"], "yellow")
        events = json.loads(stored["events_json"])
        self.assertEqual(
            [
                event["meeple_color"]
                for event in events
                if event["type"] == "playTile"
            ],
            ["blue", "yellow"],
        )
        meeple_stats = json.loads(stored["meeple_stats_json"])
        self.assertEqual(
            {
                player["player_id"]: player["meeple_color"]
                for player in meeple_stats["players"]
            },
            {"200": "blue", "100": "yellow"},
        )

    def test_failed_force_refresh_keeps_ready_fallback_replay(self) -> None:
        fetch_and_store_game_replay(
            self.db_path,
            "game-row-1",
            request=lambda *_args, **_kwargs: self._two_player_color_payload(
                beta_color=None,
                alpha_color=None,
            ),
            authenticate=lambda: None,
        )
        with sqlite3.connect(self.db_path) as conn:
            before = conn.execute(
                """
                SELECT events_json, players_json, carcassonne_lab_url,
                       meeple_stats_json, color_source
                FROM game_replays
                WHERE game_id = 'game-row-1'
                """
            ).fetchone()

        with self.assertRaises(TemporaryReplayError):
            fetch_and_store_game_replay(
                self.db_path,
                "game-row-1",
                force=True,
                request=lambda *_args, **_kwargs: {
                    "status": 0,
                    "error": "BGA is temporarily unavailable",
                },
                authenticate=lambda: None,
            )

        with sqlite3.connect(self.db_path) as conn:
            after = conn.execute(
                """
                SELECT events_json, players_json, carcassonne_lab_url,
                       meeple_stats_json, color_source, status, last_error
                FROM game_replays
                WHERE game_id = 'game-row-1'
                """
            ).fetchone()
        self.assertEqual(after[:5], before)
        self.assertEqual(after[5], "ready")
        self.assertIn(REPLAY_OUTCOME_TEMPORARY_ERROR, after[6])

    def test_color_refresh_keeps_existing_fallback_when_bga_colors_still_missing(self) -> None:
        fetch_and_store_game_replay(
            self.db_path,
            "game-row-1",
            request=lambda *_args, **_kwargs: self._two_player_color_payload(
                beta_color=None,
                alpha_color=None,
            ),
            authenticate=lambda: None,
        )
        replay_fields = (
            "events_json, players_json, carcassonne_lab_url, board_stats_json, "
            "meeple_stats_json, scoring_json, player_time_json, color_source, fetched_at"
        )
        with sqlite3.connect(self.db_path) as conn:
            before = conn.execute(
                f"SELECT {replay_fields} FROM game_replays WHERE game_id = ?",
                ("game-row-1",),
            ).fetchone()

        result = fetch_and_store_game_replay(
            self.db_path,
            "game-row-1",
            force=True,
            color_refresh=True,
            preserve_existing_fallback=True,
            request=lambda *_args, **_kwargs: self._two_player_color_payload(
                beta_color=None,
                alpha_color=None,
            ),
            authenticate=lambda: None,
        )

        with sqlite3.connect(self.db_path) as conn:
            after = conn.execute(
                f"SELECT {replay_fields}, color_refresh_count, history_request_count "
                "FROM game_replays WHERE game_id = ?",
                ("game-row-1",),
            ).fetchone()
        self.assertEqual(after[:9], before)
        self.assertEqual(after[9], 1)
        self.assertEqual(after[10], 2)
        self.assertEqual(result["color_source"], "fallback")
        self.assertFalse(result["refresh_applied"])

    def test_builds_only_final_board_dimensions(self) -> None:
        board = build_board_stats(
            [
                {"seq": 1, "type": "playTile", "x": 1, "y": 0},
                {"seq": 2, "type": "playTile", "x": 1, "y": -2},
                {"seq": 3, "type": "playTile", "x": 0, "y": -1},
            ]
        )

        self.assertEqual(board, {"width": 2, "height": 3})

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

    def test_lab_url_uses_red_and_green_when_player_colors_are_missing(self) -> None:
        events = [
            {
                "type": "playTile",
                "player_id": "100",
                "player_name": "Alpha",
                "tile_type": 17,
                "x": 0,
                "y": 1,
                "rotation": 0,
            },
            {
                "type": "playTile",
                "player_id": "200",
                "player_name": "Beta",
                "tile_type": 4,
                "x": 0,
                "y": 2,
                "rotation": 1,
            },
        ]
        players = [
            {"player_id": "100", "player_name": "Alpha", "meeple_color": None},
            {"player_id": "200", "player_name": "Beta", "meeple_color": None},
        ]

        color_source = apply_replay_player_colors(events, players)
        url = build_carcassonne_lab_url(events, players)

        self.assertEqual(color_source, "fallback")
        self.assertEqual(
            [(player["color_hex"], player["meeple_color"]) for player in players],
            [("ff0000", "red"), ("008000", "green")],
        )
        self.assertIsNotNone(url)
        self.assertTrue(url.endswith("?players=Alpha,Beta&colors=red,green"))

    def test_duplicate_or_unsupported_bga_colors_fall_back_for_both_players(self) -> None:
        events = [
            {"type": "playTile", "player_id": "200"},
            {"type": "playTile", "player_id": "100"},
        ]
        color_pairs = (
            ("ff0000", "ff0000"),
            ("ff0000", "123456"),
        )
        for beta_color, alpha_color in color_pairs:
            with self.subTest(beta_color=beta_color, alpha_color=alpha_color):
                current_events = json.loads(json.dumps(events))
                players = [
                    {"player_id": "100", "color_hex": alpha_color},
                    {"player_id": "200", "color_hex": beta_color},
                ]

                color_source = apply_replay_player_colors(
                    current_events,
                    players,
                )

                self.assertEqual(color_source, "fallback")
                colors_by_id = {
                    player["player_id"]: player["meeple_color"]
                    for player in players
                }
                self.assertEqual(colors_by_id, {"100": "green", "200": "red"})
                self.assertEqual(
                    [event["meeple_color"] for event in current_events],
                    ["red", "green"],
                )

    def test_existing_replay_table_gets_derived_columns_and_drops_legacy_columns(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE game_replays (
                  game_id TEXT PRIMARY KEY,
                  bga_table_id TEXT NOT NULL,
                  status TEXT NOT NULL DEFAULT 'pending',
                  logs_json TEXT,
                  events_json TEXT,
                  players_json TEXT,
                  event_count INTEGER NOT NULL DEFAULT 0,
                  tile_count INTEGER NOT NULL DEFAULT 0,
                  meeple_count INTEGER NOT NULL DEFAULT 0,
                  archive_requested INTEGER NOT NULL DEFAULT 0,
                  fetched_at TEXT,
                  last_attempt_at TEXT,
                  last_error TEXT,
                  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                INSERT INTO game_replays (
                  game_id, bga_table_id, status, logs_json, players_json, events_json,
                  event_count, tile_count, meeple_count, archive_requested
                ) VALUES (?, ?, ?, '[]', ?, ?, 3, 1, 1, 1)
                """,
                (
                    "game-row-1",
                    "913515989",
                    "ready",
                    json.dumps([
                        {"player_id": "100", "color_hex": "ff0000"},
                        {"player_id": "200", "color_hex": "0000ff"},
                    ]),
                    json.dumps([
                        {"type": "playTile", "player_id": "100"},
                        {"type": "playTile", "player_id": "200"},
                    ]),
                ),
            )
            conn.execute(
                """
                INSERT INTO game_replays (
                  game_id, bga_table_id, status, logs_json, players_json, events_json
                ) VALUES (?, ?, 'ready', '[]', ?, ?)
                """,
                (
                    "fallback-row",
                    "913515990",
                    json.dumps([
                        {"player_id": "100", "color_hex": "ff0000"},
                        {"player_id": "200", "color_hex": None},
                    ]),
                    json.dumps([
                        {"type": "playTile", "player_id": "100"},
                        {"type": "playTile", "player_id": "200"},
                    ]),
                ),
            )
            conn.execute(
                """
                INSERT INTO game_replays (game_id, bga_table_id, status, logs_json)
                VALUES ('error-row', '913515991', 'error', '[]')
                """
            )
            ensure_game_replays_schema(conn)
            columns = {
                row[1]
                for row in conn.execute("PRAGMA table_info(game_replays)").fetchall()
            }
            indexes = {
                row[1]: row
                for row in conn.execute("PRAGMA index_list(game_replays)").fetchall()
            }
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            migrated = conn.execute(
                """
                SELECT game_id, status, color_source, queue_class,
                       retry_reason, next_attempt_at,
                       history_request_count, color_refresh_count,
                       archive_requested_at IS NOT NULL
                FROM game_replays
                ORDER BY game_id
                """
            ).fetchall()

        self.assertTrue(
            {
                "carcassonne_lab_url",
                "board_stats_json",
                "meeple_stats_json",
                "scoring_json",
                "player_time_json",
                "next_attempt_at",
                "retry_reason",
                "queue_class",
                "queued_at",
                "historical_batch_id",
                "history_request_count",
                "color_refresh_count",
                "color_source",
                "archive_requested_at",
                "last_account_label",
                "lease_owner",
                "lease_until",
            }.issubset(columns)
        )
        self.assertTrue(set(LEGACY_GAME_REPLAY_COLUMNS).isdisjoint(columns))
        self.assertIn("idx_game_replays_due", indexes)
        self.assertTrue(
            {
                "bga_replay_requests",
                "bga_replay_account_state",
                "bga_replay_budget_overrides",
            }.issubset(tables)
        )
        self.assertEqual(
            migrated,
            [
                ("error-row", "error", None, "fresh", None, None, 0, 0, 0),
                ("fallback-row", "ready", "fallback", "fresh", None, None, 0, 0, 0),
                ("game-row-1", "ready", "bga", "fresh", None, None, 0, 0, 1),
            ],
        )

        with sqlite3.connect(self.db_path) as conn:
            request_columns = {
                row[1]
                for row in conn.execute(
                    "PRAGMA table_info(bga_replay_requests)"
                ).fetchall()
            }
            override_columns = {
                row[1]
                for row in conn.execute(
                    "PRAGMA table_info(bga_replay_budget_overrides)"
                ).fetchall()
            }
            request_indexes = {
                row[1]
                for row in conn.execute(
                    "PRAGMA index_list(bga_replay_requests)"
                ).fetchall()
            }
        self.assertTrue({"request_class", "budget_override_id"}.issubset(request_columns))
        self.assertTrue(
            {"extra_historical_limit", "total_limit_override", "expires_at"}.issubset(
                override_columns
            )
        )
        self.assertTrue(
            {
                "idx_bga_replay_requests_budget",
                "idx_bga_replay_requests_class_budget",
            }.issubset(request_indexes)
        )

    def test_requests_archive_once_without_polling(self) -> None:
        calls: list[str] = []

        def request(path, params=None, **_kwargs):
            calls.append(path)
            if path == ARCHIVE_REQUEST_PATH:
                self.assertEqual(params, {"table": "913515989"})
                return {"status": 1, "data": {}}
            return {"status": 0, "error": "Cannot find gamenotifs log file"}

        with self.assertRaises(ArchiveMissingError) as raised:
            fetch_and_store_game_replay(
                self.db_path,
                "game-row-1",
                request=request,
                authenticate=lambda: None,
            )

        self.assertEqual(calls, [LOGS_PATH, ARCHIVE_REQUEST_PATH])
        self.assertEqual(raised.exception.outcome, REPLAY_OUTCOME_ARCHIVE_MISSING)
        self.assertEqual(raised.exception.endpoint, LOGS_PATH)
        self.assertTrue(raised.exception.archive_requested)
        self.assertIn(f"endpoint={LOGS_PATH}", str(raised.exception))
        self.assertIn(f"endpoint={ARCHIVE_REQUEST_PATH}", str(raised.exception))

        with sqlite3.connect(self.db_path) as conn:
            stored = conn.execute(
                """
                SELECT status, archive_requested_at, last_error
                FROM game_replays
                WHERE game_id = 'game-row-1'
                """
            ).fetchone()
        self.assertEqual(stored[0], "error")
        self.assertIsNotNone(stored[1])
        self.assertIn(REPLAY_OUTCOME_ARCHIVE_MISSING, stored[2])

        with sqlite3.connect(self.db_path) as conn:
            audited_requests = conn.execute(
                """
                SELECT endpoint, outcome
                FROM bga_replay_requests
                ORDER BY id
                """
            ).fetchall()
        self.assertEqual(
            audited_requests,
            [
                (LOGS_PATH, REPLAY_OUTCOME_ARCHIVE_MISSING),
                (ARCHIVE_REQUEST_PATH, "archive_requested"),
            ],
        )

        calls.clear()
        with self.assertRaises(ArchiveMissingError) as repeated:
            fetch_and_store_game_replay(
                self.db_path,
                "game-row-1",
                request=request,
                authenticate=lambda: None,
            )
        self.assertEqual(calls, [LOGS_PATH])
        self.assertFalse(repeated.exception.archive_requested)
        self.assertIn("already requested", str(repeated.exception))

    def test_fetch_result_classifies_ready_limit_access_and_temporary(self) -> None:
        ready = fetch_bga_replay(
            "913515989",
            request=lambda *_args, **_kwargs: self._successful_payload(),
        )
        self.assertEqual(ready.outcome, REPLAY_OUTCOME_READY)
        self.assertEqual(ready.endpoint, LOGS_PATH)
        self.assertIsNotNone(ready.logs)

        cases = (
            (
                {"status": 0, "error": "You have reached a limit (replay)"},
                REPLAY_OUTCOME_LIMIT,
            ),
            (
                {"status": 0, "error": "Replay access denied"},
                REPLAY_OUTCOME_ACCESS_ERROR,
            ),
            (
                {"status": 0, "error": "BGA is temporarily unavailable"},
                REPLAY_OUTCOME_TEMPORARY_ERROR,
            ),
        )
        for payload, expected_outcome in cases:
            with self.subTest(expected_outcome=expected_outcome):
                result = fetch_bga_replay(
                    "913515989",
                    request=lambda *_args, _payload=payload, **_kwargs: _payload,
                )
                self.assertEqual(result.outcome, expected_outcome)
                self.assertEqual(result.endpoint, LOGS_PATH)

    def test_archive_request_failure_reports_archive_endpoint(self) -> None:
        calls: list[str] = []

        def request(path, **_kwargs):
            calls.append(path)
            if path == LOGS_PATH:
                return {"status": 0, "error": "Cannot find gamenotifs log file"}
            raise TimeoutError("archive request timed out")

        result = fetch_bga_replay("913515989", request=request)

        self.assertEqual(calls, [LOGS_PATH, ARCHIVE_REQUEST_PATH])
        self.assertEqual(result.outcome, REPLAY_OUTCOME_TEMPORARY_ERROR)
        self.assertEqual(result.endpoint, ARCHIVE_REQUEST_PATH)
        self.assertTrue(result.archive_requested)
        self.assertIn(
            f"endpoint={ARCHIVE_REQUEST_PATH}",
            str(result.to_error()),
        )

    def test_storage_path_audits_logs_and_archive_outcomes_separately(self) -> None:
        def request(path, **_kwargs):
            if path == LOGS_PATH:
                return {"status": 0, "error": "Cannot find gamenotifs log file"}
            raise TimeoutError("archive request timed out")

        with self.assertRaises(TemporaryReplayError):
            fetch_and_store_game_replay(
                self.db_path,
                "game-row-1",
                request=request,
                authenticate=lambda: None,
            )

        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT endpoint, outcome, error
                FROM bga_replay_requests
                ORDER BY id
                """
            ).fetchall()
        self.assertEqual(rows[0][0], LOGS_PATH)
        self.assertEqual(rows[0][1], REPLAY_OUTCOME_ARCHIVE_MISSING)
        self.assertIn("Cannot find", rows[0][2])
        self.assertEqual(rows[1][0], ARCHIVE_REQUEST_PATH)
        self.assertEqual(rows[1][1], REPLAY_OUTCOME_TEMPORARY_ERROR)
        self.assertIn("archive request timed out", rows[1][2])

    def test_bga_error_is_persisted(self) -> None:
        def request(_path, params=None, **_kwargs):
            return {"status": 0, "error": "Replay access denied"}

        with self.assertRaisesRegex(ReplayAccessError, "Replay access denied"):
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
        self.assertEqual(row[0], "error")
        self.assertIn(REPLAY_OUTCOME_ACCESS_ERROR, row[1])
        self.assertIn(f"endpoint={LOGS_PATH}", row[1])

    def test_replay_limit_is_audited_and_starts_account_cooldown(self) -> None:
        limited_at = datetime(2026, 9, 23, 12, 0, tzinfo=timezone.utc)
        with self.assertRaises(ReplayLimitError):
            fetch_and_store_game_replay(
                self.db_path,
                "game-row-1",
                account_labels=["account-a"],
                budget_now=limited_at,
                request=lambda *_args, **_kwargs: {
                    "status": 0,
                    "error": "You have reached a limit (replay)",
                },
                authenticate=lambda: None,
            )

        with sqlite3.connect(self.db_path) as conn:
            request_row = conn.execute(
                """
                SELECT account_label, outcome, error
                FROM bga_replay_requests
                """
            ).fetchone()
            state = conn.execute(
                """
                SELECT cooldown_until, last_limit_at, last_error
                FROM bga_replay_account_state
                WHERE account_label = 'account-a'
                """
            ).fetchone()
        self.assertEqual(request_row[0], "account-a")
        self.assertEqual(request_row[1], REPLAY_OUTCOME_LIMIT)
        self.assertIn("limit (replay)", request_row[2])
        self.assertTrue(state[0].startswith("2026-09-24 12:00:00"))
        self.assertTrue(state[1].startswith("2026-09-23 12:00:00"))
        self.assertIn("limit (replay)", state[2])

    def test_replay_limit_starts_cooldown_even_with_total_limit_one_hundred(self) -> None:
        limited_at = datetime(2026, 9, 23, 12, 0, tzinfo=timezone.utc)
        limits = ReplayBudgetLimits()
        override_id = replace_replay_budget_overrides(
            self.db_path,
            account_labels=["account-a"],
            extra_historical_limit=70,
            total_limit_override=100,
            starts_at=limited_at,
            expires_at=limited_at + timedelta(hours=2),
            created_by="admin",
            reason="controlled catch-up",
            limits=limits,
        )[0]
        attempted_at = (limited_at - timedelta(minutes=1)).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                """
                INSERT INTO bga_replay_requests (
                  account_label, bga_table_id, endpoint, request_class,
                  attempted_at, outcome
                ) VALUES ('account-a', ?, ?, 'fresh', ?, 'ready')
                """,
                [
                    (f"used-{index}", LOGS_PATH, attempted_at)
                    for index in range(80)
                ],
            )

        with self.assertRaises(ReplayLimitError):
            fetch_and_store_game_replay(
                self.db_path,
                "game-row-1",
                account_labels=["account-a"],
                request_class="historical",
                budget_limits=limits,
                budget_now=limited_at,
                request=lambda *_args, **_kwargs: {
                    "status": 0,
                    "error": "You have reached a limit (replay)",
                },
                authenticate=lambda: None,
            )

        with sqlite3.connect(self.db_path) as conn:
            request_row = conn.execute(
                """
                SELECT budget_override_id, outcome
                FROM bga_replay_requests
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            cooldown = conn.execute(
                """
                SELECT cooldown_until, last_limit_at
                FROM bga_replay_account_state
                WHERE account_label = 'account-a'
                """
            ).fetchone()
        self.assertEqual(request_row, (override_id, REPLAY_OUTCOME_LIMIT))
        self.assertTrue(cooldown[0].startswith("2026-09-24 12:00:00"))
        self.assertTrue(cooldown[1].startswith("2026-09-23 12:00:00"))

    def test_manual_fetch_cannot_bypass_the_persistent_budget(self) -> None:
        limits = ReplayBudgetLimits(
            total_limit=1,
            fresh_reserve=0,
            historical_limit=1,
            max_total_limit=1,
        )
        fetch_and_store_game_replay(
            self.db_path,
            "game-row-1",
            account_labels=["account-a"],
            budget_limits=limits,
            request=lambda *_args, **_kwargs: self._successful_payload(),
            authenticate=lambda: None,
        )
        calls: list[str] = []
        with self.assertRaises(ReplayBudgetExceededError):
            fetch_and_store_game_replay(
                self.db_path,
                "game-row-1",
                force=True,
                account_labels=["account-a"],
                budget_limits=limits,
                request=lambda *_args, **_kwargs: calls.append("request"),
                authenticate=lambda: calls.append("authenticate"),
            )
        self.assertEqual(calls, [])

    def test_access_error_does_not_cascade_to_another_account(self) -> None:
        fetch_calls: list[dict] = []
        rotation_reasons: list[str] = []

        def fetch_replay(*_args, **kwargs):
            fetch_calls.append(kwargs)
            raise ReplayAccessError("Replay access denied", endpoint=LOGS_PATH)

        with self.assertRaises(ReplayAccessError):
            game_replay_module.fetch_and_store_game_replay_with_account_rotation(
                self.db_path,
                "game-row-1",
                max_account_attempts=3,
                replay_fetcher=fetch_replay,
                account_rotator=lambda *, reason: rotation_reasons.append(reason),
            )

        self.assertEqual(len(fetch_calls), 1)
        self.assertFalse(fetch_calls[0]["force"])
        self.assertEqual(fetch_calls[0]["request_class"], "manual")
        self.assertEqual(rotation_reasons, [])

    def test_non_access_errors_never_rotate_accounts(self) -> None:
        errors = (
            ArchiveMissingError("Archive is missing", endpoint=LOGS_PATH),
            ReplayLimitError("You have reached a limit (replay)", endpoint=LOGS_PATH),
            TemporaryReplayError("Network timeout", endpoint=LOGS_PATH),
            BgaReplayError("Unclassified replay error", endpoint=LOGS_PATH),
        )
        for replay_error in errors:
            with self.subTest(outcome=replay_error.outcome):
                fetch_calls: list[dict] = []
                rotation_reasons: list[str] = []

                def fetch_replay(*_args, **kwargs):
                    fetch_calls.append(kwargs)
                    raise replay_error

                with self.assertRaises(type(replay_error)):
                    game_replay_module.fetch_and_store_game_replay_with_account_rotation(
                        self.db_path,
                        "game-row-1",
                        max_account_attempts=3,
                        replay_fetcher=fetch_replay,
                        account_rotator=lambda *, reason: rotation_reasons.append(reason),
                    )

                self.assertEqual(len(fetch_calls), 1)
                self.assertEqual(rotation_reasons, [])

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

    def test_ready_replay_compacts_legacy_board_stats_without_bga_request(self) -> None:
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
                SET board_stats_json = ?
                WHERE game_id = 'game-row-1'
                """,
                (json.dumps({"final_bounds": {"width": 99, "height": 99}}),),
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
        self.assertEqual(result["board_stats"], {"width": 3, "height": 4})
        with sqlite3.connect(self.db_path) as conn:
            stored = conn.execute(
                "SELECT board_stats_json FROM game_replays WHERE game_id = 'game-row-1'"
            ).fetchone()[0]
        self.assertEqual(json.loads(stored), {"width": 3, "height": 4})

    def test_ready_replay_builds_missing_lab_url_without_bga_request(self) -> None:
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
                SET carcassonne_lab_url = NULL,
                    players_json = ?
                WHERE game_id = 'game-row-1'
                """,
                (json.dumps([
                    {
                        "player_id": "100",
                        "player_name": "Alpha",
                        "color_hex": None,
                        "meeple_color": None,
                    }
                ]),),
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
        self.assertTrue(result["carcassonne_lab_url"].endswith(
            "?players=Alpha,Beta&colors=red,green"
        ))
        with sqlite3.connect(self.db_path) as conn:
            stored = conn.execute(
                """
                SELECT carcassonne_lab_url, players_json, events_json,
                       meeple_stats_json, color_source
                FROM game_replays
                WHERE game_id = 'game-row-1'
                """
            ).fetchone()
        self.assertEqual(stored[0], result["carcassonne_lab_url"])
        self.assertEqual(stored[4], "fallback")
        self.assertEqual(
            [player["meeple_color"] for player in json.loads(stored[1])],
            ["red", "green"],
        )
        self.assertEqual(
            [
                event["meeple_color"]
                for event in json.loads(stored[2])
                if event["type"] == "playTile"
            ],
            ["red", "green"],
        )
        self.assertEqual(
            [
                player["meeple_color"]
                for player in json.loads(stored[3])["players"]
            ],
            ["red", "green"],
        )

    def test_ready_replay_does_not_refetch_missing_derived_data_without_force(self) -> None:
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
        self.assertEqual(result["board_stats"], {"width": 3, "height": 4})
        self.assertEqual(result["meeple_stats"], {})
        self.assertEqual(result["scoring"], {})
        self.assertEqual(result["player_time"], {})

    def test_missing_game_is_rejected(self) -> None:
        with self.assertRaisesRegex(GameNotFoundError, "Game not found"):
            fetch_and_store_game_replay(
                self.db_path,
                "missing",
                request=lambda *_args, **_kwargs: self._successful_payload(),
                authenticate=lambda: None,
            )

    def _stored_replay_value(self, column_name: str):
        allowed_columns = {
            "events_json",
            "players_json",
            "meeple_stats_json",
            "carcassonne_lab_url",
            "color_source",
        }
        if column_name not in allowed_columns:
            raise ValueError(f"Unsupported test column: {column_name}")
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute(
                f"SELECT {column_name} FROM game_replays WHERE game_id = ?",
                ("game-row-1",),
            ).fetchone()[0]

    @staticmethod
    def _two_player_color_payload(*, beta_color, alpha_color):
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
                                            {"player": "200", "color": beta_color},
                                            {"player": "100", "color": alpha_color},
                                        ]
                                    }
                                },
                            },
                            {
                                "type": "playTile",
                                "time": 1_700_000_010,
                                "args": [{
                                    "piece": "tile",
                                    "player_id": "200",
                                    "player_name": "Beta",
                                    "type": "17",
                                    "x": "0",
                                    "y": "1",
                                    "ori": "1",
                                }],
                            },
                            {
                                "type": "playPartisan",
                                "time": 1_700_000_011,
                                "args": [{"piece": "partisan", "pos": "5"}],
                            },
                            {
                                "type": "playTile",
                                "time": 1_700_000_020,
                                "args": [{
                                    "piece": "tile",
                                    "player_id": "100",
                                    "player_name": "Alpha",
                                    "type": "4",
                                    "x": "1",
                                    "y": "1",
                                    "ori": "2",
                                }],
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
                                "type": "playTile",
                                "time": 1_700_000_012,
                                "args": [
                                    {
                                        "piece": "tile",
                                        "player_id": "200",
                                        "player_name": "Beta",
                                        "type": "4",
                                        "x": "-1",
                                        "y": "2",
                                        "ori": "1",
                                    }
                                ],
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
