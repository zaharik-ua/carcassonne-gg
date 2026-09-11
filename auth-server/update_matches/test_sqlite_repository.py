from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .models import MatchTable, MatchUpdateRequest, MatchUpdateResult
from .repository import TARGET_FINISHED_PENDING
from .sqlite_repository import SqliteMatchRepository


class SqliteMatchRepositoryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "auth.sqlite"
        self.past_time = self._format_utc(datetime.now(timezone.utc) - timedelta(hours=2))
        self.current_start_ts = int((datetime.now(timezone.utc) - timedelta(minutes=5)).timestamp())
        self.current_end_ts = int((datetime.now(timezone.utc) + timedelta(minutes=55)).timestamp())
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE profiles (
                  id TEXT,
                  bga_nickname TEXT,
                  gg_elo REAL,
                  deleted_at TEXT
                );
                INSERT INTO profiles VALUES
                  ('100', 'Alpha', 1600, NULL),
                  ('200', 'Beta', 1500, NULL),
                  ('300', 'Gamma', 1700, NULL),
                  ('400', 'Delta', 1400, NULL);

                CREATE TABLE duel_formats (
                  format TEXT,
                  games_to_win INTEGER,
                  minutes_to_play INTEGER
                );
                INSERT INTO duel_formats VALUES ('Bo3', 2, 60);

                CREATE TABLE matches (
                  id TEXT PRIMARY KEY,
                  tournament_id TEXT,
                  team_1 TEXT,
                  team_2 TEXT,
                  stage TEXT,
                  "group" TEXT,
                  dw1 INTEGER,
                  dw2 INTEGER,
                  gw1 INTEGER,
                  gw2 INTEGER,
                  status TEXT,
                  deleted_at TEXT,
                  updated_at TEXT
                );

                CREATE TABLE tournaments (
                  id TEXT PRIMARY KEY,
                  standings_scoring TEXT NOT NULL DEFAULT 'standard',
                  tpr_target_games INTEGER NOT NULL DEFAULT 10,
                  tpr_smoothing REAL NOT NULL DEFAULT 0.5,
                  tpr_benchmark_percentile REAL NOT NULL DEFAULT 0.75,
                  current_tpr_benchmark REAL,
                  tpr_calculated_at TEXT,
                  updated_at TEXT
                );

                CREATE TABLE duels (
                  id TEXT PRIMARY KEY,
                  tournament_id TEXT,
                  match_id TEXT,
                  duel_number INTEGER,
                  player_1_id TEXT,
                  player_2_id TEXT,
                  time_utc TEXT,
                  duel_format TEXT,
                  dw1 INTEGER,
                  dw2 INTEGER,
                  ranking INTEGER NOT NULL DEFAULT 1,
                  player1_elo_before REAL,
                  player2_elo_before REAL,
                  status TEXT,
                  results_last_error TEXT,
                  results_checked_at TEXT,
                  deleted_at TEXT,
                  updated_at TEXT
                );

                CREATE TABLE games (
                  id TEXT PRIMARY KEY,
                  duel_id TEXT,
                  bga_table_id TEXT UNIQUE,
                  game_number INTEGER,
                  player_1_score INTEGER,
                  player_2_score INTEGER,
                  player_1_rank INTEGER,
                  player_2_rank INTEGER,
                  player_1_clock INTEGER,
                  player_2_clock INTEGER,
                  status TEXT,
                  deleted_at TEXT
                );

                CREATE TABLE standings (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  tournament_id TEXT,
                  stage TEXT,
                  "group" TEXT,
                  team_id TEXT,
                  player_id TEXT,
                  mp INTEGER NOT NULL DEFAULT 0,
                  mw INTEGER NOT NULL DEFAULT 0,
                  ml INTEGER NOT NULL DEFAULT 0,
                  dw INTEGER,
                  dl INTEGER,
                  gw INTEGER NOT NULL DEFAULT 0,
                  gl INTEGER NOT NULL DEFAULT 0,
                  mdif INTEGER NOT NULL DEFAULT 0,
                  ddif INTEGER,
                  gdif INTEGER NOT NULL DEFAULT 0,
                  starting_elo REAL,
                  elo_used REAL,
                  smoothed_win_rate REAL,
                  tpr REAL,
                  tpr_confidence REAL,
                  adjusted_tpr REAL,
                  bounty REAL,
                  opponents_bounty_points REAL,
                  points REAL,
                  performance_calculated_at TEXT,
                  position INTEGER,
                  created_at TEXT,
                  updated_at TEXT
                );
                CREATE UNIQUE INDEX idx_test_standings_tournament_stage_player
                  ON standings(tournament_id, stage, player_id)
                  WHERE player_id IS NOT NULL AND team_id IS NULL;
                """
            )
            for duel_id, status, deleted_at in [
                ("planned", "Planned", None),
                ("in-progress", "In progress", None),
                ("done", "Done", None),
                ("cancelled", "Cancelled", None),
                ("draft", "Draft", None),
                ("requested-new-time", "Requested new time", None),
                ("deleted", "Planned", "2026-01-01 00:00:00"),
            ]:
                self._insert_duel(
                    conn,
                    duel_id=duel_id,
                    status=status,
                    deleted_at=deleted_at,
                    match_id="match-done" if duel_id == "done" else "match-1",
                )

        self.gg_elo_recalculations = []
        self.repository = SqliteMatchRepository(
            str(self.db_path),
            gg_elo_recalculator=lambda: self.gg_elo_recalculations.append("run"),
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_fetch_matches_to_update_skips_protected_duels(self) -> None:
        batch = self.repository.fetch_matches_to_update(target=TARGET_FINISHED_PENDING, limit=20)

        self.assertEqual({str(item.match_id) for item in batch}, {"planned", "in-progress"})

    def test_manual_fetch_skips_protected_duels(self) -> None:
        self.assertEqual(self.repository.fetch_duel_by_id(duel_id="cancelled"), [])
        self.assertEqual(self.repository.fetch_duel_by_id(duel_id="draft"), [])
        self.assertEqual(self.repository.fetch_duel_by_id(duel_id="requested-new-time"), [])
        self.assertEqual(self.repository.fetch_duel_by_id(duel_id="deleted"), [])
        self.assertEqual(
            [str(item.match_id) for item in self.repository.fetch_duel_by_id(duel_id="done")],
            ["done"],
        )

        match_duels = self.repository.fetch_duels_for_match(match_id="match-1")
        self.assertEqual({str(item.match_id) for item in match_duels}, {"planned", "in-progress"})

    def test_save_match_result_does_not_touch_protected_duels(self) -> None:
        for duel_id in ("cancelled", "draft", "requested-new-time", "deleted"):
            with self.subTest(duel_id=duel_id):
                before = self._load_duel(duel_id)

                self.repository.save_match_result(
                    self._request(duel_id),
                    MatchUpdateResult(
                        status="success",
                        wins0=1,
                        wins1=0,
                        tables=[self._table()],
                    ),
                )

                self.assertEqual(self._load_duel(duel_id), before)
                self.assertEqual(self._game_count(duel_id), 0)

    def test_save_match_error_does_not_touch_protected_duels(self) -> None:
        for duel_id in ("cancelled", "draft", "requested-new-time", "deleted"):
            with self.subTest(duel_id=duel_id):
                before = self._load_duel(duel_id)

                self.repository.save_match_error(self._request(duel_id), "BGA failed")

                self.assertEqual(self._load_duel(duel_id), before)

    def test_save_match_result_updates_planned_duel(self) -> None:
        self.repository.save_match_result(
            self._request("planned"),
            MatchUpdateResult(
                status="success",
                wins0=2,
                wins1=0,
                tables=[self._table()],
            ),
        )

        row = self._load_duel("planned")
        self.assertEqual(row["dw1"], 2)
        self.assertEqual(row["dw2"], 0)
        self.assertEqual(row["status"], "Done")
        self.assertIsNone(row["results_last_error"])
        self.assertEqual(self._game_count("planned"), 1)

    def test_ranked_done_transition_queues_one_gg_elo_recalculation(self) -> None:
        self.repository.save_match_result(
            self._request("planned"),
            MatchUpdateResult(
                status="success",
                wins0=2,
                wins1=0,
                tables=[self._table()],
            ),
        )

        self.assertEqual(self.gg_elo_recalculations, [])
        self.repository.finish_update_run()
        self.assertEqual(self.gg_elo_recalculations, ["run"])
        self.repository.finish_update_run()
        self.assertEqual(self.gg_elo_recalculations, ["run"])

    def test_unranked_done_transition_does_not_queue_gg_elo_recalculation(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE duels SET ranking = 0 WHERE id = 'planned'")

        self.repository.save_match_result(
            self._request("planned"),
            MatchUpdateResult(
                status="success",
                wins0=2,
                wins1=0,
                tables=[self._table()],
            ),
        )
        self.repository.finish_update_run()

        self.assertEqual(self.gg_elo_recalculations, [])

    def test_ranked_non_done_updates_do_not_queue_gg_elo_recalculation(self) -> None:
        incomplete_result = MatchUpdateResult(
            status="success",
            wins0=1,
            wins1=0,
            tables=[self._table()],
        )
        self.repository.save_match_result(self._request("planned"), incomplete_result)
        self.repository.finish_update_run()

        self.assertEqual(self.gg_elo_recalculations, [])

    def test_match_completion_recalculates_existing_team_and_player_standings(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO matches (
                  id, tournament_id, team_1, team_2, stage, "group", status
                )
                VALUES ('standings-match', 'TOURNAMENT-1', 'TEAM-A', 'TEAM-B', 'Stage 1', 'A', 'Planned')
                """
            )
            self._insert_duel(
                conn,
                duel_id="standings-duel",
                status="Planned",
                deleted_at=None,
                match_id="standings-match",
            )
            conn.executemany(
                """
                INSERT INTO standings (
                  tournament_id, stage, "group", team_id, player_id
                )
                VALUES ('TOURNAMENT-1', 'Stage 1', 'A', ?, ?)
                """,
                [
                    ("TEAM-A", None),
                    ("TEAM-B", None),
                    (None, "100"),
                    (None, "200"),
                ],
            )

        self.repository.save_match_result(
            self._request("standings-duel"),
            MatchUpdateResult(
                status="success",
                wins0=2,
                wins1=0,
                tables=[self._table()],
            ),
        )

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            match = conn.execute(
                "SELECT status, dw1, dw2, gw1, gw2 FROM matches WHERE id = 'standings-match'"
            ).fetchone()
            rows = conn.execute(
                """
                SELECT team_id, player_id, mp, mw, ml, dw, dl, gw, gl, mdif, ddif, gdif, position
                FROM standings
                WHERE tournament_id = 'TOURNAMENT-1'
                """
            ).fetchall()

        self.assertIsNotNone(match)
        self.assertEqual(dict(match), {"status": "Done", "dw1": 1, "dw2": 0, "gw1": 2, "gw2": 0})
        standings = {
            str(row["team_id"] or row["player_id"]): dict(row)
            for row in rows
        }
        self.assertEqual(
            standings["TEAM-A"],
            {
                "team_id": "TEAM-A", "player_id": None,
                "mp": 1, "mw": 1, "ml": 0, "dw": 1, "dl": 0,
                "gw": 2, "gl": 0, "mdif": 1, "ddif": 1, "gdif": 2, "position": 1,
            },
        )
        self.assertEqual(
            standings["TEAM-B"],
            {
                "team_id": "TEAM-B", "player_id": None,
                "mp": 1, "mw": 0, "ml": 1, "dw": 0, "dl": 1,
                "gw": 0, "gl": 2, "mdif": -1, "ddif": -1, "gdif": -2, "position": 2,
            },
        )
        self.assertEqual(
            standings["100"],
            {
                "team_id": None, "player_id": "100",
                "mp": 1, "mw": 1, "ml": 0, "dw": None, "dl": None,
                "gw": 2, "gl": 0, "mdif": 1, "ddif": None, "gdif": 2, "position": 1,
            },
        )
        self.assertEqual(
            standings["200"],
            {
                "team_id": None, "player_id": "200",
                "mp": 1, "mw": 0, "ml": 1, "dw": None, "dl": None,
                "gw": 0, "gl": 2, "mdif": -1, "ddif": None, "gdif": -2, "position": 2,
            },
        )

    def test_match_completion_skips_recalculation_without_standings(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO matches (
                  id, tournament_id, team_1, team_2, stage, status
                )
                VALUES ('no-standings-match', 'EMPTY-TOURNAMENT', 'TEAM-A', 'TEAM-B', 'Stage 1', 'Planned')
                """
            )
            self._insert_duel(
                conn,
                duel_id="no-standings-duel",
                status="Planned",
                deleted_at=None,
                match_id="no-standings-match",
            )

        self.repository.save_match_result(
            self._request("no-standings-duel"),
            MatchUpdateResult(
                status="success",
                wins0=2,
                wins1=0,
                tables=[self._table()],
            ),
        )

        with sqlite3.connect(self.db_path) as conn:
            match_status = conn.execute(
                "SELECT status FROM matches WHERE id = 'no-standings-match'"
            ).fetchone()[0]
            standings_count = conn.execute(
                "SELECT COUNT(*) FROM standings WHERE tournament_id = 'EMPTY-TOURNAMENT'"
            ).fetchone()[0]
        self.assertEqual(match_status, "Done")
        self.assertEqual(standings_count, 0)

    def test_standalone_rivals_completion_recalculates_bounty_standings(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO tournaments (
                  id,
                  standings_scoring,
                  tpr_target_games,
                  tpr_smoothing,
                  tpr_benchmark_percentile,
                  updated_at
                )
                VALUES ('RIVALS-1', 'bounty_tpr', 10, 0.5, 0.75, CURRENT_TIMESTAMP)
                """
            )
            self._insert_duel(
                conn,
                duel_id="rivals-completing",
                status="Planned",
                deleted_at=None,
                match_id=None,
                tournament_id="RIVALS-1",
            )
            for duel_id, status, player_1_id, player_2_id, dw1, dw2 in [
                ("rivals-error", "Error", "100", "300", 2, 0),
                ("rivals-in-progress", "In progress", "200", "300", 2, 0),
                ("rivals-cancelled", "Cancelled", "100", "400", 2, 0),
            ]:
                self._insert_duel(
                    conn,
                    duel_id=duel_id,
                    status=status,
                    deleted_at=None,
                    match_id=None,
                    tournament_id="RIVALS-1",
                    player_1_id=player_1_id,
                    player_2_id=player_2_id,
                    dw1=dw1,
                    dw2=dw2,
                )

        self.repository.save_match_result(
            self._request("rivals-completing"),
            MatchUpdateResult(
                status="success",
                wins0=2,
                wins1=0,
                tables=[self._table()],
            ),
        )

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT
                  player_id, mp, mw, ml, gw, gl, mdif, gdif, position,
                  smoothed_win_rate, tpr, tpr_confidence, adjusted_tpr, bounty, points
                FROM standings
                WHERE tournament_id = 'RIVALS-1'
                ORDER BY position ASC
                """
            ).fetchall()
            tournament = conn.execute(
                """
                SELECT current_tpr_benchmark, tpr_calculated_at
                FROM tournaments
                WHERE id = 'RIVALS-1'
                """
            ).fetchone()

        self.assertEqual([row["player_id"] for row in rows], ["100", "200"])
        self.assertEqual(
            [
                (row["player_id"], row["mp"], row["mw"], row["ml"], row["gw"], row["gl"], row["mdif"], row["gdif"])
                for row in rows
            ],
            [
                ("100", 1, 1, 0, 2, 0, 1, 2),
                ("200", 1, 0, 1, 0, 2, -1, -2),
            ],
        )
        self.assertAlmostEqual(rows[0]["smoothed_win_rate"], 0.75)
        self.assertAlmostEqual(rows[0]["tpr"], 1690.85)
        self.assertAlmostEqual(rows[0]["tpr_confidence"], 0.1)
        self.assertAlmostEqual(rows[0]["adjusted_tpr"], 1609.08)
        self.assertAlmostEqual(rows[0]["bounty"], 0.5424128)
        self.assertAlmostEqual(rows[0]["points"], 0.91756422)
        self.assertAlmostEqual(rows[1]["bounty"], 0.37515142)
        self.assertAlmostEqual(rows[1]["points"], 0.37515142)
        self.assertAlmostEqual(tournament["current_tpr_benchmark"], 1579.54)
        self.assertIsNotNone(tournament["tpr_calculated_at"])

    def test_standalone_rivals_completion_recalculates_standard_standings(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO tournaments (id, standings_scoring) VALUES ('RIVALS-STANDARD', 'standard')"
            )
            conn.executemany(
                """
                INSERT INTO standings (tournament_id, stage, player_id)
                VALUES ('RIVALS-STANDARD', 'Stage 1', ?)
                """,
                [("100",), ("200",)],
            )
            self._insert_duel(
                conn,
                duel_id="rivals-standard-completing",
                status="Planned",
                deleted_at=None,
                match_id=None,
                tournament_id="RIVALS-STANDARD",
            )

        self.repository.save_match_result(
            self._request("rivals-standard-completing"),
            MatchUpdateResult(
                status="success",
                wins0=2,
                wins1=0,
                tables=[self._table()],
            ),
        )

        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT player_id, mp, mw, ml, gw, gl, position
                FROM standings
                WHERE tournament_id = 'RIVALS-STANDARD'
                ORDER BY position ASC
                """
            ).fetchall()
        self.assertEqual(
            rows,
            [
                ("100", 1, 1, 0, 2, 0, 1),
                ("200", 1, 0, 1, 0, 2, 2),
            ],
        )

    def test_standalone_rivals_non_done_updates_do_not_recalculate_standings(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO tournaments (
                  id,
                  standings_scoring,
                  current_tpr_benchmark,
                  updated_at
                )
                VALUES ('RIVALS-2', 'bounty_tpr', 777, CURRENT_TIMESTAMP)
                """
            )
            conn.execute(
                """
                INSERT INTO standings (
                  tournament_id, stage, player_id, mp, points, created_at, updated_at
                )
                VALUES ('RIVALS-2', 'Stage 1', '100', 9, 9, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
            )
            self._insert_duel(
                conn,
                duel_id="rivals-incomplete",
                status="Planned",
                deleted_at=None,
                match_id=None,
                tournament_id="RIVALS-2",
            )

        request = self._request("rivals-incomplete")
        incomplete_result = MatchUpdateResult(
            status="success",
            wins0=1,
            wins1=0,
            tables=[self._table()],
        )
        self.repository.save_match_result(request, incomplete_result)
        self.assertEqual(self._load_duel("rivals-incomplete")["status"], "In progress")

        request.end_date = self.current_start_ts - 1
        self.repository.save_match_result(request, incomplete_result)
        self.assertEqual(self._load_duel("rivals-incomplete")["status"], "Error")

        with sqlite3.connect(self.db_path) as conn:
            standing = conn.execute(
                "SELECT mp, points FROM standings WHERE tournament_id = 'RIVALS-2' AND player_id = '100'"
            ).fetchone()
            benchmark = conn.execute(
                "SELECT current_tpr_benchmark FROM tournaments WHERE id = 'RIVALS-2'"
            ).fetchone()[0]
        self.assertEqual(standing, (9, 9.0))
        self.assertEqual(benchmark, 777.0)

    def test_fetches_only_completed_games_without_ready_replay(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO duels (id, time_utc, deleted_at) VALUES (?, ?, NULL)",
                ("replay-duel", self.past_time),
            )
            conn.executemany(
                """
                INSERT INTO games (id, duel_id, bga_table_id, game_number, status, deleted_at)
                VALUES (?, 'replay-duel', ?, ?, ?, ?)
                """,
                [
                    ("ready-later", "900000001", 1, "Finished", None),
                    ("conceded", "900000002", 2, "Conceded", None),
                    ("ongoing", "900000003", 3, "In progress", None),
                    ("invalid-table", "not-a-number", 4, "Finished", None),
                    ("deleted-game", "900000005", 5, "Finished", "2026-01-01"),
                ],
            )

        self.assertEqual(
            self.repository.fetch_game_ids_pending_replay(limit=100),
            ["ready-later", "conceded"],
        )

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO game_replays (game_id, bga_table_id, status)
                VALUES ('ready-later', '900000001', 'ready')
                """
            )

        self.assertEqual(
            self.repository.fetch_game_ids_pending_replay(limit=100),
            ["conceded"],
        )

    def _insert_duel(
        self,
        conn: sqlite3.Connection,
        *,
        duel_id: str,
        status: str,
        deleted_at: str | None,
        match_id: str | None = "match-1",
        tournament_id: str | None = None,
        player_1_id: str = "100",
        player_2_id: str = "200",
        dw1: int | None = None,
        dw2: int | None = None,
    ) -> None:
        conn.execute(
            """
            INSERT INTO duels (
              id,
              tournament_id,
              match_id,
              duel_number,
              player_1_id,
              player_2_id,
              time_utc,
              duel_format,
              dw1,
              dw2,
              status,
              results_last_error,
              results_checked_at,
              deleted_at,
              updated_at
            )
            VALUES (
              ?,
              ?,
              ?,
              1,
              ?,
              ?,
              ?,
              'Bo3',
              ?,
              ?,
              ?,
              'old-error',
              'old-check',
              ?,
              'old-update'
            )
            """,
            (
                duel_id,
                tournament_id,
                match_id,
                player_1_id,
                player_2_id,
                self.past_time,
                dw1,
                dw2,
                status,
                deleted_at,
            ),
        )

    def _request(self, duel_id: str) -> MatchUpdateRequest:
        return MatchUpdateRequest(
            match_id=duel_id,
            target="manual_duel",
            player0="Alpha",
            player1="Beta",
            game_id=1,
            start_date=self.current_start_ts,
            end_date=self.current_end_ts,
            player0_id=100,
            player1_id=200,
            gtw=2,
        )

    @staticmethod
    def _table() -> MatchTable:
        return MatchTable(
            id="123456789",
            url="https://boardgamearena.com/table?table=123456789",
            score0="90",
            score1="80",
            rank0="1",
            rank1="0",
            timestamp=0,
            status="Finished",
        )

    def _load_duel(self, duel_id: str) -> dict[str, object]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT dw1, dw2, status, results_last_error, results_checked_at, deleted_at, updated_at
                FROM duels
                WHERE id = ?
                """,
                (duel_id,),
            ).fetchone()
        self.assertIsNotNone(row)
        return dict(row)

    def _game_count(self, duel_id: str) -> int:
        with sqlite3.connect(self.db_path) as conn:
            return int(
                conn.execute(
                    "SELECT COUNT(*) FROM games WHERE duel_id = ?",
                    (duel_id,),
                ).fetchone()[0]
            )

    @staticmethod
    def _format_utc(value: datetime) -> str:
        return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()


if __name__ == "__main__":
    unittest.main()
