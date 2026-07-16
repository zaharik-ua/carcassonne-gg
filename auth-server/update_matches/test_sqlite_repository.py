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
                  bga_nickname TEXT
                );
                INSERT INTO profiles VALUES ('100', 'Alpha'), ('200', 'Beta');

                CREATE TABLE duel_formats (
                  format TEXT,
                  games_to_win INTEGER,
                  minutes_to_play INTEGER
                );
                INSERT INTO duel_formats VALUES ('Bo3', 2, 60);

                CREATE TABLE matches (
                  id TEXT PRIMARY KEY,
                  dw1 INTEGER,
                  dw2 INTEGER,
                  gw1 INTEGER,
                  gw2 INTEGER,
                  status TEXT,
                  updated_at TEXT
                );

                CREATE TABLE duels (
                  id TEXT PRIMARY KEY,
                  match_id TEXT,
                  duel_number INTEGER,
                  player_1_id TEXT,
                  player_2_id TEXT,
                  time_utc TEXT,
                  duel_format TEXT,
                  dw1 INTEGER,
                  dw2 INTEGER,
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

        self.repository = SqliteMatchRepository(str(self.db_path))

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

    def _insert_duel(
        self,
        conn: sqlite3.Connection,
        *,
        duel_id: str,
        status: str,
        deleted_at: str | None,
        match_id: str = "match-1",
    ) -> None:
        conn.execute(
            """
            INSERT INTO duels (
              id,
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
              1,
              '100',
              '200',
              ?,
              'Bo3',
              NULL,
              NULL,
              ?,
              'old-error',
              'old-check',
              ?,
              'old-update'
            )
            """,
            (duel_id, match_id, self.past_time, status, deleted_at),
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
