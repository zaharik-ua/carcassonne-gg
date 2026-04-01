from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .models import MatchUpdateRequest, MatchUpdateResult
from .repository import MatchRepository, TARGET_EMPTY_FINISHED, TARGET_FINISHED_PENDING, TARGET_ONGOING


class SqliteMatchRepository(MatchRepository):
    def __init__(self, db_path: str) -> None:
        self.db_path = str(Path(db_path).resolve())
        self._ensure_schema()

    def fetch_duels_for_match(self, *, match_id: str) -> list[MatchUpdateRequest]:
        sql = """
            SELECT
              l.id AS duel_id,
              l.match_id,
              l.player_1_id,
              l.player_2_id,
              l.time_utc,
              l.duel_format,
              COALESCE(df.games_to_win, 1) AS games_to_win,
              COALESCE(df.minutes_to_play, 60) AS minutes_to_play,
              p1.bga_nickname AS player_1_nickname,
              p2.bga_nickname AS player_2_nickname
            FROM duels l
            JOIN duel_formats df
              ON lower(trim(df.format)) = lower(trim(l.duel_format))
            LEFT JOIN profiles p1
              ON trim(COALESCE(p1.id, '')) = trim(COALESCE(l.player_1_id, ''))
            LEFT JOIN profiles p2
              ON trim(COALESCE(p2.id, '')) = trim(COALESCE(l.player_2_id, ''))
            WHERE l.deleted_at IS NULL
              AND trim(COALESCE(l.match_id, '')) = trim(?)
              AND trim(COALESCE(l.player_1_id, '')) <> ''
              AND trim(COALESCE(l.player_2_id, '')) <> ''
              AND trim(COALESCE(l.time_utc, '')) <> ''
              AND trim(COALESCE(l.duel_format, '')) <> ''
            ORDER BY COALESCE(l.duel_number, 999999) ASC, datetime(l.time_utc) ASC, l.id ASC
        """

        with self._connect() as conn:
            rows = conn.execute(sql, (match_id,)).fetchall()
        return [self._row_to_request(row, "manual") for row in rows]

    def fetch_matches_to_update(self, *, target: str, limit: int) -> list[MatchUpdateRequest]:
        params = {"limit": int(limit)}
        where_sql = """
            l.deleted_at IS NULL
            AND trim(COALESCE(l.player_1_id, '')) <> ''
            AND trim(COALESCE(l.player_2_id, '')) <> ''
            AND trim(COALESCE(l.time_utc, '')) <> ''
            AND trim(COALESCE(l.duel_format, '')) <> ''
        """

        if target == TARGET_ONGOING:
            where_sql += """
                AND datetime(l.time_utc) < datetime('now')
                AND datetime(l.time_utc, '+' || COALESCE(df.minutes_to_play, 60) || ' minutes') > datetime('now')
                AND COALESCE(l.status, 'Planned') <> 'Done'
            """
        elif target in {TARGET_FINISHED_PENDING, TARGET_EMPTY_FINISHED}:
            where_sql += """
                AND datetime('now') > datetime(l.time_utc, '+' || COALESCE(df.minutes_to_play, 60) || ' minutes')
                AND COALESCE(l.status, 'Planned') NOT IN ('Done', 'Error')
            """
        else:
            raise ValueError(f"Unsupported target: {target}")

        sql = f"""
            SELECT
              l.id AS duel_id,
              l.player_1_id,
              l.player_2_id,
              l.time_utc,
              l.duel_format,
              COALESCE(df.games_to_win, 1) AS games_to_win,
              COALESCE(df.minutes_to_play, 60) AS minutes_to_play,
              p1.bga_nickname AS player_1_nickname,
              p2.bga_nickname AS player_2_nickname
            FROM duels l
            JOIN duel_formats df
              ON lower(trim(df.format)) = lower(trim(l.duel_format))
            LEFT JOIN profiles p1
              ON trim(COALESCE(p1.id, '')) = trim(COALESCE(l.player_1_id, ''))
            LEFT JOIN profiles p2
              ON trim(COALESCE(p2.id, '')) = trim(COALESCE(l.player_2_id, ''))
            WHERE {where_sql}
            ORDER BY
              COALESCE(datetime(l.results_checked_at), datetime('1970-01-01 00:00:00')) ASC,
              datetime(l.time_utc) ASC,
              l.id ASC
            LIMIT :limit
        """

        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()

        return [self._row_to_request(row, target) for row in rows]

    def save_match_result(self, match: MatchUpdateRequest, result: MatchUpdateResult) -> None:
        with self._connect() as conn:
            current = conn.execute(
                """
                SELECT
                  l.id,
                  l.status,
                  l.time_utc,
                  COALESCE(df.games_to_win, ?) AS games_to_win
                FROM duels l
                LEFT JOIN duel_formats df
                  ON lower(trim(df.format)) = lower(trim(l.duel_format))
                WHERE l.id = ?
                LIMIT 1
                """,
                (match.gtw or 2, match.match_id),
            ).fetchone()
            if current is None:
                raise RuntimeError(f"Duel not found: {match.match_id}")

            target_wins = int(current["games_to_win"] or match.gtw or 2)
            now_ts = int(datetime.now(timezone.utc).timestamp())
            is_ongoing = int(match.start_date) < now_ts < int(match.end_date)
            has_winner = (
                int(result.wins0) == target_wins and int(result.wins1) < target_wins
            ) or (
                int(result.wins1) == target_wins and int(result.wins0) < target_wins
            )
            if has_winner:
                next_status = "Done"
            elif is_ongoing:
                next_status = "In progress"
            else:
                next_status = "Error"
            conn.execute(
                """
                UPDATE duels
                SET
                  dw1 = ?,
                  dw2 = ?,
                  status = ?,
                  results_last_error = NULL,
                  results_checked_at = CURRENT_TIMESTAMP,
                  updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    result.wins0,
                    result.wins1,
                    next_status,
                    match.match_id,
                ),
            )

            match_row = conn.execute(
                """
                SELECT match_id
                FROM duels
                WHERE id = ?
                LIMIT 1
                """,
                (match.match_id,),
            ).fetchone()
            parent_match_id = str(match_row["match_id"]).strip() if match_row and match_row["match_id"] is not None else ""

            incoming_ids = []
            for index, table in enumerate(result.tables, start=1):
                game_id = f"{match.match_id}-{table.id}"
                incoming_ids.append(str(table.id))
                conn.execute(
                    """
                    INSERT INTO games (
                      id,
                      duel_id,
                      bga_table_id,
                      game_number,
                      player_1_score,
                      player_2_score,
                      player_1_rank,
                      player_2_rank,
                      player_1_clock,
                      player_2_clock,
                      status
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(bga_table_id) DO UPDATE SET
                      id = excluded.id,
                      duel_id = excluded.duel_id,
                      game_number = excluded.game_number,
                      player_1_score = excluded.player_1_score,
                      player_2_score = excluded.player_2_score,
                      player_1_rank = excluded.player_1_rank,
                      player_2_rank = excluded.player_2_rank,
                      player_1_clock = excluded.player_1_clock,
                      player_2_clock = excluded.player_2_clock,
                      status = excluded.status
                    """,
                    (
                        game_id,
                        match.match_id,
                        str(table.id),
                        index,
                        self._to_int_or_none(table.score0),
                        self._to_int_or_none(table.score1),
                        self._to_int_or_none(table.rank0),
                        self._to_int_or_none(table.rank1),
                        int(table.player0_clock or 0),
                        int(table.player1_clock or 0),
                        table.status,
                    ),
                )

            if incoming_ids:
                placeholders = ",".join(["?"] * len(incoming_ids))
                conn.execute(
                    f"""
                    DELETE FROM games
                    WHERE duel_id = ?
                      AND COALESCE(bga_table_id, '') <> ''
                      AND bga_table_id NOT IN ({placeholders})
                    """,
                    [match.match_id, *incoming_ids],
                )

            if parent_match_id:
                self._update_match_aggregates(conn, match_id=parent_match_id)

            conn.commit()

    def save_match_error(self, match: MatchUpdateRequest, message: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE duels
                SET
                  results_last_error = ?,
                  results_checked_at = CURRENT_TIMESTAMP,
                  updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (message, match.match_id),
            )
            conn.commit()
        print(f"⚠️ Match update failed for duel {match.match_id}: {message}", flush=True)

    def _row_to_request(self, row: sqlite3.Row, target: str) -> MatchUpdateRequest:
        start_dt = self._parse_iso_datetime(row["time_utc"])
        end_dt = start_dt.timestamp() + (int(row["minutes_to_play"] or 60) * 60)
        player0_id = self._to_int_or_none(row["player_1_id"])
        player1_id = self._to_int_or_none(row["player_2_id"])

        return MatchUpdateRequest(
            match_id=row["duel_id"],
            target=target,
            player0=str(row["player_1_nickname"] or row["player_1_id"]),
            player1=str(row["player_2_nickname"] or row["player_2_id"]),
            game_id=1,
            start_date=int(start_dt.timestamp()),
            end_date=int(end_dt),
            player0_id=player0_id,
            player1_id=player1_id,
            gtw=int(row["games_to_win"] or 1),
            stat=False,
        )

    @staticmethod
    def _update_match_aggregates(conn: sqlite3.Connection, *, match_id: str) -> None:
        aggregate_row = conn.execute(
            """
            SELECT
              COALESCE(SUM(COALESCE(dw1, 0)), 0) AS gw1,
              COALESCE(SUM(COALESCE(dw2, 0)), 0) AS gw2,
              COALESCE(SUM(CASE
                WHEN COALESCE(status, 'Planned') = 'Done' AND COALESCE(dw1, 0) > COALESCE(dw2, 0)
                THEN 1 ELSE 0 END), 0) AS dw1,
              COALESCE(SUM(CASE
                WHEN COALESCE(status, 'Planned') = 'Done' AND COALESCE(dw2, 0) > COALESCE(dw1, 0)
                THEN 1 ELSE 0 END), 0) AS dw2,
              COUNT(*) AS total_duels,
              COALESCE(SUM(CASE WHEN COALESCE(status, 'Planned') = 'Done' THEN 1 ELSE 0 END), 0) AS done_duels,
              COALESCE(SUM(CASE WHEN COALESCE(status, 'Planned') = 'Error' THEN 1 ELSE 0 END), 0) AS error_duels,
              MIN(CASE
                WHEN datetime(l.time_utc) IS NOT NULL THEN unixepoch(l.time_utc)
                ELSE NULL
              END) AS start_ts,
              MAX(CASE
                WHEN datetime(l.time_utc) IS NOT NULL
                THEN unixepoch(l.time_utc) + (COALESCE(df.minutes_to_play, 60) * 60)
                ELSE NULL
              END) AS end_ts
            FROM duels l
            LEFT JOIN duel_formats df
              ON lower(trim(df.format)) = lower(trim(l.duel_format))
            WHERE l.match_id = ?
              AND l.deleted_at IS NULL
            """,
            (match_id,),
        ).fetchone()

        if aggregate_row is None:
            return

        total_duels = int(aggregate_row["total_duels"] or 0)
        done_duels = int(aggregate_row["done_duels"] or 0)
        error_duels = int(aggregate_row["error_duels"] or 0)
        now_ts = int(datetime.now(timezone.utc).timestamp())
        start_ts = SqliteMatchRepository._to_int_or_none(aggregate_row["start_ts"])
        end_ts = SqliteMatchRepository._to_int_or_none(aggregate_row["end_ts"])

        if error_duels > 0:
            next_status = "Error"
        elif total_duels > 0 and done_duels == total_duels:
            next_status = "Done"
        elif start_ts is not None and end_ts is not None and start_ts <= now_ts < end_ts:
            next_status = "In progress"
        else:
            next_status = "Planned"

        conn.execute(
            """
            UPDATE matches
            SET
              dw1 = ?,
              dw2 = ?,
              gw1 = ?,
              gw2 = ?,
              status = ?,
              updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                int(aggregate_row["dw1"] or 0),
                int(aggregate_row["dw2"] or 0),
                int(aggregate_row["gw1"] or 0),
                int(aggregate_row["gw2"] or 0),
                next_status,
                match_id,
            ),
        )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            duel_columns = {
                str(row["name"]).strip().lower()
                for row in conn.execute("PRAGMA table_info(duels)").fetchall()
                if row["name"] is not None
            }
            match_columns = {
                str(row["name"]).strip().lower()
                for row in conn.execute("PRAGMA table_info(matches)").fetchall()
                if row["name"] is not None
            }
            if "results_checked_at" not in duel_columns:
                conn.execute("ALTER TABLE duels ADD COLUMN results_checked_at TEXT")
            if "rating_full" not in duel_columns:
                conn.execute("ALTER TABLE duels ADD COLUMN rating_full REAL")
            if "rating" not in duel_columns:
                conn.execute("ALTER TABLE duels ADD COLUMN rating INTEGER")
            if "rating" not in match_columns:
                conn.execute("ALTER TABLE matches ADD COLUMN rating INTEGER")
            conn.commit()

    @staticmethod
    def _parse_iso_datetime(value: str) -> datetime:
        raw = str(value or "").strip()
        if not raw:
            raise ValueError("Missing lineup time_utc")
        if raw.endswith("Z"):
            raw = raw.replace("Z", "+00:00")
        return datetime.fromisoformat(raw).astimezone(timezone.utc)

    @staticmethod
    def _to_int_or_none(value) -> int | None:
        if value is None:
            return None
        raw = str(value).strip()
        if not raw or raw == "?":
            return None
        return int(float(raw))
