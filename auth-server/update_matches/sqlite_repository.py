from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .models import MatchUpdateRequest, MatchUpdateResult
from .repository import MatchRepository, TARGET_EMPTY_FINISHED, TARGET_ONGOING


class SqliteMatchRepository(MatchRepository):
    def __init__(self, db_path: str) -> None:
        self.db_path = str(Path(db_path).resolve())

    def fetch_lineups_for_match(self, *, match_id: str) -> list[MatchUpdateRequest]:
        sql = """
            SELECT
              l.id AS lineup_id,
              l.match_id,
              l.player_1_id,
              l.player_2_id,
              l.time_utc,
              l.duel_format,
              COALESCE(df.games_to_win, 1) AS games_to_win,
              COALESCE(df.minutes_to_play, 60) AS minutes_to_play,
              p1.bga_nickname AS player_1_nickname,
              p2.bga_nickname AS player_2_nickname
            FROM lineups l
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
            AND datetime(l.time_utc) <= datetime('now')
        """

        if target == TARGET_ONGOING:
            where_sql += " AND COALESCE(l.status, 'Planned') = 'Planned'"
        elif target == TARGET_EMPTY_FINISHED:
            where_sql += " AND COALESCE(l.status, '') = 'Done' AND (l.dw1 IS NULL OR l.dw2 IS NULL)"
        else:
            raise ValueError(f"Unsupported target: {target}")

        sql = f"""
            SELECT
              l.id AS lineup_id,
              l.player_1_id,
              l.player_2_id,
              l.time_utc,
              l.duel_format,
              COALESCE(df.games_to_win, 1) AS games_to_win,
              COALESCE(df.minutes_to_play, 60) AS minutes_to_play,
              p1.bga_nickname AS player_1_nickname,
              p2.bga_nickname AS player_2_nickname
            FROM lineups l
            JOIN duel_formats df
              ON lower(trim(df.format)) = lower(trim(l.duel_format))
            LEFT JOIN profiles p1
              ON trim(COALESCE(p1.id, '')) = trim(COALESCE(l.player_1_id, ''))
            LEFT JOIN profiles p2
              ON trim(COALESCE(p2.id, '')) = trim(COALESCE(l.player_2_id, ''))
            WHERE {where_sql}
            ORDER BY datetime(l.time_utc) ASC, l.id ASC
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
                  COALESCE(df.games_to_win, ?) AS games_to_win
                FROM lineups l
                LEFT JOIN duel_formats df
                  ON lower(trim(df.format)) = lower(trim(l.duel_format))
                WHERE l.id = ?
                LIMIT 1
                """,
                (match.gtw or 2, match.match_id),
            ).fetchone()
            if current is None:
                raise RuntimeError(f"Lineup not found: {match.match_id}")

            target_wins = int(current["games_to_win"] or match.gtw or 2)
            next_status = "Done" if (result.wins0 >= target_wins or result.wins1 >= target_wins) else "Planned"

            conn.execute(
                """
                UPDATE lineups
                SET
                  dw1 = ?,
                  dw2 = ?,
                  status = ?,
                  results_last_error = NULL,
                  updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (result.wins0, result.wins1, next_status, match.match_id),
            )

            incoming_ids = []
            for index, table in enumerate(result.tables, start=1):
                game_id = f"{match.match_id}-{table.id}"
                incoming_ids.append(str(table.id))
                conn.execute(
                    """
                    INSERT INTO games (
                      id,
                      lineup_id,
                      bga_table_id,
                      game_number,
                      player_1_score,
                      player_2_score,
                      player_1_rank,
                      player_2_rank,
                      status,
                      bga_flags
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(bga_table_id) DO UPDATE SET
                      id = excluded.id,
                      lineup_id = excluded.lineup_id,
                      game_number = excluded.game_number,
                      player_1_score = excluded.player_1_score,
                      player_2_score = excluded.player_2_score,
                      player_1_rank = excluded.player_1_rank,
                      player_2_rank = excluded.player_2_rank,
                      status = excluded.status,
                      bga_flags = excluded.bga_flags
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
                        table.status,
                        table.flags or None,
                    ),
                )

            if incoming_ids:
                placeholders = ",".join(["?"] * len(incoming_ids))
                conn.execute(
                    f"""
                    DELETE FROM games
                    WHERE lineup_id = ?
                      AND COALESCE(bga_table_id, '') <> ''
                      AND bga_table_id NOT IN ({placeholders})
                    """,
                    [match.match_id, *incoming_ids],
                )

            conn.commit()

    def save_match_error(self, match: MatchUpdateRequest, message: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE lineups
                SET
                  results_last_error = ?,
                  updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (message, match.match_id),
            )
            conn.commit()
        print(f"⚠️ Match update failed for lineup {match.match_id}: {message}", flush=True)

    def _row_to_request(self, row: sqlite3.Row, target: str) -> MatchUpdateRequest:
        start_dt = self._parse_iso_datetime(row["time_utc"])
        end_dt = start_dt.timestamp() + (int(row["minutes_to_play"] or 60) * 60)
        player0_id = self._to_int_or_none(row["player_1_id"])
        player1_id = self._to_int_or_none(row["player_2_id"])

        return MatchUpdateRequest(
            match_id=row["lineup_id"],
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

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

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
