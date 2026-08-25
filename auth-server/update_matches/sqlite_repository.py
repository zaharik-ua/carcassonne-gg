from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .models import MatchUpdateRequest, MatchUpdateResult
from .repository import MatchRepository, TARGET_EMPTY_FINISHED, TARGET_FINISHED_PENDING, TARGET_ONGOING


RESULT_SYNC_PROTECTED_STATUSES = {
    "cancelled",
    "canceled",
    "draft",
    "removed",
    "requested new time",
}
AUTO_RESULT_SYNC_BLOCKED_STATUSES = RESULT_SYNC_PROTECTED_STATUSES | {
    "done",
    "error",
    "no show",
}
_STANDINGS_GAME_SCORE_UNSET = object()


def _status_sql(statuses: set[str]) -> str:
    return "(" + ",".join(f"'{status}'" for status in sorted(statuses)) + ")"


RESULT_SYNC_PROTECTED_STATUS_SQL = _status_sql(RESULT_SYNC_PROTECTED_STATUSES)
AUTO_RESULT_SYNC_BLOCKED_STATUS_SQL = _status_sql(AUTO_RESULT_SYNC_BLOCKED_STATUSES)


class SqliteMatchRepository(MatchRepository):
    def __init__(self, db_path: str) -> None:
        self.db_path = str(Path(db_path).resolve())
        self._ensure_schema()

    def fetch_duel_by_id(self, *, duel_id: str) -> list[MatchUpdateRequest]:
        sql = f"""
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
            WHERE trim(COALESCE(l.deleted_at, '')) = ''
              AND trim(COALESCE(l.id, '')) = trim(?)
              AND trim(COALESCE(l.player_1_id, '')) <> ''
              AND trim(COALESCE(l.player_2_id, '')) <> ''
              AND trim(COALESCE(l.time_utc, '')) <> ''
              AND trim(COALESCE(l.duel_format, '')) <> ''
              AND lower(trim(COALESCE(l.status, 'Planned'))) NOT IN {RESULT_SYNC_PROTECTED_STATUS_SQL}
            LIMIT 1
        """

        with self._connect() as conn:
            rows = conn.execute(sql, (duel_id,)).fetchall()
        return [self._row_to_request(rows[0], "manual_duel")] if rows else []

    def fetch_duels_for_match(self, *, match_id: str) -> list[MatchUpdateRequest]:
        sql = f"""
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
            WHERE trim(COALESCE(l.deleted_at, '')) = ''
              AND trim(COALESCE(l.match_id, '')) = trim(?)
              AND trim(COALESCE(l.player_1_id, '')) <> ''
              AND trim(COALESCE(l.player_2_id, '')) <> ''
              AND trim(COALESCE(l.time_utc, '')) <> ''
              AND trim(COALESCE(l.duel_format, '')) <> ''
              AND lower(trim(COALESCE(l.status, 'Planned'))) NOT IN {RESULT_SYNC_PROTECTED_STATUS_SQL}
            ORDER BY COALESCE(l.duel_number, 999999) ASC, datetime(l.time_utc) ASC, l.id ASC
        """

        with self._connect() as conn:
            rows = conn.execute(sql, (match_id,)).fetchall()
        return [self._row_to_request(row, "manual") for row in rows]

    def fetch_matches_to_update(self, *, target: str, limit: int) -> list[MatchUpdateRequest]:
        params = {"limit": int(limit)}
        where_sql = f"""
            trim(COALESCE(l.deleted_at, '')) = ''
            AND trim(COALESCE(l.player_1_id, '')) <> ''
            AND trim(COALESCE(l.player_2_id, '')) <> ''
            AND trim(COALESCE(l.time_utc, '')) <> ''
            AND trim(COALESCE(l.duel_format, '')) <> ''
            AND lower(trim(COALESCE(l.status, 'Planned'))) NOT IN {AUTO_RESULT_SYNC_BLOCKED_STATUS_SQL}
        """

        if target == TARGET_ONGOING:
            where_sql += """
                AND datetime(l.time_utc) < datetime('now')
                AND datetime(l.time_utc, '+' || COALESCE(df.minutes_to_play, 60) || ' minutes') > datetime('now')
                AND COALESCE(l.status, 'Planned') NOT IN ('Done', 'Error', 'No Show')
            """
        elif target in {TARGET_FINISHED_PENDING, TARGET_EMPTY_FINISHED}:
            where_sql += """
                AND datetime('now') > datetime(l.time_utc, '+' || COALESCE(df.minutes_to_play, 60) || ' minutes')
                AND COALESCE(l.status, 'Planned') NOT IN ('Done', 'Error', 'No Show')
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
                  l.deleted_at,
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
            if self._is_result_sync_blocked(current):
                print(
                    f"⏭️ Skipping result sync for protected duel {match.match_id} "
                    f"(status={current['status']!r}, deleted_at={current['deleted_at']!r})",
                    flush=True,
                )
                return

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
                      status,
                      deleted_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
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
                      status = excluded.status,
                      deleted_at = NULL
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
            current = conn.execute(
                """
                SELECT id, status, deleted_at
                FROM duels
                WHERE id = ?
                LIMIT 1
                """,
                (match.match_id,),
            ).fetchone()
            if current is None:
                print(f"⚠️ Match update failed for missing duel {match.match_id}: {message}", flush=True)
                return
            if self._is_result_sync_blocked(current):
                print(
                    f"⏭️ Skipping result sync error for protected duel {match.match_id} "
                    f"(status={current['status']!r}, deleted_at={current['deleted_at']!r})",
                    flush=True,
                )
                return
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
        extra: dict[str, object] = {}
        if target == "manual_duel":
            extra["finished"] = 1

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
            extra=extra,
        )

    @staticmethod
    def _update_match_aggregates(conn: sqlite3.Connection, *, match_id: str) -> None:
        previous_match = conn.execute(
            """
            SELECT tournament_id, status
            FROM matches
            WHERE id = ?
              AND deleted_at IS NULL
            LIMIT 1
            """,
            (match_id,),
        ).fetchone()

        aggregate_row = conn.execute(
            """
            SELECT
              COALESCE(SUM(COALESCE(dw1, 0)), 0) AS gw1,
              COALESCE(SUM(COALESCE(dw2, 0)), 0) AS gw2,
              COALESCE(SUM(CASE
                WHEN COALESCE(status, 'Planned') IN ('Done', 'No Show') AND COALESCE(dw1, 0) > COALESCE(dw2, 0)
                THEN 1 ELSE 0 END), 0) AS dw1,
              COALESCE(SUM(CASE
                WHEN COALESCE(status, 'Planned') IN ('Done', 'No Show') AND COALESCE(dw2, 0) > COALESCE(dw1, 0)
                THEN 1 ELSE 0 END), 0) AS dw2,
              COUNT(*) AS total_duels,
              COALESCE(SUM(CASE WHEN COALESCE(status, 'Planned') IN ('Done', 'No Show') THEN 1 ELSE 0 END), 0) AS done_duels,
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

        transitioned_to_done = (
            previous_match is not None
            and str(previous_match["status"] or "").strip().lower() != "done"
            and next_status == "Done"
        )
        tournament_id = (
            str(previous_match["tournament_id"] or "").strip()
            if previous_match is not None
            else ""
        )
        if transitioned_to_done and tournament_id:
            SqliteMatchRepository._recalculate_standings_if_present(
                conn,
                tournament_id=tournament_id,
            )

    @staticmethod
    def _recalculate_standings_if_present(
        conn: sqlite3.Connection,
        *,
        tournament_id: str,
    ) -> bool:
        """BGA-worker equivalent of server.js recalculateTournamentStandings."""
        has_standings_table = conn.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table'
              AND name = 'standings'
            LIMIT 1
            """
        ).fetchone()
        if has_standings_table is None:
            return False

        standings_rows = conn.execute(
            """
            SELECT id, stage, "group" AS "group", team_id, player_id
            FROM standings
            WHERE upper(trim(COALESCE(tournament_id, ''))) = upper(trim(?))
              AND (
                trim(COALESCE(team_id, '')) <> ''
                OR trim(COALESCE(player_id, '')) <> ''
              )
            ORDER BY id ASC
            """,
            (tournament_id,),
        ).fetchall()
        if not standings_rows:
            return False

        team_rows: list[dict] = []
        player_rows: list[dict] = []
        team_stats_by_key: dict[tuple[str, str], dict] = {}
        player_stats_by_key: dict[tuple[str, str], dict] = {}

        for row in standings_rows:
            stage = SqliteMatchRepository._normalize_standings_stage(row["stage"])
            team_id = str(row["team_id"] or "").strip()
            player_id = str(row["player_id"] or "").strip()
            if team_id:
                stats = SqliteMatchRepository._empty_standings_stats(include_duels=True)
                entry = {
                    "id": row["id"],
                    "stage": stage,
                    "group": str(row["group"] or "").strip(),
                    "participant_id": team_id,
                    "stats": stats,
                    "type": "team",
                }
                team_rows.append(entry)
                team_stats_by_key[(stage, team_id.upper())] = stats
            elif player_id:
                stats = SqliteMatchRepository._empty_standings_stats(include_duels=False)
                entry = {
                    "id": row["id"],
                    "stage": stage,
                    "group": str(row["group"] or "").strip(),
                    "participant_id": player_id,
                    "stats": stats,
                    "type": "player",
                }
                player_rows.append(entry)
                player_stats_by_key[(stage, player_id)] = stats

        if team_rows:
            matches = conn.execute(
                """
                SELECT stage, team_1, team_2, dw1, dw2, gw1, gw2
                FROM matches
                WHERE upper(trim(COALESCE(tournament_id, ''))) = upper(trim(?))
                  AND lower(trim(COALESCE(status, ''))) = 'done'
                  AND deleted_at IS NULL
                  AND trim(COALESCE(team_1, '')) <> ''
                  AND trim(COALESCE(team_2, '')) <> ''
                """,
                (tournament_id,),
            ).fetchall()
            for match in matches:
                stage = SqliteMatchRepository._normalize_standings_stage(match["stage"])
                team_1 = str(match["team_1"] or "").strip().upper()
                team_2 = str(match["team_2"] or "").strip().upper()
                SqliteMatchRepository._apply_standings_result(
                    team_stats_by_key.get((stage, team_1)),
                    match["dw1"],
                    match["dw2"],
                    match["gw1"],
                    match["gw2"],
                )
                SqliteMatchRepository._apply_standings_result(
                    team_stats_by_key.get((stage, team_2)),
                    match["dw2"],
                    match["dw1"],
                    match["gw2"],
                    match["gw1"],
                )

        if player_rows:
            duels = conn.execute(
                """
                SELECT m.stage, d.player_1_id, d.player_2_id, d.dw1, d.dw2
                FROM duels d
                INNER JOIN matches m
                  ON trim(COALESCE(m.id, '')) = trim(COALESCE(d.match_id, ''))
                 AND m.deleted_at IS NULL
                WHERE upper(trim(COALESCE(m.tournament_id, ''))) = upper(trim(?))
                  AND lower(trim(COALESCE(m.status, ''))) = 'done'
                  AND lower(trim(COALESCE(d.status, ''))) IN ('done', 'no show')
                  AND d.deleted_at IS NULL
                  AND trim(COALESCE(d.player_1_id, '')) <> ''
                  AND trim(COALESCE(d.player_2_id, '')) <> ''
                """,
                (tournament_id,),
            ).fetchall()
            for duel in duels:
                stage = SqliteMatchRepository._normalize_standings_stage(duel["stage"])
                player_1_id = str(duel["player_1_id"] or "").strip()
                player_2_id = str(duel["player_2_id"] or "").strip()
                SqliteMatchRepository._apply_standings_result(
                    player_stats_by_key.get((stage, player_1_id)),
                    duel["dw1"],
                    duel["dw2"],
                )
                SqliteMatchRepository._apply_standings_result(
                    player_stats_by_key.get((stage, player_2_id)),
                    duel["dw2"],
                    duel["dw1"],
                )

        calculated_rows = [*team_rows, *player_rows]
        for row in calculated_rows:
            stats = row["stats"]
            stats["mdif"] = stats["mw"] - stats["ml"]
            stats["gdif"] = stats["gw"] - stats["gl"]
            if row["type"] == "team":
                stats["ddif"] = stats["dw"] - stats["dl"]

        position_buckets: dict[tuple[str, str, str], list[dict]] = {}
        for row in calculated_rows:
            bucket_key = (
                row["stage"],
                str(row["group"] or "").strip().lower(),
                row["type"],
            )
            position_buckets.setdefault(bucket_key, []).append(row)
        for rows in position_buckets.values():
            rows.sort(key=SqliteMatchRepository._standings_sort_key)
            for position, row in enumerate(rows, start=1):
                row["stats"]["position"] = position

        for row in team_rows:
            stats = row["stats"]
            conn.execute(
                """
                UPDATE standings
                SET
                  mp = ?, mw = ?, ml = ?, dw = ?, dl = ?, gw = ?, gl = ?,
                  mdif = ?, ddif = ?, gdif = ?, position = ?,
                  updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    stats["mp"],
                    stats["mw"],
                    stats["ml"],
                    stats["dw"],
                    stats["dl"],
                    stats["gw"],
                    stats["gl"],
                    stats["mdif"],
                    stats["ddif"],
                    stats["gdif"],
                    stats["position"],
                    row["id"],
                ),
            )

        for row in player_rows:
            stats = row["stats"]
            conn.execute(
                """
                UPDATE standings
                SET
                  mp = ?, mw = ?, ml = ?, gw = ?, gl = ?,
                  mdif = ?, gdif = ?, position = ?,
                  updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    stats["mp"],
                    stats["mw"],
                    stats["ml"],
                    stats["gw"],
                    stats["gl"],
                    stats["mdif"],
                    stats["gdif"],
                    stats["position"],
                    row["id"],
                ),
            )
        return True

    @staticmethod
    def _normalize_standings_stage(value: object) -> str:
        return "Stage 2" if str(value or "").strip().lower() == "stage 2" else "Stage 1"

    @staticmethod
    def _standings_score(value: object) -> int:
        try:
            return max(0, int(float(value)))
        except (TypeError, ValueError, OverflowError):
            return 0

    @staticmethod
    def _empty_standings_stats(*, include_duels: bool) -> dict:
        return {
            "mp": 0,
            "mw": 0,
            "ml": 0,
            "dw": 0 if include_duels else None,
            "dl": 0 if include_duels else None,
            "gw": 0,
            "gl": 0,
            "mdif": 0,
            "ddif": 0 if include_duels else None,
            "gdif": 0,
            "position": None,
        }

    @staticmethod
    def _apply_standings_result(
        stats: dict | None,
        score_for: object,
        score_against: object,
        game_score_for: object = _STANDINGS_GAME_SCORE_UNSET,
        game_score_against: object = _STANDINGS_GAME_SCORE_UNSET,
    ) -> None:
        if stats is None:
            return
        own_score = SqliteMatchRepository._standings_score(score_for)
        opponent_score = SqliteMatchRepository._standings_score(score_against)
        own_game_score = (
            own_score
            if game_score_for is _STANDINGS_GAME_SCORE_UNSET
            else SqliteMatchRepository._standings_score(game_score_for)
        )
        opponent_game_score = (
            opponent_score
            if game_score_against is _STANDINGS_GAME_SCORE_UNSET
            else SqliteMatchRepository._standings_score(game_score_against)
        )
        stats["mp"] += 1
        if own_score > opponent_score:
            stats["mw"] += 1
        elif own_score < opponent_score:
            stats["ml"] += 1
        stats["gw"] += own_game_score
        stats["gl"] += opponent_game_score
        if stats["dw"] is not None and stats["dl"] is not None:
            stats["dw"] += own_score
            stats["dl"] += opponent_score

    @staticmethod
    def _standings_sort_key(row: dict) -> tuple:
        stats = row["stats"]
        metrics = ("mw", "ddif", "gdif", "gw") if row["type"] == "team" else ("mw", "gdif", "gw")
        participant_parts = re.split(r"(\d+)", str(row["participant_id"] or "").casefold())
        participant_key = tuple(
            (0, int(part)) if part.isdigit() else (1, part)
            for part in participant_parts
        )
        return (*(-int(stats[metric] or 0) for metric in metrics), participant_key)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _is_result_sync_blocked(row: sqlite3.Row) -> bool:
        deleted_at = str(row["deleted_at"] or "").strip()
        status = str(row["status"] or "Planned").strip().lower()
        return bool(deleted_at) or status in RESULT_SYNC_PROTECTED_STATUSES

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
            game_columns = {
                str(row["name"]).strip().lower()
                for row in conn.execute("PRAGMA table_info(games)").fetchall()
                if row["name"] is not None
            }
            if "results_checked_at" not in duel_columns:
                conn.execute("ALTER TABLE duels ADD COLUMN results_checked_at TEXT")
            if "rating_full" not in duel_columns:
                conn.execute("ALTER TABLE duels ADD COLUMN rating_full REAL")
            if "rating" not in duel_columns:
                conn.execute("ALTER TABLE duels ADD COLUMN rating INTEGER")
            if "gg_rating_full" not in duel_columns:
                conn.execute("ALTER TABLE duels ADD COLUMN gg_rating_full REAL")
            if "gg_rating" not in duel_columns:
                conn.execute("ALTER TABLE duels ADD COLUMN gg_rating INTEGER")
            if "dw1_import" not in duel_columns:
                conn.execute("ALTER TABLE duels ADD COLUMN dw1_import INTEGER")
            if "dw2_import" not in duel_columns:
                conn.execute("ALTER TABLE duels ADD COLUMN dw2_import INTEGER")
            if "rating" not in match_columns:
                conn.execute("ALTER TABLE matches ADD COLUMN rating INTEGER")
            if "dw1_import" not in match_columns:
                conn.execute("ALTER TABLE matches ADD COLUMN dw1_import INTEGER")
            if "dw2_import" not in match_columns:
                conn.execute("ALTER TABLE matches ADD COLUMN dw2_import INTEGER")
            if "gw1_import" not in match_columns:
                conn.execute("ALTER TABLE matches ADD COLUMN gw1_import INTEGER")
            if "gw2_import" not in match_columns:
                conn.execute("ALTER TABLE matches ADD COLUMN gw2_import INTEGER")
            if "deleted_at" not in game_columns:
                conn.execute("ALTER TABLE games ADD COLUMN deleted_at TEXT")
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
