from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv() -> None:
        return None


class _TimestampedStream:
    def __init__(self, wrapped) -> None:
        self._wrapped = wrapped
        self._buffer = ""

    def write(self, data) -> int:
        text = str(data)
        if not text:
            return 0
        self._buffer += text
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            self._emit(line)
        return len(text)

    def flush(self) -> None:
        if self._buffer:
            self._emit(self._buffer)
            self._buffer = ""
        self._wrapped.flush()

    def _emit(self, line: str) -> None:
        ts = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S%z")
        self._wrapped.write(f"[{ts}] {line}\n")

    def isatty(self) -> bool:
        return bool(getattr(self._wrapped, "isatty", lambda: False)())

    @property
    def encoding(self):
        return getattr(self._wrapped, "encoding", "utf-8")


def _configure_stream_logging() -> None:
    if not isinstance(sys.stdout, _TimestampedStream):
        sys.stdout = _TimestampedStream(sys.stdout)
    if not isinstance(sys.stderr, _TimestampedStream):
        sys.stderr = _TimestampedStream(sys.stderr)


def _default_db_path() -> Path:
    return Path(__file__).resolve().parent / "data" / "auth.sqlite"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fix future matches and duels state in auth-server SQLite."
    )
    parser.add_argument(
        "--db-path",
        default=os.getenv("AUTH_SQLITE_PATH") or str(_default_db_path()),
        help="Path to auth.sqlite",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only show how many rows would be updated, without saving changes.",
    )
    return parser.parse_args()


def _get_table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]).strip().lower() for row in rows}


def _score_present_sql(column_name: str) -> str:
    return f"trim(COALESCE(CAST({column_name} AS TEXT), '')) <> ''"


def _value_changed(current, expected) -> bool:
    if current is None and expected is None:
        return False
    return current != expected


def _normalize_status(value: object) -> str:
    return str(value or "").strip().lower()


def _match_status_and_scores(row: sqlite3.Row) -> tuple[str, int | None, int | None, int | None, int | None]:
    total_duels = int(row["total_duels"] or 0)
    done_duels = int(row["done_duels"] or 0)
    error_duels = int(row["error_duels"] or 0)
    start_ts = row["start_ts"]
    end_ts = row["end_ts"]

    status = "Planned"
    now_expr = int(datetime.now(timezone.utc).timestamp())
    if error_duels > 0:
        status = "Error"
    elif total_duels > 0 and done_duels == total_duels:
        status = "Done"
    elif start_ts is not None and end_ts is not None and int(start_ts) <= now_expr < int(end_ts):
        status = "In progress"

    dw1 = int(row["dw1"] or 0)
    dw2 = int(row["dw2"] or 0)
    gw1 = int(row["gw1"] or 0)
    gw2 = int(row["gw2"] or 0)
    if _normalize_status(status) == "planned":
        return status, None, None, None, None
    return status, dw1, dw2, gw1, gw2


def fix_matches(conn: sqlite3.Connection, *, dry_run: bool) -> dict[str, object]:
    columns = _get_table_columns(conn, "matches")
    required = {"time_utc", "status", "dw1", "dw2", "gw1", "gw2"}
    missing = sorted(required - columns)
    if missing:
        raise RuntimeError(f"matches table is missing required columns: {', '.join(missing)}")

    has_deleted_at = "deleted_at" in columns
    has_updated_at = "updated_at" in columns

    deleted_filter = "AND m.deleted_at IS NULL" if has_deleted_at else ""
    duel_deleted_filter = "AND d.deleted_at IS NULL" if "deleted_at" in _get_table_columns(conn, "duels") else ""

    rows = conn.execute(
        f"""
        SELECT
          m.id,
          m.status,
          m.dw1,
          m.dw2,
          m.gw1,
          m.gw2,
          COALESCE(SUM(COALESCE(d.dw1, 0)), 0) AS gw1_calc,
          COALESCE(SUM(COALESCE(d.dw2, 0)), 0) AS gw2_calc,
          COALESCE(SUM(CASE
            WHEN COALESCE(d.status, 'Planned') IN ('Done', 'No Show') AND COALESCE(d.dw1, 0) > COALESCE(d.dw2, 0)
            THEN 1 ELSE 0 END), 0) AS dw1_calc,
          COALESCE(SUM(CASE
            WHEN COALESCE(d.status, 'Planned') IN ('Done', 'No Show') AND COALESCE(d.dw2, 0) > COALESCE(d.dw1, 0)
            THEN 1 ELSE 0 END), 0) AS dw2_calc,
          COUNT(d.id) AS total_duels,
          COALESCE(SUM(CASE WHEN COALESCE(d.status, 'Planned') IN ('Done', 'No Show') THEN 1 ELSE 0 END), 0) AS done_duels,
          COALESCE(SUM(CASE WHEN COALESCE(d.status, 'Planned') = 'Error' THEN 1 ELSE 0 END), 0) AS error_duels,
          MIN(CASE
            WHEN datetime(d.time_utc) IS NOT NULL THEN unixepoch(d.time_utc)
            ELSE NULL
          END) AS start_ts,
          MAX(CASE
            WHEN datetime(d.time_utc) IS NOT NULL
            THEN unixepoch(d.time_utc) + (COALESCE(df.minutes_to_play, 60) * 60)
            ELSE NULL
          END) AS end_ts
        FROM matches m
        LEFT JOIN duels d
          ON trim(COALESCE(d.match_id, '')) = trim(COALESCE(m.id, ''))
          {duel_deleted_filter}
        LEFT JOIN duel_formats df
          ON lower(trim(df.format)) = lower(trim(d.duel_format))
        WHERE 1 = 1
          {deleted_filter}
        GROUP BY m.id, m.status, m.dw1, m.dw2, m.gw1, m.gw2
        """
    ).fetchall()

    affected = 0
    for row in rows:
        derived_row = {
            "total_duels": row["total_duels"],
            "done_duels": row["done_duels"],
            "error_duels": row["error_duels"],
            "start_ts": row["start_ts"],
            "end_ts": row["end_ts"],
            "dw1": row["dw1_calc"],
            "dw2": row["dw2_calc"],
            "gw1": row["gw1_calc"],
            "gw2": row["gw2_calc"],
        }
        expected_status, expected_dw1, expected_dw2, expected_gw1, expected_gw2 = _match_status_and_scores(derived_row)
        if not any([
            _value_changed(row["status"], expected_status),
            _value_changed(row["dw1"], expected_dw1),
            _value_changed(row["dw2"], expected_dw2),
            _value_changed(row["gw1"], expected_gw1),
            _value_changed(row["gw2"], expected_gw2),
        ]):
            continue
        affected += 1
        if dry_run:
            continue
        update_parts = [
            "status = ?",
            "dw1 = ?",
            "dw2 = ?",
            "gw1 = ?",
            "gw2 = ?",
        ]
        if has_updated_at:
            update_parts.append("updated_at = CURRENT_TIMESTAMP")
        conn.execute(
            f"UPDATE matches SET {', '.join(update_parts)} WHERE id = ?",
            [expected_status, expected_dw1, expected_dw2, expected_gw1, expected_gw2, row["id"]],
        )

    return {
        "checked": "matches",
        "affected": affected,
        "dry_run": dry_run,
    }


def fix_duels(conn: sqlite3.Connection, *, dry_run: bool) -> dict[str, object]:
    columns = _get_table_columns(conn, "duels")
    required = {"id", "time_utc", "status", "dw1", "dw2", "duel_format"}
    missing = sorted(required - columns)
    if missing:
        raise RuntimeError(f"duels table is missing required columns: {', '.join(missing)}")

    has_deleted_at = "deleted_at" in columns
    has_updated_at = "updated_at" in columns
    deleted_filter = "AND d.deleted_at IS NULL" if has_deleted_at else ""
    rows = conn.execute(
        f"""
        SELECT
          d.id,
          d.status,
          d.dw1,
          d.dw2,
          d.time_utc,
          COALESCE(df.games_to_win, 1) AS games_to_win,
          COALESCE(df.minutes_to_play, 60) AS minutes_to_play
        FROM duels d
        LEFT JOIN duel_formats df
          ON lower(trim(df.format)) = lower(trim(d.duel_format))
        WHERE 1 = 1
          {deleted_filter}
        """
    ).fetchall()

    now_ts = int(datetime.now(timezone.utc).timestamp())
    affected = 0
    for row in rows:
        dw1 = int(row["dw1"] or 0)
        dw2 = int(row["dw2"] or 0)
        games_to_win = max(1, int(row["games_to_win"] or 1))
        minutes_to_play = max(1, int(row["minutes_to_play"] or 60))
        start_raw = row["time_utc"]
        start_ts = None
        if str(start_raw or "").strip():
            try:
                start_ts = int(datetime.fromisoformat(str(start_raw).replace("Z", "+00:00")).timestamp())
            except ValueError:
                start_ts = None
        end_ts = start_ts + minutes_to_play * 60 if start_ts is not None else None

        expected_status = "Planned"
        if (dw1 == games_to_win and dw2 < games_to_win) or (dw2 == games_to_win and dw1 < games_to_win):
            expected_status = "Done"
        elif start_ts is not None and end_ts is not None and start_ts <= now_ts < end_ts:
            expected_status = "In progress"
        elif start_ts is not None and end_ts is not None and now_ts >= end_ts:
            expected_status = "Error"

        expected_dw1 = None if _normalize_status(expected_status) == "planned" else dw1
        expected_dw2 = None if _normalize_status(expected_status) == "planned" else dw2
        if not any([
            _value_changed(row["status"], expected_status),
            _value_changed(row["dw1"], expected_dw1),
            _value_changed(row["dw2"], expected_dw2),
        ]):
            continue

        affected += 1
        if dry_run:
            continue
        update_parts = [
            "status = ?",
            "dw1 = ?",
            "dw2 = ?",
        ]
        if has_updated_at:
            update_parts.append("updated_at = CURRENT_TIMESTAMP")
        conn.execute(
            f"UPDATE duels SET {', '.join(update_parts)} WHERE id = ?",
            [expected_status, expected_dw1, expected_dw2, row["id"]],
        )

    return {
        "checked": "duels",
        "affected": affected,
        "dry_run": dry_run,
    }


def main() -> int:
    _configure_stream_logging()
    load_dotenv()
    args = parse_args()

    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row
    try:
        match_summary = fix_matches(conn, dry_run=args.dry_run)
        duel_summary = fix_duels(conn, dry_run=args.dry_run)
        if args.dry_run:
            conn.rollback()
        else:
            conn.commit()
    finally:
        conn.close()

    summary = {
        "db_path": str(args.db_path),
        "dry_run": bool(args.dry_run),
        "matches": match_summary,
        "duels": duel_summary,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
