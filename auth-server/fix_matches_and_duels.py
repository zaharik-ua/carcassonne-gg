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


def fix_matches(conn: sqlite3.Connection, *, dry_run: bool) -> dict[str, object]:
    columns = _get_table_columns(conn, "matches")
    required = {"time_utc", "status", "dw1", "dw2", "gw1", "gw2"}
    missing = sorted(required - columns)
    if missing:
        raise RuntimeError(f"matches table is missing required columns: {', '.join(missing)}")

    has_deleted_at = "deleted_at" in columns
    has_updated_at = "updated_at" in columns

    where_parts = [
        "datetime(time_utc) > datetime('now')",
        "("
        "COALESCE(status, '') <> 'Planned'"
        f" OR {_score_present_sql('dw1')}"
        f" OR {_score_present_sql('dw2')}"
        f" OR {_score_present_sql('gw1')}"
        f" OR {_score_present_sql('gw2')}"
        ")",
    ]
    if has_deleted_at:
        where_parts.append("deleted_at IS NULL")
    where_sql = " AND ".join(where_parts)

    count_sql = f"SELECT COUNT(*) FROM matches WHERE {where_sql}"
    affected = int(conn.execute(count_sql).fetchone()[0] or 0)

    if not dry_run and affected > 0:
        update_parts = [
            "status = 'Planned'",
            "dw1 = NULL",
            "dw2 = NULL",
            "gw1 = NULL",
            "gw2 = NULL",
        ]
        if has_updated_at:
            update_parts.append("updated_at = CURRENT_TIMESTAMP")
        update_sql = f"UPDATE matches SET {', '.join(update_parts)} WHERE {where_sql}"
        conn.execute(update_sql)

    return {
        "checked": "matches",
        "affected": affected,
        "dry_run": dry_run,
    }


def fix_duels(conn: sqlite3.Connection, *, dry_run: bool) -> dict[str, object]:
    _ = conn
    return {
        "checked": "duels",
        "affected": 0,
        "dry_run": dry_run,
        "skipped": True,
        "reason": "Duel logic is not implemented yet.",
    }


def main() -> int:
    _configure_stream_logging()
    load_dotenv()
    args = parse_args()

    conn = sqlite3.connect(args.db_path)
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
