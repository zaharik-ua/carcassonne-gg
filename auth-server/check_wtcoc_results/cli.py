from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv() -> None:
        return None

from sync_wtcoc_matches.client import DEFAULT_WTCOC_API_BASE_URL, DEFAULT_WTCOC_API_TOKEN, WtcocApiClient

from .service import WtcocResultsCheckService
from .sqlite_repository import SqliteWtcocResultsRepository


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


def _default_db_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "auth.sqlite"


def _configure_stream_logging() -> None:
    if not isinstance(sys.stdout, _TimestampedStream):
        sys.stdout = _TimestampedStream(sys.stdout)
    if not isinstance(sys.stderr, _TimestampedStream):
        sys.stderr = _TimestampedStream(sys.stderr)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare WTCOC finished match results from API with auth-server SQLite."
    )
    parser.add_argument(
        "--db-path",
        default=os.getenv("AUTH_SQLITE_PATH") or str(_default_db_path()),
        help="Path to auth.sqlite",
    )
    parser.add_argument(
        "--tournament-id",
        default=os.getenv("WTCOC_TOURNAMENT_ID", "WTCOC-2026"),
        help="Target tournaments.id value in auth-server.",
    )
    parser.add_argument(
        "--token",
        default=os.getenv("WTCOC_API_TOKEN", DEFAULT_WTCOC_API_TOKEN),
        help="WTCOC API token.",
    )
    parser.add_argument(
        "--base-url",
        default=os.getenv("WTCOC_API_BASE_URL", DEFAULT_WTCOC_API_BASE_URL),
        help="WTCOC API base URL.",
    )
    parser.add_argument(
        "--match-id",
        help="Optional external WTCOC match id filter, for example 12.",
    )
    parser.add_argument(
        "--skip-playoff",
        action="store_true",
        help="Do not call the playoff endpoint.",
    )
    return parser.parse_args()


def main() -> int:
    _configure_stream_logging()
    load_dotenv()
    args = parse_args()

    repository = SqliteWtcocResultsRepository(args.db_path)
    client = WtcocApiClient(token=args.token, base_url=args.base_url)
    service = WtcocResultsCheckService(repository=repository, client=client)
    report = service.build_report(
        tournament_id=args.tournament_id,
        include_playoff=not args.skip_playoff,
        external_match_id=args.match_id,
    )
    report["db_path"] = str(Path(args.db_path).resolve())
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
