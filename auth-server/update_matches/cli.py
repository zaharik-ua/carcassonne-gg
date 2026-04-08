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

from .config import MATCH_UPDATE_BATCH_SIZE
from .repository import TARGET_FINISHED_PENDING, TARGET_ONGOING
from .service import MatchUpdateService
from .sqlite_repository import SqliteMatchRepository


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update auth-server duels from BGA.")
    parser.add_argument(
        "--db-path",
        default=os.getenv("AUTH_SQLITE_PATH") or str(_default_db_path()),
        help="Path to auth.sqlite",
    )
    parser.add_argument(
        "--targets",
        default=os.getenv("MATCH_UPDATE_TARGETS", f"{TARGET_FINISHED_PENDING},{TARGET_ONGOING}"),
        help="Comma-separated targets: finished_pending,ongoing",
    )
    parser.add_argument(
        "--match-id",
        default=os.getenv("MATCH_UPDATE_MATCH_ID"),
        help="Manual match id for testing. When provided, automatic target selection is skipped.",
    )
    parser.add_argument(
        "--duel-id",
        default=os.getenv("MATCH_UPDATE_DUEL_ID"),
        help="Manual duel id for testing. When provided, only this duel is processed.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=MATCH_UPDATE_BATCH_SIZE,
        help="How many duels to process per BGA batch request.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=int(os.getenv("MATCH_UPDATE_LIMIT", "0")) or None,
        help="Optional total number of duels to process in one run.",
    )
    return parser.parse_args()


def _default_db_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "auth.sqlite"


def _configure_stream_logging() -> None:
    if not isinstance(sys.stdout, _TimestampedStream):
        sys.stdout = _TimestampedStream(sys.stdout)
    if not isinstance(sys.stderr, _TimestampedStream):
        sys.stderr = _TimestampedStream(sys.stderr)


def main() -> int:
    _configure_stream_logging()
    load_dotenv()
    args = parse_args()
    repository = SqliteMatchRepository(args.db_path)
    service = MatchUpdateService(repository=repository, batch_size=args.batch_size)
    targets = [part.strip() for part in args.targets.split(",") if part.strip()]
    summary = service.run(
        targets=targets,
        total_limit=args.limit,
        match_id=args.match_id,
        duel_id=args.duel_id,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
