from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv() -> None:
        return None

from .sqlite_repository import SqliteGgDuelRatingsRepository


def _default_db_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "auth.sqlite"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recalculate GG ratings for selected duels.")
    parser.add_argument(
        "--db-path",
        default=os.getenv("AUTH_SQLITE_PATH") or str(_default_db_path()),
        help="Path to auth.sqlite.",
    )
    parser.add_argument("--duel-id", action="append", dest="duel_ids", help="Duel id; repeatable.")
    parser.add_argument("--status", help="Select duels by status, for example Planned.")
    parser.add_argument("--source-type", help="Select duels by source_type, for example challenge.")
    parser.add_argument("--tournament-id", help="Select duels by tournament_id.")
    parser.add_argument(
        "--missing",
        action="store_true",
        help="Select duels where gg_rating_full is empty.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Calculate without updating duels.")
    args = parser.parse_args()
    if not any((args.duel_ids, args.status, args.source_type, args.tournament_id, args.missing)):
        parser.error("at least one selection filter is required")
    return args


def main() -> int:
    load_dotenv()
    args = parse_args()
    repository = SqliteGgDuelRatingsRepository(args.db_path)
    summary = repository.recalculate(
        duel_ids=args.duel_ids,
        status=args.status,
        source_type=args.source_type,
        tournament_id=args.tournament_id,
        missing_only=args.missing,
        dry_run=args.dry_run,
    )
    summary["db_path"] = repository.db_path
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
