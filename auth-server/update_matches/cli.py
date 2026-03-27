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

from .config import MATCH_UPDATE_BATCH_SIZE
from .repository import TARGET_FINISHED_PENDING, TARGET_ONGOING
from .service import MatchUpdateService
from .sqlite_repository import SqliteMatchRepository


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update auth-server duels from BGA.")
    parser.add_argument(
        "--db-path",
        default=os.getenv("AUTH_SQLITE_PATH") or str(_default_db_path()),
        help="Path to auth.sqlite",
    )
    parser.add_argument(
        "--targets",
        default=os.getenv("MATCH_UPDATE_TARGETS", f"{TARGET_ONGOING},{TARGET_FINISHED_PENDING}"),
        help="Comma-separated targets: ongoing,finished_pending",
    )
    parser.add_argument(
        "--match-id",
        default=os.getenv("MATCH_UPDATE_MATCH_ID"),
        help="Manual match id for testing. When provided, automatic target selection is skipped.",
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


def main() -> int:
    load_dotenv()
    args = parse_args()
    repository = SqliteMatchRepository(args.db_path)
    service = MatchUpdateService(repository=repository, batch_size=args.batch_size)
    targets = [part.strip() for part in args.targets.split(",") if part.strip()]
    summary = service.run(targets=targets, total_limit=args.limit, match_id=args.match_id)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
