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

from .service import ProfileGgEloUpdateService
from .sqlite_repository import SqliteProfileGgEloRepository


def _default_db_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "auth.sqlite"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Recalculate profiles.gg_elo and profiles.gg_elo_period_delta from local duels."
    )
    parser.add_argument(
        "--db-path",
        default=os.getenv("DB_PATH") or os.getenv("AUTH_SQLITE_PATH") or str(_default_db_path()),
        help="Path to auth-server SQLite database.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Calculate and print summary without updating profile rows.",
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv()
    args = parse_args()
    repository = SqliteProfileGgEloRepository(args.db_path)
    service = ProfileGgEloUpdateService(repository=repository)
    summary = service.run(dry_run=args.dry_run)
    summary["db_path"] = repository.db_path
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
