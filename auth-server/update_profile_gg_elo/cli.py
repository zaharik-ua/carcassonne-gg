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

from .client import DEFAULT_PLAYERS_URL, PlayersJsonClient
from .service import ProfileGgEloUpdateService
from .sqlite_repository import SqliteProfileGgEloRepository


def _default_db_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "auth.sqlite"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Update profiles.gg_elo from the public carcassonne.gg players JSON."
    )
    parser.add_argument(
        "--db-path",
        default=os.getenv("AUTH_SQLITE_PATH") or str(_default_db_path()),
        help="Path to auth.sqlite.",
    )
    parser.add_argument(
        "--source-url",
        default=os.getenv("GG_ELO_SOURCE_URL", DEFAULT_PLAYERS_URL),
        help="Players JSON URL.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30,
        help="HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and validate data without updating profile rows.",
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv()
    args = parse_args()
    repository = SqliteProfileGgEloRepository(args.db_path)
    client = PlayersJsonClient(url=args.source_url, timeout_seconds=args.timeout)
    service = ProfileGgEloUpdateService(repository=repository, client=client)
    summary = service.run(dry_run=args.dry_run)
    summary["db_path"] = repository.db_path
    summary["source_url"] = client.url
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
