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

from sync_wtcoc_matches.client import DEFAULT_WTCOC_API_TOKEN, WtcocApiClient

from .service import WtcocPlayersSyncService
from .sqlite_repository import SqliteWtcocPlayersRepository


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch WTCOC players and prepare missing profiles import for auth-server.")
    parser.add_argument(
        "--db-path",
        default=os.getenv("AUTH_SQLITE_PATH") or str(_default_db_path()),
        help="Path to auth.sqlite",
    )
    parser.add_argument(
        "--token",
        default=os.getenv("WTCOC_API_TOKEN", DEFAULT_WTCOC_API_TOKEN),
        help="WTCOC API token.",
    )
    parser.add_argument(
        "--base-url",
        default=os.getenv("WTCOC_API_BASE_URL"),
        help="Override WTCOC API base URL.",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=10,
        help="How many sample rows to include in output sections.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Insert missing profiles into SQLite.",
    )
    parser.add_argument(
        "--actor-id",
        default=os.getenv("WTCOC_SYNC_ACTOR_ID", "1"),
        help="created_by/updated_by value for script writes. Default: 1",
    )
    return parser.parse_args()


def _default_db_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "auth.sqlite"


def main() -> int:
    load_dotenv()
    args = parse_args()
    repository = SqliteWtcocPlayersRepository(args.db_path)
    client = WtcocApiClient(
        token=args.token,
        base_url=args.base_url or None or "https://www.carcassonne.cat/wtcoc/api",
    )
    service = WtcocPlayersSyncService(repository=repository, client=client)
    summary = service.build_import_plan(sample_limit=args.sample_limit)
    if args.apply:
        apply_payload = summary.pop("apply_payload")
        apply_result = repository.insert_profiles(
            actor_id=args.actor_id,
            profiles=apply_payload["profiles"],
        )
        summary["apply_result"] = apply_result
    else:
        summary.pop("apply_payload")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
