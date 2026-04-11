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

from .client import DEFAULT_WTCOC_API_TOKEN, WtcocApiClient
from .service import WtcocSyncService
from .sqlite_repository import SqliteWtcocRepository


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch WTCOC data and analyze mapping readiness for auth-server.")
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
        default=os.getenv("WTCOC_API_BASE_URL"),
        help="Override WTCOC API base URL.",
    )
    parser.add_argument(
        "--match-id",
        help="Optional external WTCOC match id filter, for example 1.",
    )
    parser.add_argument(
        "--skip-playoff",
        action="store_true",
        help="Do not call the playoff endpoint.",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=3,
        help="How many sample normalized matches/duels to include in output.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Upsert mapped WTCOC matches and duels into SQLite.",
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
    repository = SqliteWtcocRepository(args.db_path)
    client = WtcocApiClient(
        token=args.token,
        base_url=args.base_url or None or "https://www.carcassonne.cat/wtcoc/api",
    )
    service = WtcocSyncService(repository=repository, client=client)
    summary = service.build_apply_payload(
        tournament_id=args.tournament_id,
        include_playoff=not args.skip_playoff,
        external_match_id=args.match_id,
    )
    summary["samples"]["matches"] = summary["samples"]["matches"][:max(1, int(args.sample_limit))]
    summary["samples"]["duels"] = summary["samples"]["duels"][:max(1, int(args.sample_limit))]
    if args.apply:
        apply_payload = summary.pop("apply_payload")
        apply_result = repository.upsert_matches_and_duels(
            tournament_id=args.tournament_id,
            actor_id=args.actor_id,
            matches=apply_payload["matches"],
            duels=apply_payload["duels"],
        )
        summary["apply_result"] = apply_result
    else:
        summary.pop("apply_payload")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
