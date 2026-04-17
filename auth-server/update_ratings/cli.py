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

from .sqlite_repository import SqliteRatingsRepository


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update duel and match ratings.")
    parser.add_argument(
        "--db-path",
        default=os.getenv("DB_PATH", "./data/auth.sqlite"),
        help="Path to auth-server SQLite database.",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--duel-id",
        help="Update rating for a single duel and its parent match if present.",
    )
    mode.add_argument(
        "--match-id",
        help="Update ratings for all duels of one match and then the match itself.",
    )
    mode.add_argument(
        "--planned",
        action="store_true",
        help="Update ratings for all Planned duels first, then all Planned matches.",
    )
    mode.add_argument(
        "--planned-missing-ratings",
        action="store_true",
        help="Update ratings only for Planned duels without ratings and their parent matches.",
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv()
    args = parse_args()
    db_path = str(Path(args.db_path).resolve())
    repository = SqliteRatingsRepository(db_path)

    if args.duel_id:
        duel_result = repository.update_duel_rating(args.duel_id)
        match_result = None
        match_id = str(duel_result.get("match_id") or "").strip()
        if duel_result.get("found") and match_id:
            match_result = repository.update_match_rating(match_id)
        summary = {
            "mode": "duel",
            "duel": duel_result,
            "match": match_result,
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0 if duel_result.get("found") else 1

    if args.match_id:
        summary = {
            "mode": "match",
            **repository.update_match_with_duels(args.match_id),
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0 if summary["match"].get("found") else 1

    if args.planned_missing_ratings:
        summary = {
            "mode": "planned_missing_ratings",
            **repository.update_planned_missing_ratings(),
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    summary = {
        "mode": "planned",
        **repository.update_planned(),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
