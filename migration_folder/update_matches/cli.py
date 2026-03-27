from __future__ import annotations

import argparse
import json
import os

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    def load_dotenv() -> None:
        return None

from .config import MATCH_UPDATE_BATCH_SIZE
from .json_repository import JsonFileMatchRepository
from .repository import TARGET_EMPTY_FINISHED, TARGET_ONGOING
from .service import MatchUpdateService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update Carcassonne match results from BGA.")
    parser.add_argument(
        "--repository",
        default=os.getenv("MATCH_REPOSITORY", "json"),
        choices=["json"],
        help="Repository adapter to use. Add your DB adapter in the target project.",
    )
    parser.add_argument(
        "--json-path",
        default=os.getenv("MATCH_JSON_PATH"),
        help="Path to JSON repository file for local testing.",
    )
    parser.add_argument(
        "--targets",
        default=os.getenv("MATCH_UPDATE_TARGETS", f"{TARGET_ONGOING},{TARGET_EMPTY_FINISHED}"),
        help="Comma-separated targets: ongoing,empty_finished",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=MATCH_UPDATE_BATCH_SIZE,
        help="How many matches to process per BGA batch request.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=int(os.getenv("MATCH_UPDATE_LIMIT", "0")) or None,
        help="Optional total number of matches to process in one run.",
    )
    return parser.parse_args()


def build_repository(args: argparse.Namespace):
    if args.repository == "json":
        if not args.json_path:
            raise RuntimeError("--json-path is required when --repository=json")
        return JsonFileMatchRepository(args.json_path)
    raise RuntimeError(f"Unsupported repository adapter: {args.repository}")


def main() -> int:
    load_dotenv()
    args = parse_args()
    repository = build_repository(args)
    service = MatchUpdateService(repository=repository, batch_size=args.batch_size)
    targets = [part.strip() for part in args.targets.split(",") if part.strip()]
    summary = service.run(targets=targets, total_limit=args.limit)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0
