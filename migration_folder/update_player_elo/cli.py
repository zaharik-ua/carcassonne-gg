from __future__ import annotations

import argparse
import json
import os

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv() -> None:
        return None

from .config import PLAYER_ELO_BATCH_SIZE
from .json_repository import JsonFilePlayerEloRepository
from .service import PlayerEloUpdateService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update player Elo values from BGA.")
    parser.add_argument(
        "--repository",
        default=os.getenv("PLAYER_ELO_REPOSITORY", "json"),
        choices=["json"],
        help="Repository adapter to use. Add your DB adapter in the target project.",
    )
    parser.add_argument(
        "--json-path",
        default=os.getenv("PLAYER_ELO_JSON_PATH"),
        help="Path to JSON repository file for local testing.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=PLAYER_ELO_BATCH_SIZE,
        help="How many players to process in one DB fetch batch.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=int(os.getenv("PLAYER_ELO_LIMIT", "0")) or None,
        help="Optional total number of players to process in one run.",
    )
    return parser.parse_args()


def build_repository(args: argparse.Namespace):
    if args.repository == "json":
        if not args.json_path:
            raise RuntimeError("--json-path is required when --repository=json")
        return JsonFilePlayerEloRepository(args.json_path)
    raise RuntimeError(f"Unsupported repository adapter: {args.repository}")


def main() -> int:
    load_dotenv()
    args = parse_args()
    repository = build_repository(args)
    service = PlayerEloUpdateService(repository=repository, batch_size=args.batch_size)
    summary = service.run(total_limit=args.limit)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0
