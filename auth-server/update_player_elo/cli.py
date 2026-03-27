from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv() -> None:
        return None

from .config import PLAYER_ELO_BATCH_SIZE
from .service import PlayerEloUpdateService
from .sqlite_repository import SqlitePlayerEloRepository


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update player Elo values from BGA.")
    parser.add_argument(
        "--db-path",
        default=os.getenv("DB_PATH", "./data/auth.sqlite"),
        help="Path to auth-server SQLite database.",
    )
    parser.add_argument(
        "--player-id",
        help="Single profiles.id value to update manually.",
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


def main() -> int:
    load_dotenv()
    args = parse_args()
    db_path = str(Path(args.db_path).resolve())
    repository = SqlitePlayerEloRepository(db_path, player_id=args.player_id)
    service = PlayerEloUpdateService(repository=repository, batch_size=args.batch_size)
    effective_limit = 1 if args.player_id and args.limit is None else args.limit
    summary = service.run(total_limit=effective_limit)
    if args.player_id and summary["processed"] == 0:
        print(
            f"Profile with id={args.player_id} was not found or id is not a numeric BGA player id.",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0
