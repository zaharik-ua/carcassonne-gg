from __future__ import annotations

import argparse
import json
import os
import sqlite3
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv() -> None:
        return None

from .game_replay import (
    GameReplayError,
    fetch_and_store_game_replay_with_account_rotation,
)


def _default_db_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "auth.sqlite"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch and store one BGA game replay by games.id."
    )
    parser.add_argument("game_id", help="Exact id of the row in the games table")
    parser.add_argument(
        "--db-path",
        default=os.getenv("AUTH_SQLITE_PATH") or os.getenv("DB_PATH") or str(_default_db_path()),
        help="Path to auth.sqlite (defaults to AUTH_SQLITE_PATH, DB_PATH, or data/auth.sqlite)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Fetch again even when a ready replay is already stored",
    )
    parser.add_argument(
        "--poll-attempts",
        type=int,
        default=10,
        help="Number of log checks after asking BGA to prepare the archive",
    )
    parser.add_argument(
        "--poll-delay",
        type=float,
        default=1.0,
        help="Seconds between archive checks",
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv()
    args = parse_args()
    try:
        summary = fetch_and_store_game_replay_with_account_rotation(
            args.db_path,
            args.game_id,
            force=args.force,
            poll_attempts=args.poll_attempts,
            poll_delay=args.poll_delay,
        )
    except (GameReplayError, OSError, sqlite3.Error, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
