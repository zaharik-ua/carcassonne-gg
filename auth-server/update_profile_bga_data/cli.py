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

from .service import ProfileBgaDataUpdateService
from .sqlite_repository import SqliteProfileBgaDataRepository


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update one profile's BGA nickname and avatar from BGA.")
    parser.add_argument(
        "--db-path",
        default=os.getenv("DB_PATH", "./data/auth.sqlite"),
        help="Path to auth-server SQLite database.",
    )
    parser.add_argument(
        "--player-id",
        required=True,
        help="Single profiles.id value to update.",
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv()
    args = parse_args()
    db_path = str(Path(args.db_path).resolve())

    try:
        repository = SqliteProfileBgaDataRepository(db_path)
        service = ProfileBgaDataUpdateService(repository=repository)
        result = service.run_for_player(args.player_id)
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "player_id": str(args.player_id or "").strip(),
                    "message": str(exc),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("ok") else 1
