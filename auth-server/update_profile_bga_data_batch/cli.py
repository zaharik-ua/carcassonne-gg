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

from .service import ProfileBgaDataBatchService


PROGRESS_EVENT_PREFIX = "BGA_PROFILE_BATCH_EVENT "


def emit_batch_progress(event: dict) -> None:
    print(
        f"{PROGRESS_EVENT_PREFIX}{json.dumps(event, ensure_ascii=False, separators=(',', ':'))}",
        file=sys.stderr,
        flush=True,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch update BGA nickname/avatar for multiple profiles.")
    parser.add_argument(
        "--db-path",
        default=os.getenv("DB_PATH", "./data/auth.sqlite"),
        help="Path to auth-server SQLite database.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="How many profiles to process in one run.",
    )
    parser.add_argument(
        "--include-removed",
        action="store_true",
        help="Include profiles that already have status Removed.",
    )
    parser.add_argument(
        "--player-id",
        action="append",
        default=[],
        help="Process only explicit profile id(s). Can be passed multiple times.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process all eligible profiles in repeated batches until none are left.",
    )
    parser.add_argument(
        "--stop-after-consecutive-failures",
        type=int,
        default=5,
        help="Stop --all mode after this many failed players in a row.",
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv()
    args = parse_args()
    db_path = str(Path(args.db_path).resolve())
    service = ProfileBgaDataBatchService(
        db_path=db_path,
        include_removed=bool(args.include_removed),
    )
    if args.all and args.player_id:
        raise SystemExit("--all cannot be used together with --player-id.")

    summary = (
        service.run_all(
            limit=args.limit,
            stop_after_consecutive_failures=args.stop_after_consecutive_failures,
            on_batch=emit_batch_progress,
        )
        if args.all
        else service.run(limit=args.limit, player_ids=args.player_id)
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if int(summary.get("failed", 0)) == 0 else 1
