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

from .replay_worker import (
    DEFAULT_LEASE_SECONDS,
    DEFAULT_RUN_LIMIT,
    MAX_RUN_LIMIT,
    ReplayWorkerAlreadyRunningError,
    replay_worker_lock,
    run_replay_worker,
)


def _default_db_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "auth.sqlite"


def _default_lock_path(db_path: str | Path) -> Path:
    path = Path(db_path).expanduser()
    return path.with_name(f"{path.name}.bga-replay-worker.lock")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process due BGA replay queue entries without polling."
    )
    parser.add_argument(
        "--db-path",
        default=os.getenv("AUTH_SQLITE_PATH") or os.getenv("DB_PATH") or str(_default_db_path()),
        help="Path to auth.sqlite",
    )
    parser.add_argument(
        "--queue-class",
        choices=("fresh", "historical"),
        default="fresh",
        help="Queue lane to process (default: fresh)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_RUN_LIMIT,
        help=f"Maximum logs.html attempts for this run, from 1 to {MAX_RUN_LIMIT}",
    )
    parser.add_argument(
        "--lease-seconds",
        type=int,
        default=DEFAULT_LEASE_SECONDS,
        help="Lease duration for one claimed queue entry",
    )
    parser.add_argument(
        "--lock-file",
        help="Optional flock path; defaults next to the SQLite database",
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv()
    args = parse_args()
    lock_path = args.lock_file or str(_default_lock_path(args.db_path))
    try:
        with replay_worker_lock(lock_path):
            summary = run_replay_worker(
                args.db_path,
                queue_class=args.queue_class,
                limit=args.limit,
                lease_seconds=args.lease_seconds,
            )
    except ReplayWorkerAlreadyRunningError as exc:
        print(json.dumps({"status": "locked", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 0
    except (OSError, sqlite3.Error, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
