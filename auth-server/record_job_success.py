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

from job_runs import SqliteJobRunsRepository


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Record successful completion for a background job.")
    parser.add_argument(
        "--db-path",
        default=os.getenv("DB_PATH", "./data/auth.sqlite"),
        help="Path to auth-server SQLite database.",
    )
    parser.add_argument(
        "--job-name",
        required=True,
        help="Stable job name, for example update-player-elo-daily.",
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv()
    args = parse_args()
    db_path = str(Path(args.db_path).resolve())
    repository = SqliteJobRunsRepository(db_path)
    summary = repository.mark_success(args.job_name)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
