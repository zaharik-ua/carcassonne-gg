from __future__ import annotations

import argparse
import json
import os
import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv() -> None:
        return None

from .game_replay import fetch_and_store_game_replay_with_account_rotation


ReplayFetcher = Callable[..., dict[str, Any]]


class MatchNotFoundError(RuntimeError):
    pass


def _default_db_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "auth.sqlite"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch and store BGA replays for every game in a match."
    )
    parser.add_argument("match_id", help="Exact id of the row in the matches table")
    parser.add_argument(
        "--db-path",
        default=os.getenv("AUTH_SQLITE_PATH") or os.getenv("DB_PATH") or str(_default_db_path()),
        help="Path to auth.sqlite (defaults to AUTH_SQLITE_PATH, DB_PATH, or data/auth.sqlite)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Fetch every replay again, including ready replays",
    )
    parser.add_argument(
        "--poll-attempts",
        type=int,
        default=10,
        help="Number of log checks after asking BGA to prepare each archive",
    )
    parser.add_argument(
        "--poll-delay",
        type=float,
        default=1.0,
        help="Seconds between archive checks",
    )
    return parser.parse_args()


def fetch_and_store_match_game_replays(
    db_path: str | Path,
    match_id: str,
    *,
    force: bool = False,
    poll_attempts: int = 10,
    poll_delay: float = 1.0,
    replay_fetcher: ReplayFetcher = fetch_and_store_game_replay_with_account_rotation,
) -> dict[str, Any]:
    normalized_match_id = str(match_id or "").strip()
    if not normalized_match_id:
        raise ValueError("matches.id must not be empty")
    if poll_attempts < 1:
        raise ValueError("poll_attempts must be at least 1")
    if poll_delay < 0:
        raise ValueError("poll_delay must not be negative")

    path = Path(db_path).expanduser()
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        match = conn.execute(
            """
            SELECT id
            FROM matches
            WHERE trim(COALESCE(id, '')) = trim(?)
              AND trim(COALESCE(deleted_at, '')) = ''
            LIMIT 1
            """,
            (normalized_match_id,),
        ).fetchone()
        if match is None:
            raise MatchNotFoundError(f"Match not found: matches.id={normalized_match_id}")

        games = conn.execute(
            """
            SELECT
              g.id AS game_id,
              g.bga_table_id,
              d.id AS duel_id
            FROM duels d
            JOIN games g
              ON trim(COALESCE(g.duel_id, '')) = trim(COALESCE(d.id, ''))
            WHERE trim(COALESCE(d.match_id, '')) = trim(?)
              AND trim(COALESCE(d.deleted_at, '')) = ''
              AND trim(COALESCE(g.deleted_at, '')) = ''
              AND trim(COALESCE(g.id, '')) <> ''
            ORDER BY
              COALESCE(d.duel_number, 999999) ASC,
              d.id COLLATE NOCASE ASC,
              COALESCE(g.game_number, 999999) ASC,
              g.id COLLATE NOCASE ASC
            """,
            (normalized_match_id,),
        ).fetchall()

    summary: dict[str, Any] = {
        "status": "ok",
        "match_id": normalized_match_id,
        "games_found": len(games),
        "processed": 0,
        "ready": 0,
        "cached": 0,
        "failed": 0,
        "skipped": 0,
        "errors": [],
    }
    for game in games:
        game_id = str(game["game_id"] or "").strip()
        duel_id = str(game["duel_id"] or "").strip()
        bga_table_id = str(game["bga_table_id"] or "").strip()
        if not bga_table_id:
            summary["skipped"] += 1
            summary["errors"].append(
                {
                    "game_id": game_id,
                    "duel_id": duel_id,
                    "error": "Game has no bga_table_id",
                }
            )
            continue

        summary["processed"] += 1
        try:
            result = replay_fetcher(
                str(path),
                game_id,
                force=force,
                poll_attempts=poll_attempts,
                poll_delay=poll_delay,
            )
            summary["ready"] += 1
            if bool(result.get("cached")):
                summary["cached"] += 1
        except Exception as exc:
            summary["failed"] += 1
            summary["errors"].append(
                {
                    "game_id": game_id,
                    "duel_id": duel_id,
                    "bga_table_id": bga_table_id,
                    "error": str(exc) or exc.__class__.__name__,
                }
            )

    if summary["failed"] or summary["skipped"]:
        summary["status"] = "partial"
    return summary


def main() -> int:
    load_dotenv()
    args = parse_args()
    try:
        summary = fetch_and_store_match_game_replays(
            args.db_path,
            args.match_id,
            force=args.force,
            poll_attempts=args.poll_attempts,
            poll_delay=args.poll_delay,
        )
    except (MatchNotFoundError, OSError, sqlite3.Error, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
