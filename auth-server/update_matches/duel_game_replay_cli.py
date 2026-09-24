from __future__ import annotations

import argparse
import json
import os
import sqlite3
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv() -> None:
        return None

from .manual_replay_batch import ReplayFetcher, process_manual_replay_games
from .game_replay import ensure_game_replays_schema


class DuelNotFoundError(RuntimeError):
    pass


def _default_db_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "auth.sqlite"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch BGA replays for active games of one exact duel."
    )
    parser.add_argument("duel_id", help="Exact id of the row in the duels table")
    parser.add_argument(
        "--db-path",
        default=os.getenv("AUTH_SQLITE_PATH") or os.getenv("DB_PATH") or str(_default_db_path()),
        help="Path to auth.sqlite",
    )
    parser.add_argument("--max-requests", type=int, default=3)
    parser.add_argument("--max-failed-games", type=int, default=1)
    parser.add_argument("--include-pending", action="store_true")
    parser.add_argument("--include-errors", action="store_true")
    parser.add_argument("--include-fallback", action="store_true")
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def fetch_and_store_duel_game_replays(
    db_path: str | Path,
    duel_id: str,
    *,
    force: bool = False,
    include_pending: bool = False,
    include_errors: bool = False,
    include_fallback: bool = False,
    max_requests: int = 3,
    max_failed_games: int = 1,
    replay_fetcher: ReplayFetcher | None = None,
) -> dict[str, Any]:
    normalized_duel_id = str(duel_id or "").strip()
    if not normalized_duel_id:
        raise ValueError("duels.id must not be empty")
    path = Path(db_path).expanduser()
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        ensure_game_replays_schema(conn)
        duel = conn.execute(
            """
            SELECT id
            FROM duels
            WHERE trim(COALESCE(id, '')) = trim(?)
              AND trim(COALESCE(deleted_at, '')) = ''
            LIMIT 1
            """,
            (normalized_duel_id,),
        ).fetchone()
        if duel is None:
            raise DuelNotFoundError(f"Duel not found: duels.id={normalized_duel_id}")
        games = conn.execute(
            """
            SELECT
              g.id AS game_id,
              g.duel_id,
              g.bga_table_id,
              gr.status AS replay_status,
              gr.color_source
            FROM games g
            LEFT JOIN game_replays gr ON gr.game_id = g.id
            WHERE trim(COALESCE(g.duel_id, '')) = trim(?)
              AND trim(COALESCE(g.deleted_at, '')) = ''
            ORDER BY COALESCE(g.game_number, 999999), g.id COLLATE NOCASE
            """,
            (normalized_duel_id,),
        ).fetchall()

    kwargs: dict[str, Any] = {}
    if replay_fetcher is not None:
        kwargs["replay_fetcher"] = replay_fetcher
    summary = process_manual_replay_games(
        path,
        games,
        force=force,
        include_pending=include_pending,
        include_errors=include_errors,
        include_fallback=include_fallback,
        max_requests=max_requests,
        max_failed_games=max_failed_games,
        **kwargs,
    )
    summary["duel_id"] = normalized_duel_id
    return summary


def main() -> int:
    load_dotenv()
    args = parse_args()
    try:
        summary = fetch_and_store_duel_game_replays(
            args.db_path,
            args.duel_id,
            force=args.force,
            include_pending=args.include_pending,
            include_errors=args.include_errors,
            include_fallback=args.include_fallback,
            max_requests=args.max_requests,
            max_failed_games=args.max_failed_games,
        )
    except (DuelNotFoundError, OSError, sqlite3.Error, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 1

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["status"] in {"ok", "partial"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
