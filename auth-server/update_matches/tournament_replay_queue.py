from __future__ import annotations

import argparse
import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    def load_dotenv() -> None:
        return None

from .manual_replay_queue import populate_replay_queue
from .replay_worker import ReplayWorkerAlreadyRunningError


TOURNAMENT_ID = "ETCOC-2026"


def _default_db_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "auth.sqlite"


def _load_tournament_games(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
          g.id AS game_id,
          g.duel_id,
          g.bga_table_id,
          g.game_number,
          gr.game_id AS replay_game_id,
          gr.status AS replay_status,
          gr.events_json,
          gr.players_json,
          gr.color_source,
          gr.queue_class,
          gr.retry_reason,
          gr.next_attempt_at
        FROM games g
        JOIN duels d
          ON d.id = g.duel_id
        LEFT JOIN game_replays gr ON gr.game_id = g.id
        WHERE upper(trim(COALESCE(d.tournament_id, ''))) = upper(trim(?))
          AND trim(COALESCE(d.deleted_at, '')) = ''
          AND trim(COALESCE(g.deleted_at, '')) = ''
          AND trim(COALESCE(g.id, '')) <> ''
        ORDER BY
          d.id COLLATE NOCASE ASC,
          COALESCE(g.game_number, 999999) ASC,
          g.id COLLATE NOCASE ASC
        """,
        (TOURNAMENT_ID,),
    ).fetchall()


def populate_etcoc_2026_replay_queue(
    db_path: str | Path,
    *,
    apply: bool = False,
    batch_id: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    return populate_replay_queue(
        db_path,
        load_games=_load_tournament_games,
        required_columns={"duels": {"tournament_id"}},
        summary_fields={"tournament_id": TOURNAMENT_ID},
        batch_prefix=f"{TOURNAMENT_ID}-replay-queue",
        apply=apply,
        batch_id=batch_id,
        now=now,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            f"Populate the historical BGA replay queue for {TOURNAMENT_ID}. "
            "Dry-run is the default; pass --apply to write changes."
        )
    )
    parser.add_argument(
        "--db-path",
        default=(
            os.getenv("AUTH_SQLITE_PATH")
            or os.getenv("DB_PATH")
            or str(_default_db_path())
        ),
        help="Path to auth.sqlite",
    )
    parser.add_argument(
        "--batch-id",
        help="Optional historical batch id",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Create/requeue rows; without this flag no data is changed",
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv()
    args = parse_args()
    try:
        summary = populate_etcoc_2026_replay_queue(
            args.db_path,
            apply=args.apply,
            batch_id=args.batch_id,
        )
    except (
        FileNotFoundError,
        OSError,
        ReplayWorkerAlreadyRunningError,
        RuntimeError,
        sqlite3.Error,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"status": "error", "error": str(exc)},
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["status"] in {"ok", "partial"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
