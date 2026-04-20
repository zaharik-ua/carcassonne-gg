from __future__ import annotations

import argparse
import json
import os
import sqlite3
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Repair broken games.duel_id links after WTCOC match/duel id renames."
    )
    parser.add_argument(
        "--db-path",
        default=os.getenv("AUTH_SQLITE_PATH") or str(_default_db_path()),
        help="Path to auth.sqlite",
    )
    parser.add_argument(
        "--tournament-prefix",
        default="WTCOC",
        help="Repair only matches whose tournament_id starts with this prefix. Default: WTCOC",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing them.",
    )
    return parser.parse_args()


def _default_db_path() -> Path:
    return Path(__file__).resolve().parent / "data" / "auth.sqlite"


def _load_match_rename_rows(conn: sqlite3.Connection, tournament_prefix: str) -> list[sqlite3.Row]:
    sql = """
        SELECT
          a.id,
          trim(COALESCE(a.record_id, '')) AS new_match_id,
          json_extract(a.metadata, '$.previous_record_id') AS previous_match_id,
          trim(COALESCE(m.tournament_id, '')) AS tournament_id,
          a.created_at
        FROM audit_trail a
        JOIN matches m
          ON trim(COALESCE(m.id, '')) = trim(COALESCE(a.record_id, ''))
        WHERE a.entity_type = 'match'
          AND a.action = 'update'
          AND trim(COALESCE(json_extract(a.metadata, '$.previous_record_id'), '')) <> ''
          AND trim(COALESCE(m.tournament_id, '')) LIKE trim(?) || '%'
        ORDER BY a.created_at ASC, a.id ASC
    """
    return conn.execute(sql, (tournament_prefix,)).fetchall()


def _load_duel_map_for_match(conn: sqlite3.Connection, match_id: str) -> dict[int, str]:
    rows = conn.execute(
        """
        SELECT
          trim(COALESCE(id, '')) AS id,
          duel_number
        FROM duels
        WHERE trim(COALESCE(match_id, '')) = trim(?)
          AND trim(COALESCE(deleted_at, '')) = ''
        ORDER BY duel_number ASC, id ASC
        """,
        (match_id,),
    ).fetchall()
    result: dict[int, str] = {}
    for row in rows:
        duel_id = str(row["id"] or "").strip()
        duel_number = row["duel_number"]
        if not duel_id or duel_number is None:
            continue
        try:
            normalized_number = int(duel_number)
        except (TypeError, ValueError):
            continue
        result[normalized_number] = duel_id
    return result


def _load_games_for_old_match(conn: sqlite3.Connection, old_match_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
          trim(COALESCE(id, '')) AS id,
          trim(COALESCE(duel_id, '')) AS duel_id
        FROM games
        WHERE trim(COALESCE(duel_id, '')) LIKE trim(?) || '-D%'
          AND trim(COALESCE(deleted_at, '')) = ''
        ORDER BY id ASC
        """,
        (old_match_id,),
    ).fetchall()


def _extract_duel_number(duel_id: str) -> int | None:
    raw = str(duel_id or "").strip()
    if "-D" not in raw:
        return None
    suffix = raw.rsplit("-D", 1)[-1]
    if not suffix.isdigit():
        return None
    return int(suffix)


def main() -> int:
    args = parse_args()
    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row

    rename_rows = _load_match_rename_rows(conn, args.tournament_prefix)
    updates: list[tuple[str, str, str, str, int]] = []

    for row in rename_rows:
        old_match_id = str(row["previous_match_id"] or "").strip()
        new_match_id = str(row["new_match_id"] or "").strip()
        tournament_id = str(row["tournament_id"] or "").strip()
        if not old_match_id or not new_match_id or old_match_id == new_match_id:
            continue
        duel_map = _load_duel_map_for_match(conn, new_match_id)
        if not duel_map:
            continue
        for game_row in _load_games_for_old_match(conn, old_match_id):
            game_id = str(game_row["id"] or "").strip()
            old_duel_id = str(game_row["duel_id"] or "").strip()
            duel_number = _extract_duel_number(old_duel_id)
            new_duel_id = duel_map.get(duel_number) if duel_number is not None else None
            if not game_id or not old_duel_id or not new_duel_id or old_duel_id == new_duel_id:
                continue
            updates.append((game_id, old_duel_id, new_duel_id, tournament_id, duel_number))

    deduped_updates: list[tuple[str, str, str, str, int]] = []
    latest_by_game: dict[str, tuple[str, str, str, int]] = {}
    for game_id, old_duel_id, new_duel_id, tournament_id, duel_number in updates:
        latest_by_game[game_id] = (old_duel_id, new_duel_id, tournament_id, duel_number)
    for game_id, payload in latest_by_game.items():
        old_duel_id, new_duel_id, tournament_id, duel_number = payload
        deduped_updates.append((game_id, old_duel_id, new_duel_id, tournament_id, duel_number))
    deduped_updates.sort(key=lambda item: (item[3], item[4], item[0]))

    if not args.dry_run and deduped_updates:
        conn.execute("BEGIN IMMEDIATE TRANSACTION")
        try:
            for game_id, _old_duel_id, new_duel_id, _tournament_id, _duel_number in deduped_updates:
                conn.execute(
                    """
                    UPDATE games
                    SET duel_id = ?
                    WHERE trim(COALESCE(id, '')) = trim(?)
                    """,
                    (new_duel_id, game_id),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    print(json.dumps({
        "db_path": str(Path(args.db_path).resolve()),
        "tournament_prefix": args.tournament_prefix,
        "dry_run": bool(args.dry_run),
        "rename_events_considered": len(rename_rows),
        "games_to_update": len(deduped_updates),
        "updates_preview": [
            {
                "game_id": game_id,
                "old_duel_id": old_duel_id,
                "new_duel_id": new_duel_id,
                "tournament_id": tournament_id,
                "duel_number": duel_number,
            }
            for game_id, old_duel_id, new_duel_id, tournament_id, duel_number in deduped_updates[:50]
        ],
    }, ensure_ascii=False, indent=2))
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
