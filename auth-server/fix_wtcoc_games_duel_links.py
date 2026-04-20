from __future__ import annotations

import argparse
import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class LegacyDuelRef:
    tournament_id: str
    source: str
    external_match_id: str
    duel_number: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Repair games.duel_id values that still use legacy WTCOC duel ids."
    )
    parser.add_argument(
        "--db-path",
        default=os.getenv("AUTH_SQLITE_PATH") or str(_default_db_path()),
        help="Path to auth.sqlite",
    )
    parser.add_argument(
        "--tournament-prefix",
        default="WTCOC",
        help="Repair only legacy duel ids whose tournament id starts with this prefix. Default: WTCOC",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing them.",
    )
    return parser.parse_args()


def _default_db_path() -> Path:
    return Path(__file__).resolve().parent / "data" / "auth.sqlite"


def _load_legacy_games(conn: sqlite3.Connection, tournament_prefix: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
          trim(COALESCE(id, '')) AS id,
          trim(COALESCE(duel_id, '')) AS duel_id
        FROM games
        WHERE trim(COALESCE(deleted_at, '')) = ''
          AND trim(COALESCE(duel_id, '')) LIKE trim(?) || '%'
        ORDER BY duel_id COLLATE NOCASE ASC, id COLLATE NOCASE ASC
        """,
        (tournament_prefix,),
    ).fetchall()


def _parse_legacy_duel_id(duel_id: str) -> LegacyDuelRef | None:
    raw = str(duel_id or "").strip()
    if not raw or "-D" not in raw:
        return None
    match_part, duel_suffix = raw.rsplit("-D", 1)
    if not duel_suffix.isdigit():
        return None
    if "-PO" in match_part:
        tournament_id, external_match_id = match_part.rsplit("-PO", 1)
        source = "playoff"
    elif "-M" in match_part:
        tournament_id, external_match_id = match_part.rsplit("-M", 1)
        source = "calendar"
    else:
        return None
    normalized_tournament_id = str(tournament_id or "").strip()
    normalized_external_match_id = str(external_match_id or "").strip()
    if not normalized_tournament_id or not normalized_external_match_id:
        return None
    return LegacyDuelRef(
        tournament_id=normalized_tournament_id,
        source=source,
        external_match_id=normalized_external_match_id,
        duel_number=int(duel_suffix),
    )


def _load_wtcoc_link_map(conn: sqlite3.Connection) -> dict[tuple[str, str, str], str]:
    tables = {
        str(row["name"] or "").strip()
        for row in conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
            """
        ).fetchall()
    }
    if "wtcoc_match_links" not in tables:
        return {}
    rows = conn.execute(
        """
        SELECT
          trim(COALESCE(tournament_id, '')) AS tournament_id,
          trim(COALESCE(source, '')) AS source,
          trim(COALESCE(external_match_id, '')) AS external_match_id,
          trim(COALESCE(match_id, '')) AS match_id
        FROM wtcoc_match_links
        """
    ).fetchall()
    result: dict[tuple[str, str, str], str] = {}
    for row in rows:
        key = (
            str(row["tournament_id"] or "").strip(),
            str(row["source"] or "").strip().lower(),
            str(row["external_match_id"] or "").strip(),
        )
        match_id = str(row["match_id"] or "").strip()
        if key[0] and key[1] and key[2] and match_id:
            result[key] = match_id
    return result


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


def main() -> int:
    args = parse_args()
    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row

    link_map = _load_wtcoc_link_map(conn)
    legacy_games = _load_legacy_games(conn, args.tournament_prefix)
    updates: list[tuple[str, str, str, str, str, int]] = []
    invalid_legacy_duel_ids: list[str] = []
    missing_link_targets: list[dict[str, str | int]] = []
    missing_duel_targets: list[dict[str, str | int]] = []

    duel_maps_by_match_id: dict[str, dict[int, str]] = {}

    for row in legacy_games:
        game_id = str(row["id"] or "").strip()
        old_duel_id = str(row["duel_id"] or "").strip()
        if not game_id or not old_duel_id:
            continue
        legacy_ref = _parse_legacy_duel_id(old_duel_id)
        if legacy_ref is None:
            invalid_legacy_duel_ids.append(old_duel_id)
            continue
        link_key = (legacy_ref.tournament_id, legacy_ref.source, legacy_ref.external_match_id)
        match_id = link_map.get(link_key)
        if not match_id:
            missing_link_targets.append({
                "game_id": game_id,
                "old_duel_id": old_duel_id,
                "tournament_id": legacy_ref.tournament_id,
                "source": legacy_ref.source,
                "external_match_id": legacy_ref.external_match_id,
                "duel_number": legacy_ref.duel_number,
            })
            continue
        if match_id not in duel_maps_by_match_id:
            duel_maps_by_match_id[match_id] = _load_duel_map_for_match(conn, match_id)
        new_duel_id = duel_maps_by_match_id[match_id].get(legacy_ref.duel_number)
        if not new_duel_id:
            missing_duel_targets.append({
                "game_id": game_id,
                "old_duel_id": old_duel_id,
                "match_id": match_id,
                "duel_number": legacy_ref.duel_number,
            })
            continue
        if new_duel_id == old_duel_id:
            continue
        updates.append((
            game_id,
            old_duel_id,
            new_duel_id,
            legacy_ref.tournament_id,
            legacy_ref.source,
            legacy_ref.duel_number,
        ))

    if not args.dry_run and updates:
        conn.execute("BEGIN IMMEDIATE TRANSACTION")
        try:
            for game_id, _old_duel_id, new_duel_id, _tournament_id, _source, _duel_number in updates:
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
        "legacy_games_considered": len(legacy_games),
        "games_to_update": len(updates),
        "invalid_legacy_duel_ids_count": len(invalid_legacy_duel_ids),
        "missing_link_targets_count": len(missing_link_targets),
        "missing_duel_targets_count": len(missing_duel_targets),
        "updates_preview": [
            {
                "game_id": game_id,
                "old_duel_id": old_duel_id,
                "new_duel_id": new_duel_id,
                "tournament_id": tournament_id,
                "source": source,
                "duel_number": duel_number,
            }
            for game_id, old_duel_id, new_duel_id, tournament_id, source, duel_number in updates[:50]
        ],
        "missing_link_targets_preview": missing_link_targets[:20],
        "missing_duel_targets_preview": missing_duel_targets[:20],
        "invalid_legacy_duel_ids_preview": invalid_legacy_duel_ids[:20],
    }, ensure_ascii=False, indent=2))
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
