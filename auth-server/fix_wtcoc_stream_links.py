from __future__ import annotations

import argparse
import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class LegacyMatchRef:
    tournament_id: str
    source: str
    external_match_id: str


@dataclass(frozen=True)
class LegacyDuelRef:
    tournament_id: str
    source: str
    external_match_id: str
    duel_number: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Repair streams.entity_id values that still use legacy WTCOC match or duel ids."
    )
    parser.add_argument(
        "--db-path",
        default=os.getenv("AUTH_SQLITE_PATH") or str(_default_db_path()),
        help="Path to auth.sqlite",
    )
    parser.add_argument(
        "--tournament-prefix",
        default="WTCOC",
        help="Repair only legacy ids whose tournament id starts with this prefix. Default: WTCOC",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing them.",
    )
    return parser.parse_args()


def _default_db_path() -> Path:
    return Path(__file__).resolve().parent / "data" / "auth.sqlite"


def _load_stream_rows(conn: sqlite3.Connection, tournament_prefix: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
          id,
          trim(COALESCE(entity_type, '')) AS entity_type,
          trim(COALESCE(entity_id, '')) AS entity_id
        FROM streams
        WHERE trim(COALESCE(deleted_at, '')) = ''
          AND trim(COALESCE(entity_id, '')) LIKE trim(?) || '%'
          AND trim(COALESCE(entity_type, '')) IN ('match', 'duel')
        ORDER BY entity_type COLLATE NOCASE ASC, entity_id COLLATE NOCASE ASC, id ASC
        """,
        (tournament_prefix,),
    ).fetchall()


def _parse_legacy_match_id(entity_id: str) -> LegacyMatchRef | None:
    raw = str(entity_id or "").strip()
    if not raw:
        return None
    if "-PO" in raw:
        tournament_id, external_match_id = raw.rsplit("-PO", 1)
        source = "playoff"
    elif "-M" in raw:
        tournament_id, external_match_id = raw.rsplit("-M", 1)
        source = "calendar"
    else:
        return None
    normalized_tournament_id = str(tournament_id or "").strip()
    normalized_external_match_id = str(external_match_id or "").strip()
    if not normalized_tournament_id or not normalized_external_match_id or not normalized_external_match_id.isdigit():
        return None
    return LegacyMatchRef(
        tournament_id=normalized_tournament_id,
        source=source,
        external_match_id=normalized_external_match_id,
    )


def _parse_legacy_duel_id(entity_id: str) -> LegacyDuelRef | None:
    raw = str(entity_id or "").strip()
    if not raw or "-D" not in raw:
        return None
    match_part, duel_suffix = raw.rsplit("-D", 1)
    if not duel_suffix.isdigit():
        return None
    match_ref = _parse_legacy_match_id(match_part)
    if match_ref is None:
        return None
    return LegacyDuelRef(
        tournament_id=match_ref.tournament_id,
        source=match_ref.source,
        external_match_id=match_ref.external_match_id,
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
    stream_rows = _load_stream_rows(conn, args.tournament_prefix)
    duel_maps_by_match_id: dict[str, dict[int, str]] = {}
    updates: list[dict[str, str | int]] = []
    unresolved: list[dict[str, str | int]] = []
    invalid_legacy_entity_ids: list[dict[str, str | int]] = []

    for row in stream_rows:
        stream_id = int(row["id"])
        entity_type = str(row["entity_type"] or "").strip()
        old_entity_id = str(row["entity_id"] or "").strip()
        if entity_type == "match":
            legacy_match = _parse_legacy_match_id(old_entity_id)
            if legacy_match is None:
                invalid_legacy_entity_ids.append({
                    "stream_id": stream_id,
                    "entity_type": entity_type,
                    "entity_id": old_entity_id,
                })
                continue
            new_entity_id = link_map.get((legacy_match.tournament_id, legacy_match.source, legacy_match.external_match_id))
            if not new_entity_id:
                unresolved.append({
                    "stream_id": stream_id,
                    "entity_type": entity_type,
                    "old_entity_id": old_entity_id,
                    "tournament_id": legacy_match.tournament_id,
                    "source": legacy_match.source,
                    "external_match_id": legacy_match.external_match_id,
                })
                continue
        elif entity_type == "duel":
            legacy_duel = _parse_legacy_duel_id(old_entity_id)
            if legacy_duel is None:
                invalid_legacy_entity_ids.append({
                    "stream_id": stream_id,
                    "entity_type": entity_type,
                    "entity_id": old_entity_id,
                })
                continue
            new_match_id = link_map.get((legacy_duel.tournament_id, legacy_duel.source, legacy_duel.external_match_id))
            if not new_match_id:
                unresolved.append({
                    "stream_id": stream_id,
                    "entity_type": entity_type,
                    "old_entity_id": old_entity_id,
                    "tournament_id": legacy_duel.tournament_id,
                    "source": legacy_duel.source,
                    "external_match_id": legacy_duel.external_match_id,
                    "duel_number": legacy_duel.duel_number,
                })
                continue
            if new_match_id not in duel_maps_by_match_id:
                duel_maps_by_match_id[new_match_id] = _load_duel_map_for_match(conn, new_match_id)
            new_entity_id = duel_maps_by_match_id[new_match_id].get(legacy_duel.duel_number)
            if not new_entity_id:
                unresolved.append({
                    "stream_id": stream_id,
                    "entity_type": entity_type,
                    "old_entity_id": old_entity_id,
                    "match_id": new_match_id,
                    "duel_number": legacy_duel.duel_number,
                })
                continue
        else:
            continue

        if new_entity_id == old_entity_id:
            continue
        updates.append({
            "stream_id": stream_id,
            "entity_type": entity_type,
            "old_entity_id": old_entity_id,
            "new_entity_id": new_entity_id,
        })

    if not args.dry_run and updates:
        conn.execute("BEGIN IMMEDIATE TRANSACTION")
        try:
            for item in updates:
                conn.execute(
                    """
                    UPDATE streams
                    SET
                      entity_id = ?,
                      updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (item["new_entity_id"], item["stream_id"]),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    print(json.dumps({
        "db_path": str(Path(args.db_path).resolve()),
        "tournament_prefix": args.tournament_prefix,
        "dry_run": bool(args.dry_run),
        "streams_considered": len(stream_rows),
        "streams_to_update": len(updates),
        "unresolved_count": len(unresolved),
        "invalid_legacy_entity_ids_count": len(invalid_legacy_entity_ids),
        "updates_preview": updates[:50],
        "unresolved_preview": unresolved[:20],
        "invalid_legacy_entity_ids_preview": invalid_legacy_entity_ids[:20],
    }, ensure_ascii=False, indent=2))
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
