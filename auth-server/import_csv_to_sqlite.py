from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
from collections import Counter
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class ImportPaths:
    profiles_csv: Path | None
    matches_csv: Path | None
    duels_csv: Path | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import matches, duels, and profiles CSV files into auth-server SQLite.")
    parser.add_argument(
        "--db-path",
        default=os.getenv("AUTH_SQLITE_PATH") or str(_default_db_path()),
        help="Path to auth.sqlite",
    )
    parser.add_argument(
        "--import-dir",
        default=str(_default_import_dir()),
        help="Directory containing profiles_csv.csv, matches_csv.csv, duels_csv.csv",
    )
    parser.add_argument(
        "--actor-id",
        default=os.getenv("CSV_IMPORT_ACTOR_ID", "1"),
        help="created_by/updated_by value for script writes. Default: 1",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write changes to SQLite. Without this flag the script only prints a preview.",
    )
    parser.add_argument(
        "--only",
        choices=("profiles", "matches", "duels"),
        help="Import only one entity type.",
    )
    parser.add_argument(
        "--profiles-csv",
        help="Override path to profiles CSV file.",
    )
    parser.add_argument(
        "--matches-csv",
        help="Override path to matches CSV file.",
    )
    parser.add_argument(
        "--duels-csv",
        help="Override path to duels CSV file.",
    )
    return parser.parse_args()


def _default_db_path() -> Path:
    return Path(__file__).resolve().parent / "data" / "auth.sqlite"


def _default_import_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "import"


def _parse_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [{str(key or "").strip(): str(value or "").strip() for key, value in row.items()} for row in reader]


def _normalize_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _normalize_int(value: object) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    return int(text)


def _normalize_custom_time(value: object) -> int | str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.lstrip("-").isdigit():
        return int(text)
    return text


def _parse_csv_datetime_to_iso(value: object) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.strptime(text, "%d/%m/%Y %H:%M:%S")
    except ValueError as exc:
        raise ValueError(f"Unsupported datetime format: {text!r}. Expected DD/MM/YYYY HH:MM:SS") from exc
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def _load_table_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
        """
    ).fetchall()
    return {str(row["name"]).strip() for row in rows if str(row["name"] or "").strip()}


def _load_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row["name"]).strip() for row in rows if str(row["name"] or "").strip()}


def _build_paths(import_dir: Path, args: argparse.Namespace) -> ImportPaths:
    only = str(args.only or "").strip()
    profiles_csv = Path(args.profiles_csv).resolve() if args.profiles_csv else import_dir / "profiles_csv.csv"
    matches_csv = Path(args.matches_csv).resolve() if args.matches_csv else import_dir / "matches_csv.csv"
    duels_csv = Path(args.duels_csv).resolve() if args.duels_csv else import_dir / "duels_csv.csv"
    return ImportPaths(
        profiles_csv=None if only and only != "profiles" else profiles_csv,
        matches_csv=None if only and only != "matches" else matches_csv,
        duels_csv=None if only and only != "duels" else duels_csv,
    )


def _prepare_profiles(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    prepared: list[dict[str, str]] = []
    for index, row in enumerate(rows, start=1):
        player_id = _normalize_text(row.get("id"))
        nickname = _normalize_text(row.get("bga_nickname"))
        association = _normalize_text(row.get("association"))
        if not player_id or not nickname or not association:
            raise ValueError(f"profiles_csv.csv row {index} is missing required id/bga_nickname/association")
        prepared.append(
            {
                "id": player_id,
                "bga_nickname": nickname,
                "association": association,
            }
        )
    return prepared


def _prepare_matches(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    prepared: list[dict[str, object]] = []
    for index, row in enumerate(rows, start=1):
        match_id = _normalize_text(row.get("id"))
        tournament_id = _normalize_text(row.get("tournament_id"))
        if not match_id or not tournament_id:
            raise ValueError(f"matches_csv.csv row {index} is missing required id/tournament_id")
        prepared.append(
            {
                "id": match_id,
                "tournament_id": tournament_id,
                "time_utc": _parse_csv_datetime_to_iso(row.get("time_utc")),
                "lineup_type": _normalize_text(row.get("lineup_type")),
                "lineup_deadline_h": None,
                "lineup_deadline_utc": None,
                "number_of_duels": _normalize_int(row.get("number_of_duels")),
                "team_1": _normalize_text(row.get("team_1")),
                "team_2": _normalize_text(row.get("team_2")),
                "status": _normalize_text(row.get("status")) or "Planned",
                "created_by": _normalize_text(row.get("created_by")),
            }
        )
    return prepared


def _prepare_duels(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    prepared: list[dict[str, object]] = []
    for index, row in enumerate(rows, start=1):
        duel_id = _normalize_text(row.get("id"))
        tournament_id = _normalize_text(row.get("tournament_id"))
        match_id = _normalize_text(row.get("match_id"))
        if not duel_id or not tournament_id or not match_id:
            raise ValueError(f"duels_csv.csv row {index} is missing required id/tournament_id/match_id")
        prepared.append(
            {
                "id": duel_id,
                "tournament_id": tournament_id,
                "match_id": match_id,
                "duel_number": _normalize_int(row.get("duel_number")),
                "duel_format": _normalize_text(row.get("duel_format")),
                "time_utc": _parse_csv_datetime_to_iso(row.get("time_utc")),
                "custom_time": _normalize_custom_time(row.get("custom_time")),
                "player_1_id": _normalize_text(row.get("player_1_id")),
                "player_2_id": _normalize_text(row.get("player_2_id")),
                "status": _normalize_text(row.get("status")) or "Planned",
                "created_by": _normalize_text(row.get("created_by")),
            }
        )
    return prepared


def _dedupe_by_id(items: list[dict[str, object]]) -> tuple[list[dict[str, object]], dict[str, int]]:
    counts = Counter(str(item.get("id") or "").strip() for item in items)
    duplicate_counts = {item_id: count for item_id, count in counts.items() if item_id and count > 1}
    deduped: dict[str, dict[str, object]] = {}
    for item in items:
        item_id = str(item.get("id") or "").strip()
        if not item_id:
            continue
        deduped[item_id] = item
    return list(deduped.values()), duplicate_counts


def _validate_required_tables(conn: sqlite3.Connection, *, required_tables: set[str]) -> None:
    table_names = _load_table_names(conn)
    missing_tables = sorted(required_tables - table_names)
    if missing_tables:
        raise RuntimeError(f"SQLite DB is missing required tables: {', '.join(missing_tables)}")


def _insert_profiles(conn: sqlite3.Connection, *, actor_id: str, profiles: list[dict[str, str]]) -> dict[str, int]:
    profile_columns = _load_columns(conn, "profiles")
    required_columns = {"id", "bga_nickname", "association"}
    missing_columns = sorted(required_columns - profile_columns)
    if missing_columns:
        raise RuntimeError(f"profiles table is missing required columns: {', '.join(missing_columns)}")

    inserted = 0
    skipped_existing = 0
    for item in profiles:
        existing = conn.execute(
            """
            SELECT 1
            FROM profiles
            WHERE trim(COALESCE(id, '')) = trim(?)
            LIMIT 1
            """,
            (item["id"],),
        ).fetchone()
        if existing is not None:
            skipped_existing += 1
            continue

        insert_columns: list[str] = []
        insert_values: list[str] = []
        insert_params: list[object] = []

        def add_value(column: str, value: object) -> None:
            if column in profile_columns:
                insert_columns.append(column)
                insert_values.append("?")
                insert_params.append(value)

        def add_sql(column: str, sql_value: str) -> None:
            if column in profile_columns:
                insert_columns.append(column)
                insert_values.append(sql_value)

        add_value("id", item["id"])
        add_value("bga_nickname", item["bga_nickname"])
        add_value("association", item["association"])
        add_value("status", "Active")
        add_value("created_by", actor_id)
        add_value("updated_by", actor_id)
        add_sql("created_at", "CURRENT_TIMESTAMP")
        add_sql("updated_at", "CURRENT_TIMESTAMP")

        conn.execute(
            f"""
            INSERT INTO profiles (
              {", ".join(insert_columns)}
            )
            VALUES ({", ".join(insert_values)})
            """,
            insert_params,
        )
        inserted += 1

    return {
        "profiles_requested": len(profiles),
        "profiles_inserted": inserted,
        "profiles_skipped_existing": skipped_existing,
    }


def _upsert_matches(conn: sqlite3.Connection, *, actor_id: str, matches: list[dict[str, object]]) -> dict[str, int]:
    match_columns = _load_columns(conn, "matches")
    required_columns = {
        "id",
        "tournament_id",
        "time_utc",
        "lineup_type",
        "lineup_deadline_h",
        "lineup_deadline_utc",
        "number_of_duels",
        "team_1",
        "team_2",
        "status",
        "dw1",
        "dw2",
        "gw1",
        "gw2",
    }
    missing_columns = sorted(required_columns - match_columns)
    if missing_columns:
        raise RuntimeError(f"matches table is missing required columns: {', '.join(missing_columns)}")

    inserted = 0
    updated = 0
    for item in matches:
        existing = conn.execute(
            """
            SELECT 1
            FROM matches
            WHERE trim(COALESCE(id, '')) = trim(?)
            LIMIT 1
            """,
            (item["id"],),
        ).fetchone()

        conn.execute(
            """
            INSERT INTO matches (
              id,
              tournament_id,
              time_utc,
              lineup_type,
              lineup_deadline_h,
              lineup_deadline_utc,
              number_of_duels,
              team_1,
              team_2,
              status,
              dw1,
              dw2,
              gw1,
              gw2,
              created_by,
              updated_by,
              deleted_by,
              deleted_at,
              created_at,
              updated_at,
              rating
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?, ?, NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL)
            ON CONFLICT(id) DO UPDATE SET
              tournament_id = excluded.tournament_id,
              time_utc = excluded.time_utc,
              lineup_type = excluded.lineup_type,
              lineup_deadline_h = NULL,
              lineup_deadline_utc = NULL,
              number_of_duels = excluded.number_of_duels,
              team_1 = excluded.team_1,
              team_2 = excluded.team_2,
              status = excluded.status,
              dw1 = NULL,
              dw2 = NULL,
              gw1 = NULL,
              gw2 = NULL,
              updated_by = excluded.updated_by,
              deleted_by = NULL,
              deleted_at = NULL,
              updated_at = CURRENT_TIMESTAMP,
              rating = NULL
            """,
            (
                item["id"],
                item["tournament_id"],
                item["time_utc"],
                item["lineup_type"],
                None,
                None,
                item["number_of_duels"],
                item["team_1"],
                item["team_2"],
                item["status"],
                actor_id,
                actor_id,
            ),
        )
        if existing is None:
            inserted += 1
        else:
            updated += 1

    return {
        "matches_requested": len(matches),
        "matches_inserted": inserted,
        "matches_updated": updated,
    }


def _upsert_duels(conn: sqlite3.Connection, *, actor_id: str, duels: list[dict[str, object]]) -> dict[str, int]:
    duel_columns = _load_columns(conn, "duels")
    required_columns = {
        "id",
        "tournament_id",
        "match_id",
        "duel_number",
        "duel_format",
        "time_utc",
        "custom_time",
        "player_1_id",
        "player_2_id",
        "dw1",
        "dw2",
        "rating_full",
        "rating",
        "status",
    }
    missing_columns = sorted(required_columns - duel_columns)
    if missing_columns:
        raise RuntimeError(f"duels table is missing required columns: {', '.join(missing_columns)}")

    inserted = 0
    updated = 0
    for item in duels:
        existing = conn.execute(
            """
            SELECT 1
            FROM duels
            WHERE trim(COALESCE(id, '')) = trim(?)
            LIMIT 1
            """,
            (item["id"],),
        ).fetchone()

        conn.execute(
            """
            INSERT INTO duels (
              id,
              tournament_id,
              match_id,
              duel_number,
              duel_format,
              time_utc,
              custom_time,
              player_1_id,
              player_2_id,
              dw1,
              dw2,
              rating_full,
              rating,
              status,
              results_last_error,
              results_checked_at,
              created_by,
              updated_by,
              deleted_by,
              deleted_at,
              created_at,
              updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?, NULL, NULL, ?, ?, NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(id) DO UPDATE SET
              tournament_id = excluded.tournament_id,
              match_id = excluded.match_id,
              duel_number = excluded.duel_number,
              duel_format = excluded.duel_format,
              time_utc = excluded.time_utc,
              custom_time = excluded.custom_time,
              player_1_id = excluded.player_1_id,
              player_2_id = excluded.player_2_id,
              dw1 = NULL,
              dw2 = NULL,
              rating_full = NULL,
              rating = NULL,
              status = excluded.status,
              results_last_error = NULL,
              results_checked_at = NULL,
              updated_by = excluded.updated_by,
              deleted_by = NULL,
              deleted_at = NULL,
              updated_at = CURRENT_TIMESTAMP
            """,
            (
                item["id"],
                item["tournament_id"],
                item["match_id"],
                item["duel_number"],
                item["duel_format"],
                item["time_utc"],
                item["custom_time"],
                item["player_1_id"],
                item["player_2_id"],
                item["status"],
                actor_id,
                actor_id,
            ),
        )
        if existing is None:
            inserted += 1
        else:
            updated += 1

    return {
        "duels_requested": len(duels),
        "duels_inserted": inserted,
        "duels_updated": updated,
    }


def build_import_plan(paths: ImportPaths) -> dict[str, object]:
    profiles, duplicate_profile_ids = (
        _dedupe_by_id(_prepare_profiles(_parse_csv_rows(paths.profiles_csv))) if paths.profiles_csv else ([], {})
    )
    matches, duplicate_match_ids = (
        _dedupe_by_id(_prepare_matches(_parse_csv_rows(paths.matches_csv))) if paths.matches_csv else ([], {})
    )
    duels, duplicate_duel_ids = (
        _dedupe_by_id(_prepare_duels(_parse_csv_rows(paths.duels_csv))) if paths.duels_csv else ([], {})
    )
    tournament_ids = sorted({str(item["tournament_id"]) for item in matches} | {str(item["tournament_id"]) for item in duels})
    duels_by_match: dict[str, int] = defaultdict(int)
    for duel in duels:
        duels_by_match[str(duel["match_id"])] += 1
    return {
        "profiles": profiles,
        "matches": matches,
        "duels": duels,
        "preview": {
            "profiles_count": len(profiles),
            "matches_count": len(matches),
            "duels_count": len(duels),
            "duplicate_profile_ids_in_csv": duplicate_profile_ids,
            "duplicate_match_ids_in_csv": duplicate_match_ids,
            "duplicate_duel_ids_in_csv": duplicate_duel_ids,
            "tournament_ids": tournament_ids,
            "duels_without_matching_match_in_csv": sorted(
                match_id for match_id in duels_by_match if match_id not in {str(item["id"]) for item in matches}
            ),
            "sample_matches": matches[:3],
            "sample_duels": duels[:3],
            "sample_profiles": profiles[:3],
        },
    }


def apply_import(*, db_path: Path, actor_id: str, plan: dict[str, object]) -> dict[str, object]:
    preview = plan["preview"]
    blocking_duplicates: dict[str, dict[str, int]] = {}
    if preview["duplicate_match_ids_in_csv"]:
        blocking_duplicates["matches"] = preview["duplicate_match_ids_in_csv"]
    if preview["duplicate_duel_ids_in_csv"]:
        blocking_duplicates["duels"] = preview["duplicate_duel_ids_in_csv"]
    if blocking_duplicates:
        raise RuntimeError(
            "CSV contains duplicate primary keys and cannot be imported safely: "
            + json.dumps(blocking_duplicates, ensure_ascii=False)
        )

    required_tables: set[str] = set()
    if plan["profiles"]:
        required_tables.add("profiles")
    if plan["matches"]:
        required_tables.add("matches")
    if plan["duels"]:
        required_tables.add("duels")

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        _validate_required_tables(conn, required_tables=required_tables)
        conn.execute("BEGIN IMMEDIATE TRANSACTION")
        try:
            profile_result = (
                _insert_profiles(conn, actor_id=actor_id, profiles=plan["profiles"])
                if plan["profiles"]
                else {
                    "profiles_requested": 0,
                    "profiles_inserted": 0,
                    "profiles_skipped_existing": 0,
                }
            )
            match_result = (
                _upsert_matches(conn, actor_id=actor_id, matches=plan["matches"])
                if plan["matches"]
                else {
                    "matches_requested": 0,
                    "matches_inserted": 0,
                    "matches_updated": 0,
                }
            )
            duel_result = (
                _upsert_duels(conn, actor_id=actor_id, duels=plan["duels"])
                if plan["duels"]
                else {
                    "duels_requested": 0,
                    "duels_inserted": 0,
                    "duels_updated": 0,
                }
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return {
        "db_path": str(db_path),
        **profile_result,
        **match_result,
        **duel_result,
    }


def main() -> int:
    args = parse_args()
    import_dir = Path(args.import_dir).resolve()
    paths = _build_paths(import_dir, args)
    missing_files = [str(path) for path in paths.__dict__.values() if path is not None and not Path(path).is_file()]
    if missing_files:
        raise SystemExit(f"Missing CSV files: {', '.join(missing_files)}")

    plan = build_import_plan(paths)
    output: dict[str, object] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "db_path": str(Path(args.db_path).resolve()),
        "import_dir": str(import_dir),
        "preview": plan["preview"],
    }
    if args.apply:
        output["apply_result"] = apply_import(
            db_path=Path(args.db_path).resolve(),
            actor_id=str(args.actor_id or "").strip() or "1",
            plan=plan,
        )
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
