from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path


MATCH_RESULT_FIELDS = ("dw1", "dw2", "gw1", "gw2")
DUEL_RESULT_FIELDS = ("dw1", "dw2")


def _default_db_path() -> Path:
    base_dir = Path(__file__).resolve().parent
    candidates = [
        Path(os.getenv("AUTH_SQLITE_PATH", "")).expanduser(),
        base_dir / "data" / "auth.sqlite",
        base_dir / "data.sqlite",
    ]
    for candidate in candidates:
        if str(candidate).strip() and candidate.exists():
            return candidate
    return base_dir / "data" / "auth.sqlite"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Find matches and duels where current result columns differ from non-empty *_import columns."
        )
    )
    parser.add_argument(
        "--db-path",
        default=str(_default_db_path()),
        help="Path to auth.sqlite",
    )
    parser.add_argument(
        "--tournament-id",
        help="Optional tournament id filter.",
    )
    parser.add_argument(
        "--match-id",
        help="Optional match id filter.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON instead of text output.",
    )
    return parser.parse_args()


def _load_table_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row["name"] or "").strip().lower() for row in rows if str(row["name"] or "").strip()}


def _load_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row["name"] or "").strip().lower() for row in rows if str(row["name"] or "").strip()}


def _deleted_filter(alias: str, columns: set[str]) -> str:
    return f"AND {alias}.deleted_at IS NULL" if "deleted_at" in columns else ""


def _validate_schema(conn: sqlite3.Connection) -> None:
    tables = _load_table_names(conn)
    missing_tables = sorted({"matches", "duels"} - tables)
    if missing_tables:
        raise RuntimeError(f"Missing required tables: {', '.join(missing_tables)}")

    required_columns = {
        "matches": {"id", "tournament_id", "team_1", "team_2", *MATCH_RESULT_FIELDS}
        | {f"{field}_import" for field in MATCH_RESULT_FIELDS},
        "duels": {"id", "match_id", "duel_number", "player_1_id", "player_2_id", *DUEL_RESULT_FIELDS}
        | {f"{field}_import" for field in DUEL_RESULT_FIELDS},
    }
    for table_name, expected_columns in required_columns.items():
        actual_columns = _load_columns(conn, table_name)
        missing_columns = sorted(expected_columns - actual_columns)
        if missing_columns:
            raise RuntimeError(
                f"Table {table_name} is missing required columns: {', '.join(missing_columns)}"
            )


def _to_int_or_none(value: object) -> int | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return int(float(raw))
    except ValueError:
        return None


def _build_field_diffs(row: sqlite3.Row, fields: tuple[str, ...]) -> list[dict[str, object]]:
    diffs: list[dict[str, object]] = []
    for field in fields:
        current_value = _to_int_or_none(row[field])
        import_value = _to_int_or_none(row[f"{field}_import"])
        if import_value is None:
            continue
        if current_value == import_value:
            continue
        diffs.append(
            {
                "field": field,
                "current": current_value,
                "import": import_value,
            }
        )
    return diffs


def _optional_name_join(tables: set[str], table_name: str, alias: str, id_expr: str) -> str:
    if table_name not in tables:
        return ""
    return f"LEFT JOIN {table_name} {alias} ON trim(COALESCE({alias}.id, '')) = trim(COALESCE({id_expr}, ''))"


def load_mismatch_rows(
    conn: sqlite3.Connection,
    *,
    tournament_id: str | None,
    match_id: str | None,
) -> tuple[list[sqlite3.Row], list[sqlite3.Row]]:
    tables = _load_table_names(conn)
    match_columns = _load_columns(conn, "matches")
    duel_columns = _load_columns(conn, "duels")

    has_tournaments = "tournaments" in tables
    has_teams = "teams" in tables
    has_profiles = "profiles" in tables

    where_parts = ["1 = 1", _deleted_filter("m", match_columns).removeprefix("AND ")]
    params: list[object] = []
    if str(tournament_id or "").strip():
        where_parts.append("upper(trim(COALESCE(m.tournament_id, ''))) = upper(trim(?))")
        params.append(str(tournament_id).strip())
    if str(match_id or "").strip():
        where_parts.append("trim(COALESCE(m.id, '')) = trim(?)")
        params.append(str(match_id).strip())
    where_sql = " AND ".join(part for part in where_parts if part)

    tournament_name_expr = "trim(COALESCE(t.name, ''))" if has_tournaments else "''"
    team_1_name_expr = "trim(COALESCE(tm1.name, ''))" if has_teams else "''"
    team_2_name_expr = "trim(COALESCE(tm2.name, ''))" if has_teams else "''"
    player_1_name_expr = "trim(COALESCE(p1.bga_nickname, p1.name, ''))" if has_profiles else "''"
    player_2_name_expr = "trim(COALESCE(p2.bga_nickname, p2.name, ''))" if has_profiles else "''"

    match_rows = conn.execute(
        f"""
        SELECT
          trim(COALESCE(m.id, '')) AS match_id,
          trim(COALESCE(m.tournament_id, '')) AS tournament_id,
          {tournament_name_expr} AS tournament_name,
          trim(COALESCE(m.time_utc, '')) AS match_time_utc,
          trim(COALESCE(m.status, '')) AS match_status,
          trim(COALESCE(m.team_1, '')) AS team_1,
          {team_1_name_expr} AS team_1_name,
          trim(COALESCE(m.team_2, '')) AS team_2,
          {team_2_name_expr} AS team_2_name,
          m.dw1 AS dw1,
          m.dw2 AS dw2,
          m.gw1 AS gw1,
          m.gw2 AS gw2,
          m.dw1_import AS dw1_import,
          m.dw2_import AS dw2_import,
          m.gw1_import AS gw1_import,
          m.gw2_import AS gw2_import
        FROM matches m
        {_optional_name_join(tables, "tournaments", "t", "m.tournament_id")}
        {_optional_name_join(tables, "teams", "tm1", "m.team_1")}
        {_optional_name_join(tables, "teams", "tm2", "m.team_2")}
        WHERE {where_sql}
        ORDER BY
          datetime(COALESCE(m.time_utc, '1970-01-01 00:00:00')) DESC,
          m.tournament_id COLLATE NOCASE ASC,
          m.id COLLATE NOCASE ASC
        """,
        params,
    ).fetchall()

    duel_where_parts = [
        "1 = 1",
        _deleted_filter("m", match_columns).removeprefix("AND "),
        _deleted_filter("d", duel_columns).removeprefix("AND "),
    ]
    duel_params = list(params)
    duel_where_parts.extend(where_parts[2:] if not str(tournament_id or "").strip() else where_parts[1:])
    duel_where_sql = " AND ".join(part for part in duel_where_parts if part)

    duel_rows = conn.execute(
        f"""
        SELECT
          trim(COALESCE(d.id, '')) AS duel_id,
          d.duel_number AS duel_number,
          trim(COALESCE(d.match_id, '')) AS match_id,
          trim(COALESCE(d.time_utc, '')) AS duel_time_utc,
          trim(COALESCE(d.status, '')) AS duel_status,
          trim(COALESCE(d.player_1_id, '')) AS player_1_id,
          {player_1_name_expr} AS player_1_name,
          trim(COALESCE(d.player_2_id, '')) AS player_2_id,
          {player_2_name_expr} AS player_2_name,
          d.dw1 AS dw1,
          d.dw2 AS dw2,
          d.dw1_import AS dw1_import,
          d.dw2_import AS dw2_import,
          trim(COALESCE(m.id, '')) AS parent_match_id,
          trim(COALESCE(m.tournament_id, '')) AS tournament_id,
          {tournament_name_expr} AS tournament_name,
          trim(COALESCE(m.time_utc, '')) AS match_time_utc,
          trim(COALESCE(m.team_1, '')) AS team_1,
          {team_1_name_expr} AS team_1_name,
          trim(COALESCE(m.team_2, '')) AS team_2,
          {team_2_name_expr} AS team_2_name
        FROM duels d
        JOIN matches m
          ON trim(COALESCE(m.id, '')) = trim(COALESCE(d.match_id, ''))
        {_optional_name_join(tables, "tournaments", "t", "m.tournament_id")}
        {_optional_name_join(tables, "teams", "tm1", "m.team_1")}
        {_optional_name_join(tables, "teams", "tm2", "m.team_2")}
        {_optional_name_join(tables, "profiles", "p1", "d.player_1_id")}
        {_optional_name_join(tables, "profiles", "p2", "d.player_2_id")}
        WHERE {duel_where_sql}
        ORDER BY
          datetime(COALESCE(m.time_utc, d.time_utc, '1970-01-01 00:00:00')) DESC,
          m.tournament_id COLLATE NOCASE ASC,
          m.id COLLATE NOCASE ASC,
          CASE WHEN d.duel_number IS NULL THEN 1 ELSE 0 END ASC,
          d.duel_number ASC,
          d.id COLLATE NOCASE ASC
        """,
        duel_params,
    ).fetchall()

    return match_rows, duel_rows


def _name_or_id(identifier: object, name: object) -> str:
    normalized_id = str(identifier or "").strip()
    normalized_name = str(name or "").strip()
    if normalized_name and normalized_id:
        return f"{normalized_id} ({normalized_name})"
    return normalized_id or normalized_name or "-"


def build_report(
    match_rows: list[sqlite3.Row],
    duel_rows: list[sqlite3.Row],
    *,
    tournament_id: str | None,
    match_id: str | None,
) -> dict[str, object]:
    matches: list[dict[str, object]] = []
    for row in match_rows:
        diffs = _build_field_diffs(row, MATCH_RESULT_FIELDS)
        if not diffs:
            continue
        matches.append(
            {
                "match_id": row["match_id"],
                "tournament_id": row["tournament_id"],
                "tournament_name": row["tournament_name"] or None,
                "time_utc": row["match_time_utc"] or None,
                "status": row["match_status"] or None,
                "team_1": row["team_1"],
                "team_1_name": row["team_1_name"] or None,
                "team_2": row["team_2"],
                "team_2_name": row["team_2_name"] or None,
                "current_score": {
                    "dw1": _to_int_or_none(row["dw1"]),
                    "dw2": _to_int_or_none(row["dw2"]),
                    "gw1": _to_int_or_none(row["gw1"]),
                    "gw2": _to_int_or_none(row["gw2"]),
                },
                "import_score": {
                    "dw1": _to_int_or_none(row["dw1_import"]),
                    "dw2": _to_int_or_none(row["dw2_import"]),
                    "gw1": _to_int_or_none(row["gw1_import"]),
                    "gw2": _to_int_or_none(row["gw2_import"]),
                },
                "diffs": diffs,
            }
        )

    duels: list[dict[str, object]] = []
    for row in duel_rows:
        diffs = _build_field_diffs(row, DUEL_RESULT_FIELDS)
        if not diffs:
            continue
        duels.append(
            {
                "duel_id": row["duel_id"],
                "duel_number": _to_int_or_none(row["duel_number"]),
                "match_id": row["match_id"],
                "tournament_id": row["tournament_id"],
                "tournament_name": row["tournament_name"] or None,
                "match_time_utc": row["match_time_utc"] or None,
                "duel_time_utc": row["duel_time_utc"] or None,
                "status": row["duel_status"] or None,
                "team_1": row["team_1"],
                "team_1_name": row["team_1_name"] or None,
                "team_2": row["team_2"],
                "team_2_name": row["team_2_name"] or None,
                "player_1_id": row["player_1_id"],
                "player_1_name": row["player_1_name"] or None,
                "player_2_id": row["player_2_id"],
                "player_2_name": row["player_2_name"] or None,
                "current_score": {
                    "dw1": _to_int_or_none(row["dw1"]),
                    "dw2": _to_int_or_none(row["dw2"]),
                },
                "import_score": {
                    "dw1": _to_int_or_none(row["dw1_import"]),
                    "dw2": _to_int_or_none(row["dw2_import"]),
                },
                "diffs": diffs,
            }
        )

    return {
        "filters": {
            "tournament_id": str(tournament_id or "").strip() or None,
            "match_id": str(match_id or "").strip() or None,
        },
        "matches_count": len(matches),
        "duels_count": len(duels),
        "matches": matches,
        "duels": duels,
    }


def _format_diffs(diffs: list[dict[str, object]]) -> str:
    return ", ".join(f"{item['field']}: {item['current']} -> {item['import']}" for item in diffs)


def print_text_report(report: dict[str, object]) -> None:
    matches = report["matches"]
    duels = report["duels"]
    if not matches and not duels:
        print("No result mismatches found.")
        return

    print(f"Found {report['matches_count']} match mismatch(es) and {report['duels_count']} duel mismatch(es).")
    filters = report["filters"]
    if filters["tournament_id"] or filters["match_id"]:
        print(f"Filters: tournament_id={filters['tournament_id'] or '-'}, match_id={filters['match_id'] or '-'}")
    print()

    if matches:
        print("MATCHES")
        for match in matches:
            print(
                f"- Match {match['match_id']} | tournament={_name_or_id(match['tournament_id'], match['tournament_name'])} | "
                f"time_utc={match['time_utc'] or '-'} | status={match['status'] or '-'}"
            )
            print(
                f"  teams: {_name_or_id(match['team_1'], match['team_1_name'])} vs "
                f"{_name_or_id(match['team_2'], match['team_2_name'])}"
            )
            print(f"  diffs: {_format_diffs(match['diffs'])}")
        print()

    if duels:
        print("DUELS")
        for duel in duels:
            duel_number = duel["duel_number"] if duel["duel_number"] is not None else "?"
            print(
                f"- Duel #{duel_number} {duel['duel_id']} | match={duel['match_id']} | "
                f"tournament={_name_or_id(duel['tournament_id'], duel['tournament_name'])}"
            )
            print(
                f"  match_time_utc={duel['match_time_utc'] or '-'} | duel_time_utc={duel['duel_time_utc'] or '-'} | "
                f"status={duel['status'] or '-'}"
            )
            print(
                f"  teams: {_name_or_id(duel['team_1'], duel['team_1_name'])} vs "
                f"{_name_or_id(duel['team_2'], duel['team_2_name'])}"
            )
            print(
                f"  players: {_name_or_id(duel['player_1_id'], duel['player_1_name'])} vs "
                f"{_name_or_id(duel['player_2_id'], duel['player_2_name'])}"
            )
            print(f"  diffs: {_format_diffs(duel['diffs'])}")
        print()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser()
    if not db_path.exists():
        print(f"Database file not found: {db_path}", file=sys.stderr)
        return 2

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        _validate_schema(conn)
        match_rows, duel_rows = load_mismatch_rows(
            conn,
            tournament_id=str(args.tournament_id or "").strip() or None,
            match_id=str(args.match_id or "").strip() or None,
        )

    report = build_report(
        match_rows,
        duel_rows,
        tournament_id=str(args.tournament_id or "").strip() or None,
        match_id=str(args.match_id or "").strip() or None,
    )
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print_text_report(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
