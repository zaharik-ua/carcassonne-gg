from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path


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
            "Find Friendly-Matches matches where duel players do not belong to the teams playing "
            "(profiles.association must match match.team_1 / match.team_2)."
        )
    )
    parser.add_argument(
        "--db-path",
        default=str(_default_db_path()),
        help="Path to auth.sqlite",
    )
    parser.add_argument(
        "--tournament-id",
        default="Friendly-Matches",
        help="Tournament id to inspect. Default: Friendly-Matches",
    )
    parser.add_argument(
        "--match-id",
        help="Inspect only one match id.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON instead of text output.",
    )
    return parser.parse_args()


def _load_table_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
        """
    ).fetchall()
    return {str(row["name"]).strip().lower() for row in rows if str(row["name"] or "").strip()}


def _load_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row["name"]).strip().lower() for row in rows if str(row["name"] or "").strip()}


def _deleted_filter(alias: str, columns: set[str]) -> str:
    return f"AND {alias}.deleted_at IS NULL" if "deleted_at" in columns else ""


def _validate_schema(conn: sqlite3.Connection) -> None:
    tables = _load_table_names(conn)
    missing_tables = [name for name in ("matches", "duels", "profiles") if name not in tables]
    if missing_tables:
        raise RuntimeError(f"Missing required tables: {', '.join(missing_tables)}")

    required_columns = {
        "matches": {"id", "tournament_id", "team_1", "team_2"},
        "duels": {"id", "match_id", "player_1_id", "player_2_id"},
        "profiles": {"id", "association"},
    }
    for table_name, expected_columns in required_columns.items():
        actual_columns = _load_columns(conn, table_name)
        missing_columns = sorted(expected_columns - actual_columns)
        if missing_columns:
            raise RuntimeError(
                f"Table {table_name} is missing required columns: {', '.join(missing_columns)}"
            )


def load_invalid_rows(
    conn: sqlite3.Connection,
    *,
    tournament_id: str,
    match_id: str | None,
) -> list[sqlite3.Row]:
    match_columns = _load_columns(conn, "matches")
    duel_columns = _load_columns(conn, "duels")
    profile_columns = _load_columns(conn, "profiles")

    params: list[object] = [tournament_id]
    match_id_filter = ""
    if str(match_id or "").strip():
        match_id_filter = "AND trim(COALESCE(m.id, '')) = trim(?)"
        params.append(str(match_id).strip())

    return conn.execute(
        f"""
        WITH inspected AS (
          SELECT
            m.id AS match_id,
            m.time_utc AS match_time_utc,
            upper(trim(COALESCE(m.team_1, ''))) AS team_1,
            upper(trim(COALESCE(m.team_2, ''))) AS team_2,
            d.id AS duel_id,
            d.duel_number,
            d.time_utc AS duel_time_utc,
            trim(COALESCE(d.player_1_id, '')) AS player_1_id,
            trim(COALESCE(p1.bga_nickname, '')) AS player_1_name,
            upper(trim(COALESCE(p1.association, ''))) AS player_1_association,
            trim(COALESCE(d.player_2_id, '')) AS player_2_id,
            trim(COALESCE(p2.bga_nickname, '')) AS player_2_name,
            upper(trim(COALESCE(p2.association, ''))) AS player_2_association,
            CASE
              WHEN trim(COALESCE(d.player_1_id, '')) = '' THEN 'player_missing'
              WHEN trim(COALESCE(p1.id, '')) = '' THEN 'profile_missing'
              WHEN upper(trim(COALESCE(p1.association, ''))) = '' THEN 'association_missing'
              WHEN upper(trim(COALESCE(p1.association, ''))) <> upper(trim(COALESCE(m.team_1, ''))) THEN 'association_mismatch'
              ELSE NULL
            END AS player_1_issue,
            CASE
              WHEN trim(COALESCE(d.player_2_id, '')) = '' THEN 'player_missing'
              WHEN trim(COALESCE(p2.id, '')) = '' THEN 'profile_missing'
              WHEN upper(trim(COALESCE(p2.association, ''))) = '' THEN 'association_missing'
              WHEN upper(trim(COALESCE(p2.association, ''))) <> upper(trim(COALESCE(m.team_2, ''))) THEN 'association_mismatch'
              ELSE NULL
            END AS player_2_issue
          FROM matches m
          JOIN duels d
            ON trim(COALESCE(d.match_id, '')) = trim(COALESCE(m.id, ''))
            {_deleted_filter("d", duel_columns)}
          LEFT JOIN profiles p1
            ON trim(COALESCE(p1.id, '')) = trim(COALESCE(d.player_1_id, ''))
            {_deleted_filter("p1", profile_columns)}
          LEFT JOIN profiles p2
            ON trim(COALESCE(p2.id, '')) = trim(COALESCE(d.player_2_id, ''))
            {_deleted_filter("p2", profile_columns)}
          WHERE upper(trim(COALESCE(m.tournament_id, ''))) = upper(trim(?))
            {_deleted_filter("m", match_columns)}
            {match_id_filter}
        )
        SELECT *
        FROM inspected
        WHERE player_1_issue IS NOT NULL
           OR player_2_issue IS NOT NULL
        ORDER BY
          datetime(COALESCE(match_time_utc, '1970-01-01 00:00:00')) DESC,
          match_id ASC,
          CASE WHEN duel_number IS NULL THEN 1 ELSE 0 END ASC,
          duel_number ASC,
          duel_id ASC
        """,
        params,
    ).fetchall()


def _row_to_issue(row: sqlite3.Row, player_index: int) -> dict[str, object] | None:
    issue = str(row[f"player_{player_index}_issue"] or "").strip()
    if not issue:
        return None
    return {
        "side": f"player_{player_index}",
        "issue": issue,
        "expected_team": row[f"team_{player_index}"],
        "player_id": row[f"player_{player_index}_id"],
        "player_name": row[f"player_{player_index}_name"] or None,
        "player_association": row[f"player_{player_index}_association"] or None,
    }


def build_report(rows: list[sqlite3.Row], *, tournament_id: str) -> dict[str, object]:
    matches: dict[str, dict[str, object]] = {}

    for row in rows:
        match_key = str(row["match_id"]).strip()
        if match_key not in matches:
            matches[match_key] = {
                "match_id": match_key,
                "time_utc": row["match_time_utc"],
                "team_1": row["team_1"],
                "team_2": row["team_2"],
                "duels": [],
            }

        issues = [issue for issue in (_row_to_issue(row, 1), _row_to_issue(row, 2)) if issue]
        matches[match_key]["duels"].append(
            {
                "duel_id": row["duel_id"],
                "duel_number": row["duel_number"],
                "time_utc": row["duel_time_utc"],
                "issues": issues,
            }
        )

    total_duels = sum(len(match["duels"]) for match in matches.values())
    total_issues = sum(len(duel["issues"]) for match in matches.values() for duel in match["duels"])

    return {
        "tournament_id": tournament_id,
        "matches_count": len(matches),
        "duels_count": total_duels,
        "issues_count": total_issues,
        "matches": list(matches.values()),
    }


def print_text_report(report: dict[str, object]) -> None:
    matches = report["matches"]
    if not matches:
        print(f"No invalid matches found in tournament {report['tournament_id']}.")
        return

    print(
        f"Found {report['matches_count']} invalid match(es), "
        f"{report['duels_count']} invalid duel(s), "
        f"{report['issues_count']} issue(s) in tournament {report['tournament_id']}."
    )
    print()

    for match in matches:
        print(
            f"Match {match['match_id']} "
            f"({match['team_1']} vs {match['team_2']}, time_utc={match['time_utc'] or '-'})"
        )
        for duel in match["duels"]:
            duel_label = duel["duel_number"] if duel["duel_number"] is not None else "?"
            print(
                f"  Duel #{duel_label}: {duel['duel_id']} "
                f"(time_utc={duel['time_utc'] or '-'})"
            )
            for issue in duel["issues"]:
                print(
                    "    "
                    f"{issue['side']}: issue={issue['issue']}, "
                    f"expected_team={issue['expected_team']}, "
                    f"player_id={issue['player_id'] or '-'}, "
                    f"player_name={issue['player_name'] or '-'}, "
                    f"player_association={issue['player_association'] or '-'}"
                )
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
        rows = load_invalid_rows(
            conn,
            tournament_id=str(args.tournament_id or "").strip() or "Friendly-Matches",
            match_id=str(args.match_id or "").strip() or None,
        )

    report = build_report(rows, tournament_id=str(args.tournament_id or "").strip() or "Friendly-Matches")
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print_text_report(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
