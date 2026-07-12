from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, time, timezone
from pathlib import Path


@dataclass(frozen=True)
class GgRatingSettings:
    base_date: datetime
    delta_start_date: datetime


@dataclass(frozen=True)
class GgEloProfileRow:
    profile_id: str
    gg_base_elo: float | None
    is_active: bool


@dataclass(frozen=True)
class GgEloDuelRow:
    duel_id: str
    time_utc: datetime
    player_1_id: str
    player_2_id: str
    dw1: int | None
    dw2: int | None
    duel_format: str | None


@dataclass(frozen=True)
class GgEloDuelRatingUpdate:
    duel_id: str
    player1_elo_before: float
    player1_elo_after: float
    player2_elo_before: float
    player2_elo_after: float


class SqliteProfileGgEloRepository:
    def __init__(self, db_path: str) -> None:
        self.db_path = str(Path(db_path).resolve())
        self._ensure_schema()

    def load_rating_settings(self) -> GgRatingSettings:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT setting_key, setting_value
                FROM system_settings
                WHERE setting_key IN ('gg_rating_base_date', 'gg_rating_delta_start_date')
                """
            ).fetchall()
        values = {str(row["setting_key"]): str(row["setting_value"] or "").strip() for row in rows}
        base_date = _parse_date_setting(values.get("gg_rating_base_date"), "gg_rating_base_date")
        delta_start_date = _parse_date_setting(
            values.get("gg_rating_delta_start_date"),
            "gg_rating_delta_start_date",
        )
        return GgRatingSettings(base_date=base_date, delta_start_date=delta_start_date)

    def load_profiles(self) -> list[GgEloProfileRow]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                  trim(COALESCE(id, '')) AS id,
                  gg_base_elo,
                  lower(trim(COALESCE(status, ''))) = 'active' AS is_active
                FROM profiles
                WHERE trim(COALESCE(id, '')) <> ''
                  AND deleted_at IS NULL
                ORDER BY id COLLATE NOCASE ASC
                """
            ).fetchall()
        return [
            GgEloProfileRow(
                profile_id=str(row["id"]).strip(),
                gg_base_elo=_float_or_none(row["gg_base_elo"]),
                is_active=bool(row["is_active"]),
            )
            for row in rows
        ]

    def load_duels_after(self, base_date: datetime) -> list[GgEloDuelRow]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                  trim(COALESCE(id, '')) AS id,
                  time_utc,
                  trim(COALESCE(player_1_id, '')) AS player_1_id,
                  trim(COALESCE(player_2_id, '')) AS player_2_id,
                  dw1,
                  dw2,
                  duel_format
                FROM duels
                WHERE deleted_at IS NULL
                  AND COALESCE(ranking, 0) = 1
                  AND trim(COALESCE(id, '')) <> ''
                  AND trim(COALESCE(time_utc, '')) <> ''
                  AND trim(COALESCE(player_1_id, '')) <> ''
                  AND trim(COALESCE(player_2_id, '')) <> ''
                  AND trim(COALESCE(player_1_id, '')) <> trim(COALESCE(player_2_id, ''))
                  AND dw1 IS NOT NULL
                  AND dw2 IS NOT NULL
                ORDER BY time_utc ASC, id COLLATE NOCASE ASC
                """
            ).fetchall()

        duels: list[GgEloDuelRow] = []
        for row in rows:
            parsed_time = _parse_datetime_utc(row["time_utc"])
            if parsed_time <= base_date:
                continue
            duels.append(
                GgEloDuelRow(
                    duel_id=str(row["id"]).strip(),
                    time_utc=parsed_time,
                    player_1_id=str(row["player_1_id"]).strip(),
                    player_2_id=str(row["player_2_id"]).strip(),
                    dw1=_int_or_none(row["dw1"]),
                    dw2=_int_or_none(row["dw2"]),
                    duel_format=str(row["duel_format"]).strip() if row["duel_format"] is not None else None,
                )
            )
        duels.sort(key=lambda duel: (duel.time_utc, duel.duel_id.lower()))
        return duels

    def update_profile_ratings(
        self,
        *,
        ratings_by_id: dict[str, float],
        deltas_by_id: dict[str, float],
        positions_by_id: dict[str, int],
        base_elo_backfills_by_id: dict[str, float],
        duel_rating_updates: list[GgEloDuelRatingUpdate],
    ) -> int:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE TRANSACTION")
            try:
                conn.execute(
                    """
                    UPDATE profiles
                    SET gg_rating_position = NULL
                    WHERE deleted_at IS NULL
                    """
                )
                updated = 0
                for profile_id, gg_elo in ratings_by_id.items():
                    cursor = conn.execute(
                        """
                        UPDATE profiles
                        SET
                          gg_elo = ?,
                          gg_base_elo = CASE
                            WHEN gg_base_elo IS NULL OR trim(CAST(gg_base_elo AS TEXT)) = ''
                            THEN ?
                            ELSE gg_base_elo
                          END,
                          gg_elo_period_delta = ?,
                          gg_elo_updated_at = CURRENT_TIMESTAMP,
                          gg_rating_position = ?
                        WHERE trim(COALESCE(id, '')) = ?
                          AND deleted_at IS NULL
                        """,
                        (
                            gg_elo,
                            base_elo_backfills_by_id.get(profile_id),
                            deltas_by_id.get(profile_id),
                            positions_by_id.get(profile_id),
                            profile_id,
                        ),
                    )
                    updated += int(cursor.rowcount or 0)
                conn.executemany(
                    """
                    UPDATE duels
                    SET
                      player1_elo_before = ?,
                      player1_elo_after = ?,
                      player2_elo_before = ?,
                      player2_elo_after = ?
                    WHERE trim(COALESCE(id, '')) = ?
                      AND deleted_at IS NULL
                      AND COALESCE(ranking, 0) = 1
                    """,
                    [
                        (
                            item.player1_elo_before,
                            item.player1_elo_after,
                            item.player2_elo_before,
                            item.player2_elo_after,
                            item.duel_id,
                        )
                        for item in duel_rating_updates
                    ],
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return updated

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            tables = {
                str(row["name"]).strip()
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
            }
            missing_tables = sorted({"profiles", "duels", "system_settings"} - tables)
            if missing_tables:
                raise RuntimeError(f"SQLite DB is missing required tables: {', '.join(missing_tables)}")

            profile_columns = _load_columns(conn, "profiles")
            duel_columns = _load_columns(conn, "duels")
            settings_columns = _load_columns(conn, "system_settings")

            required_profile_columns = {"id", "status", "deleted_at", "gg_base_elo"}
            required_duel_columns = {
                "id",
                "time_utc",
                "player_1_id",
                "player_2_id",
                "dw1",
                "dw2",
                "duel_format",
                "ranking",
                "deleted_at",
            }
            required_settings_columns = {"setting_key", "setting_value"}
            missing_columns = {
                "profiles": sorted(required_profile_columns - profile_columns),
                "duels": sorted(required_duel_columns - duel_columns),
                "system_settings": sorted(required_settings_columns - settings_columns),
            }
            errors = [
                f"{table}: {', '.join(columns)}"
                for table, columns in missing_columns.items()
                if columns
            ]
            if errors:
                raise RuntimeError(f"SQLite DB is missing required columns: {'; '.join(errors)}")

            if "gg_elo" not in profile_columns:
                conn.execute("ALTER TABLE profiles ADD COLUMN gg_elo REAL")
            if "gg_elo_period_delta" not in profile_columns:
                conn.execute("ALTER TABLE profiles ADD COLUMN gg_elo_period_delta REAL")
            if "gg_elo_updated_at" not in profile_columns:
                conn.execute("ALTER TABLE profiles ADD COLUMN gg_elo_updated_at TEXT")
            if "gg_rating_position" not in profile_columns:
                conn.execute("ALTER TABLE profiles ADD COLUMN gg_rating_position INTEGER")
            if "player1_elo_before" not in duel_columns:
                conn.execute("ALTER TABLE duels ADD COLUMN player1_elo_before REAL")
            if "player1_elo_after" not in duel_columns:
                conn.execute("ALTER TABLE duels ADD COLUMN player1_elo_after REAL")
            if "player2_elo_before" not in duel_columns:
                conn.execute("ALTER TABLE duels ADD COLUMN player2_elo_before REAL")
            if "player2_elo_after" not in duel_columns:
                conn.execute("ALTER TABLE duels ADD COLUMN player2_elo_after REAL")
            conn.commit()


def _load_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {
        str(row["name"]).strip()
        for row in conn.execute(f"PRAGMA table_info({_quote_identifier(table_name)})").fetchall()
        if row["name"] is not None
    }


def _parse_date_setting(value: str | None, setting_key: str) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        raise RuntimeError(f"system_settings.{setting_key} must be set before recalculating GG Elo")
    try:
        parsed_date = datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError as exc:
        raise RuntimeError(f"system_settings.{setting_key} must use YYYY-MM-DD format") from exc
    return datetime.combine(parsed_date, time.min, tzinfo=timezone.utc)


def _parse_datetime_utc(value: object) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("Missing duel time_utc")
    normalized = raw.replace("Z", "+00:00") if raw.endswith("Z") else raw
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _float_or_none(value: object) -> float | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _int_or_none(value: object) -> int | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return int(float(raw))
    except ValueError:
        return None


def _quote_identifier(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'
