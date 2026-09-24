from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from .config import (
    BGA_REPLAY_COOLDOWN_HOURS,
    BGA_REPLAY_FRESH_RESERVE,
    BGA_REPLAY_HISTORICAL_LIMIT,
    BGA_REPLAY_MAX_TOTAL_LIMIT,
    BGA_REPLAY_TOTAL_LIMIT,
)


LOGS_ENDPOINT = "/archive/archive/logs.html"
REQUEST_CLASSES = frozenset({"fresh", "historical", "manual"})


class ReplayBudgetUnavailableError(RuntimeError):
    pass


@dataclass(frozen=True)
class ReplayBudgetLimits:
    total_limit: int = BGA_REPLAY_TOTAL_LIMIT
    fresh_reserve: int = BGA_REPLAY_FRESH_RESERVE
    historical_limit: int = BGA_REPLAY_HISTORICAL_LIMIT
    max_total_limit: int = BGA_REPLAY_MAX_TOTAL_LIMIT
    cooldown_hours: int = BGA_REPLAY_COOLDOWN_HOURS

    def __post_init__(self) -> None:
        if self.total_limit < 1:
            raise ValueError("total_limit must be positive")
        if not 0 <= self.fresh_reserve <= self.total_limit:
            raise ValueError("fresh_reserve must be between zero and total_limit")
        if not 0 <= self.historical_limit <= self.total_limit:
            raise ValueError("historical_limit must be between zero and total_limit")
        if self.max_total_limit < self.total_limit:
            raise ValueError("max_total_limit must not be lower than total_limit")
        if self.cooldown_hours < 1:
            raise ValueError("cooldown_hours must be positive")


@dataclass(frozen=True)
class EffectiveReplayLimits:
    total_limit: int
    historical_limit: int
    fresh_reserve: int
    override_id: int | None


@dataclass(frozen=True)
class ReplayRequestReservation:
    request_id: int
    account_label: str
    request_class: str
    budget_override_id: int | None
    total_used_before: int
    historical_used_before: int
    effective_total_limit: int
    effective_historical_limit: int
    effective_fresh_reserve: int


def ensure_replay_budget_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS bga_replay_budget_overrides (
          id INTEGER PRIMARY KEY,
          account_label TEXT NOT NULL,
          extra_historical_limit INTEGER NOT NULL DEFAULT 0,
          total_limit_override INTEGER,
          starts_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          expires_at TEXT NOT NULL,
          created_by TEXT NOT NULL,
          reason TEXT,
          revoked_at TEXT,
          created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS bga_replay_requests (
          id INTEGER PRIMARY KEY,
          account_label TEXT NOT NULL,
          bga_table_id TEXT NOT NULL,
          endpoint TEXT NOT NULL,
          request_class TEXT NOT NULL,
          budget_override_id INTEGER,
          attempted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          outcome TEXT,
          error TEXT
        );

        CREATE TABLE IF NOT EXISTS bga_replay_account_state (
          account_label TEXT PRIMARY KEY,
          cooldown_until TEXT,
          last_limit_at TEXT,
          last_selected_at TEXT,
          last_error TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_bga_replay_requests_budget
          ON bga_replay_requests(account_label, endpoint, attempted_at, request_class);

        CREATE INDEX IF NOT EXISTS idx_bga_replay_requests_class_budget
          ON bga_replay_requests(account_label, endpoint, request_class, attempted_at);

        CREATE INDEX IF NOT EXISTS idx_bga_replay_budget_overrides_active
          ON bga_replay_budget_overrides(account_label, revoked_at, expires_at, starts_at);
        """
    )


def reserve_replay_request(
    db_path: str | Path,
    *,
    account_labels: Iterable[str],
    bga_table_id: str,
    request_class: str,
    limits: ReplayBudgetLimits | None = None,
    now: datetime | None = None,
) -> ReplayRequestReservation:
    normalized_class = _normalize_request_class(request_class)
    normalized_labels = _normalize_account_labels(account_labels)
    if not normalized_labels:
        raise ReplayBudgetUnavailableError("No BGA replay accounts are configured")

    active_limits = limits or ReplayBudgetLimits()
    attempted_at = _format_timestamp(now or datetime.now(timezone.utc))
    path = Path(db_path).expanduser()
    with sqlite3.connect(path, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        ensure_replay_budget_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            for label in normalized_labels:
                conn.execute(
                    """
                    INSERT INTO bga_replay_account_state (account_label)
                    VALUES (?)
                    ON CONFLICT(account_label) DO NOTHING
                    """,
                    (label,),
                )

            candidates: list[tuple] = []
            for label_index, label in enumerate(normalized_labels):
                state = conn.execute(
                    """
                    SELECT cooldown_until, last_selected_at
                    FROM bga_replay_account_state
                    WHERE account_label = ?
                    """,
                    (label,),
                ).fetchone()
                if state is not None and _is_future_timestamp(
                    state["cooldown_until"], attempted_at
                ):
                    continue

                usage = conn.execute(
                    """
                    SELECT
                      COUNT(*) AS total_used,
                      COALESCE(SUM(CASE WHEN request_class = 'historical' THEN 1 ELSE 0 END), 0)
                        AS historical_used,
                      MAX(id) AS last_request_id
                    FROM bga_replay_requests
                    WHERE account_label = ?
                      AND endpoint = ?
                      AND datetime(attempted_at) > datetime(?, '-24 hours')
                    """,
                    (label, LOGS_ENDPOINT, attempted_at),
                ).fetchone()
                total_used = int(usage["total_used"] or 0)
                historical_used = int(usage["historical_used"] or 0)
                effective = _load_effective_limits(
                    conn,
                    account_label=label,
                    at=attempted_at,
                    limits=active_limits,
                )
                if not _request_is_allowed(
                    request_class=normalized_class,
                    total_used=total_used,
                    historical_used=historical_used,
                    effective=effective,
                ):
                    continue

                base_allowed = _request_is_allowed(
                    request_class=normalized_class,
                    total_used=total_used,
                    historical_used=historical_used,
                    effective=EffectiveReplayLimits(
                        total_limit=active_limits.total_limit,
                        historical_limit=active_limits.historical_limit,
                        fresh_reserve=active_limits.fresh_reserve,
                        override_id=None,
                    ),
                )
                applied_override_id = (
                    effective.override_id
                    if effective.override_id is not None and not base_allowed
                    else None
                )
                candidates.append(
                    (
                        total_used,
                        int(usage["last_request_id"] or 0),
                        label_index,
                        label,
                        historical_used,
                        effective,
                        applied_override_id,
                    )
                )

            if not candidates:
                raise ReplayBudgetUnavailableError(
                    f"No account has budget for request_class={normalized_class}"
                )

            (
                total_used,
                _last_request_id,
                _label_index,
                account_label,
                historical_used,
                effective,
                applied_override_id,
            ) = min(candidates, key=lambda candidate: candidate[:3])
            cursor = conn.execute(
                """
                INSERT INTO bga_replay_requests (
                  account_label,
                  bga_table_id,
                  endpoint,
                  request_class,
                  budget_override_id,
                  attempted_at,
                  outcome
                )
                VALUES (?, ?, ?, ?, ?, ?, 'reserved')
                """,
                (
                    account_label,
                    str(bga_table_id),
                    LOGS_ENDPOINT,
                    normalized_class,
                    applied_override_id,
                    attempted_at,
                ),
            )
            request_id = int(cursor.lastrowid)
            conn.execute(
                """
                UPDATE bga_replay_account_state
                SET last_selected_at = ?
                WHERE account_label = ?
                """,
                (attempted_at, account_label),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    return ReplayRequestReservation(
        request_id=request_id,
        account_label=account_label,
        request_class=normalized_class,
        budget_override_id=applied_override_id,
        total_used_before=total_used,
        historical_used_before=historical_used,
        effective_total_limit=effective.total_limit,
        effective_historical_limit=effective.historical_limit,
        effective_fresh_reserve=effective.fresh_reserve,
    )


def start_replay_request_audit(
    db_path: str | Path,
    *,
    account_label: str,
    bga_table_id: str,
    endpoint: str,
    request_class: str,
    now: datetime | None = None,
) -> int:
    normalized_class = _normalize_request_class(request_class)
    attempted_at = _format_timestamp(now or datetime.now(timezone.utc))
    with sqlite3.connect(Path(db_path).expanduser(), timeout=30) as conn:
        ensure_replay_budget_schema(conn)
        cursor = conn.execute(
            """
            INSERT INTO bga_replay_requests (
              account_label, bga_table_id, endpoint, request_class,
              attempted_at, outcome
            )
            VALUES (?, ?, ?, ?, ?, 'reserved')
            """,
            (
                str(account_label),
                str(bga_table_id),
                str(endpoint),
                normalized_class,
                attempted_at,
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)


def finish_replay_request_audit(
    db_path: str | Path,
    request_id: int,
    *,
    outcome: str,
    error: str | None = None,
) -> None:
    with sqlite3.connect(Path(db_path).expanduser(), timeout=30) as conn:
        conn.execute(
            """
            UPDATE bga_replay_requests
            SET outcome = ?, error = ?
            WHERE id = ?
            """,
            (str(outcome), str(error) if error else None, int(request_id)),
        )
        conn.commit()


def mark_replay_account_cooldown(
    db_path: str | Path,
    *,
    account_label: str,
    error: str,
    limits: ReplayBudgetLimits | None = None,
    now: datetime | None = None,
) -> str:
    active_limits = limits or ReplayBudgetLimits()
    limited_at_value = now or datetime.now(timezone.utc)
    limited_at = _format_timestamp(limited_at_value)
    cooldown_until = _format_timestamp(
        _as_utc(limited_at_value) + timedelta(hours=active_limits.cooldown_hours)
    )
    with sqlite3.connect(Path(db_path).expanduser(), timeout=30) as conn:
        ensure_replay_budget_schema(conn)
        conn.execute(
            """
            INSERT INTO bga_replay_account_state (
              account_label, cooldown_until, last_limit_at, last_error
            )
            VALUES (?, ?, ?, ?)
            ON CONFLICT(account_label) DO UPDATE SET
              cooldown_until = excluded.cooldown_until,
              last_limit_at = excluded.last_limit_at,
              last_error = excluded.last_error
            """,
            (str(account_label), cooldown_until, limited_at, str(error)),
        )
        conn.commit()
    return cooldown_until


def replace_replay_budget_overrides(
    db_path: str | Path,
    *,
    account_labels: Iterable[str],
    extra_historical_limit: int,
    total_limit_override: int | None,
    expires_at: datetime | str,
    created_by: str,
    reason: str,
    limits: ReplayBudgetLimits | None = None,
    starts_at: datetime | None = None,
) -> list[int]:
    active_limits = limits or ReplayBudgetLimits()
    labels = _normalize_account_labels(account_labels)
    if not labels:
        raise ValueError("At least one account_label is required")
    if isinstance(extra_historical_limit, bool) or not isinstance(
        extra_historical_limit, int
    ):
        raise ValueError("extra_historical_limit must be an integer")
    if total_limit_override is not None and (
        isinstance(total_limit_override, bool)
        or not isinstance(total_limit_override, int)
        or not active_limits.total_limit
        < total_limit_override
        <= active_limits.max_total_limit
    ):
        raise ValueError(
            "total_limit_override must be an integer above the base limit "
            f"and no higher than {active_limits.max_total_limit}"
        )
    effective_total = total_limit_override or active_limits.total_limit
    max_extra = effective_total - active_limits.historical_limit
    if not 0 <= extra_historical_limit <= max_extra:
        raise ValueError(
            f"extra_historical_limit must be between 0 and {max_extra}"
        )
    actor = str(created_by or "").strip()
    explanation = str(reason or "").strip()
    if not actor:
        raise ValueError("created_by is required")
    if not explanation:
        raise ValueError("reason is required")

    start_value = _as_utc(starts_at or datetime.now(timezone.utc))
    expiry_value = _parse_timestamp(expires_at)
    if expiry_value <= start_value:
        raise ValueError("expires_at must be later than starts_at")
    start_text = _format_timestamp(start_value)
    expiry_text = _format_timestamp(expiry_value)

    with sqlite3.connect(Path(db_path).expanduser(), timeout=30) as conn:
        ensure_replay_budget_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            placeholders = ",".join("?" for _ in labels)
            conn.execute(
                f"""
                UPDATE bga_replay_budget_overrides
                SET revoked_at = ?
                WHERE account_label IN ({placeholders})
                  AND revoked_at IS NULL
                  AND datetime(expires_at) > datetime(?)
                """,
                (start_text, *labels, start_text),
            )
            override_ids: list[int] = []
            for label in labels:
                cursor = conn.execute(
                    """
                    INSERT INTO bga_replay_budget_overrides (
                      account_label,
                      extra_historical_limit,
                      total_limit_override,
                      starts_at,
                      expires_at,
                      created_by,
                      reason
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        label,
                        extra_historical_limit,
                        total_limit_override,
                        start_text,
                        expiry_text,
                        actor,
                        explanation,
                    ),
                )
                override_ids.append(int(cursor.lastrowid))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return override_ids


def revoke_replay_budget_override(
    db_path: str | Path,
    override_id: int,
    *,
    revoked_at: datetime | None = None,
) -> bool:
    revoked_text = _format_timestamp(revoked_at or datetime.now(timezone.utc))
    with sqlite3.connect(Path(db_path).expanduser(), timeout=30) as conn:
        cursor = conn.execute(
            """
            UPDATE bga_replay_budget_overrides
            SET revoked_at = ?
            WHERE id = ? AND revoked_at IS NULL
            """,
            (revoked_text, int(override_id)),
        )
        conn.commit()
        return cursor.rowcount > 0


def _load_effective_limits(
    conn: sqlite3.Connection,
    *,
    account_label: str,
    at: str,
    limits: ReplayBudgetLimits,
) -> EffectiveReplayLimits:
    override = conn.execute(
        """
        SELECT id, extra_historical_limit, total_limit_override
        FROM bga_replay_budget_overrides
        WHERE account_label = ?
          AND revoked_at IS NULL
          AND datetime(starts_at) <= datetime(?)
          AND datetime(expires_at) > datetime(?)
        ORDER BY datetime(starts_at) DESC, id DESC
        LIMIT 1
        """,
        (account_label, at, at),
    ).fetchone()
    if override is None:
        return EffectiveReplayLimits(
            total_limit=limits.total_limit,
            historical_limit=limits.historical_limit,
            fresh_reserve=limits.fresh_reserve,
            override_id=None,
        )

    requested_total = override["total_limit_override"]
    total_limit = (
        int(requested_total)
        if requested_total is not None
        and limits.total_limit < int(requested_total) <= limits.max_total_limit
        else limits.total_limit
    )
    extra = max(0, int(override["extra_historical_limit"] or 0))
    historical_limit = min(limits.historical_limit + extra, total_limit)
    return EffectiveReplayLimits(
        total_limit=total_limit,
        historical_limit=historical_limit,
        fresh_reserve=total_limit - historical_limit,
        override_id=int(override["id"]),
    )


def _request_is_allowed(
    *,
    request_class: str,
    total_used: int,
    historical_used: int,
    effective: EffectiveReplayLimits,
) -> bool:
    if request_class != "historical":
        return total_used < effective.total_limit
    historical_total_ceiling = effective.total_limit - effective.fresh_reserve
    return (
        historical_used < effective.historical_limit
        and total_used < historical_total_ceiling
    )


def _normalize_request_class(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized not in REQUEST_CLASSES:
        raise ValueError(
            f"request_class must be one of {', '.join(sorted(REQUEST_CLASSES))}"
        )
    return normalized


def _normalize_account_labels(values: Iterable[str]) -> list[str]:
    labels: list[str] = []
    for value in values:
        label = str(value or "").strip()
        if label and label not in labels:
            labels.append(label)
    return labels


def _is_future_timestamp(value: object, reference: str) -> bool:
    if value is None or not str(value).strip():
        return False
    return _parse_timestamp(str(value)) > _parse_timestamp(reference)


def _parse_timestamp(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return _as_utc(value)
    raw = str(value or "").strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    parsed = datetime.fromisoformat(raw)
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _format_timestamp(value: datetime) -> str:
    return _as_utc(value).strftime("%Y-%m-%d %H:%M:%S.%f")
