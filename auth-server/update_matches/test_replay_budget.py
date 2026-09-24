from __future__ import annotations

import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .replay_budget import (
    LOGS_ENDPOINT,
    ReplayBudgetLimits,
    ReplayBudgetUnavailableError,
    ensure_replay_budget_schema,
    finish_replay_request_audit,
    mark_replay_account_cooldown,
    replace_replay_budget_overrides,
    reserve_replay_request,
    revoke_replay_budget_override,
    start_replay_request_audit,
)


class ReplayBudgetTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "auth.sqlite"
        self.now = datetime(2026, 9, 23, 12, 0, tzinfo=timezone.utc)
        with sqlite3.connect(self.db_path) as conn:
            ensure_replay_budget_schema(conn)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_balances_equal_usage_across_all_accounts(self) -> None:
        selected = []
        for index in range(6):
            reservation = reserve_replay_request(
                self.db_path,
                account_labels=["account-a", "account-b", "account-c"],
                bga_table_id=str(100 + index),
                request_class="manual",
                now=self.now + timedelta(seconds=index),
            )
            selected.append(reservation.account_label)

        self.assertEqual(
            selected,
            [
                "account-a",
                "account-b",
                "account-c",
                "account-a",
                "account-b",
                "account-c",
            ],
        )

    def test_total_limit_is_atomic_and_uses_a_rolling_window(self) -> None:
        limits = ReplayBudgetLimits(
            total_limit=1,
            fresh_reserve=0,
            historical_limit=1,
            max_total_limit=2,
        )

        def reserve_once(table_id: str) -> str:
            try:
                reservation = reserve_replay_request(
                    self.db_path,
                    account_labels=["account-a"],
                    bga_table_id=table_id,
                    request_class="fresh",
                    limits=limits,
                    now=self.now,
                )
                return f"reserved:{reservation.request_id}"
            except ReplayBudgetUnavailableError:
                return "unavailable"

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(reserve_once, ["101", "102"]))

        self.assertEqual(sum(result.startswith("reserved:") for result in results), 1)
        self.assertEqual(results.count("unavailable"), 1)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE bga_replay_requests SET attempted_at = ?",
                ((self.now - timedelta(hours=25)).strftime("%Y-%m-%d %H:%M:%S"),),
            )
        reservation = reserve_replay_request(
            self.db_path,
            account_labels=["account-a"],
            bga_table_id="103",
            request_class="fresh",
            limits=limits,
            now=self.now,
        )
        self.assertEqual(reservation.total_used_before, 0)

    def test_historical_requests_cannot_consume_the_fresh_reserve(self) -> None:
        limits = ReplayBudgetLimits(
            total_limit=8,
            fresh_reserve=5,
            historical_limit=3,
            max_total_limit=10,
        )
        reserve_replay_request(
            self.db_path,
            account_labels=["account-a"],
            bga_table_id="fresh",
            request_class="fresh",
            limits=limits,
            now=self.now,
        )
        for index in range(2):
            reserve_replay_request(
                self.db_path,
                account_labels=["account-a"],
                bga_table_id=f"historical-{index}",
                request_class="historical",
                limits=limits,
                now=self.now + timedelta(seconds=index + 1),
            )
        with self.assertRaises(ReplayBudgetUnavailableError):
            reserve_replay_request(
                self.db_path,
                account_labels=["account-a"],
                bga_table_id="historical-blocked",
                request_class="historical",
                limits=limits,
                now=self.now + timedelta(seconds=3),
            )

    def test_manual_usage_also_reduces_historical_capacity(self) -> None:
        limits = ReplayBudgetLimits(
            total_limit=8,
            fresh_reserve=5,
            historical_limit=3,
            max_total_limit=10,
        )
        reserve_replay_request(
            self.db_path,
            account_labels=["account-a"],
            bga_table_id="manual",
            request_class="manual",
            limits=limits,
            now=self.now,
        )
        for index in range(2):
            reserve_replay_request(
                self.db_path,
                account_labels=["account-a"],
                bga_table_id=f"historical-{index}",
                request_class="historical",
                limits=limits,
                now=self.now + timedelta(seconds=index + 1),
            )
        with self.assertRaises(ReplayBudgetUnavailableError):
            reserve_replay_request(
                self.db_path,
                account_labels=["account-a"],
                bga_table_id="historical-blocked",
                request_class="historical",
                limits=limits,
                now=self.now + timedelta(seconds=3),
            )

    def test_cooldown_excludes_only_the_limited_account(self) -> None:
        cooldown_until = mark_replay_account_cooldown(
            self.db_path,
            account_label="account-a",
            error="limit (replay)",
            now=self.now,
        )
        self.assertTrue(cooldown_until.startswith("2026-09-24 12:00:00"))

        reservation = reserve_replay_request(
            self.db_path,
            account_labels=["account-a", "account-b"],
            bga_table_id="101",
            request_class="manual",
            now=self.now + timedelta(minutes=1),
        )
        self.assertEqual(reservation.account_label, "account-b")

        with self.assertRaises(ReplayBudgetUnavailableError):
            reserve_replay_request(
                self.db_path,
                account_labels=["account-a"],
                bga_table_id="102",
                request_class="manual",
                now=self.now + timedelta(hours=23),
            )

        recovered = reserve_replay_request(
            self.db_path,
            account_labels=["account-a"],
            bga_table_id="103",
            request_class="manual",
            now=self.now + timedelta(hours=25),
        )
        self.assertEqual(recovered.account_label, "account-a")

    def test_overrides_expand_only_the_configured_limits_and_replace_previous(self) -> None:
        limits = ReplayBudgetLimits(
            total_limit=8,
            fresh_reserve=5,
            historical_limit=3,
            max_total_limit=10,
        )
        first_override_id = replace_replay_budget_overrides(
            self.db_path,
            account_labels=["account-a"],
            extra_historical_limit=2,
            total_limit_override=10,
            starts_at=self.now,
            expires_at=self.now + timedelta(hours=2),
            created_by="admin",
            reason="historical catch-up",
            limits=limits,
        )[0]

        reservations = []
        for index in range(5):
            reservations.append(
                reserve_replay_request(
                    self.db_path,
                    account_labels=["account-a"],
                    bga_table_id=f"historical-{index}",
                    request_class="historical",
                    limits=limits,
                    now=self.now + timedelta(seconds=index + 1),
                )
            )
        self.assertEqual(
            [reservation.budget_override_id for reservation in reservations],
            [None, None, None, first_override_id, first_override_id],
        )

        replacement_id = replace_replay_budget_overrides(
            self.db_path,
            account_labels=["account-a"],
            extra_historical_limit=0,
            total_limit_override=10,
            starts_at=self.now + timedelta(minutes=1),
            expires_at=self.now + timedelta(hours=3),
            created_by="admin",
            reason="keep only total boost",
            limits=limits,
        )[0]
        with self.assertRaises(ReplayBudgetUnavailableError):
            reserve_replay_request(
                self.db_path,
                account_labels=["account-a"],
                bga_table_id="historical-blocked",
                request_class="historical",
                limits=limits,
                now=self.now + timedelta(minutes=2),
            )

        fresh_reservations = []
        for index in range(4):
            fresh_reservations.append(
                reserve_replay_request(
                    self.db_path,
                    account_labels=["account-a"],
                    bga_table_id=f"fresh-{index}",
                    request_class="fresh",
                    limits=limits,
                    now=self.now + timedelta(minutes=2, seconds=index),
                )
            )
        self.assertEqual(
            [reservation.budget_override_id for reservation in fresh_reservations],
            [None, None, None, replacement_id],
        )

        with sqlite3.connect(self.db_path) as conn:
            old_revoked_at = conn.execute(
                "SELECT revoked_at FROM bga_replay_budget_overrides WHERE id = ?",
                (first_override_id,),
            ).fetchone()[0]
        self.assertIsNotNone(old_revoked_at)
        self.assertTrue(revoke_replay_budget_override(self.db_path, replacement_id))
        with self.assertRaises(ReplayBudgetUnavailableError):
            reserve_replay_request(
                self.db_path,
                account_labels=["account-a"],
                bga_table_id="fresh-after-revoke",
                request_class="fresh",
                limits=limits,
                now=self.now + timedelta(minutes=3),
            )

    def test_total_eighty_with_boost_twenty_rebalances_historical_capacity(self) -> None:
        limits = ReplayBudgetLimits()
        replace_replay_budget_overrides(
            self.db_path,
            account_labels=["account-a"],
            extra_historical_limit=20,
            total_limit_override=None,
            starts_at=self.now,
            expires_at=self.now + timedelta(hours=1),
            created_by="admin",
            reason="formula regression test",
            limits=limits,
        )

        reservation = reserve_replay_request(
            self.db_path,
            account_labels=["account-a"],
            bga_table_id="historical",
            request_class="historical",
            limits=limits,
            now=self.now + timedelta(seconds=1),
        )

        self.assertEqual(reservation.effective_total_limit, 80)
        self.assertEqual(reservation.effective_historical_limit, 50)
        self.assertEqual(reservation.effective_fresh_reserve, 30)

    def test_expired_override_restores_base_limits_and_keeps_request_audit(self) -> None:
        limits = ReplayBudgetLimits(
            total_limit=2,
            fresh_reserve=1,
            historical_limit=1,
            max_total_limit=3,
        )
        override_id = replace_replay_budget_overrides(
            self.db_path,
            account_labels=["account-a"],
            extra_historical_limit=2,
            total_limit_override=3,
            starts_at=self.now,
            expires_at=self.now + timedelta(minutes=30),
            created_by="admin",
            reason="short catch-up",
            limits=limits,
        )[0]
        base_request = reserve_replay_request(
            self.db_path,
            account_labels=["account-a"],
            bga_table_id="historical-base",
            request_class="historical",
            limits=limits,
            now=self.now + timedelta(seconds=1),
        )
        override_request = reserve_replay_request(
            self.db_path,
            account_labels=["account-a"],
            bga_table_id="historical-override",
            request_class="historical",
            limits=limits,
            now=self.now + timedelta(seconds=2),
        )
        self.assertIsNone(base_request.budget_override_id)
        self.assertEqual(override_request.budget_override_id, override_id)

        after_expiry = self.now + timedelta(hours=1)
        with self.assertRaises(ReplayBudgetUnavailableError):
            reserve_replay_request(
                self.db_path,
                account_labels=["account-a"],
                bga_table_id="fresh-after-expiry",
                request_class="fresh",
                limits=limits,
                now=after_expiry,
            )

        with sqlite3.connect(self.db_path) as conn:
            audited = conn.execute(
                """
                SELECT budget_override_id
                FROM bga_replay_requests
                WHERE id = ?
                """,
                (override_request.request_id,),
            ).fetchone()
            stored_override = conn.execute(
                """
                SELECT revoked_at, expires_at
                FROM bga_replay_budget_overrides
                WHERE id = ?
                """,
                (override_id,),
            ).fetchone()
        self.assertEqual(audited[0], override_id)
        self.assertIsNone(stored_override[0])
        self.assertLess(stored_override[1], after_expiry.strftime("%Y-%m-%d %H:%M:%S.%f"))

    def test_override_validation_uses_dynamic_safe_range(self) -> None:
        limits = ReplayBudgetLimits(
            total_limit=80,
            fresh_reserve=50,
            historical_limit=30,
            max_total_limit=100,
        )
        for index, extra in enumerate((10, 20, 50), start=1):
            override_ids = replace_replay_budget_overrides(
                self.db_path,
                account_labels=[f"historical-preset-{index}"],
                extra_historical_limit=extra,
                total_limit_override=None,
                starts_at=self.now,
                expires_at=self.now + timedelta(hours=1),
                created_by="admin",
                reason="valid historical preset",
                limits=limits,
            )
            self.assertEqual(len(override_ids), 1)
        for total in (81, 100):
            override_ids = replace_replay_budget_overrides(
                self.db_path,
                account_labels=[f"total-{total}"],
                extra_historical_limit=0,
                total_limit_override=total,
                starts_at=self.now,
                expires_at=self.now + timedelta(hours=1),
                created_by="admin",
                reason="valid total boundary",
                limits=limits,
            )
            self.assertEqual(len(override_ids), 1)

        cases = (
            {"extra_historical_limit": -1, "total_limit_override": None},
            {"extra_historical_limit": 51, "total_limit_override": None},
            {"extra_historical_limit": 0, "total_limit_override": -1},
            {"extra_historical_limit": 0, "total_limit_override": 80},
            {"extra_historical_limit": 0, "total_limit_override": 101},
            {"extra_historical_limit": 0, "total_limit_override": 81.5},
            {"extra_historical_limit": 1.5, "total_limit_override": None},
        )
        for values in cases:
            with self.subTest(values=values), self.assertRaises(ValueError):
                replace_replay_budget_overrides(
                    self.db_path,
                    account_labels=["account-a"],
                    starts_at=self.now,
                    expires_at=self.now + timedelta(hours=1),
                    created_by="admin",
                    reason="invalid",
                    limits=limits,
                    **values,
                )

    def test_total_and_historical_overrides_are_independent(self) -> None:
        limits = ReplayBudgetLimits()
        expected = {
            "account-a": (0, 100, 30, 70),
            "account-b": (20, 100, 50, 50),
            "account-c": (70, 100, 100, 0),
        }
        for account_label, (extra, total, _historical, _reserve) in expected.items():
            replace_replay_budget_overrides(
                self.db_path,
                account_labels=[account_label],
                extra_historical_limit=extra,
                total_limit_override=total,
                starts_at=self.now,
                expires_at=self.now + timedelta(hours=1),
                created_by="admin",
                reason="formula test",
                limits=limits,
            )

        for account_label, (_extra, total, historical, reserve) in expected.items():
            with self.subTest(account_label=account_label):
                reservation = reserve_replay_request(
                    self.db_path,
                    account_labels=[account_label],
                    bga_table_id=account_label,
                    request_class="historical",
                    limits=limits,
                    now=self.now + timedelta(seconds=1),
                )
                self.assertEqual(reservation.effective_total_limit, total)
                self.assertEqual(
                    reservation.effective_historical_limit,
                    historical,
                )
                self.assertEqual(reservation.effective_fresh_reserve, reserve)

    def test_non_logs_endpoint_is_audited_without_consuming_budget(self) -> None:
        request_id = start_replay_request_audit(
            self.db_path,
            account_label="account-a",
            bga_table_id="101",
            endpoint="/gamereview/gamereview/requestTableArchive.html",
            request_class="manual",
            now=self.now,
        )
        finish_replay_request_audit(
            self.db_path,
            request_id,
            outcome="archive_requested",
        )
        reservation = reserve_replay_request(
            self.db_path,
            account_labels=["account-a"],
            bga_table_id="101",
            request_class="manual",
            limits=ReplayBudgetLimits(
                total_limit=1,
                fresh_reserve=0,
                historical_limit=1,
                max_total_limit=1,
            ),
            now=self.now + timedelta(seconds=1),
        )
        self.assertEqual(reservation.total_used_before, 0)

        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """
                SELECT endpoint, outcome
                FROM bga_replay_requests
                ORDER BY id
                """
            ).fetchall()
        self.assertEqual(
            rows,
            [
                (
                    "/gamereview/gamereview/requestTableArchive.html",
                    "archive_requested",
                ),
                (LOGS_ENDPOINT, "reserved"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
