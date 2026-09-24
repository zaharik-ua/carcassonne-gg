from __future__ import annotations

import unittest
from pathlib import Path


SYSTEMD_DIR = Path(__file__).resolve().parents[1] / "systemd"


class ReplayWorkerSystemdTest(unittest.TestCase):
    def test_template_service_uses_worker_mode_and_fixed_run_limit(self) -> None:
        unit = (SYSTEMD_DIR / "bga-replay-worker@.service").read_text()

        self.assertIn("Type=oneshot", unit)
        self.assertIn("retry_pending_game_replays.py", unit)
        self.assertIn("--queue-class %i --limit 3", unit)
        self.assertIn("data/auth.sqlite", unit)
        self.assertIn("bga-replay-worker.log", unit)

    def test_fresh_and_historical_timers_are_independent(self) -> None:
        fresh = (SYSTEMD_DIR / "bga-replay-fresh.timer").read_text()
        historical = (SYSTEMD_DIR / "bga-replay-historical.timer").read_text()

        self.assertIn("OnUnitInactiveSec=2min", fresh)
        self.assertIn("Unit=bga-replay-worker@fresh.service", fresh)
        self.assertIn("OnUnitInactiveSec=30min", historical)
        self.assertIn("Unit=bga-replay-worker@historical.service", historical)
        self.assertIn("WantedBy=timers.target", fresh)
        self.assertIn("WantedBy=timers.target", historical)


if __name__ == "__main__":
    unittest.main()

