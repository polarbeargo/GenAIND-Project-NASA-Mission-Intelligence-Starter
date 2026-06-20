#!/usr/bin/env python3
"""Focused tests for security dashboard threshold alerts."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from monitoring.security_dashboard import EventType, SecurityDashboard, Severity


class TestSecurityDashboardAlerts(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.NamedTemporaryFile(delete=False)
        self.log_file = Path(tmp.name)
        tmp.close()
        self.dashboard = SecurityDashboard(max_events=500, log_file=self.log_file)

    def tearDown(self) -> None:
        self.dashboard.reset()
        self.log_file.unlink(missing_ok=True)

    def test_rate_limit_spike_raises_alert(self):
        threshold = self.dashboard.ALERT_THRESHOLDS["rate_limit_per_hour"]
        for i in range(threshold):
            self.dashboard.log_event(
                event_type=EventType.RATE_LIMIT_EXCEEDED.value,
                severity=Severity.MEDIUM.value,
                user_id=f"user-{i}",
                ip_address="127.0.0.1",
                details={"probe": "rate-limit"},
            )

        alerts = self.dashboard.get_alerts()
        self.assertTrue(any(a.get("type") == "RATE_LIMIT_SPIKE" for a in alerts))


if __name__ == "__main__":
    unittest.main(verbosity=2)
