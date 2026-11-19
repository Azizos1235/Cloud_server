import os
import secrets
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

os.environ.setdefault("JWT_SECRET", secrets.token_urlsafe(32))

from app.services.alerts import appetite_alert


class DailyCursor:
    def __init__(self, records):
        self.records = records

    def sort(self, *_args):
        self.records.sort(key=lambda item: item["date"], reverse=True)
        return self

    def limit(self, count):
        self.records = self.records[:count]
        return self

    def __aiter__(self):
        async def records():
            for record in self.records:
                yield record

        return records()


class AppetiteStreakTests(unittest.IsolatedAsyncioTestCase):
    day = datetime(2026, 11, 19, tzinfo=timezone.utc)

    def setup_collections(self, labels, day_offsets=None):
        offsets = day_offsets or [0, 1, 2]
        records = [
            {
                "cow_id": "COW-1",
                "date": self.day - timedelta(days=offset),
                "appetite_class": label,
                "appetite_pct": {label: 80.0},
            }
            for label, offset in zip(labels, offsets)
        ]
        alerts = []

        class AlertCollection:
            async def find_one(self, query, *_args):
                return next(
                    (alert for alert in alerts if alert.get("dedupe_key") == query["dedupe_key"]),
                    None,
                )

            async def insert_one(self, alert):
                alerts.append(alert)

        cols = SimpleNamespace(
            daily=SimpleNamespace(
                find=lambda *_args: DailyCursor(list(records)),
            ),
            profiles=SimpleNamespace(
                find_one=lambda *_args: _profile(),
            ),
            alerts=AlertCollection(),
        )
        return cols, alerts

    async def test_three_consecutive_high_days_create_alert(self):
        cols, alerts = self.setup_collections(["high", "high", "high"])

        await appetite_alert(cols, "COW-1", self.day.date(), "high", {"high": 80})

        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["type"], "appetite_high")
        self.assertEqual(alerts[0]["dedupe_key"], "appetite:COW-1:high:2026-11-19")
        self.assertIsNone(alerts[0]["task_id"])
        self.assertFalse(alerts[0]["handled"])

    async def test_three_consecutive_low_days_create_alert(self):
        cols, alerts = self.setup_collections(["low", "poor", "faible"])

        await appetite_alert(cols, "COW-1", self.day.date())

        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["type"], "appetite_low")
        self.assertEqual(alerts[0]["appetite_class"], "low")

    async def test_mixed_classes_do_not_create_alert(self):
        cols, alerts = self.setup_collections(["high", "normal", "high"])

        await appetite_alert(cols, "COW-1", self.day.date())

        self.assertEqual(alerts, [])

    async def test_missing_calendar_day_breaks_streak(self):
        cols, alerts = self.setup_collections(
            ["high", "high", "high"],
            day_offsets=[0, 2, 3],
        )

        await appetite_alert(cols, "COW-1", self.day.date())

        self.assertEqual(alerts, [])

    async def test_repeated_run_does_not_duplicate_alert(self):
        cols, alerts = self.setup_collections(["high", "high", "high"])

        await appetite_alert(cols, "COW-1", self.day.date())
        await appetite_alert(cols, "COW-1", self.day.date())

        self.assertEqual(len(alerts), 1)


async def _profile():
    return {"_id": "COW-1", "cow_id": "COW-1", "admin_id": "admin-1"}


if __name__ == "__main__":
    unittest.main()
