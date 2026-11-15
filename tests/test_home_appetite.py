import os
import secrets
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace


os.environ.setdefault("JWT_SECRET", secrets.token_urlsafe(32))

from app.deps import owner_filter
from app.routers.cows import home_appetite


class AsyncCursor:
    def __init__(self, records):
        self.records = records
        self.limit_value = None

    def sort(self, *_args):
        return self

    def limit(self, value):
        self.limit_value = value
        return self

    def __aiter__(self):
        async def records():
            values = self.records
            if self.limit_value is not None:
                values = values[: self.limit_value]
            for record in values:
                yield record

        return records()


class HomeAppetiteEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_latest_classes_are_mapped_and_missing_daily_record_is_null(self):
        profiles = [
            {"_id": "RFID-01", "cow_id": "COW001"},
            {"_id": "RFID-02", "cow_id": "COW002"},
            {"_id": "RFID-03", "cow_id": "COW003"},
        ]
        latest_records = [
            {
                "_id": "RFID-01",
                "rfid": "RFID-01",
                "date": datetime(2026, 11, 15, tzinfo=timezone.utc),
                "appetite_class": "high",
            },
            {
                "_id": "COW003",
                "date": datetime(2026, 11, 14, tzinfo=timezone.utc),
                "appetite_class": "unexpected-label",
            },
        ]
        profile_cursor = AsyncCursor(profiles)
        profile_find_calls = []
        aggregate_calls = []

        class DailyCollection:
            def aggregate(self, pipeline):
                aggregate_calls.append(pipeline)
                return AsyncCursor(latest_records)

        cols = SimpleNamespace(
            profiles=SimpleNamespace(
                find=lambda *args: (
                    profile_find_calls.append(args) or profile_cursor
                ),
            ),
            daily=DailyCollection(),
        )
        request = SimpleNamespace(
            app=SimpleNamespace(state=SimpleNamespace(cols=cols))
        )

        actor = {"admin_id": "admin-1"}
        response = await home_appetite(request, actor)

        self.assertEqual(
            response,
            {
                "appetite": {
                    "RFID-01": {
                        "cow_id": "COW001",
                        "appetite": "increasing",
                        "date": "2026-11-15",
                    },
                    "RFID-02": {
                        "cow_id": "COW002",
                        "appetite": None,
                        "date": None,
                    },
                    "RFID-03": {
                        "cow_id": "COW003",
                        "appetite": None,
                        "date": "2026-11-14",
                    },
                }
            },
        )
        self.assertEqual(len(aggregate_calls), 1)
        self.assertEqual(profile_cursor.limit_value, 501)
        self.assertEqual(profile_find_calls[0][0], owner_filter(actor))
        stages = aggregate_calls[0]
        self.assertLess(
            next(i for i, stage in enumerate(stages) if "$sort" in stage),
            next(i for i, stage in enumerate(stages) if "$group" in stage),
        )

    async def test_stored_rule_and_trend_labels_map_to_contract_classes(self):
        profiles = [{"_id": "RFID-01", "cow_id": "COW001"}]
        cursor = AsyncCursor(profiles)
        results_by_label = {
            "low": "decreasing",
            "decreasing": "decreasing",
            "normal": "stable",
            "stable": "stable",
            "high": "increasing",
            "increasing": "increasing",
            "": None,
        }

        for stored_label, expected in results_by_label.items():
            with self.subTest(stored_label=stored_label):
                cols = SimpleNamespace(
                    profiles=SimpleNamespace(find=lambda *_args: cursor),
                    daily=SimpleNamespace(
                        aggregate=lambda _pipeline: AsyncCursor(
                            [
                                {
                                    "_id": "RFID-01",
                                    "rfid": "RFID-01",
                                    "appetite_class": stored_label,
                                    "date": datetime(2026, 11, 15, tzinfo=timezone.utc),
                                }
                            ]
                        )
                    ),
                )
                request = SimpleNamespace(
                    app=SimpleNamespace(state=SimpleNamespace(cols=cols))
                )

                response = await home_appetite(
                    request,
                    {"admin_id": "admin-1"},
                )

                self.assertEqual(response["appetite"]["RFID-01"]["appetite"], expected)
