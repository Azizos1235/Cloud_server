import os
import re
import secrets
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace


os.environ.setdefault("JWT_SECRET", secrets.token_urlsafe(32))

from app.deps import owner_filter
from app.routers.cows import home_appetite, home_appetite_debug


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


def _normalized_cow_id(value):
    return re.sub(r"[\s_-]+", "", str(value)).casefold()


def _has_appetite(record):
    if isinstance(record.get("appetite_class"), str):
        if record["appetite_class"].strip():
            return True
    appetite = record.get("appetite")
    if isinstance(appetite, dict):
        if any(
            isinstance(appetite.get(field), str) and appetite[field].strip()
            for field in ("category", "status")
        ):
            return True
        if "percentages" in appetite:
            return True
    return "appetite_pct" in record


class FakeDailyCollection:
    def __init__(self, records):
        self.records = records
        self.pipelines = []

    def aggregate(self, pipeline):
        self.pipelines.append(pipeline)
        match = pipeline[0]["$match"]
        identifiers = match["$expr"]["$in"][1]
        require_appetite = "$or" in match
        records = [
            record
            for record in self.records
            if _normalized_cow_id(record.get("cow_id")) in identifiers
            and (not require_appetite or _has_appetite(record))
        ]
        records.sort(key=lambda record: record["date"], reverse=True)

        latest_by_cow = {}
        for record in records:
            identifier = _normalized_cow_id(record["cow_id"])
            latest_by_cow.setdefault(identifier, record)

        grouped = [
            {"_id": identifier, "doc": record}
            for identifier, record in latest_by_cow.items()
        ]
        grouped.sort(key=lambda item: item["doc"]["date"], reverse=True)
        return AsyncCursor(grouped)


class HomeAppetiteEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_latest_record_with_appetite_and_all_supported_sources(self):
        profiles = [
            {"_id": "RFID-01", "cow_id": "COW001"},
            {"_id": "RFID-02", "cow_id": "COW002"},
            {"_id": "RFID-03", "cow_id": "COW003"},
            {"_id": "RFID-04", "cow_id": "COW004"},
            {"_id": "RFID-05", "cow_id": "COW005"},
        ]
        daily = FakeDailyCollection(
            [
                {
                    "_id": "newest-COW001",
                    "cow_id": "cow 001",
                    "date": datetime(2026, 11, 15, tzinfo=timezone.utc),
                },
                {
                    "_id": "older-COW001",
                    "cow_id": "COW-001",
                    "date": datetime(2026, 11, 14, tzinfo=timezone.utc),
                    "appetite_class": " low ",
                },
                {
                    "_id": "nested-COW002",
                    "cow_id": "COW_002",
                    "date": datetime(2026, 11, 14, tzinfo=timezone.utc),
                    "appetite": {
                        "category": "Decreased",
                        "status": "increased",
                        "percentages": {"increased": 99},
                    },
                },
                {
                    "_id": "pct-COW003",
                    "cow_id": "cow-003",
                    "date": datetime(2026, 11, 13, tzinfo=timezone.utc),
                    "appetite_pct": {"high": 70},
                },
                {
                    "_id": "percentages-COW004",
                    "cow_id": "COW004",
                    "date": datetime(2026, 11, 12, tzinfo=timezone.utc),
                    "appetite": {"percentages": {"increased": 80}},
                },
            ]
        )
        profile_cursor = AsyncCursor(profiles)
        profile_find_calls = []
        cols = SimpleNamespace(
            profiles=SimpleNamespace(
                find=lambda *args: (
                    profile_find_calls.append(args) or profile_cursor
                )
            ),
            daily=daily,
        )
        request = SimpleNamespace(
            app=SimpleNamespace(state=SimpleNamespace(cols=cols))
        )
        actor = {"admin_id": "admin-1"}

        response = await home_appetite(request, actor)

        result = response["appetite"]
        self.assertEqual(
            result["RFID-01"],
            {
                "cow_id": "COW001",
                "appetite": "decreasing",
                "source": "appetite_class",
                "raw": " low ",
                "date": "2026-11-14",
            },
        )
        self.assertEqual(result["RFID-02"]["appetite"], "decreasing")
        self.assertEqual(result["RFID-02"]["source"], "appetite.category")
        self.assertEqual(result["RFID-02"]["raw"], "Decreased")
        self.assertEqual(result["RFID-03"]["appetite"], "increasing")
        self.assertEqual(result["RFID-03"]["source"], "appetite_pct")
        self.assertEqual(result["RFID-03"]["raw"], {"high": 70})
        self.assertEqual(result["RFID-04"]["appetite"], "increasing")
        self.assertEqual(result["RFID-04"]["source"], "appetite.percentages")
        self.assertEqual(result["RFID-04"]["raw"], {"increased": 80})
        self.assertEqual(
            result["RFID-05"],
            {
                "cow_id": "COW005",
                "appetite": None,
                "source": None,
                "raw": None,
                "date": None,
            },
        )
        self.assertEqual(profile_find_calls[0][0], owner_filter(actor))
        pipeline = daily.pipelines[0]
        self.assertIn("$or", pipeline[0]["$match"])
        self.assertEqual(pipeline[1], {"$sort": {"date": -1}})
        self.assertIn("$first", pipeline[2]["$group"]["doc"])

    async def test_status_and_substring_labels_map_case_insensitively(self):
        profiles = [{"_id": "RFID-01", "cow_id": "COW001"}]
        cases = [
            ("appetite.status", "  Stable status  ", "stable"),
            ("appetite_class", "decrease", "decreasing"),
            ("appetite_class", "increased", "increasing"),
            ("appetite_class", "normal appetite", "stable"),
            ("appetite_class", "unrecognized", None),
        ]

        for source, label, expected in cases:
            with self.subTest(source=source, label=label):
                record = {
                    "_id": f"daily-{label}",
                    "cow_id": "COW-001",
                    "date": datetime(2026, 11, 15, tzinfo=timezone.utc),
                }
                if source == "appetite.status":
                    record["appetite"] = {"status": label}
                else:
                    record = {
                        **record,
                        "appetite_class": label,
                    }
                cols = SimpleNamespace(
                    profiles=SimpleNamespace(
                        find=lambda *_args: AsyncCursor(profiles)
                    ),
                    daily=FakeDailyCollection([record]),
                )
                request = SimpleNamespace(
                    app=SimpleNamespace(state=SimpleNamespace(cols=cols))
                )

                response = await home_appetite(
                    request,
                    {"admin_id": "admin-1"},
                )

                result = response["appetite"]["RFID-01"]
                self.assertEqual(result["appetite"], expected)
                self.assertEqual(result["raw"], label)
                self.assertEqual(result["source"], source)

    async def test_debug_endpoint_limits_fields_and_reports_raw_label_counts(self):
        profile_cursor = AsyncCursor(
            [{"_id": "RFID-01", "cow_id": "COW001", "admin_id": "secret"}]
        )
        date = datetime(2026, 11, 15, tzinfo=timezone.utc)
        daily = SimpleNamespace(
            responses=[
                AsyncCursor(
                    [
                        {
                            "_id": "cow001",
                            "doc": {
                                "_id": "RFID-01-2026-11-15",
                                "cow_id": "COW001",
                                "date": date,
                                "appetite_class": "Low",
                                "appetite_pct": {"low": 60},
                                "appetite_computed_at": date,
                                "admin_id": "secret",
                            },
                        }
                    ]
                ),
                AsyncCursor(
                    [
                        {
                            "_id": "cow001",
                            "doc": {
                                "_id": "RFID-01-2026-11-15",
                                "cow_id": "COW001",
                                "date": date,
                                "appetite_class": "Low",
                                "appetite_pct": {"low": 60},
                                "appetite_computed_at": date,
                                "admin_id": "secret",
                            },
                        }
                    ]
                ),
                AsyncCursor(
                    [
                        {
                            "total": [{"count": 1}],
                            "labels": [{"raw": "Low", "count": 1}],
                        }
                    ]
                ),
            ],
            aggregate=lambda _pipeline: daily.responses.pop(0),
        )
        cols = SimpleNamespace(
            profiles=SimpleNamespace(find=lambda *_args: profile_cursor),
            daily=daily,
        )
        request = SimpleNamespace(
            app=SimpleNamespace(state=SimpleNamespace(cols=cols))
        )

        response = await home_appetite_debug(request, {"admin_id": "admin-1"})

        self.assertEqual(response["total_cows"], 1)
        self.assertEqual(response["total_daily_docs_with_appetite_class"], 1)
        self.assertEqual(response["top_raw_labels"], [{"raw": "Low", "count": 1}])
        cow = response["cows"][0]
        self.assertEqual(cow["profile"], {"_id": "RFID-01", "cow_id": "COW001"})
        self.assertEqual(cow["newest_daily"]["date"], "2026-11-15")
        self.assertEqual(cow["chosen_doc"]["source"], "appetite_class")
        self.assertEqual(cow["chosen_doc"]["raw"], "Low")
        self.assertNotIn("admin_id", repr(response))
