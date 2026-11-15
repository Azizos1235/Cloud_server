import json
import os
import re
import secrets
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from bson import ObjectId

os.environ.setdefault("JWT_SECRET", secrets.token_urlsafe(32))

from app.deps import owner_filter
from app.routers.cows import home_appetite, home_appetite_debug, router


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
    return re.sub(r"[-_]", "", str(value).strip()).upper()


def _pipeline_strings(value):
    if isinstance(value, dict):
        for key, item in value.items():
            yield key
            yield from _pipeline_strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from _pipeline_strings(item)
    elif isinstance(value, str):
        yield value


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

    async def aggregate(self, pipeline):
        self.pipelines.append(pipeline)
        match = pipeline[0]["$match"]
        conditions = match["$and"]
        identifier_match = conditions[0]["$or"]
        variants = identifier_match[0]["cow_id"]["$in"]
        if "$facet" in pipeline[1]:
            matched = [
                record
                for record in self.records
                if record.get("cow_id") in variants or record.get("rfid") in variants
            ]
            labels = {}
            for record in matched:
                label = record.get("appetite_class")
                if isinstance(label, str) and label.strip():
                    labels[label] = labels.get(label, 0) + 1
            stats = {
                "total": (
                    [{"count": sum("appetite_class" in item for item in matched)}]
                    if any("appetite_class" in item for item in matched)
                    else []
                ),
                "labels": [
                    {"raw": label, "count": count}
                    for label, count in sorted(
                        labels.items(), key=lambda item: (-item[1], item[0])
                    )[:10]
                ],
            }
            return AsyncCursor([stats])

        require_appetite = len(conditions) > 1
        records = [
            record
            for record in self.records
            if (
                record.get("cow_id") in variants
                or record.get("rfid") in variants
            )
            and (not require_appetite or _has_appetite(record))
        ]
        records.sort(key=lambda record: record["date"], reverse=True)

        latest_by_identifiers = {}
        for record in records:
            identifier_pair = (record.get("cow_id"), record.get("rfid"))
            latest_by_identifiers.setdefault(identifier_pair, record)

        grouped = [
            {
                "_id": {"cow": identifier_pair[0], "rfid": identifier_pair[1]},
                "doc": record,
            }
            for identifier_pair, record in latest_by_identifiers.items()
        ]
        return AsyncCursor(grouped)


class HomeAppetiteEndpointTests(unittest.IsolatedAsyncioTestCase):
    def test_debug_route_is_registered_once(self):
        routes = [
            route
            for route in router.routes
            if getattr(route, "path", None) == "/home/appetite/debug"
        ]

        self.assertEqual(len(routes), 1)

    async def test_admin_and_eleveur_match_profile_aliases_and_return_json_safe_data(self):
        profile_id = ObjectId()
        profile = {
            "_id": profile_id,
            "cow_id": "COW001",
            "rfid": "RFID-01",
            "nfc_uid": "NFC-0001",
        }
        profile_with_object_id_raw = {
            "_id": ObjectId(),
            "cow_id": "COW002",
            "rfid": "RFID-02",
            "nfc_uid": "NFC-0002",
        }
        newest_without_appetite = {
            "_id": ObjectId(),
            "cow_id": "COW-001",
            "date": datetime(2026, 11, 16, tzinfo=timezone.utc),
        }
        older_with_appetite = {
            "_id": ObjectId(),
            "cow_id": "RFID-01",
            "rfid": "RFID01",
            "date": datetime(2026, 11, 15, tzinfo=timezone.utc),
            "appetite_class": "low",
            "appetite_pct": {"low": float("nan")},
            "appetite_computed_at": datetime(2026, 11, 15, tzinfo=timezone.utc),
            "appetite": {"category": "low", "source_id": ObjectId()},
        }
        percentage_with_object_id = {
            "_id": ObjectId(),
            "cow_id": "COW002",
            "rfid": "RFID-02",
            "date": datetime(2026, 11, 17, tzinfo=timezone.utc),
            "appetite_pct": {"high": 70, "metadata": ObjectId()},
        }
        percentage_metadata_id = percentage_with_object_id["appetite_pct"]["metadata"]

        for role in ("admin", "eleveur"):
            with self.subTest(role=role):
                daily = FakeDailyCollection(
                    [
                        newest_without_appetite,
                        older_with_appetite,
                        percentage_with_object_id,
                    ]
                )
                cols = SimpleNamespace(
                    profiles=SimpleNamespace(
                        find=lambda *_args: AsyncCursor(
                            [profile, profile_with_object_id_raw]
                        )
                    ),
                    daily=daily,
                )
                request = SimpleNamespace(
                    app=SimpleNamespace(state=SimpleNamespace(cols=cols))
                )
                actor = {"role": role, "admin_id": ObjectId()}

                response = await home_appetite(request, actor)
                self.assertEqual(
                    response["appetite"][str(profile_id)],
                    {
                        "cow_id": "COW001",
                        "appetite": "decreasing",
                        "source": "appetite_class",
                        "raw": "low",
                        "date": "2026-11-15",
                    },
                )
                self.assertEqual(
                    response["appetite"][str(profile_with_object_id_raw["_id"])][
                        "raw"
                    ],
                    {
                        "high": 70,
                        "metadata": str(percentage_metadata_id),
                    },
                )
                json.dumps(response, allow_nan=False)
                debug = await home_appetite_debug(request, actor)
                self.assertEqual(debug["cows"][0]["profile"]["cow_id"], "COW001")
                self.assertEqual(
                    debug["cows"][0]["chosen_doc"]["date"], "2026-11-15"
                )
                serialized = json.dumps(debug, allow_nan=False)
                self.assertIn(
                    str(older_with_appetite["appetite"]["source_id"]), serialized
                )
                self.assertIn('"low": null', serialized)
                for pipeline in daily.pipelines:
                    forbidden = {
                        "$regexReplace",
                        "$replaceAll",
                        "$regex",
                        "$toLower",
                        "$toUpper",
                    }
                    self.assertFalse(
                        any(
                            token in value
                            for value in _pipeline_strings(pipeline)
                            for token in forbidden
                        ),
                        msg=f"Unsupported string operator in pipeline: {pipeline!r}",
                    )

    async def test_cow_and_rfid_spellings_match_and_latest_date_wins(self):
        profile = {"_id": "RFID-01", "cow_id": "COW001", "rfid": "RFID-01"}
        daily = FakeDailyCollection(
            [
                {
                    "_id": "dash-cow-id",
                    "cow_id": "COW-001",
                    "date": datetime(2026, 11, 16, tzinfo=timezone.utc),
                    "appetite_class": "low",
                },
                {
                    "_id": "lower-cow-id",
                    "cow_id": "cow001",
                    "date": datetime(2026, 11, 17, tzinfo=timezone.utc),
                    "appetite_class": "normal",
                },
                {
                    "_id": "rfid-spelling",
                    "cow_id": "legacy-id",
                    "rfid": "rfid_01",
                    "date": datetime(2026, 11, 18, tzinfo=timezone.utc),
                    "appetite_class": "high",
                },
            ]
        )
        request = SimpleNamespace(
            app=SimpleNamespace(
                state=SimpleNamespace(
                    cols=SimpleNamespace(
                        profiles=SimpleNamespace(
                            find=lambda *_args: AsyncCursor([profile])
                        ),
                        daily=daily,
                    )
                )
            )
        )

        response = await home_appetite(request, {"admin_id": "admin-1"})

        self.assertEqual(
            response["appetite"]["RFID-01"],
            {
                "cow_id": "COW001",
                "appetite": "increasing",
                "source": "appetite_class",
                "raw": "high",
                "date": "2026-11-18",
            },
        )
        variants = daily.pipelines[0][0]["$match"]["$and"][0]["$or"][0]["cow_id"][
            "$in"
        ]
        self.assertTrue({"COW001", "COW-001", "COW_001", "cow001"} <= set(variants))
        self.assertTrue({"RFID-01", "RFID01", "RFID_01", "rfid_01"} <= set(variants))

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
                    "cow_id": "cow001",
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
        self.assertIn("$and", pipeline[0]["$match"])
        self.assertEqual(len(pipeline), 3)
        self.assertEqual(pipeline[1], {"$sort": {"date": -1}})
        self.assertEqual(
            pipeline[2]["$group"]["_id"],
            {"cow": "$cow_id", "rfid": "$rfid"},
        )
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
        )

        async def aggregate(_pipeline):
            return daily.responses.pop(0)

        daily.aggregate = aggregate
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

    async def test_debug_endpoint_returns_temporary_traceback_details(self):
        async def fail_aggregate(_pipeline):
            raise RuntimeError("fake aggregation failure")

        request = SimpleNamespace(
            app=SimpleNamespace(
                state=SimpleNamespace(
                    cols=SimpleNamespace(
                        profiles=SimpleNamespace(
                            find=lambda *_args: AsyncCursor(
                                [{"_id": "RFID-01", "cow_id": "COW001"}]
                            )
                        ),
                        daily=SimpleNamespace(aggregate=fail_aggregate),
                    )
                )
            )
        )

        response = await home_appetite_debug(
            request, {"role": "admin", "admin_id": "admin-1"}
        )

        self.assertEqual(response["error"], "RuntimeError")
        self.assertEqual(response["message"], "fake aggregation failure")
        self.assertLessEqual(len(response["traceback"]), 10)
