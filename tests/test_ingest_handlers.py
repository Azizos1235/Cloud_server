import json
import os
import secrets
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from pymongo.errors import BulkWriteError, WriteError


def setUpModule():
    os.environ.setdefault("JWT_SECRET", secrets.token_urlsafe(32))
    global handle_daily, handle_realtime
    from app.ingest.handlers import handle_daily, handle_realtime


class IngestHandlerTests(unittest.IsolatedAsyncioTestCase):
    def make_message(self, body):
        return SimpleNamespace(
            body=json.dumps(body).encode("utf-8"),
            ack=AsyncMock(),
            nack=AsyncMock(),
            reject=AsyncMock(),
        )

    def make_collection(self, name, bulk_write):
        return SimpleNamespace(name=name, bulk_write=bulk_write)

    def make_profiles(self, profile=None):
        return SimpleNamespace(find_one=AsyncMock(return_value=profile or {
            "_id": "COW123",
            "cow_id": "COW001",
        }))

    async def test_daily_ack_follows_successful_upsert_and_logs_identity(self):
        events = []

        async def write(operations, ordered):
            events.append("write")
            return SimpleNamespace(upserted_ids={0: "COW123-2026-11-14"})

        async def acknowledge():
            events.append("ack")

        message = self.make_message({
            "cow_id": "COW123",
            "date": "2026-11-14",
            "food_consumed_g": 830,
            "appetite": {"status": "unavailable", "detail": "test"},
        })
        message.ack.side_effect = acknowledge
        cols = SimpleNamespace(
            daily=self.make_collection("daily_summaries", AsyncMock(side_effect=write)),
            profiles=self.make_profiles(),
        )

        with self.assertLogs("ingest", level="INFO") as logs:
            await handle_daily(message, cols)

        self.assertEqual(events, ["write", "ack"])
        self.assertTrue(any("queue=cow.daily cow_id=COW123 date=2026-11-14" in line
                            for line in logs.output))
        self.assertTrue(any("inserted daily_summaries id=COW123-2026-11-14" in line
                            for line in logs.output))
        message.nack.assert_not_awaited()
        message.reject.assert_not_awaited()

    async def test_realtime_ack_follows_write_and_keeps_timestamp_id(self):
        events = []

        async def write(operations, ordered):
            events.append("write")
            self.assertEqual(operations[0]._filter["_id"], "COW123|2026-11-14T21:20:01.383+00:00")
            stored = operations[0]._doc["$setOnInsert"]
            self.assertEqual(stored["visits"], 1)
            self.assertEqual(stored["food_consumed_g"], 830.5)
            return SimpleNamespace(upserted_ids={0: "realtime-id"})

        async def acknowledge():
            events.append("ack")

        message = self.make_message({
            "cow_id": "COW123",
            "date": "2026-11-14",
            "received_at": "2026-11-14T21:20:01.383Z",
            "visits": 1,
            "food_consumed_g": 830.5,
        })
        message.ack.side_effect = acknowledge
        cols = SimpleNamespace(
            realtime=self.make_collection("realtime_readings", AsyncMock(side_effect=write)),
            profiles=self.make_profiles(),
        )

        with patch("app.ingest.handlers.hub.broadcast", new_callable=AsyncMock):
            with self.assertLogs("ingest", level="INFO") as logs:
                await handle_realtime(message, cols)

        self.assertEqual(events, ["write", "ack"])
        self.assertTrue(any("queue=cow.realtime cow_id=COW123 date=2026-11-14" in line
                            for line in logs.output))
        self.assertTrue(any("inserted realtime_readings id=COW123|2026-11-14T21:20:01.383+00:00"
                            in line for line in logs.output))
        message.nack.assert_not_awaited()
        message.reject.assert_not_awaited()

    async def test_document_validation_error_is_logged_and_rejected_without_requeue(self):
        message = self.make_message({
            "cow_id": "COW123",
            "date": "2026-11-14",
        })
        cols = SimpleNamespace(
            daily=self.make_collection(
                "daily_summaries",
                AsyncMock(side_effect=WriteError("validation failed", code=121)),
            ),
            profiles=self.make_profiles(),
        )

        with self.assertLogs("ingest", level="ERROR") as logs:
            await handle_daily(message, cols)

        self.assertTrue(any("MongoDB document validation failed" in line for line in logs.output))
        self.assertTrue(any("Traceback" in line for line in logs.output))
        message.reject.assert_awaited_once_with(requeue=False)
        message.ack.assert_not_awaited()
        message.nack.assert_not_awaited()

    async def test_bulk_document_validation_error_is_rejected_without_requeue(self):
        message = self.make_message({
            "cow_id": "COW123",
            "date": "2026-11-14",
        })
        bulk_error = BulkWriteError({
            "writeErrors": [{"code": 121, "errmsg": "validation failed"}],
            "nInserted": 0,
            "nMatched": 0,
            "nModified": 0,
            "nRemoved": 0,
            "nUpserted": 0,
        })
        cols = SimpleNamespace(
            daily=self.make_collection("daily_summaries", AsyncMock(side_effect=bulk_error)),
            profiles=self.make_profiles(),
        )

        with self.assertLogs("ingest", level="ERROR") as logs:
            await handle_daily(message, cols)

        self.assertTrue(any("MongoDB document validation failed" in line for line in logs.output))
        message.reject.assert_awaited_once_with(requeue=False)
        message.ack.assert_not_awaited()
        message.nack.assert_not_awaited()

    async def test_transient_database_error_is_logged_and_nacked_for_retry(self):
        message = self.make_message({
            "cow_id": "COW123",
            "date": "2026-11-14",
        })
        cols = SimpleNamespace(
            daily=self.make_collection(
                "daily_summaries",
                AsyncMock(side_effect=RuntimeError("database unavailable")),
            ),
            profiles=self.make_profiles(),
        )

        with patch("app.ingest.handlers.asyncio.sleep", new_callable=AsyncMock):
            with self.assertLogs("ingest", level="ERROR") as logs:
                await handle_daily(message, cols)

        self.assertTrue(any("Failed to process message queue=cow.daily" in line
                            for line in logs.output))
        self.assertTrue(any("Traceback" in line for line in logs.output))
        message.nack.assert_awaited_once_with(requeue=True)
        message.ack.assert_not_awaited()
        message.reject.assert_not_awaited()

    async def test_malformed_json_is_logged_and_rejected(self):
        message = self.make_message({})
        message.body = b"{not-json"
        cols = SimpleNamespace(
            daily=self.make_collection("daily_summaries", AsyncMock()),
            profiles=self.make_profiles(),
        )

        with self.assertLogs("ingest", level="ERROR") as logs:
            await handle_daily(message, cols)

        self.assertTrue(any("Invalid message queue=cow.daily" in line for line in logs.output))
        self.assertTrue(any("Traceback" in line for line in logs.output))
        message.reject.assert_awaited_once_with(requeue=False)
        message.ack.assert_not_awaited()
        cols.daily.bulk_write.assert_not_awaited()

    async def test_empty_json_list_is_rejected_without_a_database_write(self):
        message = self.make_message({})
        message.body = b"[]"
        cols = SimpleNamespace(
            daily=self.make_collection("daily_summaries", AsyncMock()),
            profiles=self.make_profiles(),
        )

        with self.assertLogs("ingest", level="ERROR"):
            await handle_daily(message, cols)

        message.reject.assert_awaited_once_with(requeue=False)
        message.ack.assert_not_awaited()
        cols.daily.bulk_write.assert_not_awaited()

    async def test_invalid_realtime_timestamp_is_logged_and_rejected(self):
        message = self.make_message({
            "cow_id": "COW123",
            "date": "2026-11-14",
            "received_at": "not-a-timestamp",
        })
        cols = SimpleNamespace(
            realtime=self.make_collection("realtime_readings", AsyncMock()),
            profiles=self.make_profiles(),
        )

        with self.assertLogs("ingest", level="ERROR") as logs:
            await handle_realtime(message, cols)

        self.assertTrue(any("Invalid message queue=cow.realtime" in line for line in logs.output))
        self.assertTrue(any("Traceback" in line for line in logs.output))
        message.reject.assert_awaited_once_with(requeue=False)
        message.ack.assert_not_awaited()
        cols.realtime.bulk_write.assert_not_awaited()

    async def test_unresolved_daily_rfid_is_saved_with_null_cow_id(self):
        operations = []

        async def write(ops, ordered):
            operations.extend(ops)
            return SimpleNamespace(upserted_ids={0: "99887766-2026-11-14"})

        message = self.make_message({
            "cow_id": " 99887766 ",
            "date": "2026-11-14",
        })
        cols = SimpleNamespace(
            daily=self.make_collection("daily_summaries", AsyncMock(side_effect=write)),
            profiles=SimpleNamespace(find_one=AsyncMock(return_value=None)),
        )

        with self.assertLogs("ingest", level="WARNING") as logs:
            await handle_daily(message, cols)

        update = operations[0]._doc[0]["$set"]
        self.assertEqual(operations[0]._filter, {"_id": "99887766-2026-11-14"})
        self.assertIsNone(update["cow_id"])
        self.assertEqual(update["rfid"], "99887766")
        self.assertTrue(any("No cow profile for RFID 99887766" in line for line in logs.output))
        message.ack.assert_awaited_once()
        message.nack.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
