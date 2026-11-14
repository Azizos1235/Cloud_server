import json
import unittest
from datetime import datetime, timezone

from app.ingest.parsing import daily_update, parse_items


class DailyAppetiteIngestionTests(unittest.TestCase):
    now = datetime(2026, 11, 14, tzinfo=timezone.utc)

    def operation(self, message):
        item = parse_items(json.dumps(message).encode("utf-8"))[0]
        return daily_update(item, self.now)

    def test_appetite_statuses_are_stored_as_received(self):
        appetites = [
            {
                "status": "ok",
                "category": "decreasing",
                "percentages": {
                    "normal": 12.0,
                    "decreasing": 81.5,
                    "increasing": 6.5,
                },
                "alert": True,
                "p_flagged_pct": 88.0,
            },
            {
                "status": "insufficient_history",
                "days_stored": 3,
                "days_needed": 7,
            },
            {
                "status": "unavailable",
                "detail": "model unavailable",
            },
            {
                "category": "normal",
                "percentages": {
                    "normal": 82.5,
                    "decreasing": 12.0,
                    "increasing": 5.5,
                },
                "alert": False,
            },
        ]

        for appetite in appetites:
            with self.subTest(status=appetite.get("status", "unspecified")):
                operation = self.operation({
                    "cow_id": "COW123",
                    "date": "2026-11-14",
                    "appetite": appetite,
                })
                self.assertEqual(
                    operation._doc[0]["$set"]["appetite"],
                    {"$literal": appetite},
                )

    def test_old_message_without_appetite_remains_valid(self):
        operation = self.operation({
            "cow_id": "COW123",
            "date": "2026-11-14",
            "food_consumed_g": 830,
            "feed_offered_g": 8208.5,
        })

        self.assertNotIn("appetite", operation._doc[0]["$set"])
        self.assertEqual(
            operation._doc[0]["$set"]["total_food_consumed_g"]["$max"][-1],
            830.0,
        )
        self.assertEqual(
            operation._doc[0]["$set"]["total_feed_offered_g"]["$max"][-1],
            8208.5,
        )

    def test_empty_message_list_is_not_accepted_as_a_successful_message(self):
        with self.assertRaises(ValueError):
            parse_items(b"[]")

    def test_resending_same_cow_and_date_updates_the_same_upsert(self):
        first_appetite = {"status": "unavailable", "detail": "pending"}
        resent_appetite = {
            "status": "ok",
            "category": "normal",
            "percentages": {
                "normal": 90.0,
                "decreasing": 5.0,
                "increasing": 5.0,
            },
            "alert": False,
        }
        first = self.operation({
            "cow_id": "COW123",
            "date": "2026-11-14",
            "appetite": first_appetite,
        })
        resent = self.operation({
            "cow_id": "COW123",
            "date": "2026-11-14",
            "appetite": resent_appetite,
        })

        self.assertEqual(first._filter, resent._filter)
        self.assertEqual(first._filter, {"_id": "COW123-2026-11-14"})
        self.assertTrue(first._upsert)
        self.assertTrue(resent._upsert)
        self.assertEqual(
            resent._doc[0]["$set"]["appetite"],
            {"$literal": resent_appetite},
        )


if __name__ == "__main__":
    unittest.main()
