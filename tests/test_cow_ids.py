import os
import secrets
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


os.environ.setdefault("JWT_SECRET", secrets.token_urlsafe(32))

from app.cow_ids import resolve_cow_id
from app.routers.cows import cows, latest_http, predict_http
from app.utils import age_months


class CowIdResolutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_known_rfid_resolves_to_cow_code(self):
        cols = SimpleNamespace(
            profiles=SimpleNamespace(
                find_one=AsyncMock(return_value={"cow_id": "COW001"})
            )
        )

        self.assertEqual(await resolve_cow_id(cols, " 3c43fa39 "), "COW001")
        cols.profiles.find_one.assert_awaited_once_with(
            {"_id": "3C43FA39"}, {"cow_id": 1}
        )

    async def test_profile_created_after_first_lookup_is_not_missed(self):
        profiles = SimpleNamespace(
            find_one=AsyncMock(side_effect=[
                None,
                {"cow_id": "COW008"},
            ])
        )
        cols = SimpleNamespace(profiles=profiles)

        self.assertIsNone(await resolve_cow_id(cols, "AABBCCDD"))
        self.assertEqual(await resolve_cow_id(cols, "AABBCCDD"), "COW008")
        self.assertEqual(profiles.find_one.await_count, 2)

    async def test_predict_accepts_rfid_and_cow_code(self):
        profile = {"_id": "3C43FA39", "cow_id": "COW001"}

        async def find_profile(query, projection=None):
            alternatives = query.get("$or", [])
            if query.get("_id") == "3C43FA39" or any(
                condition.get("_id") == "3C43FA39"
                or condition.get("cow_id") == "COW001"
                for condition in alternatives
            ):
                return profile
            return None

        cols = SimpleNamespace(
            profiles=SimpleNamespace(find_one=AsyncMock(side_effect=find_profile)),
            daily=SimpleNamespace(find_one=AsyncMock(return_value={
                "_id": "3C43FA39-2026-11-14",
                "cow_id": "COW001",
            })),
        )
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(cols=cols)))
        actor = {"admin_id": "admin"}

        for requested_id in ("3C43FA39", "COW001"):
            with self.subTest(requested_id=requested_id):
                with patch(
                    "app.routers.cows.predict",
                    new_callable=AsyncMock,
                    return_value={"prediction": "normal"},
                ) as predict:
                    result = await predict_http(requested_id, request, actor)

                self.assertEqual(result, {"prediction": "normal"})
                predict.assert_awaited_once_with({"cow_id": "COW001"})


class CowEndpointResponseTests(unittest.IsolatedAsyncioTestCase):
    async def test_cows_response_keeps_internal_ids_and_adds_display_ids(self):
        profile = {"_id": "RFID-17", "cow_id": "COW-17"}

        class Cursor:
            def sort(self, *_args):
                return self

            def __aiter__(self):
                async def records():
                    yield profile

                return records()

        cols = SimpleNamespace(profiles=SimpleNamespace(find=lambda *_args: Cursor()))
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(cols=cols)))

        result = await cows(request, {"admin_id": "admin"})

        self.assertEqual(result["cows"], ["RFID-17"])
        self.assertEqual(
            result["cow_profiles"],
            [{"internal_id": "RFID-17", "cow_id": "COW-17"}],
        )

    async def test_latest_adds_age_and_sensor_aliases_for_the_app_model(self):
        birth_date = datetime(2023, 1, 1)
        cols = SimpleNamespace(
            profiles=SimpleNamespace(
                find_one=AsyncMock(return_value={"birth_date": birth_date})
            ),
            daily=SimpleNamespace(),
        )
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(cols=cols)))
        daily = {
            "cow_id": None,
            "total_food_consumed_g": 12000,
            "total_feed_offered_g": 15000,
            "total_eating_minutes": 24,
            "visits": 3,
            "no_food_visits": 1,
            "avg_meal_duration_min": 8,
        }

        with (
            patch("app.routers.cows.owned_cow", new_callable=AsyncMock, return_value="COW-17"),
            patch("app.routers.cows.cow_data_filter", new_callable=AsyncMock, return_value={}),
            patch("app.routers.cows.latest", new_callable=AsyncMock, return_value=daily),
        ):
            result = await latest_http("RFID-17", request, {"admin_id": "admin"})

        self.assertEqual(result["cow_id"], "COW-17")
        self.assertEqual(result["age_months"], age_months(birth_date.date()))
        self.assertEqual(result["food_consumed_g"], 12000)
        self.assertEqual(result["feed_offered_g"], 15000)
        self.assertEqual(result["eating_minutes"], 24)
