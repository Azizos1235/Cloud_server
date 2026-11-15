import os
import secrets
import unittest
from datetime import datetime
from types import SimpleNamespace

from bson import ObjectId
from fastapi import FastAPI, HTTPException, Request
from httpx import ASGITransport, AsyncClient

os.environ.setdefault("JWT_SECRET", secrets.token_urlsafe(32))

from app.deps import get_actor
from app.routers.profiles import router
from app.utils import age_months


class FakeProfiles:
    def __init__(self, profiles):
        self.profiles = profiles

    async def find_one(self, query, _projection=None):
        for profile in self.profiles:
            alternatives = query.get("$or")
            identifier_matches = alternatives is None or any(
                profile.get(field) == condition.get(field)
                for condition in alternatives
                for field in ("_id", "cow_id")
                if field in condition
            )
            if not identifier_matches:
                continue
            if "admin_id" in query and profile.get("admin_id") != query["admin_id"]:
                continue
            if "nfc_uid" in query:
                if profile.get("nfc_uid") != query["nfc_uid"]:
                    continue
                excluded_id = query.get("_id", {}).get("$ne")
                if excluded_id is not None and profile.get("_id") == excluded_id:
                    continue
            return profile
        return None

    async def update_one(self, query, update):
        for profile in self.profiles:
            if all(profile.get(key) == value for key, value in query.items()):
                profile.update(update.get("$set", {}))
                for key in update.get("$unset", {}):
                    profile.pop(key, None)
                return SimpleNamespace(matched_count=1)
        return SimpleNamespace(matched_count=0)


class ProfileUpdateAuthorizationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.admin_id = ObjectId("65a000000000000000000001")
        self.other_admin_id = ObjectId("65a000000000000000000002")
        self.eleveur_id = ObjectId("65a000000000000000000003")
        self.profile = {
            "_id": "RFID-002",
            "cow_id": "COW002",
            "admin_id": self.admin_id,
            "birth_date": datetime(2020, 1, 15),
            "age_months": age_months(datetime(2020, 1, 15).date()),
            "breed": "Holstein",
            "type": "dairy",
            "weight_kg": 400.0,
            "nfc_uid": "A1B2C3D4",
            "created_at": datetime(2024, 1, 1),
        }
        profiles = FakeProfiles([self.profile])
        app = FastAPI()
        app.state.cols = SimpleNamespace(profiles=profiles)
        app.include_router(router)

        async def actor_for_test(request: Request):
            token = request.headers.get("authorization")
            if token == "Bearer admin-token":
                return {"id": self.admin_id, "role": "admin", "admin_id": self.admin_id}
            if token == "Bearer eleveur-token":
                return {
                    "id": self.eleveur_id,
                    "role": "eleveur",
                    "admin_id": self.admin_id,
                }
            if token == "Bearer other-eleveur-token":
                return {
                    "id": self.eleveur_id,
                    "role": "eleveur",
                    "admin_id": self.other_admin_id,
                }
            raise HTTPException(401, "Not authenticated")

        app.dependency_overrides[get_actor] = actor_for_test
        self.app = app

    async def _put(self, cow_id="COW002", token="admin-token"):
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        async with AsyncClient(
            transport=ASGITransport(app=self.app), base_url="http://test"
        ) as client:
            return await client.put(
                f"/profiles/{cow_id}",
                headers=headers,
                json={
                    "birth_date": "2021-10-10",
                    "weight_kg": 425.5,
                    "type": "dairy",
                    "breed": "Holstein",
                    "_id": "spoofed-id",
                    "cow_id": "COW999",
                    "admin_id": str(self.other_admin_id),
                    "created_at": "2000-01-01T00:00:00",
                },
            )

    async def test_admin_can_update_own_cow(self):
        response = await self._put()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.profile["weight_kg"], 425.5)
        self.assertEqual(self.profile["age_months"], age_months(datetime(2021, 10, 10).date()))
        self.assertEqual(self.profile["_id"], "RFID-002")
        self.assertEqual(self.profile["cow_id"], "COW002")
        self.assertEqual(self.profile["admin_id"], self.admin_id)
        self.assertEqual(self.profile["created_at"], datetime(2024, 1, 1))
        self.assertEqual(self.profile["nfc_uid"], "A1B2C3D4")

    async def test_eleveur_can_update_cow_of_their_admin(self):
        response = await self._put(token="eleveur-token")

        self.assertEqual(response.status_code, 200)

    async def test_eleveur_of_another_admin_gets_not_found(self):
        response = await self._put(token="other-eleveur-token")

        self.assertEqual(response.status_code, 404)

    async def test_missing_token_gets_unauthorized(self):
        response = await self._put(token="")

        self.assertEqual(response.status_code, 401)

    async def test_unknown_cow_gets_not_found(self):
        response = await self._put(cow_id="COW404")

        self.assertEqual(response.status_code, 404)
