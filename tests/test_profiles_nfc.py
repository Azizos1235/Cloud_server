import os
import secrets
import unittest
from datetime import datetime
from types import SimpleNamespace

from bson import ObjectId
from fastapi import FastAPI, HTTPException, Request
from httpx import ASGITransport, AsyncClient
from pymongo.errors import DuplicateKeyError

os.environ.setdefault("JWT_SECRET", secrets.token_urlsafe(32))

from app.deps import get_actor
from app.routers.profiles import router


class FakeProfiles:
    def __init__(self, profiles=(), *, duplicate_on_insert=False):
        self.profiles = list(profiles)
        self.duplicate_on_insert = duplicate_on_insert

    async def find_one(self, query, _projection=None):
        for profile in self.profiles:
            if "admin_id" in query and profile.get("admin_id") != query["admin_id"]:
                continue
            alternatives = query.get("$or")
            if alternatives is not None:
                if not any(
                    profile.get(field) == value
                    for alternative in alternatives
                    for field, value in alternative.items()
                ):
                    continue
            elif any(profile.get(field) != value for field, value in query.items()):
                continue
            return profile
        return None

    async def insert_one(self, profile):
        if self.duplicate_on_insert:
            raise DuplicateKeyError("duplicate key _id_")
        self.profiles.append(profile)


class ProfileNfcTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.admin_id = ObjectId("65a000000000000000000001")
        self.other_admin_id = ObjectId("65a000000000000000000002")
        self.eleveur_id = ObjectId("65a000000000000000000003")
        self.owned_profile = {
            "_id": "RFID-001",
            "cow_id": "COW001",
            "admin_id": self.admin_id,
            "birth_date": datetime(2023, 1, 1),
            "breed": "Holstein",
            "type": "dairy",
            "weight_kg": 400.0,
            "nfc_uid": "A1B2C3D4",
        }
        self.other_profile = {
            **self.owned_profile,
            "_id": "RFID-002",
            "cow_id": "COW002",
            "admin_id": self.other_admin_id,
            "nfc_uid": "11223344",
        }
        self.profiles = FakeProfiles([self.owned_profile, self.other_profile])
        app = FastAPI()
        app.state.cols = SimpleNamespace(profiles=self.profiles)

        async def actor_for_test(request: Request):
            token = request.headers.get("authorization")
            if token == "Bearer admin-token":
                return {
                    "id": self.admin_id,
                    "role": "admin",
                    "admin_id": self.admin_id,
                }
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
        app.include_router(router)
        self.app = app

    async def _request(self, method, path, *, token="admin-token", json=None):
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        async with AsyncClient(
            transport=ASGITransport(app=self.app), base_url="http://test"
        ) as client:
            return await client.request(method, path, headers=headers, json=json)

    async def test_admin_finds_cow_by_nfc_uid(self):
        response = await self._request("GET", "/profiles/by-nfc/a1:b2:c3:d4")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["cow_id"], "COW001")

    async def test_lookup_matches_uid_stored_as_internal_id(self):
        profile = {
            **self.owned_profile,
            "_id": "11223344",
            "cow_id": "COW003",
            "nfc_uid": None,
        }
        self.profiles.profiles.append(profile)

        response = await self._request("GET", "/profiles/by-nfc/11223344")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["cow_id"], "COW003")

    async def test_linked_eleveur_finds_admin_cow(self):
        response = await self._request(
            "GET", "/profiles/by-nfc/A1B2C3D4", token="eleveur-token"
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["cow_id"], "COW001")

    async def test_other_admin_eleveur_gets_generic_conflict_without_profile(self):
        response = await self._request(
            "GET", "/profiles/by-nfc/A1B2C3D4", token="other-eleveur-token"
        )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json(), {"detail": "Tag already registered elsewhere"})
        self.assertNotIn("cow_id", response.json())
        self.assertNotIn("internal_id", response.json())

    async def test_unknown_tag_returns_not_found(self):
        response = await self._request("GET", "/profiles/by-nfc/FFEEDDCC")

        self.assertEqual(response.status_code, 404)

    async def test_post_rejects_tag_present_in_nfc_uid_or_internal_id(self):
        request_body = {
            "cow_id": "COW099",
            "birth_date": "2023-01-01",
            "weight_kg": 400,
            "type": "dairy",
            "breed": "Holstein",
            "nfc_uid": "A1B2C3D4",
        }

        response = await self._request("POST", "/profiles", json=request_body)

        self.assertEqual(response.status_code, 409)
        self.assertEqual(len(self.profiles.profiles), 2)

    async def test_post_rejects_tag_matching_internal_id(self):
        self.profiles.profiles[1].pop("nfc_uid")
        self.profiles.profiles[1]["_id"] = "11223344"
        request_body = {
            "cow_id": "COW099",
            "birth_date": "2023-01-01",
            "weight_kg": 400,
            "type": "dairy",
            "breed": "Holstein",
            "nfc_uid": "11223344",
        }

        response = await self._request("POST", "/profiles", json=request_body)

        self.assertEqual(response.status_code, 409)
        self.assertEqual(len(self.profiles.profiles), 2)

    async def test_duplicate_key_error_on_id_during_post_returns_conflict(self):
        self.profiles.profiles = []
        self.profiles.duplicate_on_insert = True
        request_body = {
            "cow_id": "COW099",
            "birth_date": "2023-01-01",
            "weight_kg": 400,
            "type": "dairy",
            "breed": "Holstein",
            "nfc_uid": "AABBCCDD",
        }

        response = await self._request("POST", "/profiles", json=request_body)

        self.assertEqual(response.status_code, 409)

    async def test_eleveur_post_still_requires_nfc_uid(self):
        request_body = {
            "cow_id": "COW099",
            "birth_date": "2023-01-01",
            "weight_kg": 400,
            "type": "dairy",
            "breed": "Holstein",
        }

        response = await self._request(
            "POST", "/profiles", token="eleveur-token", json=request_body
        )

        self.assertEqual(response.status_code, 422)


if __name__ == "__main__":
    unittest.main()
