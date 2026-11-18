import hashlib
import os
import secrets
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from bson import ObjectId
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

os.environ.setdefault("JWT_SECRET", secrets.token_urlsafe(32))

from app.deps import admin_only, get_actor_allow_expired, rate_limit
from app.routers.auth import router as auth_router
from app.routers.eleveurs import router as eleveurs_router
from app.routers.invites import router as invites_router


class FakeCollection:
    def __init__(self, documents=()):
        self.documents = list(documents)

    async def find_one(self, query, _projection=None):
        return next(
            (document for document in self.documents if _matches(document, query)),
            None,
        )

    async def update_one(self, query, update, upsert=False):
        document = await self.find_one(query)
        if document is None:
            if not upsert:
                return SimpleNamespace(matched_count=0, upserted_id=None)
            document = {
                key: value
                for key, value in query.items()
                if not isinstance(value, dict)
            }
            self.documents.append(document)
        document.update(update.get("$set", {}))
        return SimpleNamespace(matched_count=1, upserted_id=None)

    async def insert_one(self, document):
        stored = dict(document)
        stored.setdefault("_id", ObjectId())
        self.documents.append(stored)
        return SimpleNamespace(inserted_id=stored["_id"])

    async def find_one_and_update(
        self, query, update, return_document=None
    ):
        document = await self.find_one(query)
        if document is None:
            return None
        if isinstance(update, list):
            original = dict(document)
            assignments = update[0]["$set"]
            document.update(
                {
                    key: _pipeline_value(value, original)
                    for key, value in assignments.items()
                }
            )
        else:
            document.update(update.get("$set", {}))
        return document


def _matches(document, query):
    for key, expected in query.items():
        value = document.get(key)
        if isinstance(expected, dict):
            if "$gt" in expected and not value > expected["$gt"]:
                return False
            if "$lt" in expected and not value < expected["$lt"]:
                return False
            if "$ne" in expected and value == expected["$ne"]:
                return False
        elif value != expected:
            return False
    return True


def _pipeline_value(expression, document):
    if isinstance(expression, str) and expression.startswith("$"):
        return document.get(expression[1:])
    if not isinstance(expression, dict):
        return expression
    if "$add" in expression:
        return sum(_pipeline_value(value, document) for value in expression["$add"])
    if "$ifNull" in expression:
        value, fallback = expression["$ifNull"]
        result = _pipeline_value(value, document)
        return fallback if result is None else result
    if "$gte" in expression:
        left, right = expression["$gte"]
        return _pipeline_value(left, document) >= right
    return expression


class QrLoginTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.admin_id = ObjectId("65a000000000000000000001")
        self.eleveur_id = ObjectId("65a000000000000000000002")
        self.eleveur = {
            "_id": self.eleveur_id,
            "admin_id": self.admin_id,
            "name": "Élodie Ben Salah",
            "created_at": datetime.now(timezone.utc),
        }
        self.secret = secrets.token_urlsafe(32)
        self.qr = self._qr_document()
        self.qr_tokens = FakeCollection([self.qr])
        self.eleveurs = FakeCollection([self.eleveur])
        self.admins = FakeCollection()
        self.invites = FakeCollection()
        self.app = FastAPI()
        self.app.state.cols = SimpleNamespace(
            qr_tokens=self.qr_tokens,
            eleveurs=self.eleveurs,
            admins=self.admins,
            invites=self.invites,
        )
        self.app.dependency_overrides[rate_limit] = lambda: None
        self.app.dependency_overrides[admin_only] = lambda: {
            "id": self.admin_id,
            "role": "admin",
            "admin_id": self.admin_id,
        }
        self.app.dependency_overrides[get_actor_allow_expired] = lambda: {
            "id": self.eleveur_id,
            "role": "eleveur",
            "admin_id": self.admin_id,
        }
        self.app.include_router(auth_router)
        self.app.include_router(eleveurs_router)
        self.app.include_router(invites_router)

    def _qr_document(self, *, expires_at=None):
        now = datetime.now(timezone.utc)
        return {
            "token_hash": hashlib.sha256(self.secret.encode()).hexdigest(),
            "eleveur_id": self.eleveur_id,
            "admin_id": self.admin_id,
            "created_at": now,
            "expires_at": expires_at or now + timedelta(minutes=3),
            "used": False,
            "used_at": None,
            "used_by": None,
            "revoked": False,
            "failed_attempts": 0,
        }

    async def _post(self, path, body, *, token=None):
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        async with AsyncClient(
            transport=ASGITransport(app=self.app), base_url="http://test"
        ) as client:
            return await client.post(path, headers=headers, json=body)

    async def test_valid_scan_accepts_case_and_accent_variants(self):
        response = await self._post(
            "/auth/eleveur/qr-login",
            {"token": self.secret, "name": "elodie ben salah"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["user"]["id"], str(self.eleveur_id))
        self.assertTrue(self.qr["used"])
        self.assertEqual(self.qr["used_by"], self.eleveur_id)
        self.assertIsNotNone(self.qr["used_at"].tzinfo)

    async def test_expired_scan_returns_gone(self):
        self.qr["expires_at"] = datetime.now(timezone.utc) - timedelta(seconds=1)

        response = await self._post(
            "/auth/eleveur/qr-login",
            {"token": self.secret, "name": self.eleveur["name"]},
        )

        self.assertEqual(response.status_code, 410)

    async def test_reused_scan_returns_conflict(self):
        self.qr.update(
            used=True,
            used_at=datetime.now(timezone.utc),
            used_by=self.eleveur_id,
        )

        response = await self._post(
            "/auth/eleveur/qr-login",
            {"token": self.secret, "name": self.eleveur["name"]},
        )

        self.assertEqual(response.status_code, 409)

    async def test_wrong_name_does_not_consume_qr_and_valid_name_can_retry(self):
        wrong_name = await self._post(
            "/auth/eleveur/qr-login",
            {"token": self.secret, "name": "Someone Else"},
        )
        correct_name = await self._post(
            "/auth/eleveur/qr-login",
            {"token": self.secret, "name": self.eleveur["name"]},
        )

        self.assertEqual(wrong_name.status_code, 401)
        self.assertEqual(wrong_name.json()["detail"], "Name does not match")
        self.assertEqual(correct_name.status_code, 200)
        self.assertTrue(self.qr["used"])

    async def test_fifth_wrong_name_revokes_qr(self):
        for attempt in range(5):
            response = await self._post(
                "/auth/eleveur/qr-login",
                {"token": self.secret, "name": f"Wrong {attempt}"},
            )

        self.assertEqual(response.status_code, 429)
        self.assertTrue(self.qr["revoked"])
        self.assertEqual(self.qr["failed_attempts"], 5)

        retry = await self._post(
            "/auth/eleveur/qr-login",
            {"token": self.secret, "name": self.eleveur["name"]},
        )
        self.assertEqual(retry.status_code, 429)

    async def test_unknown_scan_returns_not_found(self):
        response = await self._post(
            "/auth/eleveur/qr-login",
            {"token": "unknown", "name": self.eleveur["name"]},
        )

        self.assertEqual(response.status_code, 404)

    async def test_legacy_invite_uses_three_minute_expiry_and_32_random_bytes(self):
        response = await self._post("/invites", {}, token="admin-token")

        self.assertEqual(response.status_code, 201)
        payload = response.json()
        stored = self.invites.documents[0]
        self.assertEqual(
            stored["expires_at"] - stored["created_at"],
            timedelta(minutes=3),
        )
        self.assertEqual(len(payload["code"]), 43)
        self.assertNotIn("code", stored)
        self.assertEqual(
            stored["code_hash"],
            hashlib.sha256(payload["code"].encode()).hexdigest(),
        )

    async def test_new_qr_replaces_previous_unused_token(self):
        previous_hash = self.qr["token_hash"]
        response = await self._post(
            f"/eleveurs/{self.eleveur_id}/qr",
            {},
            token="admin-token",
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(len(self.qr_tokens.documents), 1)
        replacement = self.qr_tokens.documents[0]
        self.assertNotEqual(replacement["token_hash"], previous_hash)
        self.assertEqual(replacement["expires_at"].tzinfo, timezone.utc)
        self.assertNotIn("token", replacement)

        old_scan = await self._post(
            "/auth/eleveur/qr-login",
            {"token": self.secret, "name": self.eleveur["name"]},
        )
        new_token = response.json()["qr_payload"].split("token=", 1)[1]
        new_scan = await self._post(
            "/auth/eleveur/qr-login",
            {"token": new_token, "name": self.eleveur["name"]},
        )
        self.assertEqual(old_scan.status_code, 404)
        self.assertEqual(new_scan.status_code, 200)

    async def test_new_qr_can_be_issued_after_previous_token_was_used(self):
        self.qr.update(
            used=True,
            used_at=datetime.now(timezone.utc),
            used_by=self.eleveur_id,
        )

        response = await self._post(
            f"/eleveurs/{self.eleveur_id}/qr",
            {},
            token="admin-token",
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(len(self.qr_tokens.documents), 2)
        active = [
            document
            for document in self.qr_tokens.documents
            if not document["used"]
        ]
        self.assertEqual(len(active), 1)

    async def test_session_name_check_requires_matching_eleveur_name(self):
        wrong_name = await self._post(
            "/auth/eleveur/verify-name",
            {"name": "Someone Else"},
            token="session-token",
        )
        correct_name = await self._post(
            "/auth/eleveur/verify-name",
            {"name": "elodie ben salah"},
            token="session-token",
        )

        self.assertEqual(wrong_name.status_code, 401)
        self.assertEqual(correct_name.status_code, 200)


if __name__ == "__main__":
    unittest.main()
