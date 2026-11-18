import asyncio
import hashlib
import os
import secrets
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

from bson import ObjectId
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from pymongo import AsyncMongoClient, MongoClient

from app.db import Cols, ensure_index
from app.deps import admin_only, rate_limit
from app.routers.auth import router as auth_router
from app.routers.eleveurs import router as eleveurs_router


class RealMongoConcurrentQrScanTests(unittest.TestCase):
    @unittest.skipUnless(
        os.environ.get("MONGO_TEST_URI"),
        "Set MONGO_TEST_URI to run the real-Mongo concurrency test.",
    )
    def test_only_one_of_two_concurrent_scans_consumes_qr(self):
        uri = os.environ["MONGO_TEST_URI"]
        database_name = f"qr_auth_test_{uuid4().hex}"
        eleveur_id = ObjectId()
        admin_id = ObjectId()
        token = secrets.token_urlsafe(32)
        now = datetime.now(timezone.utc)
        token_document = {
            "token_hash": hashlib.sha256(token.encode()).hexdigest(),
            "eleveur_id": eleveur_id,
            "admin_id": admin_id,
            "created_at": now,
            "expires_at": now + timedelta(minutes=3),
            "used": False,
            "used_at": None,
            "used_by": None,
            "revoked": False,
            "failed_attempts": 0,
        }

        sync_client = MongoClient(
            uri,
            tz_aware=True,
            serverSelectionTimeoutMS=5000,
        )
        try:
            sync_client.admin.command("ping")
            database = sync_client[database_name]
            async def create_indexes():
                mongo = AsyncMongoClient(uri, tz_aware=True)
                try:
                    await ensure_index(Cols(mongo[database_name]))
                finally:
                    await mongo.close()

            asyncio.run(create_indexes())
            qr_tokens = database["qr_tokens"]
            qr_tokens.insert_one(token_document)
            database["eleveurs"].insert_one(
                {
                    "_id": eleveur_id,
                    "admin_id": admin_id,
                    "name": "Concurrent Test",
                }
            )
        except Exception:
            sync_client.drop_database(database_name)
            sync_client.close()
            raise

        def scan():
            async def send():
                mongo = AsyncMongoClient(uri, tz_aware=True)
                try:
                    app = FastAPI()
                    app.state.cols = SimpleNamespace(
                        qr_tokens=mongo[database_name]["qr_tokens"],
                        eleveurs=mongo[database_name]["eleveurs"],
                    )
                    app.dependency_overrides[rate_limit] = lambda: None
                    app.include_router(auth_router)
                    async with AsyncClient(
                        transport=ASGITransport(app=app),
                        base_url="http://test",
                    ) as client:
                        response = await client.post(
                            "/auth/eleveur/qr-login",
                            json={"token": token, "name": "Concurrent Test"},
                        )
                        return response.status_code
                finally:
                    await mongo.close()

            return asyncio.run(send())

        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                statuses = list(executor.map(lambda _: scan(), range(2)))
            self.assertCountEqual(statuses, [200, 409])
            stored = qr_tokens.find_one({"token_hash": token_document["token_hash"]})
            self.assertTrue(stored["used"])
            self.assertEqual(stored["used_by"], eleveur_id)

            async def generate_replacement():
                mongo = AsyncMongoClient(uri, tz_aware=True)
                try:
                    app = FastAPI()
                    app.state.cols = Cols(mongo[database_name])
                    app.dependency_overrides[rate_limit] = lambda: None
                    app.dependency_overrides[admin_only] = lambda: {
                        "id": admin_id,
                        "admin_id": admin_id,
                        "role": "admin",
                    }
                    app.include_router(eleveurs_router)
                    async with AsyncClient(
                        transport=ASGITransport(app=app),
                        base_url="http://test",
                    ) as client:
                        response = await client.post(
                            f"/eleveurs/{eleveur_id}/qr"
                        )
                        return response.status_code
                finally:
                    await mongo.close()

            self.assertEqual(asyncio.run(generate_replacement()), 201)
            self.assertEqual(
                qr_tokens.count_documents({"eleveur_id": eleveur_id}),
                2,
            )
        finally:
            sync_client.drop_database(database_name)
            sync_client.close()


if __name__ == "__main__":
    unittest.main()
