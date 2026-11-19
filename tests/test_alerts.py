import os
import secrets
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient


os.environ.setdefault("JWT_SECRET", secrets.token_urlsafe(32))

from app.deps import get_actor
from app.routers.alerts import list_alerts, router


class Cursor:
    def __init__(self, records):
        self.records = records

    def sort(self, *_args):
        return self

    def limit(self, *_args):
        return self

    def __aiter__(self):
        async def records():
            for record in self.records:
                yield record

        return records()


class AlertResponseTests(unittest.IsolatedAsyncioTestCase):
    async def test_list_alerts_resolves_legacy_rfid_to_public_cow_id(self):
        alert = {
            "_id": "alert-1",
            "source": "system",
            "cow_id": "RFID-17",
            "message": "Check this cow",
            "severity": "warning",
            "type": "appetite",
            "status": "unread",
            "created_at": datetime.now(timezone.utc),
            "admin_id": "admin-1",
        }
        profile = {"_id": "RFID-17", "cow_id": "COW-17"}
        cols = SimpleNamespace(
            alerts=SimpleNamespace(find=lambda *_args: Cursor([alert])),
            profiles=SimpleNamespace(find=lambda *_args: Cursor([profile])),
        )
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(cols=cols)))

        result = await list_alerts(
            request,
            admin={"id": "admin-1", "admin_id": "admin-1"},
        )

        self.assertEqual(result[0]["cow_id"], "COW-17")
        self.assertEqual(result[0]["id"], "alert-1")

    async def test_eleveur_cannot_list_alerts_and_manual_alert_post_is_disabled(self):
        app = FastAPI()
        app.include_router(router)
        actor = {
            "id": "eleveur-1",
            "admin_id": "admin-1",
            "role": "eleveur",
        }
        app.dependency_overrides[get_actor] = lambda: actor
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            listing = await client.get("/alerts")
            actor["role"] = "admin"
            posting = await client.post("/alerts", json={"message": "manual"})

        self.assertEqual(listing.status_code, 403)
        self.assertEqual(posting.status_code, 405)
