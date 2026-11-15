import os
import secrets
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace


os.environ.setdefault("JWT_SECRET", secrets.token_urlsafe(32))

from app.routers.alerts import list_alerts


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

        result = await list_alerts(request, actor={"admin_id": "admin-1"})

        self.assertEqual(result[0]["cow_id"], "COW-17")
        self.assertEqual(result[0]["id"], "alert-1")
