import asyncio
import os
import secrets
import unittest
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from bson import ObjectId
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from pymongo import AsyncMongoClient

os.environ.setdefault("JWT_SECRET", secrets.token_urlsafe(32))

from app.db import Cols, ensure_index
from app.deps import get_actor, get_superuser
from app.routers import eleveurs, superuser, tasks


@unittest.skipUnless(
    os.environ.get("MONGO_TEST_URI"),
    "Set MONGO_TEST_URI to run the real-Mongo task tests.",
)
class RealMongoTaskTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.uri = os.environ["MONGO_TEST_URI"]
        self.database_name = f"task_api_test_{uuid4().hex}"
        self.client = AsyncMongoClient(
            self.uri,
            tz_aware=True,
            serverSelectionTimeoutMS=5000,
        )
        await self.client.admin.command("ping")
        self.database = self.client[self.database_name]
        self.cols = Cols(self.database)
        await ensure_index(self.cols)

        self.admin_id = ObjectId()
        self.other_admin_id = ObjectId()
        self.eleveur_id = ObjectId()
        self.other_eleveur_id = ObjectId()
        await self.cols.admins.insert_many(
            [
                {
                    "_id": self.admin_id,
                    "email": f"admin-{self.admin_id}@test.invalid",
                    "name": "Admin One",
                },
                {
                    "_id": self.other_admin_id,
                    "email": f"admin-{self.other_admin_id}@test.invalid",
                    "name": "Admin Two",
                },
            ]
        )
        await self.cols.eleveurs.insert_many(
            [
                {
                    "_id": self.eleveur_id,
                    "admin_id": self.admin_id,
                    "name": "Farmer One",
                },
                {
                    "_id": self.other_eleveur_id,
                    "admin_id": self.other_admin_id,
                    "name": "Farmer Two",
                },
            ]
        )
        await self.cols.profiles.insert_one(
            {
                "_id": "RFID-TASK-001",
                "cow_id": "COW001",
                "admin_id": self.admin_id,
            }
        )

    async def asyncTearDown(self):
        if hasattr(self, "client"):
            await self.client.drop_database(self.database_name)
            await self.client.close()

    def _app(self, role: str, user_id: ObjectId, admin_id: ObjectId | None = None):
        actor = {
            "id": user_id,
            "admin_id": admin_id or user_id,
            "role": role,
        }
        app = FastAPI()
        app.state.cols = self.cols
        app.dependency_overrides[get_actor] = lambda: actor
        app.dependency_overrides[get_superuser] = lambda: actor
        app.include_router(tasks.router)
        app.include_router(eleveurs.router)
        app.include_router(superuser.router)
        return app

    async def _request(
        self,
        app: FastAPI,
        method: str,
        path: str,
        body: dict | None = None,
    ):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            return await client.request(method, path, json=body)

    async def _create_task(
        self,
        *,
        eleveur_id: ObjectId | None = None,
        admin_id: ObjectId | None = None,
        title: str = "Inspect feeder",
        cow_id: str | None = None,
        due_at: str | None = None,
    ):
        admin_id = admin_id or self.admin_id
        eleveur_id = eleveur_id or self.eleveur_id
        app = self._app("admin", admin_id)
        body = {"eleveur_id": str(eleveur_id), "title": title}
        if cow_id is not None:
            body["cow_id"] = cow_id
        if due_at is not None:
            body["due_at"] = due_at
        response = await self._request(app, "POST", "/tasks", body)
        return response

    async def test_create_validates_timezone_and_cow_ownership(self):
        no_timezone = await self._create_task(due_at="2026-11-18T12:00:00")
        self.assertEqual(no_timezone.status_code, 422)
        self.assertIn(
            "due_at must include a timezone",
            str(no_timezone.json()["detail"]),
        )

        created = await self._create_task(
            cow_id="cow001",
            due_at="2026-11-18T12:00:00+01:00",
        )
        self.assertEqual(created.status_code, 201)
        self.assertEqual(created.json()["cow_id"], "COW001")
        due_at = datetime.fromisoformat(created.json()["due_at"].replace("Z", "+00:00"))
        self.assertEqual(due_at.utcoffset(), timezone.utc.utcoffset(due_at))
        stored = await self.cols.tasks.find_one({"_id": ObjectId(created.json()["id"])})
        self.assertEqual(stored["due_at"].utcoffset(), timedelta(0))

        bad_cow = await self._create_task(cow_id="COW999")
        self.assertEqual(bad_cow.status_code, 422)

    async def test_admin_and_eleveur_lists_are_scoped_and_paginated(self):
        first = await self._create_task(title="First")
        second = await self._create_task(title="Second")
        self.assertEqual(first.status_code, 201)
        self.assertEqual(second.status_code, 201)
        other = await self._create_task(
            admin_id=self.other_admin_id,
            eleveur_id=self.other_eleveur_id,
            title="Other farm",
        )
        self.assertEqual(other.status_code, 201)

        admin_app = self._app("admin", self.admin_id)
        listed = await self._request(admin_app, "GET", "/tasks?limit=1&skip=1")
        self.assertEqual(listed.status_code, 200)
        self.assertEqual(len(listed.json()), 1)
        self.assertEqual(
            [item["title"] for item in (await self._request(
                admin_app, "GET", "/tasks?status=pending"
            )).json()],
            ["Second", "First"],
        )
        filtered = await self._request(
            admin_app,
            "GET",
            f"/tasks?eleveur_id={self.eleveur_id}&status=pending",
        )
        self.assertEqual(len(filtered.json()), 2)
        self.assertEqual(
            (await self._request(admin_app, "GET", "/tasks?limit=501")).status_code,
            422,
        )

        mine = await self._request(
            self._app("eleveur", self.eleveur_id, self.admin_id),
            "GET",
            "/tasks/mine",
        )
        self.assertEqual([item["title"] for item in mine.json()], ["Second", "First"])
        self.assertNotIn(str(self.other_eleveur_id), str(mine.json()))

    async def test_done_is_idempotent_hides_from_farmer_and_keeps_admin_record(self):
        created = await self._create_task(title="Clean trough")
        task_id = created.json()["id"]
        farmer_app = self._app("eleveur", self.eleveur_id, self.admin_id)

        first = await self._request(
            farmer_app,
            "POST",
            f"/tasks/{task_id}/done",
            {
                "completed_weight_kg": 412.5,
                "completion_note": "État corporel correct",
            },
        )
        second = await self._request(
            farmer_app,
            "POST",
            f"/tasks/{task_id}/done",
            {"completed_by": str(self.other_eleveur_id)},
        )
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(first.json()["completed_by"], str(self.eleveur_id))
        self.assertEqual(first.json()["completed_at"], second.json()["completed_at"])
        self.assertEqual(first.json()["completed_weight_kg"], 412.5)
        self.assertEqual(first.json()["completion_note"], "État corporel correct")
        self.assertEqual((await self._request(
            farmer_app, "GET", "/tasks/mine"
        )).json(), [])
        completion_alerts = await self.cols.alerts.find(
            {"type": "task_completed"}
        ).to_list()
        self.assertEqual(len(completion_alerts), 1)
        self.assertIn("Farmer One", completion_alerts[0]["message"])
        self.assertIn("412.5 kg", completion_alerts[0]["message"])

        admin_list = await self._request(
            self._app("admin", self.admin_id),
            "GET",
            "/tasks?status=done",
        )
        self.assertEqual(len(admin_list.json()), 1)
        self.assertEqual(admin_list.json()[0]["status"], "done")
        self.assertEqual(admin_list.json()[0]["completed_by_name"], "Farmer One")

    async def test_create_task_links_and_handles_appetite_alert(self):
        alert_id = ObjectId()
        await self.cols.alerts.insert_one(
            {
                "_id": alert_id,
                "source": "system",
                "type": "appetite_high",
                "admin_id": self.admin_id,
                "cow_id": "COW001",
                "handled": False,
                "task_id": None,
            }
        )

        response = await self._request(
            self._app("admin", self.admin_id),
            "POST",
            "/tasks",
            {
                "eleveur_id": str(self.eleveur_id),
                "title": "Vérifier le poids",
                "task_type": "check_weight",
                "alert_id": str(alert_id),
            },
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json()["task_type"], "check_weight")
        self.assertIn("Peser la vache", response.json()["plan"])
        self.assertEqual(response.json()["alert_id"], str(alert_id))
        alert = await self.cols.alerts.find_one({"_id": alert_id})
        self.assertTrue(alert["handled"])
        self.assertEqual(alert["task_id"], ObjectId(response.json()["id"]))

    async def test_farmer_cannot_see_or_complete_another_farmers_task(self):
        created = await self._create_task(
            admin_id=self.other_admin_id,
            eleveur_id=self.other_eleveur_id,
        )
        farmer_app = self._app("eleveur", self.eleveur_id, self.admin_id)

        listed = await self._request(farmer_app, "GET", "/tasks/mine")
        completed = await self._request(
            farmer_app, "POST", f"/tasks/{created.json()['id']}/done"
        )
        self.assertEqual(listed.json(), [])
        self.assertEqual(completed.status_code, 404)

    async def test_roles_cannot_cross_admin_and_farmer_routes(self):
        created = await self._create_task()
        task_id = created.json()["id"]
        farmer_app = self._app("eleveur", self.eleveur_id, self.admin_id)
        admin_app = self._app("admin", self.admin_id)

        self.assertEqual(
            (await self._request(farmer_app, "GET", "/tasks")).status_code,
            403,
        )
        self.assertEqual(
            (await self._request(farmer_app, "DELETE", f"/tasks/{task_id}")).status_code,
            403,
        )
        self.assertEqual(
            (await self._request(farmer_app, "PATCH", f"/tasks/{task_id}")).status_code,
            405,
        )
        self.assertEqual(
            (await self._request(admin_app, "GET", "/tasks/mine")).status_code,
            403,
        )
        self.assertEqual(
            (await self._request(admin_app, "POST", f"/tasks/{task_id}/done")).status_code,
            403,
        )

    async def test_admin_cannot_create_or_delete_another_admins_task(self):
        other_task = await self._create_task(
            admin_id=self.other_admin_id,
            eleveur_id=self.other_eleveur_id,
        )
        admin_app = self._app("admin", self.admin_id)

        create_for_other = await self._request(
            admin_app,
            "POST",
            "/tasks",
            {
                "eleveur_id": str(self.other_eleveur_id),
                "title": "Cross-owner task",
            },
        )
        delete_other = await self._request(
            admin_app, "DELETE", f"/tasks/{other_task.json()['id']}"
        )
        list_own = await self._request(admin_app, "GET", "/tasks")
        self.assertEqual(create_for_other.status_code, 404)
        self.assertEqual(delete_other.status_code, 404)
        self.assertEqual(list_own.json(), [])

    async def test_admin_hard_delete_removes_task_for_both_roles(self):
        created = await self._create_task()
        task_id = ObjectId(created.json()["id"])
        admin_app = self._app("admin", self.admin_id)

        deleted = await self._request(admin_app, "DELETE", f"/tasks/{task_id}")
        mine = await self._request(
            self._app("eleveur", self.eleveur_id, self.admin_id),
            "GET",
            "/tasks/mine",
        )
        listed = await self._request(admin_app, "GET", "/tasks")
        stored = await self.cols.tasks.find_one({"_id": task_id})
        self.assertEqual(deleted.status_code, 200)
        self.assertEqual(mine.json(), [])
        self.assertEqual(listed.json(), [])
        self.assertIsNone(stored)

    async def test_task_keeps_stored_cow_id_if_profile_is_removed(self):
        created = await self._create_task(cow_id="COW001")
        await self.cols.profiles.delete_one({"_id": "RFID-TASK-001"})

        mine = await self._request(
            self._app("eleveur", self.eleveur_id, self.admin_id),
            "GET",
            "/tasks/mine",
        )
        self.assertEqual(mine.status_code, 200)
        self.assertEqual(mine.json()[0]["cow_id"], "COW001")
        self.assertEqual(created.status_code, 201)

    async def test_concurrent_done_requests_both_succeed_safely(self):
        created = await self._create_task()
        app = self._app("eleveur", self.eleveur_id, self.admin_id)
        task_id = created.json()["id"]

        async def mark_done():
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
            ) as client:
                return await client.post(f"/tasks/{task_id}/done", json={})

        first, second = await asyncio.gather(mark_done(), mark_done())
        self.assertEqual([first.status_code, second.status_code], [200, 200])
        self.assertEqual(first.json()["completed_by"], str(self.eleveur_id))
        self.assertEqual(second.json()["completed_by"], str(self.eleveur_id))
        self.assertEqual(first.json()["completed_at"], second.json()["completed_at"])

    async def test_deleting_eleveur_and_admin_cascades_tasks(self):
        await self._create_task()
        admin_app = self._app("admin", self.admin_id)

        deleted = await self._request(
            admin_app, "DELETE", f"/eleveurs/{self.eleveur_id}"
        )
        self.assertEqual(deleted.status_code, 200)
        self.assertEqual(
            await self.cols.tasks.count_documents({"eleveur_id": self.eleveur_id}),
            0,
        )

        await self._create_task(
            admin_id=self.other_admin_id,
            eleveur_id=self.other_eleveur_id,
        )
        superuser_app = self._app("superuser", ObjectId())
        deleted_admin = await self._request(
            superuser_app,
            "DELETE",
            f"/superuser/admins/{self.other_admin_id}?confirm=true",
        )
        self.assertEqual(deleted_admin.status_code, 200)
        self.assertEqual(
            await self.cols.tasks.count_documents({"admin_id": self.other_admin_id}),
            0,
        )

    async def test_required_task_indexes_exist(self):
        indexes = []
        cursor = await self.cols.tasks.list_indexes()
        async for index in cursor:
            indexes.append(tuple(index["key"].items()))
        self.assertIn(
            (("eleveur_id", 1), ("status", 1), ("due_at", 1)),
            indexes,
        )
        self.assertIn((("admin_id", 1), ("created_at", -1)), indexes)


if __name__ == "__main__":
    unittest.main()
