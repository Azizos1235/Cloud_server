import os
import secrets
import unittest
from copy import deepcopy
from types import SimpleNamespace

from bson import ObjectId
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

os.environ.setdefault("JWT_SECRET", secrets.token_urlsafe(32))

from app.deps import admin_only, get_actor
from app.routers import tasks


_DEFAULTS = {
    "sell": (
        "Vendre la vache",
        "Préparer la vache pour la vente.\n"
        "Vérifier son état et ses documents.\n"
        "Informer l’administrateur une fois la vente effectuée.",
    ),
    "vet": (
        "Emmener la vache chez le vétérinaire",
        "Emmener la vache chez le vétérinaire.\n"
        "Expliquer les symptômes ou le motif de consultation.\n"
        "Suivre les recommandations du vétérinaire et faire un compte rendu.",
    ),
    "butcher": (
        "Envoyer la vache à la boucherie",
        "Organiser le transport de la vache à la boucherie.\n"
        "Vérifier les documents nécessaires.\n"
        "Informer l’administrateur une fois la démarche effectuée.",
    ),
    "check_weight": (
        "Vérifier le poids de la vache",
        "Peser la vache.\n"
        "Noter son poids.\n"
        "Vérifier son état corporel.\n"
        "Signaler toute anomalie.",
    ),
    "custom": ("Nouvelle tâche", ""),
}


class _MemoryCollection:
    def __init__(self, documents=None):
        self.documents = documents or {}

    async def find_one(self, query, projection=None):
        return deepcopy(self.documents.get(query.get("_id")))

    async def insert_one(self, document):
        stored = deepcopy(document)
        stored.setdefault("_id", ObjectId())
        self.documents[stored["_id"]] = stored
        return SimpleNamespace(inserted_id=stored["_id"])

    async def find_one_and_update(self, query, update, return_document=None):
        document = self.documents.get(query.get("_id"))
        if document is None:
            return None
        if any(document.get(key) != value for key, value in query.items() if key != "_id"):
            return None
        document.update(deepcopy(update.get("$set", {})))
        return deepcopy(document)


class TaskTypesApiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.admin_id = ObjectId()
        self.eleveur_id = ObjectId()
        self.eleveurs = _MemoryCollection({
            self.eleveur_id: {
                "_id": self.eleveur_id,
                "admin_id": self.admin_id,
                "name": "Éleveur Test",
            }
        })
        self.cols = SimpleNamespace(
            eleveurs=self.eleveurs,
            tasks=_MemoryCollection(),
            alerts=_MemoryCollection(),
        )
        self.actor = {
            "id": self.admin_id,
            "admin_id": self.admin_id,
            "role": "admin",
        }
        self.app = FastAPI()
        self.app.state.cols = self.cols
        self.app.dependency_overrides[admin_only] = lambda: self.actor
        self.app.dependency_overrides[get_actor] = lambda: self.actor
        self.app.include_router(tasks.router)
        self.client = AsyncClient(
            transport=ASGITransport(app=self.app),
            base_url="http://test",
        )

    async def asyncTearDown(self):
        await self.client.aclose()

    async def _create(self, body):
        return await self.client.post(
            "/tasks",
            json={"eleveur_id": str(self.eleveur_id), **body},
        )

    async def test_post_accepts_supported_types_and_fills_each_default(self):
        for task_type, (title, plan) in _DEFAULTS.items():
            with self.subTest(task_type=task_type):
                response = await self._create({"task_type": task_type})
                self.assertEqual(response.status_code, 201)
                self.assertEqual(response.json()["task_type"], task_type)
                self.assertEqual(response.json()["title"], title)
                self.assertEqual(response.json()["plan"], plan)

    async def test_post_rejects_unknown_type(self):
        response = await self._create({"task_type": "feed"})
        self.assertEqual(response.status_code, 422)

    async def test_post_preserves_admin_edits_and_vet_reason(self):
        response = await self._create({
            "task_type": "vet",
            "title": "Consultation urgente",
            "description": "La vache refuse de manger.",
            "plan": "Appeler le vétérinaire aujourd’hui.",
        })
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json()["title"], "Consultation urgente")
        self.assertEqual(
            response.json()["description"],
            "La vache refuse de manger.",
        )
        self.assertEqual(
            response.json()["plan"],
            "Appeler le vétérinaire aujourd’hui.",
        )

    async def test_completion_succeeds_for_each_task_type(self):
        for task_type in _DEFAULTS:
            with self.subTest(task_type=task_type):
                created = await self._create({"task_type": task_type})
                self.assertEqual(created.status_code, 201)
                self.actor = {
                    "id": self.eleveur_id,
                    "admin_id": self.admin_id,
                    "role": "eleveur",
                }
                completion = {"completion_note": "Tâche effectuée."}
                if task_type == "check_weight":
                    completion["completed_weight_kg"] = 401.5
                done = await self.client.post(
                    f"/tasks/{created.json()['id']}/done",
                    json=completion,
                )
                self.assertEqual(done.status_code, 200)
                self.assertEqual(done.json()["status"], "done")
                self.assertEqual(
                    done.json()["completion_note"],
                    "Tâche effectuée.",
                )
                if task_type == "check_weight":
                    self.assertEqual(done.json()["completed_weight_kg"], 401.5)
                self.actor = {
                    "id": self.admin_id,
                    "admin_id": self.admin_id,
                    "role": "admin",
                }


if __name__ == "__main__":
    unittest.main()
