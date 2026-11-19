from datetime import datetime, timezone
from typing import Literal

from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field, field_validator
from pymongo import ReturnDocument

from app.deps import admin_only, get_actor, owned_cow, owner_filter
from app.utils import new_alert, sid

router = APIRouter(prefix="/tasks")

TASK_DEFAULTS = {
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


class TaskCreateIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    eleveur_id: str
    title: str | None = Field(default=None, max_length=120)
    description: str | None = Field(default=None, max_length=1000)
    due_at: datetime | None = None
    cow_id: str | None = Field(default=None, max_length=80)
    task_type: Literal["sell", "vet", "butcher", "check_weight", "custom"] = "custom"
    plan: str | None = Field(default=None, max_length=2000)
    alert_id: str | None = None

    @field_validator("title")
    @classmethod
    def validate_title(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            return None
        if len(value) > 120:
            raise ValueError("title must be at most 120 characters")
        return value

    @field_validator("description")
    @classmethod
    def normalize_description(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if len(value) > 1000:
            raise ValueError("description must be at most 1000 characters")
        return value or None

    @field_validator("due_at", mode="before")
    @classmethod
    def require_timezone(cls, value):
        if value is None:
            return None
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(
                    value[:-1] + "+00:00" if value.endswith("Z") else value
                )
            except ValueError:
                return value
        elif isinstance(value, datetime):
            parsed = value
        else:
            return value
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError(
                "due_at must include a timezone; send a UTC ISO-8601 string"
            )
        return value

    @field_validator("due_at")
    @classmethod
    def normalize_due_at(cls, value: datetime | None) -> datetime | None:
        return value.astimezone(timezone.utc) if value is not None else None

    @field_validator("cow_id")
    @classmethod
    def normalize_cow_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip().upper()
        return value or None

    @field_validator("plan")
    @classmethod
    def normalize_plan(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return value.strip()


class TaskCompletionIn(BaseModel):
    model_config = ConfigDict(extra="ignore")

    completed_weight_kg: float | None = Field(default=None, gt=0, le=1500)
    completion_note: str | None = Field(default=None, max_length=1000)

    @field_validator("completion_note")
    @classmethod
    def normalize_completion_note(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        return value or None


class TaskOut(BaseModel):
    id: str
    admin_id: str
    eleveur_id: str
    eleveur_name: str | None = None
    title: str
    description: str | None = None
    cow_id: str | None = None
    task_type: Literal["sell", "vet", "butcher", "check_weight", "custom"] = "custom"
    plan: str = ""
    alert_id: str | None = None
    due_at: datetime | None = None
    status: Literal["pending", "done"]
    created_at: datetime
    completed_at: datetime | None = None
    completed_by: str | None = None
    completed_by_name: str | None = None
    completion_note: str | None = None
    completed_weight_kg: float | None = None


def _task_out(task: dict, names: dict[ObjectId, str]) -> dict:
    eleveur_id = task["eleveur_id"]
    completed_by = task.get("completed_by")
    return TaskOut(
        id=sid(task["_id"]),
        admin_id=sid(task["admin_id"]),
        eleveur_id=sid(eleveur_id),
        eleveur_name=names.get(eleveur_id),
        title=task["title"],
        description=task.get("description"),
        cow_id=task.get("cow_id"),
        task_type=task.get("task_type", "custom"),
        plan=task.get("plan", ""),
        alert_id=sid(task.get("alert_id")) if task.get("alert_id") else None,
        due_at=task.get("due_at"),
        status=task["status"],
        created_at=task["created_at"],
        completed_at=task.get("completed_at"),
        completed_by=sid(completed_by),
        completed_by_name=names.get(completed_by),
        completion_note=task.get("completion_note"),
        completed_weight_kg=task.get("completed_weight_kg"),
    ).model_dump(mode="json")


def _task_id(value: str) -> ObjectId | None:
    try:
        return ObjectId(value)
    except (InvalidId, TypeError):
        return None


async def _names_for_tasks(request: Request, admin: dict, tasks: list[dict]):
    ids = {
        identifier
        for task in tasks
        for identifier in (task["eleveur_id"], task.get("completed_by"))
        if isinstance(identifier, ObjectId)
    }
    if not ids:
        return {}
    cursor = request.app.state.cols.eleveurs.find(
        {"_id": {"$in": list(ids)}, **owner_filter(admin)},
        {"name": 1},
    )
    return {
        item["_id"]: item["name"]
        async for item in cursor
        if isinstance(item.get("name"), str)
    }


@router.post("", status_code=201)
async def create_task(
    body: TaskCreateIn,
    request: Request,
    admin: dict = Depends(admin_only),
):
    cols = request.app.state.cols
    eleveur_id = _task_id(body.eleveur_id)
    if eleveur_id is None:
        raise HTTPException(422, "eleveur_id must be a valid account ID")
    eleveur = await cols.eleveurs.find_one(
        {"_id": eleveur_id, **owner_filter(admin)},
        {"_id": 1, "name": 1},
    )
    if eleveur is None:
        raise HTTPException(404, "Éleveur not found")

    cow_id = None
    if body.cow_id is not None:
        try:
            cow_id = await owned_cow(request, admin, body.cow_id)
        except HTTPException as error:
            if error.status_code != 404:
                raise
            raise HTTPException(
                422, "cow_id must reference a cow owned by this admin"
            ) from error

    linked_alert = None
    alert_oid = None
    if body.alert_id is not None:
        alert_oid = _task_id(body.alert_id)
        if alert_oid is None:
            raise HTTPException(422, "alert_id must be a valid alert ID")
        linked_alert = await cols.alerts.find_one(
            {
                "_id": alert_oid,
                "admin_id": admin["id"],
                "source": "system",
                "type": {"$in": ["appetite_high", "appetite_low"]},
                "handled": {"$ne": True},
            }
        )
        if linked_alert is None:
            raise HTTPException(404, "Unhandled appetite alert not found")
        if cow_id is None and isinstance(linked_alert.get("cow_id"), str):
            cow_id = linked_alert["cow_id"]

    default_title, default_plan = TASK_DEFAULTS[body.task_type]
    title = body.title or default_title
    plan = body.plan if body.plan is not None else default_plan

    now = datetime.now(timezone.utc)
    task = {
        "admin_id": admin["admin_id"],
        "eleveur_id": eleveur_id,
        "title": title,
        "description": body.description,
        "cow_id": cow_id,
        "task_type": body.task_type,
        "plan": plan,
        "alert_id": alert_oid,
        "due_at": body.due_at,
        "status": "pending",
        "created_at": now,
        "completed_at": None,
        "completed_by": None,
        "completion_note": None,
        "completed_weight_kg": None,
    }
    result = await cols.tasks.insert_one(task)
    task["_id"] = result.inserted_id
    if alert_oid is not None:
        claimed = await cols.alerts.update_one(
            {
                "_id": alert_oid,
                "admin_id": admin["id"],
                "handled": {"$ne": True},
                "task_id": None,
            },
            {"$set": {"task_id": result.inserted_id, "handled": True}},
        )
        if claimed.matched_count == 0:
            await cols.tasks.delete_one({"_id": result.inserted_id})
            raise HTTPException(409, "Appetite alert has already been handled")
    names = (
        {eleveur_id: eleveur["name"]}
        if isinstance(eleveur.get("name"), str)
        else {}
    )
    return _task_out(task, names)


@router.get("")
async def list_admin_tasks(
    request: Request,
    eleveur_id: str | None = None,
    status: Literal["pending", "done"] | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    skip: int = Query(default=0, ge=0),
    admin: dict = Depends(admin_only),
):
    query = owner_filter(admin)
    if eleveur_id is not None:
        parsed_eleveur_id = _task_id(eleveur_id)
        if parsed_eleveur_id is None:
            raise HTTPException(422, "eleveur_id must be a valid account ID")
        query["eleveur_id"] = parsed_eleveur_id
    if status is not None:
        query["status"] = status

    cursor = (
        request.app.state.cols.tasks.find(query)
        .sort([("created_at", -1), ("_id", -1)])
        .skip(skip)
        .limit(limit)
    )
    tasks = [task async for task in cursor]
    names = await _names_for_tasks(request, admin, tasks)
    return [_task_out(task, names) for task in tasks]


@router.delete("/{task_id}")
async def delete_task(
    task_id: str,
    request: Request,
    admin: dict = Depends(admin_only),
):
    oid = _task_id(task_id)
    if oid is None:
        raise HTTPException(404, "Task not found")
    result = await request.app.state.cols.tasks.delete_one(
        {"_id": oid, **owner_filter(admin)}
    )
    if result.deleted_count == 0:
        raise HTTPException(404, "Task not found")
    return {"deleted": task_id}


def _require_eleveur(actor: dict):
    if actor["role"] != "eleveur":
        raise HTTPException(403, "Éleveur access required")


@router.get("/mine")
async def list_my_tasks(request: Request, actor: dict = Depends(get_actor)):
    _require_eleveur(actor)
    cursor = request.app.state.cols.tasks.find(
        {
            **owner_filter(actor),
            "eleveur_id": actor["id"],
            "status": "pending",
        }
    ).sort([("due_at", 1), ("created_at", -1), ("_id", -1)])
    tasks = [task async for task in cursor]
    return [_task_out(task, {}) for task in tasks]


@router.post("/{task_id}/done")
async def mark_task_done(
    task_id: str,
    request: Request,
    body: TaskCompletionIn = TaskCompletionIn(),
    actor: dict = Depends(get_actor),
):
    _require_eleveur(actor)
    oid = _task_id(task_id)
    if oid is None:
        raise HTTPException(404, "Task not found")

    cols = request.app.state.cols
    task = await cols.tasks.find_one_and_update(
        {
            "_id": oid,
            **owner_filter(actor),
            "eleveur_id": actor["id"],
            "status": "pending",
        },
        {
            "$set": {
                "status": "done",
                "completed_at": datetime.now(timezone.utc),
                "completed_by": actor["id"],
                "completion_note": body.completion_note,
                "completed_weight_kg": body.completed_weight_kg,
            }
        },
        return_document=ReturnDocument.AFTER,
    )
    newly_completed = task is not None
    if task is None:
        task = await cols.tasks.find_one(
            {
                "_id": oid,
                **owner_filter(actor),
                "eleveur_id": actor["id"],
                "status": "done",
            }
        )
    if task is None:
        raise HTTPException(404, "Task not found")
    if newly_completed:
        eleveur = await cols.eleveurs.find_one(
            {"_id": actor["id"], "admin_id": actor["admin_id"]},
            {"name": 1},
        )
        weight_text = (
            f" Poids relevé : {body.completed_weight_kg:g} kg."
            if body.completed_weight_kg is not None
            else ""
        )
        await cols.alerts.insert_one(
            new_alert(
                "system",
                "task_completed",
                "info",
                f"{(eleveur or {}).get('name') or 'L’éleveur'} a terminé "
                f"la tâche : {task['title']}.{weight_text}",
                task.get("cow_id"),
                task["admin_id"],
                actor["id"],
                datetime.now(timezone.utc),
                task_id=task["_id"],
                completion_note=body.completion_note,
                completed_weight_kg=body.completed_weight_kg,
            )
        )
    return _task_out(task, {})
