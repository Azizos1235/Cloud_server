from datetime import datetime, timedelta, timezone
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from app.config import settings
from app.deps import get_superuser
from app.security import hash_password, new_temporary_password
from app.utils import extended_subscription, is_subscription_expired, sid, to_oid

router = APIRouter(prefix="/superuser")


class MonthsIn(BaseModel):
    months: int = Field(default=1, ge=1, le=12)


class RejectIn(BaseModel):
    reason: Optional[str] = None


async def _release_request_claim(cols, request_id, decided_at):
    await cols.subscription_requests.update_one(
        {"_id": request_id, "status": "approved", "decided_at": decided_at},
        {"$set": {"status": "pending", "decided_at": None}, "$unset": {"decision_reason": ""}},
    )


@router.get("/requests")
async def list_requests(
    request: Request,
    status_filter: Optional[Literal["pending", "approved", "rejected"]] = Query(default=None, alias="status"),
    type_filter: Optional[Literal["signup", "renewal"]] = Query(default=None, alias="type"),
    _: dict = Depends(get_superuser),
):
    flt = {}
    if status_filter:
        flt["status"] = status_filter
    if type_filter:
        flt["type"] = type_filter
    cur = request.app.state.cols.subscription_requests.find(flt).sort("created_at", -1)
    result = []
    async for item in cur:
        out = {
            "id": sid(item["_id"]),
            "type": item["type"],
            "name": item.get("name"),
            "email": item.get("email"),
            "phone": item.get("phone"),
            "message": item.get("message"),
            "status": item["status"],
            "created_at": item["created_at"],
        }
        if item.get("admin_id") is not None:
            out["admin_id"] = sid(item["admin_id"])
        result.append(out)
    return result


@router.post("/requests/{request_id}/approve")
async def approve_request(
    request_id: str,
    request: Request,
    response: Response,
    body: MonthsIn = MonthsIn(),
    _: dict = Depends(get_superuser),
):
    cols = request.app.state.cols
    oid = to_oid(request_id)
    now = datetime.now(timezone.utc)
    signup = await cols.subscription_requests.find_one_and_update(
        {"_id": oid, "status": "pending"},
        {"$set": {"status": "approved", "decided_at": now}},
        return_document=ReturnDocument.BEFORE,
    )
    if signup is None:
        raise HTTPException(409, "Request is no longer pending")

    if signup["type"] == "signup":
        temporary_password = new_temporary_password()
        admin = {
            "name": signup["name"],
            "email": signup["email"],
            "phone": signup.get("phone"),
            "password_hash": hash_password(temporary_password),
            "must_change_password": True,
            "subscription_expires_at": now + timedelta(
                days=body.months * settings.subscription_days_per_month
            ),
            "created_at": now,
            "status": "active",
        }
        try:
            inserted = await cols.admins.insert_one(admin)
        except DuplicateKeyError:
            await _release_request_claim(cols, oid, now)
            raise HTTPException(409, "An admin with this email already exists")
        except Exception:
            await _release_request_claim(cols, oid, now)
            raise
        response.status_code = 201
        return {
            "admin": {
                "id": sid(inserted.inserted_id),
                "name": admin["name"],
                "email": admin["email"],
                "subscription_expires_at": admin["subscription_expires_at"],
            },
            "temp_password": temporary_password,
        }

    if signup["type"] == "renewal":
        admin = await cols.admins.find_one({"_id": signup.get("admin_id")})
        if admin is None:
            await _release_request_claim(cols, oid, now)
            raise HTTPException(409, "The requesting admin no longer exists")
        expires_at = extended_subscription(
            admin.get("subscription_expires_at"), body.months,
            settings.subscription_days_per_month, now,
        )
        result = await cols.admins.update_one(
            {"_id": admin["_id"]},
            {"$set": {"subscription_expires_at": expires_at, "status": "active"}},
        )
        if result.matched_count == 0:
            await _release_request_claim(cols, oid, now)
            raise HTTPException(409, "The requesting admin no longer exists")
        return {"admin": {
            "id": sid(admin["_id"]),
            "name": admin.get("name"),
            "email": admin["email"],
            "subscription_expires_at": expires_at,
        }}

    await _release_request_claim(cols, oid, now)
    raise HTTPException(409, "Unsupported request type")


@router.post("/requests/{request_id}/reject")
async def reject_request(
    request_id: str,
    request: Request,
    body: Optional[RejectIn] = None,
    _: dict = Depends(get_superuser),
):
    fields = {"status": "rejected", "decided_at": datetime.now(timezone.utc)}
    if body and body.reason:
        fields["decision_reason"] = body.reason
    result = await request.app.state.cols.subscription_requests.update_one(
        {"_id": to_oid(request_id), "status": "pending"}, {"$set": fields}
    )
    if result.matched_count == 0:
        raise HTTPException(409, "Request is no longer pending")
    return {"ok": True}


@router.get("/admins")
async def list_admins(request: Request, _: dict = Depends(get_superuser)):
    cols = request.app.state.cols
    cur = cols.admins.find({}).sort("created_at", -1)
    result = []
    now = datetime.now(timezone.utc)
    async for admin in cur:
        expires_at = admin.get("subscription_expires_at")
        eleveurs_count = await cols.eleveurs.count_documents({"admin_id": admin["_id"]})
        cows_count = await cols.profiles.count_documents({"admin_id": admin["_id"]})
        result.append({
            "id": sid(admin["_id"]),
            "name": admin.get("name"),
            "email": admin.get("email"),
            "phone": admin.get("phone"),
            "created_at": admin.get("created_at"),
            "subscription_expires_at": expires_at,
            "status": "expired" if is_subscription_expired(expires_at, now) else "active",
            "eleveurs_count": eleveurs_count,
            "cows_count": cows_count,
        })
    return result


@router.post("/admins/{admin_id}/renew")
async def renew_admin(
    admin_id: str, request: Request, body: MonthsIn = MonthsIn(), _: dict = Depends(get_superuser)
):
    cols = request.app.state.cols
    oid = to_oid(admin_id)
    admin = await cols.admins.find_one({"_id": oid})
    if admin is None:
        raise HTTPException(404, "Admin not found")
    expires_at = extended_subscription(
        admin.get("subscription_expires_at"), body.months,
        settings.subscription_days_per_month,
    )
    result = await cols.admins.update_one(
        {"_id": oid}, {"$set": {"subscription_expires_at": expires_at, "status": "active"}}
    )
    if result.matched_count == 0:
        raise HTTPException(404, "Admin not found")
    return {"id": admin_id, "subscription_expires_at": expires_at}


@router.post("/admins/{admin_id}/reset-password")
async def reset_admin_password(
    admin_id: str, request: Request, _: dict = Depends(get_superuser)
):
    cols = request.app.state.cols
    oid = to_oid(admin_id)
    temporary_password = new_temporary_password()
    result = await cols.admins.update_one(
        {"_id": oid},
        {"$set": {"password_hash": hash_password(temporary_password), "must_change_password": True}},
    )
    if result.matched_count == 0:
        raise HTTPException(404, "Admin not found")
    return {"temp_password": temporary_password}


@router.delete("/admins/{admin_id}")
async def delete_admin(
    admin_id: str,
    request: Request,
    confirm: bool = Query(default=False),
    _: dict = Depends(get_superuser),
):
    if not confirm:
        raise HTTPException(400, "Set confirm=true to delete this admin")
    cols = request.app.state.cols
    oid = to_oid(admin_id)
    if not await cols.admins.find_one({"_id": oid}, {"_id": 1}):
        raise HTTPException(404, "Admin not found")

    eleveur_ids = [item["_id"] async for item in cols.eleveurs.find({"admin_id": oid}, {"_id": 1})]
    cow_ids = [item["_id"] async for item in cols.profiles.find({"admin_id": oid}, {"_id": 1})]
    alert_filters = [{"admin_id": oid}]
    if eleveur_ids:
        alert_filters.append({"eleveur_id": {"$in": eleveur_ids}})
    if cow_ids:
        alert_filters.append({"cow_id": {"$in": cow_ids}})

    await cols.eleveurs.delete_many({"admin_id": oid})
    await cols.invites.delete_many({"admin_id": oid})
    await cols.profiles.delete_many({"admin_id": oid})
    await cols.alerts.delete_many({"$or": alert_filters})
    await cols.admins.delete_one({"_id": oid})
    return {"deleted": admin_id}
