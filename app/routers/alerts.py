"""Alerte: AlerteSysteme (see services/alerts.py) and AlerteAdministrateur."""
from datetime import datetime, timezone
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from app.cow_ids import normalize_cow_identifier
from app.deps import admin_only, get_actor, owned_cow, owner_filter
from app.utils import alert_out, to_oid

router = APIRouter(prefix="/alerts")


class AdminAlertIn(BaseModel):
    message: str
    severity: Literal["info", "warning", "critical"] = "info"
    cow_id: Optional[str] = None
    action: Optional[Literal["sell", "vet", "butcher"]] = None


@router.post("")
async def create_admin_alert(body: AdminAlertIn, request: Request, admin: dict = Depends(admin_only)):
    cols = request.app.state.cols
    cow_id = await owned_cow(request, admin, body.cow_id) if body.cow_id is not None else None
    now = datetime.now(timezone.utc)
    await cols.alerts.insert_one({
        "source": "admin",
        "type": "custom_message",
        "severity": body.severity,
        "message": body.message,
        "cow_id": cow_id,
        "admin_id": admin["admin_id"],
        "action": body.action,
        "status": "unread",
        "created_at": now,
        "read_at": None,
    })
    return {"created": 1}


@router.get("")
async def list_alerts(request: Request, status: Optional[Literal["unread", "read"]] = None,
                      limit: int = Query(50, ge=1, le=200), actor: dict = Depends(get_actor)):
    flt = {"admin_id": actor["admin_id"]}
    if status:
        flt["status"] = status
    cur = request.app.state.cols.alerts.find(flt).sort("created_at", -1).limit(limit)
    alerts = [a async for a in cur]
    identifiers = {
        normalize_cow_identifier(alert["cow_id"])
        for alert in alerts
        if isinstance(alert.get("cow_id"), str) and alert["cow_id"].strip()
    }

    public_ids = {}
    if identifiers:
        profiles = request.app.state.cols.profiles.find(
            {
                "$or": [
                    {"_id": {"$in": list(identifiers)}},
                    {"cow_id": {"$in": list(identifiers)}},
                ],
                **owner_filter(actor),
            },
            {"_id": 1, "cow_id": 1},
        )
        async for profile in profiles:
            cow_id = profile.get("cow_id")
            if not isinstance(cow_id, str) or not cow_id.strip():
                continue
            public_id = cow_id.strip().upper()
            public_ids[normalize_cow_identifier(str(profile["_id"]))] = public_id
            public_ids[normalize_cow_identifier(cow_id)] = public_id

    result = []
    for alert in alerts:
        output = alert_out(alert)
        alert_cow_id = alert.get("cow_id")
        if isinstance(alert_cow_id, str):
            output["cow_id"] = public_ids.get(
                normalize_cow_identifier(alert_cow_id), alert_cow_id
            )
        result.append(output)
    return result


@router.patch("/{alert_id}/read")
async def mark_read(alert_id: str, request: Request, actor: dict = Depends(get_actor)):
    if actor["role"] != "eleveur":
        raise HTTPException(403, "Only the éleveur marks his alerts as read")
    res = await request.app.state.cols.alerts.update_one(
        {"_id": to_oid(alert_id), "admin_id": actor["admin_id"]},
        {"$set": {"status": "read", "read_at": datetime.now(timezone.utc)}})
    if res.matched_count == 0:
        raise HTTPException(404, "Alert not found")
    return {"status": "read"}
