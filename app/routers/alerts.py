"""Alerte: AlerteSysteme (see services/alerts.py) and AlerteAdministrateur."""
from datetime import datetime, timezone
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from app.deps import admin_only, get_actor, owned_cow
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
    return [alert_out(a) async for a in cur]


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
