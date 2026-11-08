"""Alerte: AlerteSysteme (see services/alerts.py) and AlerteAdministrateur."""
from datetime import datetime, timezone
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

from app.deps import admin_only, get_actor, owned_cow
from app.utils import alert_out, new_alert, to_oid

router = APIRouter(prefix="/alerts")


class AdminAlertIn(BaseModel):
    message: str = Field(min_length=1, max_length=500)
    severity: Literal["info", "warning", "critical"] = "info"
    eleveur_id: Optional[str] = None      # empty = all the admin's éleveurs
    cow_id: Optional[str] = None


@router.post("")
async def create_admin_alert(body: AdminAlertIn, request: Request, admin: dict = Depends(admin_only)):
    cols = request.app.state.cols
    cow_id = await owned_cow(request, admin, body.cow_id) if body.cow_id else None
    if body.eleveur_id:
        eid = to_oid(body.eleveur_id)
        if not await cols.eleveurs.find_one({"_id": eid, "admin_id": admin["id"]}, {"_id": 1}):
            raise HTTPException(400, "This éleveur does not belong to you")
        targets = [eid]
    else:
        targets = [e["_id"] async for e in cols.eleveurs.find({"admin_id": admin["id"]}, {"_id": 1})]
    if not targets:
        raise HTTPException(400, "No éleveur to notify")
    now = datetime.now(timezone.utc)
    # one alert per éleveur, so each one has his own read/unread status
    await cols.alerts.insert_many([
        new_alert("admin", "custom_message", body.severity, body.message.strip(),
                  cow_id, admin["id"], eid, now) for eid in targets])
    return {"created": len(targets)}


@router.get("")
async def list_alerts(request: Request, status: Optional[Literal["unread", "read"]] = None,
                      limit: int = Query(50, ge=1, le=200), actor: dict = Depends(get_actor)):
    flt = ({"admin_id": actor["id"]} if actor["role"] == "admin" else {"eleveur_id": actor["id"]})
    if status:
        flt["status"] = status
    cur = request.app.state.cols.alerts.find(flt).sort("created_at", -1).limit(limit)
    return [alert_out(a) async for a in cur]


@router.patch("/{alert_id}/read")
async def mark_read(alert_id: str, request: Request, actor: dict = Depends(get_actor)):
    if actor["role"] != "eleveur":
        raise HTTPException(403, "Only the éleveur marks his alerts as read")
    res = await request.app.state.cols.alerts.update_one(
        {"_id": to_oid(alert_id), "eleveur_id": actor["id"]},
        {"$set": {"status": "read", "read_at": datetime.now(timezone.utc)}})
    if res.matched_count == 0:
        raise HTTPException(404, "Alert not found")
    return {"status": "read"}
