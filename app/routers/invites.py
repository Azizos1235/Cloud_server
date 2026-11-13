import hashlib
import secrets
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel

from app.config import settings
from app.deps import admin_only, rate_limit
from app.utils import as_utc, sid, to_oid

router = APIRouter(prefix="/invites")


class InviteCheckIn(BaseModel):
    code: str


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_invite(request: Request, admin: dict = Depends(admin_only)):
    now = datetime.now(timezone.utc)
    code = secrets.token_urlsafe(24)
    document = {
        "code_hash": hashlib.sha256(code.encode("utf-8")).hexdigest(),
        "admin_id": admin["id"],
        "created_at": now,
        "expires_at": now + timedelta(hours=settings.invite_ttl_hours),
        "used_at": None,
        "used_by": None,
    }
    result = await request.app.state.cols.invites.insert_one(document)
    return {
        "id": sid(result.inserted_id),
        "code": code,
        "qr_payload": f"cowfarm://invite?code={code}",
        "expires_at": document["expires_at"],
    }


@router.get("")
async def list_invites(request: Request, admin: dict = Depends(admin_only)):
    cur = request.app.state.cols.invites.find({"admin_id": admin["id"]}).sort("created_at", -1)
    invites = []
    now = datetime.now(timezone.utc)
    async for invite in cur:
        if invite.get("used_at") is not None:
            invite_status = "used"
        elif as_utc(invite["expires_at"]) <= now:
            invite_status = "expired"
        else:
            invite_status = "active"
        result = {
            "id": sid(invite["_id"]),
            "status": invite_status,
            "created_at": invite["created_at"],
            "expires_at": invite["expires_at"],
        }
        if invite_status == "used":
            user = await request.app.state.cols.eleveurs.find_one(
                {"_id": invite.get("used_by")}, {"name": 1}
            )
            result["used_by_name"] = user.get("name") if user else None
        invites.append(result)
    return invites


@router.delete("/{invite_id}")
async def delete_invite(invite_id: str, request: Request, admin: dict = Depends(admin_only)):
    now = datetime.now(timezone.utc)
    result = await request.app.state.cols.invites.delete_one({
        "_id": to_oid(invite_id),
        "admin_id": admin["id"],
        "used_at": None,
        "expires_at": {"$gt": now},
    })
    if result.deleted_count == 0:
        raise HTTPException(404, "Invitation not found or inactive")
    return {"deleted": invite_id}


@router.post("/check", dependencies=[Depends(rate_limit)])
async def check_invite(body: InviteCheckIn, request: Request):
    code_hash = hashlib.sha256(body.code.encode("utf-8")).hexdigest()
    now = datetime.now(timezone.utc)
    invite = await request.app.state.cols.invites.find_one({
        "code_hash": code_hash,
        "used_at": None,
        "expires_at": {"$gt": now},
    })
    if invite is None:
        raise HTTPException(404, "Invitation invalid or expired")
    admin = await request.app.state.cols.admins.find_one({"_id": invite["admin_id"]}, {"name": 1})
    if admin is None:
        raise HTTPException(404, "Invitation invalid or expired")
    return {"valid": True, "admin_name": admin.get("name")}
