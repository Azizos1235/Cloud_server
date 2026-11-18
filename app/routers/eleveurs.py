import hashlib
import secrets
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Request

from app.deps import admin_only
from app.utils import sid, to_oid

router = APIRouter(prefix="/eleveurs")


@router.get("")
async def list_eleveurs(request: Request, admin: dict = Depends(admin_only)):
    cur = request.app.state.cols.eleveurs.find(
        {"admin_id": admin["admin_id"]},
        {"name": 1, "created_at": 1},
    ).sort("created_at", -1)
    result = []
    async for eleveur in cur:
        result.append({
            "id": sid(eleveur["_id"]),
            "name": eleveur.get("name"),
            "created_at": eleveur.get("created_at"),
        })
    return result


@router.post("/{eleveur_id}/qr", status_code=201)
async def create_eleveur_qr(
    eleveur_id: str,
    request: Request,
    admin: dict = Depends(admin_only),
):
    cols = request.app.state.cols
    user_id = to_oid(eleveur_id)
    eleveur = await cols.eleveurs.find_one(
        {"_id": user_id, "admin_id": admin["id"]},
        {"name": 1},
    )
    if eleveur is None:
        raise HTTPException(404, "Éleveur not found")

    now = datetime.now(timezone.utc)
    token = secrets.token_urlsafe(32)
    expires_at = now + timedelta(minutes=3)
    await cols.qr_tokens.update_one(
        {"eleveur_id": user_id, "used": False},
        {
            "$set": {
                "token_hash": hashlib.sha256(token.encode("utf-8")).hexdigest(),
                "eleveur_id": user_id,
                "admin_id": admin["id"],
                "created_at": now,
                "expires_at": expires_at,
                "used": False,
                "used_at": None,
                "used_by": None,
                "revoked": False,
                "failed_attempts": 0,
            }
        },
        upsert=True,
    )
    return {
        "eleveur_id": str(user_id),
        "eleveur_name": eleveur.get("name"),
        "qr_payload": f"cowfarm://login?token={token}",
        "expires_at": expires_at,
    }


@router.delete("/{eleveur_id}")
async def delete_eleveur(eleveur_id: str, request: Request, admin: dict = Depends(admin_only)):
    cols = request.app.state.cols
    oid = to_oid(eleveur_id)
    eleveur_filter = {"_id": oid, "admin_id": admin["admin_id"]}
    if await cols.eleveurs.find_one(eleveur_filter, {"_id": 1}) is None:
        raise HTTPException(404, "Éleveur not found")
    await cols.tasks.delete_many(
        {"eleveur_id": oid, "admin_id": admin["admin_id"]}
    )
    result = await cols.eleveurs.delete_one(eleveur_filter)
    if result.deleted_count == 0:
        raise HTTPException(404, "Éleveur not found")
    return {"deleted": eleveur_id}
