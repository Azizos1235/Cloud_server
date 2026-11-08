from typing import Optional

from bson import ObjectId
from bson.errors import InvalidId
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .database import Cols
from .security import decode_token
from .services.cow_service import owns

bearer = HTTPBearer()


async def resolve_actor(cols: Cols, token: str) -> Optional[dict]:
    """Token -> {"id": ObjectId, "role": "admin" | "eleveur"}, or None."""
    decoded = decode_token(token)
    if decoded is None:
        return None
    role, user_id = decoded
    coll = cols.admins if role == "admin" else cols.eleveurs
    user = await coll.find_one({"_id": user_id}, {"active": 1})
    if user is None or user.get("active") is False:
        return None
    return {"id": user_id, "role": role}


async def get_actor(request: Request, creds: HTTPAuthorizationCredentials = Depends(bearer)):
    actor = await resolve_actor(request.app.state.cols, creds.credentials)
    if actor is None:
        raise HTTPException(401, "Invalid or expired token")
    return actor


async def admin_only(actor: dict = Depends(get_actor)):
    if actor["role"] != "admin":
        raise HTTPException(403, "Admin only")
    return actor


async def owned_cow(request: Request, actor: dict, cow_id: str) -> str:
    cid = cow_id.strip().upper()
    if not await owns(request.app.state.cols, actor, cid):
        raise HTTPException(404, f"No cow {cid}")
    return cid


def to_oid(value: str) -> ObjectId:
    try:
        return ObjectId(value)
    except (InvalidId, TypeError):
        raise HTTPException(400, "Invalid id")
