"""Authentication: who is calling? (the JWT is issued by the auth backend)"""
from typing import Optional

import jwt
from bson import ObjectId
from bson.errors import InvalidId
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.config import settings
from app.db import Cols

bearer = HTTPBearer()


async def resolve_actor(cols: Cols, token: str) -> Optional[dict]:
    """JWT -> {"id": ObjectId, "role": "admin" | "eleveur"}, or None if invalid."""
    try:
        data = jwt.decode(token, settings.jwt_secret, algorithms=["HS256"])
        role, user_id = data["role"], ObjectId(data["sub"])
    except (jwt.PyJWTError, KeyError, InvalidId, TypeError):
        return None
    if role not in ("admin", "eleveur"):
        return None
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


def owner_filter(actor: dict) -> dict:
    """An admin sees his cows (admin_id), an éleveur sees the cows he looks after (eleveur_id)."""
    return {"admin_id": actor["id"]} if actor["role"] == "admin" else {"eleveur_id": actor["id"]}


async def owns(cols: Cols, actor: dict, cow_id: str) -> bool:
    return await cols.profiles.find_one({"_id": cow_id, **owner_filter(actor)}, {"_id": 1}) is not None


async def owned_cow(request: Request, actor: dict, cow_id: str) -> str:
    cid = cow_id.strip().upper()
    if not await owns(request.app.state.cols, actor, cid):
        raise HTTPException(404, f"No cow {cid}")
    return cid
