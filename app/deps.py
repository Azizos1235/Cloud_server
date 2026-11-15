import time
from collections import deque

from bson import ObjectId
from bson.errors import InvalidId
from fastapi import Depends, HTTPException, Request
from jwt import InvalidTokenError

from app.cow_ids import normalize_cow_identifier, resolve_cow_id
from app.security import decode_access_token
from app.utils import is_subscription_expired

_requests_by_ip: dict[str, deque[float]] = {}


def _bearer_token(request: Request) -> str:
    value = request.headers.get("authorization", "")
    scheme, separator, token = value.partition(" ")
    if not separator or scheme.lower() != "bearer" or not token.strip():
        raise HTTPException(401, "Not authenticated")
    return token.strip()


async def _load_actor(cols, token: str, *, allow_expired: bool, allow_password_change: bool = False):
    try:
        claims = decode_access_token(token)
    except InvalidTokenError:
        raise HTTPException(401, "Invalid or expired token")

    role = claims.get("role")
    if role == "superuser":
        raise HTTPException(403, "Superuser tokens cannot access this endpoint")
    if role not in ("admin", "eleveur"):
        raise HTTPException(401, "Invalid token")
    subject = claims.get("sub")
    if not isinstance(subject, str):
        raise HTTPException(401, "Invalid token")
    try:
        user_id = ObjectId(subject)
    except (InvalidId, TypeError):
        raise HTTPException(401, "Invalid token")

    collection = cols.admins if role == "admin" else cols.eleveurs
    user = await collection.find_one({"_id": user_id})
    if user is None:
        raise HTTPException(401, "Invalid token")

    if role == "admin":
        if not allow_password_change and user.get("must_change_password", False):
            raise HTTPException(403, "Password change required")
        if not allow_expired and is_subscription_expired(user.get("subscription_expires_at")):
            raise HTTPException(402, "Subscription expired")
        admin_id = user_id
    else:
        admin_id = user.get("admin_id")
        if not isinstance(admin_id, ObjectId):
            raise HTTPException(401, "Invalid token")
        admin = await cols.admins.find_one({"_id": admin_id})
        if admin is None:
            raise HTTPException(401, "Invalid token")
        if not allow_expired and is_subscription_expired(admin.get("subscription_expires_at")):
            raise HTTPException(402, "Subscription expired")

    return {"id": user_id, "role": role, "admin_id": admin_id}


async def get_actor(request: Request):
    return await _load_actor(request.app.state.cols, _bearer_token(request), allow_expired=False)


async def get_actor_allow_expired(request: Request):
    return await _load_actor(
        request.app.state.cols, _bearer_token(request), allow_expired=True, allow_password_change=True
    )


async def get_superuser(request: Request):
    try:
        claims = decode_access_token(_bearer_token(request))
    except InvalidTokenError:
        raise HTTPException(401, "Invalid or expired token")
    role = claims.get("role")
    if role != "superuser":
        raise HTTPException(403, "Superuser access required")
    subject = claims.get("sub")
    if not isinstance(subject, str):
        raise HTTPException(401, "Invalid token")
    try:
        user_id = ObjectId(subject)
    except (InvalidId, TypeError):
        raise HTTPException(401, "Invalid token")
    if not await request.app.state.cols.superusers.find_one({"_id": user_id}, {"_id": 1}):
        raise HTTPException(401, "Invalid token")
    return {"id": user_id, "role": "superuser"}


async def admin_only(actor: dict = Depends(get_actor)):
    if actor["role"] != "admin":
        raise HTTPException(403, "Admin access required")
    return actor


async def admin_or_eleveur(actor: dict = Depends(get_actor)) -> ObjectId:
    if actor.get("role") not in ("admin", "eleveur"):
        raise HTTPException(403, "Admin or éleveur access required")
    admin_id = actor.get("admin_id")
    if not isinstance(admin_id, ObjectId):
        raise HTTPException(401, "Invalid token")
    return admin_id


def owner_filter(actor: dict) -> dict:
    return {"admin_id": actor["admin_id"]}


async def owns(cols, actor: dict, cow_id: str) -> bool:
    cid = normalize_cow_identifier(cow_id)
    result = await cols.profiles.find_one(
        {"$or": [{"_id": cid}, {"cow_id": cid}], **owner_filter(actor)},
        {"_id": 1, "cow_id": 1},
    )
    return result is not None


async def owned_cow(request: Request, actor: dict, cow_id: str) -> str:
    cols = request.app.state.cols
    identifier = normalize_cow_identifier(cow_id)
    profile = await cols.profiles.find_one(
        {"$or": [{"_id": identifier}, {"cow_id": identifier}], **owner_filter(actor)},
        {"_id": 1, "cow_id": 1},
    )
    if profile is None:
        raise HTTPException(404, f"No profile for cow {identifier}")

    rfid = normalize_cow_identifier(profile["_id"])
    resolved = await resolve_cow_id(cols, rfid)
    if resolved:
        return resolved
    profile_cow_id = profile.get("cow_id")
    if isinstance(profile_cow_id, str) and profile_cow_id.strip():
        return profile_cow_id.strip().upper()
    # Compatibility for profiles created before the RFID/cow-code schema.
    return rfid


async def resolve_actor(cols, token: str):
    try:
        return await _load_actor(cols, token, allow_expired=False)
    except HTTPException:
        return None


async def rate_limit(request: Request):
    forwarded = request.headers.get("x-forwarded-for", "")
    ip = forwarded.split(",", 1)[0].strip() if forwarded else ""
    if not ip and request.client is not None:
        ip = request.client.host
    if not ip:
        ip = "unknown"

    now = time.monotonic()
    for known_ip, timestamps in list(_requests_by_ip.items()):
        while timestamps and timestamps[0] <= now - 60:
            timestamps.popleft()
        if not timestamps:
            del _requests_by_ip[known_ip]

    recent = _requests_by_ip.setdefault(ip, deque())
    while recent and recent[0] <= now - 60:
        recent.popleft()
    if len(recent) >= 10:
        raise HTTPException(429, "Too many requests")
    recent.append(now)
