import math
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel

from app.deps import get_actor_allow_expired, rate_limit
from app.utils import as_utc, is_subscription_expired, sid

router = APIRouter()


class SignupRequestIn(BaseModel):
    name: str
    email: str
    phone: Optional[str] = None
    message: Optional[str] = None


class RenewalRequestIn(BaseModel):
    message: Optional[str] = None


def _email(value: str) -> str:
    return value.strip().lower()


@router.post("/subscription-requests", status_code=status.HTTP_201_CREATED,
             dependencies=[Depends(rate_limit)])
async def create_signup_request(body: SignupRequestIn, request: Request):
    cols = request.app.state.cols
    email = _email(body.email)
    if await cols.admins.find_one({"email": email}, {"_id": 1}):
        raise HTTPException(409, "An admin with this email already exists")
    if await cols.subscription_requests.find_one({"email": email, "status": "pending"}, {"_id": 1}):
        raise HTTPException(409, "A pending request with this email already exists")
    document = {
        "type": "signup",
        "name": body.name.strip(),
        "email": email,
        "phone": body.phone,
        "message": body.message,
        "status": "pending",
        "created_at": datetime.now(timezone.utc),
        "decided_at": None,
    }
    result = await cols.subscription_requests.insert_one(document)
    return {"id": sid(result.inserted_id), "status": "pending"}


@router.get("/subscription/me")
async def subscription_me(request: Request, actor: dict = Depends(get_actor_allow_expired)):
    if actor["role"] != "admin":
        raise HTTPException(403, "Admin access required")
    admin = await request.app.state.cols.admins.find_one({"_id": actor["id"]})
    if admin is None:
        raise HTTPException(401, "Invalid token")
    expires_at = admin.get("subscription_expires_at")
    expired = is_subscription_expired(expires_at)
    days_left = 0
    if expires_at is not None and not expired:
        delta = as_utc(expires_at) - datetime.now(timezone.utc)
        days_left = max(0, math.ceil(delta.total_seconds() / 86400))
    return {
        "status": "expired" if expired else "active",
        "expires_at": expires_at,
        "days_left": days_left,
    }


@router.post("/subscription/renewal-request", status_code=status.HTTP_201_CREATED)
async def create_renewal_request(
    body: RenewalRequestIn,
    request: Request,
    actor: dict = Depends(get_actor_allow_expired),
):
    if actor["role"] != "admin":
        raise HTTPException(403, "Admin access required")
    cols = request.app.state.cols
    admin = await cols.admins.find_one({"_id": actor["id"]})
    if admin is None:
        raise HTTPException(401, "Invalid token")
    if await cols.subscription_requests.find_one(
        {"type": "renewal", "admin_id": actor["id"], "status": "pending"}, {"_id": 1}
    ):
        raise HTTPException(409, "A pending renewal request already exists")
    document = {
        "type": "renewal",
        "name": admin.get("name"),
        "email": admin["email"],
        "phone": admin.get("phone"),
        "message": body.message,
        "admin_id": actor["id"],
        "status": "pending",
        "created_at": datetime.now(timezone.utc),
        "decided_at": None,
    }
    result = await cols.subscription_requests.insert_one(document)
    return {"id": sid(result.inserted_id)}
