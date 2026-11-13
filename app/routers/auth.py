import hashlib
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from pymongo.errors import DuplicateKeyError

from app.deps import get_actor_allow_expired, rate_limit
from app.security import create_access_token, hash_password, verify_password
from app.utils import sid

router = APIRouter(prefix="/auth")


class LoginIn(BaseModel):
    email: str
    password: str


class EleveurRegisterIn(BaseModel):
    invite_code: str
    name: str
    email: str
    password: str = Field(min_length=8)
    phone: Optional[str] = None


class ChangePasswordIn(BaseModel):
    current_password: str
    new_password: str = Field(min_length=8)


def _email(value: str) -> str:
    return value.strip().lower()


def _public_user(document: dict, role: str) -> dict:
    user = {
        "id": sid(document["_id"]),
        "name": document.get("name"),
        "email": document["email"],
        "role": role,
    }
    if role == "eleveur":
        user["admin_id"] = sid(document.get("admin_id"))
    if role == "admin":
        user["must_change_password"] = document.get("must_change_password", False)
        user["subscription_expires_at"] = document.get("subscription_expires_at")
    return user


async def _login(request: Request, role: str, body: LoginIn):
    cols = request.app.state.cols
    collection = cols.admins if role == "admin" else cols.eleveurs if role == "eleveur" else cols.superusers
    user = await collection.find_one({"email": _email(body.email)})
    if user is None or not verify_password(body.password, user.get("password_hash", "")):
        raise HTTPException(401, "Invalid email or password")
    return {
        "token": create_access_token(str(user["_id"]), role),
        "user": _public_user(user, role),
    }


@router.post("/admin/login")
async def admin_login(body: LoginIn, request: Request, _: None = Depends(rate_limit)):
    return await _login(request, "admin", body)


@router.post("/eleveur/login")
async def eleveur_login(body: LoginIn, request: Request, _: None = Depends(rate_limit)):
    return await _login(request, "eleveur", body)


@router.post("/superuser/login")
async def superuser_login(body: LoginIn, request: Request, _: None = Depends(rate_limit)):
    return await _login(request, "superuser", body)


@router.post("/eleveur/register", status_code=201)
async def eleveur_register(
    body: EleveurRegisterIn, request: Request, _: None = Depends(rate_limit)
):
    cols = request.app.state.cols
    email = _email(body.email)
    if await cols.eleveurs.find_one({"email": email}, {"_id": 1}):
        raise HTTPException(409, "Email already registered")

    password_hash = hash_password(body.password)
    now = datetime.now(timezone.utc)
    code_hash = hashlib.sha256(body.invite_code.encode("utf-8")).hexdigest()
    invite = await cols.invites.find_one_and_update(
        {"code_hash": code_hash, "used_at": None, "expires_at": {"$gt": now}},
        {"$set": {"used_at": now}},
    )
    if invite is None:
        raise HTTPException(404, "Invitation invalid or expired")

    document = {
        "name": body.name.strip(),
        "email": email,
        "phone": body.phone,
        "password_hash": password_hash,
        "admin_id": invite["admin_id"],
        "created_at": now,
    }
    try:
        inserted = await cols.eleveurs.insert_one(document)
    except DuplicateKeyError:
        await cols.invites.update_one(
            {"_id": invite["_id"], "used_at": now},
            {"$set": {"used_at": None, "used_by": None}},
        )
        raise HTTPException(409, "Email already registered")
    document["_id"] = inserted.inserted_id
    await cols.invites.update_one(
        {"_id": invite["_id"], "used_at": now},
        {"$set": {"used_by": inserted.inserted_id}},
    )
    return {
        "token": create_access_token(str(inserted.inserted_id), "eleveur"),
        "user": _public_user(document, "eleveur"),
    }


@router.get("/me")
async def auth_me(request: Request, actor: dict = Depends(get_actor_allow_expired)):
    cols = request.app.state.cols
    collection = cols.admins if actor["role"] == "admin" else cols.eleveurs
    user = await collection.find_one({"_id": actor["id"]})
    if user is None:
        raise HTTPException(401, "Invalid token")
    return _public_user(user, actor["role"])


@router.post("/change-password")
async def change_password(
    body: ChangePasswordIn,
    request: Request,
    actor: dict = Depends(get_actor_allow_expired),
):
    if actor["role"] != "admin":
        raise HTTPException(403, "Admin access required")
    cols = request.app.state.cols
    admin = await cols.admins.find_one({"_id": actor["id"]})
    if admin is None:
        raise HTTPException(401, "Invalid token")
    if not verify_password(body.current_password, admin.get("password_hash", "")):
        raise HTTPException(401, "Invalid current password")
    await cols.admins.update_one(
        {"_id": actor["id"]},
        {"$set": {"password_hash": hash_password(body.new_password), "must_change_password": False}},
    )
    return {"ok": True}
