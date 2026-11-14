import hashlib
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, field_validator
from jwt import InvalidTokenError

from app.deps import _bearer_token, get_actor_allow_expired, get_superuser, rate_limit
from app.security import create_access_token, decode_access_token, hash_password, verify_password
from app.utils import sid

router = APIRouter(prefix="/auth")
log = logging.getLogger(__name__)


class LoginIn(BaseModel):
    email: str
    password: str


class EleveurRegisterIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    invite_code: str
    name: str

    @field_validator("name")
    @classmethod
    def trim_and_validate_name(cls, value: str) -> str:
        value = value.strip()
        if not 2 <= len(value) <= 60:
            raise ValueError("name must be between 2 and 60 characters")
        return value


class ChangePasswordIn(BaseModel):
    current_password: str
    new_password: str = Field(min_length=8)


def _email(value: str) -> str:
    return value.strip().lower()


def _public_user(document: dict, role: str) -> dict:
    user = {
        "id": sid(document["_id"]),
        "name": document.get("name"),
        "role": role,
    }
    if role != "eleveur":
        user["email"] = document["email"]
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


@router.post("/superuser/login")
async def superuser_login(body: LoginIn, request: Request, _: None = Depends(rate_limit)):
    return await _login(request, "superuser", body)


@router.post("/eleveur/register", status_code=201)
async def eleveur_register(
    body: EleveurRegisterIn, request: Request, _: None = Depends(rate_limit)
):
    cols = request.app.state.cols
    now = datetime.now(timezone.utc)
    code_hash = hashlib.sha256(body.invite_code.encode("utf-8")).hexdigest()
    invite = await cols.invites.find_one_and_update(
        {"code_hash": code_hash, "used_at": None, "expires_at": {"$gt": now}},
        {"$set": {"used_at": now}},
    )
    if invite is None:
        raise HTTPException(404, "Invitation invalid or expired")

    document = {
        "name": body.name,
        "admin_id": invite["admin_id"],
        "created_at": now,
    }
    inserted_id = None
    try:
        inserted = await cols.eleveurs.insert_one(document)
        inserted_id = inserted.inserted_id
        invite_update = await cols.invites.update_one(
            {"_id": invite["_id"], "used_at": now},
            {"$set": {"used_by": inserted_id}},
        )
        if invite_update.matched_count == 0:
            raise RuntimeError("Invitation claim was lost during registration")
        document["_id"] = inserted_id
        return {
            "token": create_access_token(str(inserted_id), "eleveur"),
            "user": _public_user(document, "eleveur"),
        }
    except Exception:
        try:
            if inserted_id is not None:
                await cols.eleveurs.delete_one({"_id": inserted_id})
            await cols.invites.update_one(
                {"_id": invite["_id"], "used_at": now},
                {"$set": {"used_at": None, "used_by": None}},
            )
        except Exception:
            log.exception("Failed to roll back éleveur invite registration")
        raise


@router.get("/me")
async def auth_me(request: Request):
    try:
        claims = decode_access_token(_bearer_token(request))
    except InvalidTokenError:
        raise HTTPException(401, "Invalid or expired token")

    if claims.get("role") == "superuser":
        actor = await get_superuser(request)
        collection_name = "superusers"
    else:
        actor = await get_actor_allow_expired(request)
        collection_name = "admins" if actor["role"] == "admin" else "eleveurs"

    cols = request.app.state.cols
    collection = getattr(cols, collection_name)
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
