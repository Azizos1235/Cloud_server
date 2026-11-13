import hashlib
import secrets
from datetime import datetime, timedelta, timezone

import bcrypt
import jwt
from pymongo.errors import DuplicateKeyError

from app.config import settings


def _bcrypt_input(password: str) -> bytes:
    """Pre-hash to support passwords longer than bcrypt's 72-byte input limit."""
    return hashlib.sha256(password.encode("utf-8")).digest()


def hash_password(password: str) -> str:
    return bcrypt.hashpw(_bcrypt_input(password), bcrypt.gensalt()).decode("ascii")


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return bcrypt.checkpw(_bcrypt_input(password), password_hash.encode("ascii"))
    except (AttributeError, UnicodeEncodeError, ValueError):
        return False


def create_access_token(user_id: str, role: str) -> str:
    expires = datetime.now(timezone.utc) + timedelta(hours=settings.jwt_expire_hours)
    return jwt.encode({"sub": user_id, "role": role, "exp": expires}, settings.jwt_secret, algorithm="HS256")


def decode_access_token(token: str) -> dict:
    return jwt.decode(token, settings.jwt_secret, algorithms=["HS256"])


def new_temporary_password(length: int = 16) -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789"
    return "".join(secrets.choice(alphabet) for _ in range(length))


async def ensure_bootstrap_superuser(cols) -> None:
    if not settings.superuser_email.strip() or not settings.superuser_password:
        return
    if await cols.superusers.find_one({}, {"_id": 1}):
        return

    email = settings.superuser_email.strip().lower()
    document = {
        "name": email.split("@", 1)[0],
        "email": email,
        "password_hash": hash_password(settings.superuser_password),
        "created_at": datetime.now(timezone.utc),
    }
    try:
        await cols.superusers.insert_one(document)
    except DuplicateKeyError:
        if not await cols.superusers.find_one({}):
            raise
