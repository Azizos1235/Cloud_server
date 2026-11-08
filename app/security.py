from typing import Optional

import jwt
from bson import ObjectId
from bson.errors import InvalidId

from .config import settings


def decode_token(token: str) -> Optional[tuple[str, ObjectId]]:
    """JWT -> (role, user_id), or None if the token is invalid or expired."""
    try:
        data = jwt.decode(token, settings.jwt_secret, algorithms=["HS256"])
        role, user_id = data["role"], ObjectId(data["sub"])
    except (jwt.PyJWTError, KeyError, InvalidId, TypeError):
        return None
    if role not in ("admin", "eleveur"):
        return None
    return role, user_id
