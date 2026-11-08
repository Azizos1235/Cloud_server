import math
from datetime import date as Date, datetime, timezone
from typing import Optional

from bson import ObjectId
from bson.errors import InvalidId
from fastapi import HTTPException


def to_oid(value: str) -> ObjectId:
    try:
        return ObjectId(value)
    except (InvalidId, TypeError):
        raise HTTPException(400, "Invalid id")


def sid(v):
    return str(v) if v is not None else None


def num(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def opt(v) -> Optional[float]:
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def age_months(birth: Date) -> int:
    t = datetime.now(timezone.utc).date()
    return (t.year - birth.year) * 12 + (t.month - birth.month) - (1 if t.day < birth.day else 0)


def profile_out(p: dict) -> dict:
    birth = p["birth_date"].date()
    return {"cow_id": p["_id"], "admin_id": sid(p.get("admin_id")), "eleveur_id": sid(p.get("eleveur_id")),
            "breed": p["breed"], "type": p["type"], "weight_kg": p["weight_kg"],
            "birth_date": birth.isoformat(), "age_months": age_months(birth)}   # age is computed


def alert_out(a: dict) -> dict:
    out = {k: v for k, v in a.items() if k not in ("_id", "dedupe_key")}
    out.update(id=str(a["_id"]), admin_id=sid(a.get("admin_id")), eleveur_id=sid(a.get("eleveur_id")))
    return out


def new_alert(source, type_, severity, message, cow_id, admin_id, eleveur_id, now, **extra) -> dict:
    return {"source": source, "type": type_, "severity": severity, "message": message,
            "cow_id": cow_id, "admin_id": admin_id, "eleveur_id": eleveur_id,
            "status": "unread", "created_at": now, "read_at": None, **extra}


async def latest(col, cow_id: str):
    return await col.find_one({"cow_id": cow_id}, {"_id": 0}, sort=[("date", -1)])
