import math
import re
from datetime import date as Date, datetime, timedelta, timezone
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
    return {"cow_id": p["_id"], "admin_id": sid(p.get("admin_id")),
            "breed": p["breed"], "type": p["type"], "weight_kg": p["weight_kg"],
            "birth_date": birth.isoformat(), "age_months": age_months(birth),
            "nfc_uid": p.get("nfc_uid")}   # age is computed


def normalize_nfc_uid(value: str) -> str:
    uid = value.strip().upper().replace(":", "").replace("-", "")
    uid = "".join(uid.split())
    if not re.fullmatch(r"[0-9A-F]{8,32}", uid):
        raise ValueError("nfc_uid must be 8 to 32 hexadecimal characters")
    return uid


def is_subscription_expired(expires_at: Optional[datetime], now: Optional[datetime] = None) -> bool:
    if expires_at is None:
        return True
    end = expires_at.replace(tzinfo=timezone.utc) if expires_at.tzinfo is None else expires_at.astimezone(timezone.utc)
    current = now or datetime.now(timezone.utc)
    return end < current


def as_utc(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def extended_subscription(expires_at: Optional[datetime], months: int, days_per_month: int,
                          now: Optional[datetime] = None) -> datetime:
    current = now or datetime.now(timezone.utc)
    previous_end = as_utc(expires_at)
    return max(current, previous_end or current) + timedelta(days=months * days_per_month)


def alert_out(a: dict) -> dict:
    out = {k: v for k, v in a.items() if k not in ("_id", "dedupe_key", "eleveur_id")}
    for field in ("message", "severity", "cow_id", "action", "type",
                  "appetite_class", "appetite_pct", "status", "created_at"):
        out.setdefault(field, None)
    out.update(id=str(a["_id"]), admin_id=sid(a.get("admin_id")))
    return out


def new_alert(source, type_, severity, message, cow_id, admin_id, eleveur_id, now, **extra) -> dict:
    return {"source": source, "type": type_, "severity": severity, "message": message,
            "cow_id": cow_id, "admin_id": admin_id,
            "status": "unread", "created_at": now, "read_at": None, **extra}


async def latest(col, cow_id: str):
    return await col.find_one({"cow_id": cow_id}, {"_id": 0}, sort=[("date", -1)])
