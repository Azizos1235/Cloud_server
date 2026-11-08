from datetime import datetime, timezone

from pymongo.errors import DuplicateKeyError

from ..database import Cols
from .utils import sid


def new_alert(source, type_, severity, message, cow_id, admin_id, eleveur_id, now, **extra) -> dict:
    return {"source": source, "type": type_, "severity": severity, "message": message,
            "cow_id": cow_id, "admin_id": admin_id, "eleveur_id": eleveur_id,
            "status": "unread", "created_at": now, "read_at": None, **extra}


def alert_out(a: dict) -> dict:
    out = {k: v for k, v in a.items() if k not in ("_id", "dedupe_key")}
    out.update(id=str(a["_id"]), admin_id=sid(a.get("admin_id")), eleveur_id=sid(a.get("eleveur_id")))
    return out


async def system_alert(cols: Cols, cow_id: str, day: str, kind: str, severity: str, message: str):
    """AlerteSysteme. Sent to the cow's admin and éleveur, once per cow, type and day."""
    p = await cols.profiles.find_one({"_id": cow_id}, {"admin_id": 1, "eleveur_id": 1})
    if not p:
        return
    doc = new_alert("system", kind, severity, message, cow_id,
                    p.get("admin_id"), p.get("eleveur_id"), datetime.now(timezone.utc),
                    dedupe_key=f"{cow_id}|{day}|{kind}")
    try:
        await cols.alerts.insert_one(doc)
    except DuplicateKeyError:
        pass
