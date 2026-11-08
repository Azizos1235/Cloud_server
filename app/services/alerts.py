from datetime import datetime, timezone

from pymongo.errors import DuplicateKeyError

from app.db import Cols
from app.utils import new_alert


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


async def low_appetite_alert(cols: Cols, cow_id: str, day: str):
    await system_alert(cols, cow_id, day, "low_appetite", "warning",
                       f'La vache {cow_id} a un appétit faible aujourd\'hui')
