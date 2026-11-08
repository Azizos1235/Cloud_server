from ..database import Cols
from .utils import age_months, sid


async def latest(col, cow_id: str):
    return await col.find_one({"cow_id": cow_id}, {"_id": 0}, sort=[("date", -1)])


def owner_filter(actor: dict) -> dict:
    """An admin sees his cows (admin_id), an éleveur sees the cows he looks after (eleveur_id)."""
    return {"admin_id": actor["id"]} if actor["role"] == "admin" else {"eleveur_id": actor["id"]}


async def owns(cols: Cols, actor: dict, cow_id: str) -> bool:
    return await cols.profiles.find_one({"_id": cow_id, **owner_filter(actor)}, {"_id": 1}) is not None


def profile_out(p: dict) -> dict:
    birth = p["birth_date"].date()
    return {"cow_id": p["_id"], "admin_id": sid(p.get("admin_id")), "eleveur_id": sid(p.get("eleveur_id")),
            "breed": p["breed"], "type": p["type"], "weight_kg": p["weight_kg"],
            "birth_date": birth.isoformat(), "age_months": age_months(birth)}   # age is computed
