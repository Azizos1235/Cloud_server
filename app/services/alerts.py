import math
from datetime import datetime, timezone

from pymongo.errors import DuplicateKeyError

from app.config import LOW_APPETITE, settings
from app.db import Cols
from app.utils import new_alert


def _canonical_appetite_class(value) -> str | None:
    if not isinstance(value, str):
        return None
    label = value.strip().lower()
    if label in LOW_APPETITE:
        return "low"
    if label in {"normal", "high"}:
        return label
    return None


def _canonical_appetite_pct(value) -> dict[str, float]:
    if not isinstance(value, dict):
        return {}
    result: dict[str, float] = {}
    for raw_label, raw_percentage in value.items():
        label = _canonical_appetite_class(raw_label)
        if label is None:
            continue
        try:
            percentage = float(raw_percentage)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(percentage) or not 0 <= percentage <= 100:
            continue
        result[label] = round(min(100, result.get(label, 0) + percentage), 2)
    return result


async def appetite_alert(cols: Cols, cow_id: str, day: str, appetite_class, appetite_pct):
    label = _canonical_appetite_class(appetite_class)
    percentages = _canonical_appetite_pct(appetite_pct)
    if label not in {"low", "high"} or percentages.get(label, 0) <= settings.appetite_alert_threshold:
        return

    profile = await cols.profiles.find_one({"_id": cow_id}, {"admin_id": 1})
    if not profile or profile.get("admin_id") is None:
        return

    doc = new_alert(
        "system",
        "appetite",
        "warning",
        f'La vache {cow_id} a un appétit {label} aujourd\'hui',
        cow_id,
        profile["admin_id"],
        None,
        datetime.now(timezone.utc),
        appetite_class=label,
        appetite_pct=percentages,
        dedupe_key=f"{cow_id}|{day}|appetite",
    )
    try:
        await cols.alerts.insert_one(doc)
    except DuplicateKeyError:
        pass
