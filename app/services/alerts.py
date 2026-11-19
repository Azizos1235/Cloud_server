import logging
import math
from datetime import date as Date, datetime, time, timedelta, timezone

from pymongo.errors import DuplicateKeyError

from app.config import LOW_APPETITE, settings
from app.cow_ids import cow_data_filter
from app.db import Cols
from app.utils import new_alert

log = logging.getLogger("ingest")


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


async def appetite_alert(
    cols: Cols,
    cow_id: str,
    day: str | Date,
    appetite_class=None,
    appetite_pct=None,
):
    """Create one admin alert for each third day of a consecutive abnormal streak."""
    try:
        target_day = Date.fromisoformat(day) if isinstance(day, str) else day
    except ValueError:
        log.warning("Invalid appetite alert day %r for cow=%s", day, cow_id)
        return
    if not isinstance(target_day, Date):
        log.warning("Invalid appetite alert day %r for cow=%s", day, cow_id)
        return

    # Include the target date, but never let later summaries change a historical run.
    through_day = datetime.combine(target_day + timedelta(days=1), time.min, tzinfo=timezone.utc)
    cow_filter = await cow_data_filter(cols, cow_id)
    cursor = (
        cols.daily.find({"$and": [cow_filter, {"date": {"$lt": through_day}}]})
        .sort("date", -1)
        .limit(3)
    )
    summaries = [summary async for summary in cursor]
    if len(summaries) != 3:
        return

    labels = [_canonical_appetite_class(summary.get("appetite_class")) for summary in summaries]
    if labels[0] not in {"low", "high"} or labels.count(labels[0]) != 3:
        return
    dates = [summary.get("date") for summary in summaries]
    if any(not isinstance(value, datetime) for value in dates):
        return
    if any(
        value.hour != 0
        or value.minute != 0
        or value.second != 0
        or value.microsecond != 0
        or value.utcoffset() != timedelta(0)
        for value in dates
    ):
        return
    if dates[0].date() != target_day or any(
        (dates[index].date() - dates[index + 1].date()).days != 1
        for index in range(2)
    ):
        return

    label = labels[0]
    third_day_percentages = _canonical_appetite_pct(summaries[0].get("appetite_pct"))
    if appetite_class is not None and _canonical_appetite_class(appetite_class) != label:
        return
    if appetite_pct is not None:
        third_day_percentages = _canonical_appetite_pct(appetite_pct)

    profile = await cols.profiles.find_one(
        {"$or": [{"cow_id": cow_id}, {"_id": cow_id}]}, {"admin_id": 1}
    )
    if not profile or profile.get("admin_id") is None:
        return

    dedupe_key = f"appetite:{cow_id}:{label}:{target_day.isoformat()}"
    if await cols.alerts.find_one({"dedupe_key": dedupe_key}, {"_id": 1}):
        return

    start_day = dates[2].date().isoformat()
    end_day = dates[0].date().isoformat()
    doc = new_alert(
        "system",
        f"appetite_{label}",
        "warning",
        f"La vache {cow_id} a un appétit {label} depuis 3 jours "
        f"({start_day} au {end_day}).",
        cow_id,
        profile["admin_id"],
        None,
        datetime.now(timezone.utc),
        appetite_class=label,
        appetite_pct=third_day_percentages,
        dedupe_key=dedupe_key,
        task_id=None,
        handled=False,
    )
    try:
        await cols.alerts.insert_one(doc)
    except DuplicateKeyError:
        log.info("Duplicate appetite alert ignored for cow=%s date=%s", cow_id, day)
