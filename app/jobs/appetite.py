"""End-of-day job: writes `appetite_class` into daily_summaries.

Every day at EOD_HOUR:EOD_MINUTE (local time, TIMEZONE) it looks at today's
summaries that have no appetite_class yet, classifies each cow and stores:

    appetite_class        "low" | "normal" | "high"   (or whatever the Pi returns)
    appetite_source       "ai" | "rule"
    appetite_computed_at  UTC timestamp

A value already sent by the ESP32 is never overwritten. Low appetite raises
the same system alert as the live path (deduplicated per cow and day).
"""
import asyncio
import logging
from datetime import date as Date, datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from app.config import LOW_APPETITE, settings
from app.db import Cols
from app.services.ai import predict
from app.services.alerts import low_appetite_alert

log = logging.getLogger("ingest")

AI_KEYS = ("appetite_class", "appetite", "class", "label", "prediction")


def _from_ai(result: dict) -> Optional[str]:
    for k in AI_KEYS:
        v = result.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _from_rule(doc: dict) -> str:
    """Fallback: share of the offered feed that was eaten."""
    offered = doc.get("total_feed_offered_g") or 0
    eaten = doc.get("total_food_consumed_g") or 0
    if offered <= 0:
        return "low" if eaten <= 0 else "normal"
    ratio = eaten / offered
    if ratio < settings.appetite_low_ratio:
        return "low"
    if ratio >= settings.appetite_high_ratio:
        return "high"
    return "normal"


async def classify(doc: dict) -> tuple[str, str]:
    if settings.appetite_use_ai:
        try:
            label = _from_ai(await predict({"cow_id": doc["cow_id"]}))
            if label:
                return label, "ai"
            log.warning("AI answer for %s has no appetite field, using the rule", doc["cow_id"])
        except Exception as e:
            log.warning("AI unavailable for %s (%s), using the rule", doc["cow_id"], e)
    return _from_rule(doc), "rule"


async def run_for_day(cols: Cols, day: Date, cow_ids: Optional[list[str]] = None) -> int:
    """Classify every summary of `day` that has no appetite_class. Returns how many were written."""
    day_dt = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    flt: dict = {"date": day_dt,
                 "$or": [{"appetite_class": {"$exists": False}},
                         {"appetite_class": None}, {"appetite_class": ""}]}
    if cow_ids is not None:
        flt["cow_id"] = {"$in": cow_ids}

    written = 0
    async for doc in cols.daily.find(flt):
        try:
            label, source = await classify(doc)
            res = await cols.daily.update_one(
                {"_id": doc["_id"], "$or": flt["$or"]},     # never overwrite a value that just arrived
                {"$set": {"appetite_class": label, "appetite_source": source,
                          "appetite_computed_at": datetime.now(timezone.utc)}})
            if res.modified_count:
                written += 1
                if label.lower() in LOW_APPETITE:
                    await low_appetite_alert(cols, doc["cow_id"], day.isoformat())
        except Exception:
            log.exception("Appetite job failed for %s", doc.get("_id"))
    log.info("Appetite job %s: %d summary(ies) updated", day, written)
    return written


def _next_run(now: datetime) -> datetime:
    target = now.replace(hour=settings.eod_hour, minute=settings.eod_minute, second=0, microsecond=0)
    return target if target > now else target + timedelta(days=1)


async def appetite_scheduler(cols: Cols):
    tz = ZoneInfo(settings.timezone)
    log.info("Appetite job scheduled daily at %02d:%02d (%s)",
             settings.eod_hour, settings.eod_minute, settings.timezone)
    while True:
        now = datetime.now(tz)
        run_at = _next_run(now)
        await asyncio.sleep((run_at - now).total_seconds())
        try:
            await run_for_day(cols, run_at.date())
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Appetite job crashed, will run again tomorrow")
        await asyncio.sleep(1)   # make sure we do not fire twice in the same minute
