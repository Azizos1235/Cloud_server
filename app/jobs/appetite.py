"""End-of-day job: writes `appetite_class` and `appetite_pct` into daily_summaries.

Every day at EOD_HOUR:EOD_MINUTE (local time, TIMEZONE) it looks at today's
summaries that have no appetite_class or no appetite_pct yet and stores:

    appetite_class        "low" | "normal" | "high"   (or whatever the Pi returns)
    appetite_pct          {"low": 10.0, "normal": 60.0, "high": 30.0}
    appetite_source       "ai" | "rule"
    appetite_computed_at  UTC timestamp

A value already sent by the ESP32 is never overwritten: if the device sent the
class but no percentages, only the percentages are filled in. Low appetite
raises the same system alert as the live path (deduplicated per cow and day).
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

AI_KEYS = ("appetite_class", "category", "appetite", "class", "label", "prediction")
PCT_KEYS = ("appetite_pct", "probabilities", "percentages", "proba")


def _unwrap(result: dict) -> dict:
    """The Pi answers {"model": {...}}; use the inner dict when present."""
    m = result.get("model") if isinstance(result, dict) else None
    return m if isinstance(m, dict) else (result if isinstance(result, dict) else {})


def _from_ai(result: dict) -> Optional[str]:
    for k in AI_KEYS:
        v = result.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _pct_from_ai(result: dict) -> Optional[dict]:
    """Percentages per class from the Pi answer (accepts 0-1 probabilities or 0-100)."""
    for k in PCT_KEYS:
        v = result.get(k)
        if isinstance(v, dict) and v:
            try:
                pct = {str(c).strip().lower(): float(p) for c, p in v.items()}
            except (TypeError, ValueError):
                continue
            if sum(pct.values()) <= 1.5:            # probabilities 0-1 -> percentages
                pct = {c: p * 100 for c, p in pct.items()}
            return {c: round(p, 2) for c, p in pct.items()}
    return None


def _ratio(doc: dict) -> Optional[float]:
    offered = doc.get("total_feed_offered_g") or 0
    eaten = doc.get("total_food_consumed_g") or 0
    if offered <= 0:
        return None
    return eaten / offered


def _from_rule(doc: dict) -> str:
    """Fallback: share of the offered feed that was eaten."""
    ratio = _ratio(doc)
    if ratio is None:
        eaten = doc.get("total_food_consumed_g") or 0
        return "low" if eaten <= 0 else "normal"
    if ratio < settings.appetite_low_ratio:
        return "low"
    if ratio >= settings.appetite_high_ratio:
        return "high"
    return "normal"


def _pct_from_rule(doc: dict, label: str) -> dict:
    """Fallback: percentage of the offered feed that was eaten, attached to the class."""
    ratio = _ratio(doc)
    return {label.strip().lower(): round(min(ratio, 1) * 100, 2) if ratio is not None else 0.0}


async def classify(doc: dict, known_label: Optional[str] = None) -> tuple[str, str, Optional[dict]]:
    """Returns (class, source, percentages). If known_label is given, it is kept as the class."""
    if settings.appetite_use_ai:
        try:
            result = _unwrap(await predict({"cow_id": doc["cow_id"]}))
            label = known_label or _from_ai(result)
            if label:
                return label, "ai", _pct_from_ai(result) or _pct_from_rule(doc, label)
            log.warning("AI answer for %s has no appetite field (status=%s), using the rule",
                        doc["cow_id"], result.get("status"))
        except Exception as e:
            log.warning("AI unavailable for %s (%s), using the rule", doc["cow_id"], e)
    label = known_label or _from_rule(doc)
    return label, "rule", _pct_from_rule(doc, label)


def _missing(field: str) -> list[dict]:
    return [{field: {"$exists": False}}, {field: None}, {field: ""}]


async def run_for_day(cols: Cols, day: Date, cow_ids: Optional[list[str]] = None) -> int:
    """Fill the missing appetite_class / appetite_pct of every summary of `day`. Returns how many were written."""
    day_dt = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    missing_any = _missing("appetite_class") + _missing("appetite_pct")
    flt: dict = {"date": day_dt, "$or": missing_any}
    if cow_ids is not None:
        flt["cow_id"] = {"$in": cow_ids}

    written = 0
    async for doc in cols.daily.find(flt):
        try:
            has_class = bool(str(doc.get("appetite_class") or "").strip())
            has_pct = bool(doc.get("appetite_pct"))

            label, source, pct = await classify(
                doc, known_label=doc["appetite_class"].strip() if has_class else None)

            update: dict = {}
            if not has_class:
                update.update({"appetite_class": label, "appetite_source": source,
                               "appetite_computed_at": datetime.now(timezone.utc)})
            if not has_pct and pct:
                update["appetite_pct"] = pct
            if not update:
                continue

            res = await cols.daily.update_one(
                {"_id": doc["_id"], "$or": missing_any},   # never overwrite a value that just arrived
                {"$set": update})
            if res.modified_count:
                written += 1
                if not has_class and label.lower() in LOW_APPETITE:
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