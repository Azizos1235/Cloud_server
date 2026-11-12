import json
import math
from datetime import datetime, timezone

from pymongo import UpdateOne

from app.utils import num, opt


def load_payload(body: bytes) -> list:
    payload = json.loads(body)
    if isinstance(payload, str):
        payload = json.loads(payload)
    return payload if isinstance(payload, list) else [payload]


def parse_items(body: bytes) -> list[dict]:
    out = []
    for d in load_payload(body):
        if not isinstance(d, dict) or "cow_id" not in d or "date" not in d:
            raise TypeError("every item must be an object with cow_id and date")
        d = dict(d)
        d["cow_id"] = str(d["cow_id"]).strip().upper()
        if not d["cow_id"]:
            raise ValueError("cow_id is empty")
        d["date"] = str(d["date"]).strip()[:10]
        datetime.strptime(d["date"], "%Y-%m-%d")
        d["_id"] = f'{d["cow_id"]}-{d["date"]}'
        out.append(d)
    return out


def parse_ts(value, default: datetime) -> datetime:
    if isinstance(value, str):
        try:
            t = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return default
        return t.astimezone(timezone.utc) if t.tzinfo else t.replace(tzinfo=timezone.utc)
    return default


def daily_update(d: dict, now: datetime) -> UpdateOne:
    """One ResumeJournalier per cow per day.

    The ESP32 sends running totals, so $max keeps the highest value: a retried or
    out-of-order message never double counts or lowers a total. The fields that
    used to live in feeding_events are merged here.
    """
    day = datetime.strptime(d["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    stage = {
        "cow_id": d["cow_id"],
        "date": day,                                           # real date, not a string
        "first_reading_at": {"$ifNull": ["$first_reading_at", now]},
        "last_updated": now,
        "total_food_consumed_g": {"$max": ["$total_food_consumed_g", num(d.get("food_consumed_g"))]},
        "total_feed_offered_g": {"$max": ["$total_feed_offered_g", num(d.get("feed_offered_g"))]},
        "total_eating_minutes": {"$max": ["$total_eating_minutes", num(d.get("eating_minutes"))]},
        "visits": {"$max": ["$visits", num(d.get("visits"))]},
        "no_food_visits": {"$max": ["$no_food_visits", num(d.get("no_food_visits"))]},
    }
    first, last = opt(d.get("first_visit_hour")), opt(d.get("last_visit_hour"))
    if first is not None:
        stage["first_visit_hour"] = {"$min": ["$first_visit_hour", first]}
    if last is not None:
        stage["last_visit_hour"] = {"$max": ["$last_visit_hour", last]}
    appetite = d.get("appetite_class")
    if isinstance(appetite, str) and appetite.strip():
        stage["appetite_class"] = {"$literal": appetite.strip()}

    # NEW: percentages per appetite class, e.g. {"low": 10, "normal": 60, "high": 30}
    pct = parse_appetite_pct(d.get("appetite_pct"))
    if pct:
        stage["appetite_pct"] = {"$literal": pct}              # $literal: this is a pipeline update

    avg = {"$set": {"avg_meal_duration_min": {"$cond": [
        {"$gt": ["$visits", 0]},
        {"$round": [{"$divide": ["$total_eating_minutes", "$visits"]}, 1]}, 0]}}}
    return UpdateOne({"_id": d["_id"]}, [{"$set": stage}, avg], upsert=True)


def parse_dispatch(body: bytes) -> list[dict]:
    out = []
    for p in load_payload(body):
        if not isinstance(p, dict) or "cow_id" not in p or "food_g" not in p:
            raise TypeError("every item must be an object with cow_id and food_g")
        cid = str(p["cow_id"]).strip().upper()
        if not cid:
            raise ValueError("cow_id is empty")
        day = str(p.get("date") or datetime.now().strftime("%Y-%m-%d")).strip()[:10]
        datetime.strptime(day, "%Y-%m-%d")
        food = float(p["food_g"])
        if not math.isfinite(food) or food < 0:
            raise ValueError("food_g must be a finite number >= 0")
        out.append({"_id": f"{cid}-{day}", "cow_id": cid, "date": day, "food_g": round(food),
                    "served_at": str(p.get("served_at")
                                     or datetime.now().isoformat(timespec="seconds"))})
    return out



def parse_appetite_pct(raw) -> dict | None:
    """{"low": 10, "normal": 60, "high": 30} -> cleaned dict, or None if invalid/missing."""
    if not isinstance(raw, dict):
        return None
    out = {}
    for k, v in raw.items():
        try:
            p = float(v)
        except (TypeError, ValueError):
            continue
        if 0 <= p <= 100:
            out[str(k).strip().lower()] = round(p, 2)
    return out or None
