"""RabbitMQ -> MongoDB: sensor readings (daily, realtime) and dispense confirmations."""
import asyncio
import json
import logging
import math
from datetime import datetime, timezone

import aio_pika
from pymongo import UpdateOne
from pymongo.errors import DuplicateKeyError

from ..config import LOW_APPETITE, READING_FIELDS, settings
from ..database import Cols
from .alert_service import system_alert
from .hub import hub
from .utils import load_payload, num, opt, parse_ts

log = logging.getLogger("ingest")


# ---- daily ----------------------------------------------------------------
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


def daily_update(d: dict, now: datetime) -> UpdateOne:
    """One ResumeJournalier per cow per day.

    The ESP32 sends running totals, so $max keeps the highest value: a retried or
    out-of-order message never double counts or lowers a total.
    """
    day = datetime.strptime(d["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    stage = {
        "cow_id": d["cow_id"],
        "date": day,
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
    avg = {"$set": {"avg_meal_duration_min": {"$cond": [
        {"$gt": ["$visits", 0]},
        {"$round": [{"$divide": ["$total_eating_minutes", "$visits"]}, 1]}, 0]}}}
    return UpdateOne({"_id": d["_id"]}, [{"$set": stage}, avg], upsert=True)


async def handle_daily(message: aio_pika.abc.AbstractIncomingMessage, cols: Cols):
    try:
        items = parse_items(message.body)
        if items:
            now = datetime.now(timezone.utc)
            await cols.daily.bulk_write([daily_update(d, now) for d in items], ordered=False)
            for d in items:
                if str(d.get("appetite_class", "")).strip().lower() in LOW_APPETITE:
                    await system_alert(cols, d["cow_id"], d["date"], "low_appetite", "warning",
                                       f'La vache {d["cow_id"]} a un appétit faible aujourd\'hui')
            log.info("Daily: stored %d record(s), e.g. %s", len(items), items[0]["_id"])
        await message.ack()
    except (ValueError, TypeError):
        log.warning("Bad daily message dropped: %r", message.body[:200])
        await message.reject(requeue=False)
    except Exception:
        log.exception("DB error on daily message, requeueing")
        await asyncio.sleep(2)
        await message.nack(requeue=True)


# ---- realtime -------------------------------------------------------------
async def handle_realtime(message: aio_pika.abc.AbstractIncomingMessage, cols: Cols):
    """LectureTempsReel: stored with the diagram fields only, then pushed to the owners."""
    docs: list[dict] = []
    try:
        now = datetime.now(timezone.utc)
        ops = []
        for d in parse_items(message.body):
            ts = parse_ts(d.get("received_at"), now)
            doc = {"cow_id": d["cow_id"], "timestamp": ts,
                   **{k: d[k] for k in READING_FIELDS if k in d}}
            docs.append(doc)
            key = f'{d["cow_id"]}|{ts.isoformat(timespec="milliseconds")}'   # same reading twice = stored once
            ops.append(UpdateOne({"_id": key}, {"$setOnInsert": doc}, upsert=True))
        if ops:
            await cols.realtime.bulk_write(ops, ordered=False)
            log.info("Realtime: logged %d reading(s) for %s", len(docs), docs[0]["cow_id"])
        await message.ack()
    except (ValueError, TypeError):
        log.warning("Bad realtime message dropped: %r", message.body[:200])
        await message.reject(requeue=False)
        return
    except Exception:
        log.exception("DB error on realtime message, requeueing")
        await asyncio.sleep(2)
        await message.nack(requeue=True)
        return

    for doc in docs:
        await hub.broadcast(cols, doc["cow_id"], {"type": "update", "cow_id": doc["cow_id"], "data": doc})


# ---- dispatch -------------------------------------------------------------
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


async def handle_dispatch(message: aio_pika.abc.AbstractIncomingMessage, cols: Cols):
    try:
        for d in parse_dispatch(message.body):
            try:
                await cols.dispatch.insert_one(d)
            except DuplicateKeyError:
                log.info("Dispatch already recorded, ignored: %s", d["_id"])
        await message.ack()
    except (ValueError, TypeError):
        log.warning("Bad dispatch message dropped: %r", message.body[:200])
        await message.reject(requeue=False)
    except Exception:
        log.exception("DB error on dispatch, requeueing")
        await asyncio.sleep(2)
        await message.nack(requeue=True)


# ---- consumers ------------------------------------------------------------
async def pump(connection, queue_name: str, handler, cols: Cols):
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=20)
    queue = await channel.declare_queue(queue_name, passive=True)
    log.info("Consuming from %s", queue_name)
    async with queue.iterator() as it:
        async for message in it:
            await handler(message, cols)


async def run_queue(queue_name: str, handler, cols: Cols):
    """Consume one queue for ever, with its own connection and retry loop."""
    while True:
        try:
            connection = await aio_pika.connect_robust(settings.amqp_url)
            async with connection:
                await pump(connection, queue_name, handler, cols)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Queue %s: consumer stopped, retrying in 5 s", queue_name)
        await asyncio.sleep(5)


def start_consumers(cols: Cols) -> list[asyncio.Task]:
    queues = [(settings.daily_queue, handle_daily),
              (settings.realtime_queue, handle_realtime),
              (settings.dispatch_queue, handle_dispatch)]
    return [asyncio.create_task(run_queue(q, h, cols)) for q, h in queues if q]
