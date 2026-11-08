import asyncio
import logging
from datetime import datetime, timezone

import aio_pika
from pymongo import UpdateOne
from pymongo.errors import DuplicateKeyError

from app.config import LOW_APPETITE, READING_FIELDS
from app.db import Cols
from app.hub import hub
from app.ingest.parsing import daily_update, parse_dispatch, parse_items, parse_ts
from app.services.alerts import low_appetite_alert

log = logging.getLogger("ingest")


async def handle_daily(message: aio_pika.abc.AbstractIncomingMessage, cols: Cols):
    try:
        items = parse_items(message.body)
        if items:
            now = datetime.now(timezone.utc)
            await cols.daily.bulk_write([daily_update(d, now) for d in items], ordered=False)
            for d in items:
                if str(d.get("appetite_class", "")).strip().lower() in LOW_APPETITE:
                    await low_appetite_alert(cols, d["cow_id"], d["date"])
            log.info("Daily: stored %d record(s), e.g. %s", len(items), items[0]["_id"])
        await message.ack()
    except (ValueError, TypeError):
        log.warning("Bad daily message dropped: %r", message.body[:200])
        await message.reject(requeue=False)
    except Exception:
        log.exception("DB error on daily message, requeueing")
        await asyncio.sleep(2)
        await message.nack(requeue=True)


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
