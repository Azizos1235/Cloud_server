import asyncio
import json
import logging
from datetime import datetime, timezone

import aio_pika
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError, DuplicateKeyError, WriteError
from pymongo.results import BulkWriteResult

from app.config import READING_FIELDS, settings
from app.cow_ids import resolve_cow_id
from app.db import Cols
from app.hub import hub
from app.ingest.parsing import daily_update, parse_dispatch, parse_items, parse_ts
from app.services.alerts import appetite_alert

log = logging.getLogger("ingest")


async def _resolve_items(items: list[dict], cols: Cols) -> None:
    for item in items:
        rfid = item["rfid"]
        cow_id = await resolve_cow_id(cols, rfid)
        if cow_id is None:
            log.warning("No cow profile for RFID %s; storing unresolved message", rfid)
        item["cow_id"] = cow_id


def _log_message_received(queue_name: str, body: bytes) -> None:
    try:
        payload = json.loads(body)
        if isinstance(payload, str):
            payload = json.loads(payload)
        records = payload if isinstance(payload, list) else [payload]
    except Exception:
        log.exception("Could not read message metadata queue=%s", queue_name)
        records = [None]

    if not records:
        records = [None]
    for record in records:
        if isinstance(record, dict):
            cow_id = record.get("cow_id", "unknown")
            day = record.get("date", "unknown")
        else:
            cow_id = day = "unknown"
        cow_id = str(cow_id).replace("\r", " ").replace("\n", " ")[:64]
        day = str(day).replace("\r", " ").replace("\n", " ")[:32]
        log.info(
            "Received message queue=%s cow_id=%s date=%s",
            queue_name,
            cow_id,
            day,
        )


def _is_document_validation_error(exc: Exception) -> bool:
    if getattr(exc, "code", None) == 121:
        return True
    details = getattr(exc, "details", None) or {}
    if details.get("code") == 121:
        return True
    return any(
        error.get("code") == 121
        for error in details.get("writeErrors", [])
        if isinstance(error, dict)
    )


def _write_action(result: BulkWriteResult, operation_index: int) -> str:
    return "inserted" if operation_index in result.upserted_ids else "updated"


async def _handle_write_failure(
    message: aio_pika.abc.AbstractIncomingMessage,
    queue_name: str,
    exc: Exception,
) -> None:
    if _is_document_validation_error(exc):
        log.exception("MongoDB document validation failed queue=%s", queue_name)
        await message.reject(requeue=False)
        return

    log.exception("MongoDB write failed queue=%s; requeueing", queue_name)
    await asyncio.sleep(2)
    await message.nack(requeue=True)


async def handle_daily(message: aio_pika.abc.AbstractIncomingMessage, cols: Cols):
    _log_message_received(settings.daily_queue, message.body)
    try:
        items = parse_items(message.body)
        await _resolve_items(items, cols)
        now = datetime.now(timezone.utc)
        result = await cols.daily.bulk_write(
            [daily_update(d, now) for d in items],
            ordered=False,
        )
        for index, item in enumerate(items):
            log.info(
                "%s %s id=%s",
                _write_action(result, index),
                cols.daily.name,
                item["_id"],
            )
        for d in items:
            if d["cow_id"] and d.get("appetite_class") and d.get("appetite_pct"):
                await appetite_alert(
                    cols, d["cow_id"], d["date"], d["appetite_class"], d["appetite_pct"]
                )
        await message.ack()
    except (ValueError, TypeError):
        log.exception("Invalid message queue=%s; rejecting", settings.daily_queue)
        await message.reject(requeue=False)
    except (WriteError, BulkWriteError) as exc:
        await _handle_write_failure(message, settings.daily_queue, exc)
    except Exception:
        log.exception("Failed to process message queue=%s; requeueing", settings.daily_queue)
        await asyncio.sleep(2)
        await message.nack(requeue=True)


async def handle_realtime(message: aio_pika.abc.AbstractIncomingMessage, cols: Cols):
    """LectureTempsReel: stored with the diagram fields only, then pushed to the owners."""
    _log_message_received(settings.realtime_queue, message.body)
    docs: list[dict] = []
    try:
        now = datetime.now(timezone.utc)
        ops = []
        keys = []
        items = parse_items(message.body)
        await _resolve_items(items, cols)
        for d in items:
            ts = parse_ts(d.get("received_at"), now)
            doc = {"cow_id": d["cow_id"], "rfid": d["rfid"], "timestamp": ts,
                   **{k: d[k] for k in READING_FIELDS if k in d}}
            docs.append(doc)
            key = f'{d["rfid"]}|{ts.isoformat(timespec="milliseconds")}'   # same reading twice = stored once
            keys.append(key)
            ops.append(UpdateOne({"_id": key}, {"$setOnInsert": doc}, upsert=True))
        result = await cols.realtime.bulk_write(ops, ordered=False)
        for index, key in enumerate(keys):
            log.info(
                "%s %s id=%s",
                _write_action(result, index),
                cols.realtime.name,
                key,
            )
        await message.ack()
    except (ValueError, TypeError):
        log.exception("Invalid message queue=%s; rejecting", settings.realtime_queue)
        await message.reject(requeue=False)
        return
    except (WriteError, BulkWriteError) as exc:
        await _handle_write_failure(message, settings.realtime_queue, exc)
        return
    except Exception:
        log.exception("Failed to process message queue=%s; requeueing", settings.realtime_queue)
        await asyncio.sleep(2)
        await message.nack(requeue=True)
        return

    for doc in docs:
        await hub.broadcast(cols, doc["rfid"], {"type": "update", "cow_id": doc["cow_id"], "data": doc})


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
