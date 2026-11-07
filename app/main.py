import asyncio, contextlib, json, logging, math
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Literal

import aio_pika
import httpx
from fastapi import FastAPI, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from pymongo import AsyncMongoClient, ReplaceOne, UpdateOne
from pymongo.errors import DuplicateKeyError

log = logging.getLogger("ingest")
logging.basicConfig(level=logging.INFO)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    amqp_url: str
    mongo_uri: str
    mongo_db: str = "farm"
    # latest record per cow per day (the classifier service reads it)
    mongo_collection: str = "feeding_events"
    mongo_realtime_collection: str = "realtime_readings"   # every ESP32 message, append only
    mongo_daily_collection: str = "daily_consumption"      # one document per cow per day
    mongo_profiles_collection: str = "cow_profiles"        # one profile per cow (regression service reads it)
    mongo_dispatch_collection: str = "feed_dispatch"       # one dispense per cow per day (regression service reads it)
    # RabbitMQ queues. This service only reads them: they must already exist
    # (they are checked, not created) and the AMQP_URL user needs read access.
    daily_queue: str = "cow.daily"        # the day's running record, one message per ESP32 post
    realtime_queue: str = "cow.realtime"  # live readings: logged, then pushed to the apps
    # Optional queue with dispense confirmations from the ESP32.
    # Empty = disabled. The queue must already exist (it is checked, not created).
    dispatch_queue: str = ""
    # Classifier service on the Raspberry Pi, reached over Tailscale.
    pi_ai_url: str = "http://127.0.0.1:8001"
    pi_proxy: str = ""
    pi_ai_timeout: float = 30.0
    # Delete raw real-time readings after this many days (0 = keep forever)
    realtime_ttl_days: int = 90


settings = Settings()
log.info("AI service URL: %r (proxy: %r)", settings.pi_ai_url, settings.pi_proxy)

BREED_TYPE = {"Holstein": "dairy", "Jersey": "dairy", "Angus": "beef",
              "Limousin": "beef", "Charolais": "beef", "Simmental": "dual"}


class Cols:
    """All MongoDB collections used by the service. One role per collection:

    events    feeding_events     latest full record per cow per day  (classifier service reads it)
    daily     daily_consumption  running totals + appetite class per cow per day
    realtime  realtime_readings  append-only log of live readings, deleted after N days
    profiles  cow_profiles       one static profile per cow          (regression service reads it)
    dispatch  feed_dispatch      one dispense per cow per day        (regression service reads it)
    """

    def __init__(self, db):
        self.events = db[settings.mongo_collection]
        self.realtime = db[settings.mongo_realtime_collection]
        self.daily = db[settings.mongo_daily_collection]
        self.profiles = db[settings.mongo_profiles_collection]
        self.dispatch = db[settings.mongo_dispatch_collection]


# --------------------------------------------------------------------------
# WebSocket hub: keeps the connected Flutter clients and pushes live updates
# --------------------------------------------------------------------------
class Hub:
    def __init__(self):
        self.clients: set[WebSocket] = set()

    async def broadcast(self, msg: dict):
        try:
            text = json.dumps(jsonable_encoder(msg))
        except Exception:
            log.exception("Could not encode broadcast message")
            return
        dead = []
        for ws in list(self.clients):
            try:
                await ws.send_text(text)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)


hub = Hub()


async def reply(ws: WebSocket, msg: dict):
    await ws.send_text(json.dumps(jsonable_encoder(msg)))


# --------------------------------------------------------------------------
# Data helpers
# --------------------------------------------------------------------------
async def latest(col, cow_id: str):
    return await col.find_one({"cow_id": cow_id}, {"_id": 0}, sort=[("date", -1)])


async def predict(record: dict) -> dict:
    """Ask the classifier service running on the Pi.

    It reads the cow's history from MongoDB itself and returns
    {...record, "model": {...}}, which is the shape AiResult.fromResponse
    expects in the Flutter app.
    """
    if not settings.pi_ai_url:
        raise NotImplementedError("PI_AI_URL is not set on the server")
    url = f'{settings.pi_ai_url.rstrip("/")}/predict/{record["cow_id"]}'
    async with httpx.AsyncClient(
        timeout=settings.pi_ai_timeout,
        proxy=settings.pi_proxy or None,
    ) as client:
        r = await client.get(url)
    if r.status_code != 200:
        raise RuntimeError(f"AI service HTTP {r.status_code}: {r.text[:200]}")
    return r.json()


def num(v) -> float:
    """Safe number conversion for the cumulative fields."""
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


# --------------------------------------------------------------------------
# RabbitMQ -> MongoDB ingestion: sensor readings (daily_queue and realtime_queue)
# --------------------------------------------------------------------------
def load_payload(body: bytes) -> list:
    """Accept one object, a list of objects, or double-encoded JSON."""
    payload = json.loads(body)
    if isinstance(payload, str):  # JSON that was stringified twice
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
        datetime.strptime(d["date"], "%Y-%m-%d")        # ValueError if it is not YYYY-MM-DD
        d["_id"] = f'{d["cow_id"]}-{d["date"]}'  # one document per cow per day
        out.append(d)
    return out


def parse_ts(value, default: datetime) -> datetime:
    """ISO-8601 string (the 'received_at' Node-RED adds) -> aware UTC datetime."""
    if isinstance(value, str):
        try:
            t = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return default
        return t.astimezone(timezone.utc) if t.tzinfo else t.replace(tzinfo=timezone.utc)
    return default


def daily_update(d: dict, now: datetime) -> UpdateOne:
    """Running totals for one cow and day.

    The ESP32 sends running totals, so $max keeps the highest value: a retried
    or out-of-order message can never double count or lower a total.
    """
    update = {
        "$setOnInsert": {"cow_id": d["cow_id"], "date": d["date"], "first_reading_at": now},
        "$max": {"total_food_consumed_g": num(d.get("food_consumed_g")),
                 "total_feed_offered_g": num(d.get("feed_offered_g")),
                 "visits": num(d.get("visits")),
                 "total_eating_minutes": num(d.get("eating_minutes"))},
        "$set": {"last_updated": now},
    }
    appetite = d.get("appetite_class")             # stored when the sender provides it
    if isinstance(appetite, str) and appetite.strip():
        update["$set"]["appetite_class"] = appetite.strip()
    return UpdateOne({"_id": d["_id"]}, update, upsert=True)


async def handle_daily(message: aio_pika.abc.AbstractIncomingMessage, cols: Cols):
    """cow.daily: the day's record of a cow (running totals)."""
    try:
        items = parse_items(message.body)
        if items:
            now = datetime.now(timezone.utc)

            # 1) latest full record per cow per day (the classifier service reads this)
            await cols.events.bulk_write(
                [ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in items],
                ordered=False)

            # 2) running totals per cow per day
            await cols.daily.bulk_write([daily_update(d, now) for d in items], ordered=False)

            log.info("Daily: stored %d record(s), e.g. %s", len(items), items[0]["_id"])
        await message.ack()
    except (ValueError, TypeError):  # JSONDecodeError and UnicodeDecodeError are ValueErrors
        log.warning("Bad daily message dropped: %r", message.body[:200])
        await message.reject(requeue=False)
    except Exception:
        log.exception("DB error on daily message, requeueing")
        await asyncio.sleep(2)
        await message.nack(requeue=True)


async def handle_realtime(message: aio_pika.abc.AbstractIncomingMessage, cols: Cols):
    """cow.realtime: live readings. Logged, then pushed to every connected app.

    This queue is lossy by design (short TTL, not persistent); cow.daily is the
    authoritative record.
    """
    try:
        items = parse_items(message.body)
        if items:
            now = datetime.now(timezone.utc)
            ops = []
            for d in items:
                ts = parse_ts(d.get("received_at"), now)
                doc = {k: v for k, v in d.items() if k != "_id"}
                doc["received_at"] = ts        # when the gateway got the reading (event time)
                doc["stored_at"] = now         # when this service wrote it
                # the same reading delivered twice gets the same _id, so it is stored once
                key = f'{d["cow_id"]}|{ts.isoformat(timespec="milliseconds")}'
                ops.append(UpdateOne({"_id": key}, {"$setOnInsert": doc}, upsert=True))
            await cols.realtime.bulk_write(ops, ordered=False)
            log.info("Realtime: logged %d reading(s) for %s", len(items), items[0]["cow_id"])
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

    # push to every connected app (never affects the ack above)
    for d in items:
        data = {k: v for k, v in d.items() if k != "_id"}
        await hub.broadcast({"type": "update", "cow_id": d["cow_id"], "data": data})



# --------------------------------------------------------------------------
# RabbitMQ -> MongoDB ingestion: dispense confirmations (settings.dispatch_queue)
# --------------------------------------------------------------------------
def parse_dispatch(body: bytes) -> list[dict]:
    out = []
    for p in load_payload(body):
        if not isinstance(p, dict) or "cow_id" not in p or "food_g" not in p:
            raise TypeError("every item must be an object with cow_id and food_g")
        cid = str(p["cow_id"]).strip().upper()
        if not cid:
            raise ValueError("cow_id is empty")
        date = str(p.get("date") or datetime.now().strftime("%Y-%m-%d")).strip()[:10]
        datetime.strptime(date, "%Y-%m-%d")          # raises if invalid
        food = float(p["food_g"])
        if not math.isfinite(food) or food < 0:
            raise ValueError("food_g must be a finite number >= 0")
        out.append({"_id": f"{cid}-{date}",           # one dispense per cow per day
                    "cow_id": cid, "date": date, "food_g": round(food),
                    "served_at": str(p.get("served_at")
                                     or datetime.now().isoformat(timespec="seconds"))})
    return out


async def handle_dispatch(message: aio_pika.abc.AbstractIncomingMessage, cols: Cols):
    try:
        docs = parse_dispatch(message.body)
        for d in docs:
            try:
                await cols.dispatch.insert_one(d)
                log.info("Dispatch stored: %s", d["_id"])
            except DuplicateKeyError:                 # first one wins
                log.info("Dispatch already recorded, ignored: %s", d["_id"])
        await message.ack()
    except (ValueError, TypeError):
        log.warning("Bad dispatch message dropped: %r", message.body[:200])
        await message.reject(requeue=False)
    except Exception:
        log.exception("DB error on dispatch, requeueing")
        await asyncio.sleep(2)
        await message.nack(requeue=True)


async def ensure_index(cols: Cols):
    # (collection, keys, options). Each index is created on its own, so one that
    # fails (for example because an option changed) never blocks the others.
    specs = [
        (cols.events,   [("cow_id", 1), ("date", -1)], {}),
        (cols.daily,    [("cow_id", 1), ("date", -1)], {}),
        (cols.realtime, [("cow_id", 1), ("received_at", -1)], {}),
        # one feeding per cow per day, enforced by the database
        (cols.dispatch, [("cow_id", 1), ("date", 1)], {"unique": True}),
    ]
    if settings.realtime_ttl_days > 0:
        # auto-delete raw readings after N days
        specs.append((cols.realtime, [("received_at", 1)],
                      {"expireAfterSeconds": settings.realtime_ttl_days * 86400}))
    for col, keys, options in specs:
        try:
            await col.create_index(keys, **options)
        except Exception:
            log.exception("Could not create index %s on %s (will continue)", keys, col.name)


async def pump(connection, queue_name: str, handler, cols: Cols):
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=20)
    # passive: only checks the existing queue, never changes it
    queue = await channel.declare_queue(queue_name, passive=True)
    log.info("Consuming from %s", queue_name)
    async with queue.iterator() as it:
        async for message in it:
            await handler(message, cols)


async def run_queue(queue_name: str, handler, cols: Cols):
    """Consume one queue for ever.

    Every queue gets its own connection and retry loop, so a queue that is
    missing or unreachable never stops the others. Retrying matters here: the
    Tailscale tunnel may need a few seconds and the Pi may be offline.
    """
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = AsyncMongoClient(settings.mongo_uri)
    cols = Cols(client[settings.mongo_db])
    app.state.cols = cols
    app.state.col = cols.events      # existing endpoints keep using this
    queues = [(settings.daily_queue, handle_daily),
              (settings.realtime_queue, handle_realtime),
              (settings.dispatch_queue, handle_dispatch)]
    # one independent consumer per queue; an empty name disables that queue
    tasks = [asyncio.create_task(run_queue(q, h, cols)) for q, h in queues if q]
    tasks.append(asyncio.create_task(ensure_index(cols)))
    yield
    for t in tasks:
        t.cancel()
    for t in tasks:
        with contextlib.suppress(asyncio.CancelledError):
            await t
    await client.close()


app = FastAPI(lifespan=lifespan)


# --------------------------------------------------------------------------
# HTTP endpoints
# --------------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "ok", "pi_ai_configured": bool(settings.pi_ai_url),
            "daily_queue": settings.daily_queue or None,
            "realtime_queue": settings.realtime_queue or None,
            "dispatch_queue": settings.dispatch_queue or None}


@app.get("/events/{cow_id}")
async def events(cow_id: str, request: Request, limit: int = Query(30, ge=1, le=500)):
    cur = request.app.state.col.find(
        {"cow_id": cow_id.strip().upper()}, {"_id": 0}
    ).sort("date", -1).limit(limit)
    return await cur.to_list(length=limit)


# These three are what the Flutter CowService calls
@app.get("/cows")
async def cows(request: Request):
    return {"cows": sorted(await request.app.state.col.distinct("cow_id"))}


@app.get("/latest/{cow_id}")
async def latest_http(cow_id: str, request: Request):
    doc = await latest(request.app.state.col, cow_id.strip().upper())
    if doc is None:
        return JSONResponse({"error": f"No data for cow {cow_id}"}, status_code=404)
    return doc


@app.get("/predict/{cow_id}")
async def predict_http(cow_id: str, request: Request):
    doc = await latest(request.app.state.col, cow_id.strip().upper())
    if doc is None:
        return JSONResponse({"error": f"No data for cow {cow_id}"}, status_code=404)
    try:
        return await predict(doc)
    except NotImplementedError as e:
        return JSONResponse({"error": str(e)}, status_code=501)
    except Exception as e:
        log.exception("AI service call failed")
        return JSONResponse(
            {"error": f"AI service unreachable: {type(e).__name__}: {e}"},
            status_code=502,
        )


# --------------------------------------------------------------------------
# Real-time readings, daily consumption, dispatches
# --------------------------------------------------------------------------
@app.get("/realtime/{cow_id}")
async def realtime(cow_id: str, request: Request, limit: int = Query(50, ge=1, le=1000)):
    cur = request.app.state.cols.realtime.find(
        {"cow_id": cow_id.strip().upper()}, {"_id": 0}
    ).sort("received_at", -1).limit(limit)
    return await cur.to_list(length=limit)


@app.get("/daily/{cow_id}")
async def daily(cow_id: str, request: Request, limit: int = Query(30, ge=1, le=366)):
    cur = request.app.state.cols.daily.find(
        {"cow_id": cow_id.strip().upper()}, {"_id": 0}
    ).sort("date", -1).limit(limit)
    return await cur.to_list(length=limit)


@app.get("/dispatch/{cow_id}")
async def dispatches(cow_id: str, request: Request, limit: int = Query(30, ge=1, le=366)):
    cur = request.app.state.cols.dispatch.find(
        {"cow_id": cow_id.strip().upper()}, {"_id": 0}
    ).sort("date", -1).limit(limit)
    return await cur.to_list(length=limit)


# --------------------------------------------------------------------------
# Cow profiles
# --------------------------------------------------------------------------
class CowProfile(BaseModel):
    # same limits as the regression service, so it never rejects a stored profile
    age_months: int = Field(gt=0, le=240)
    weight_kg: float = Field(gt=0, le=1500)
    type: Literal["beef", "dairy", "dual"]
    breed: Literal["Holstein", "Jersey", "Angus", "Limousin", "Charolais", "Simmental"]

    @model_validator(mode="after")
    def type_matches_breed(self):
        if BREED_TYPE[self.breed] != self.type:
            raise ValueError(f"{self.breed} is a {BREED_TYPE[self.breed]} breed, not {self.type}")
        return self


@app.put("/profiles/{cow_id}")
async def put_profile(cow_id: str, p: CowProfile, request: Request):
    cid = cow_id.strip().upper()
    doc = {"_id": cid, "cow_id": cid, **p.model_dump()}
    await request.app.state.cols.profiles.replace_one({"_id": cid}, doc, upsert=True)
    return {k: v for k, v in doc.items() if k != "_id"}


@app.get("/profiles")
async def list_profiles(request: Request):
    return await request.app.state.cols.profiles.find(
        {}, {"_id": 0}).sort("cow_id", 1).to_list(length=1000)


@app.get("/profiles/{cow_id}")
async def get_profile(cow_id: str, request: Request):
    doc = await request.app.state.cols.profiles.find_one(
        {"_id": cow_id.strip().upper()}, {"_id": 0})
    if doc is None:
        return JSONResponse({"error": f"No profile for cow {cow_id}"}, status_code=404)
    return doc


@app.delete("/profiles/{cow_id}")
async def delete_profile(cow_id: str, request: Request):
    res = await request.app.state.cols.profiles.delete_one({"_id": cow_id.strip().upper()})
    if res.deleted_count == 0:
        return JSONResponse({"error": f"No profile for cow {cow_id}"}, status_code=404)
    return {"deleted": cow_id.strip().upper()}


# --------------------------------------------------------------------------
# WebSocket endpoint (optional live updates for the Flutter app)
# --------------------------------------------------------------------------
async def on_client_message(ws: WebSocket, col, msg: dict):
    kind = msg.get("type")
    echo_id = msg.get("cow_id")  # sent back untouched so the app can match it
    cow_id = str(echo_id).strip().upper() if echo_id is not None else None
    try:
        if kind == "list":
            ids = sorted(await col.distinct("cow_id"))
            await reply(ws, {"type": "cows", "ids": ids})

        elif kind == "get" and cow_id:
            doc = await latest(col, cow_id)
            if doc is None:
                await reply(ws, {"type": "error", "cow_id": echo_id, "where": "data",
                                 "message": f"No data for cow {cow_id}"})
            else:
                await reply(ws, {"type": "cow", "cow_id": echo_id, "data": doc})

        elif kind == "ask" and cow_id:
            doc = await latest(col, cow_id)
            if doc is None:
                raise LookupError(f"No data for cow {cow_id}")
            result = await predict(doc)
            await reply(ws, {"type": "ai", "cow_id": echo_id, "result": result})

    except Exception as e:
        if not isinstance(e, (NotImplementedError, LookupError)):
            log.exception("WebSocket request failed: %r", msg)
        await reply(ws, {"type": "error", "cow_id": echo_id,
                         "where": "ai" if kind == "ask" else "data",
                         "message": str(e)})


@app.websocket("/ws/cows")
async def ws_cows(ws: WebSocket):
    await ws.accept()
    hub.clients.add(ws)
    col = ws.app.state.col
    try:
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except ValueError:
                continue
            if isinstance(msg, dict):
                await on_client_message(ws, col, msg)
    except WebSocketDisconnect:
        pass
    except Exception:
        log.exception("WebSocket crashed")
    finally:
        hub.clients.discard(ws)