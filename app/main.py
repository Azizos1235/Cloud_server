import asyncio, contextlib, json, logging
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
from pymongo import AsyncMongoClient, InsertOne, ReplaceOne, UpdateOne

log = logging.getLogger("ingest")
logging.basicConfig(level=logging.INFO)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    amqp_url: str
    mongo_uri: str
    mongo_db: str = "farm"
    # existing collection: latest record per cow per day (the Pi AI service reads it)
    mongo_collection: str = "feeding_events"
    # new collections
    mongo_realtime_collection: str = "realtime_readings"   # every ESP32 message, append only
    mongo_daily_collection: str = "daily_consumption"      # one document per cow per day
    mongo_profiles_collection: str = "cow_profiles"        # one profile per cow
    queue_name: str = "test_queue"
    # Flask AI service on the Raspberry Pi, reached over Tailscale.
    # e.g. PI_AI_URL=http://100.109.169.109:8001
    # Default = the local socat forward started by start.sh (127.0.0.1:8001 -> Pi).
    # The PI_AI_URL environment variable overrides it when set.
    pi_ai_url: str = "http://127.0.0.1:8001"
    # Only if outgoing Tailscale traffic on this host needs a proxy,
    # e.g. PI_PROXY=socks5://localhost:1055  (requires: pip install "httpx[socks]")
    pi_proxy: str = ""
    pi_ai_timeout: float = 30.0
    # Delete raw real-time readings after this many days (0 = keep forever)
    realtime_ttl_days: int = 90


settings = Settings()
log.info("AI service URL: %r (proxy: %r)", settings.pi_ai_url, settings.pi_proxy)

BREED_TYPE = {"Holstein": "dairy", "Jersey": "dairy", "Angus": "beef",
              "Limousin": "beef", "Charolais": "beef", "Simmental": "dual"}


class Cols:
    """All MongoDB collections used by the service."""

    def __init__(self, db):
        self.events = db[settings.mongo_collection]
        self.realtime = db[settings.mongo_realtime_collection]
        self.daily = db[settings.mongo_daily_collection]
        self.profiles = db[settings.mongo_profiles_collection]


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
    """Ask the Flask model service running on the Pi.

    The Pi service reads the cow's history from MongoDB itself and returns
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
# RabbitMQ -> MongoDB ingestion
# --------------------------------------------------------------------------
def parse_items(body: bytes) -> list[dict]:
    """Accept one cow object, a list of cow objects, or double-encoded JSON."""
    payload = json.loads(body)
    if isinstance(payload, str):  # JSON that was stringified twice
        payload = json.loads(payload)
    items = payload if isinstance(payload, list) else [payload]
    out = []
    for d in items:
        if not isinstance(d, dict) or "cow_id" not in d or "date" not in d:
            raise TypeError("every item must be an object with cow_id and date")
        d = dict(d)
        d["cow_id"] = str(d["cow_id"]).strip().upper()
        d["date"] = str(d["date"]).strip()
        d["_id"] = f'{d["cow_id"]}-{d["date"]}'  # one document per cow per day
        out.append(d)
    return out


async def handle(message: aio_pika.abc.AbstractIncomingMessage, cols: Cols):
    try:
        items = parse_items(message.body)
        if items:
            now = datetime.now(timezone.utc)

            # 1) latest record per cow per day (the Pi AI service reads this)
            await cols.events.bulk_write(
                [ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in items],
                ordered=False)

            # 2) raw real-time log: every message is appended, never overwritten
            await cols.realtime.bulk_write(
                [InsertOne({**{k: v for k, v in d.items() if k != "_id"},
                            "received_at": now}) for d in items],
                ordered=False)

            # 3) daily totals. The ESP32 sends running totals for the day, so
            #    $max keeps the highest value: a retried or out-of-order message
            #    can never double count or lower the total.
            await cols.daily.bulk_write([
                UpdateOne(
                    {"_id": d["_id"]},
                    {"$setOnInsert": {"cow_id": d["cow_id"], "date": d["date"],
                                      "first_reading_at": now},
                     "$max": {"total_food_consumed_g": num(d.get("food_consumed_g")),
                              "total_feed_offered_g": num(d.get("feed_offered_g")),
                              "visits": num(d.get("visits")),
                              "total_eating_minutes": num(d.get("eating_minutes"))},
                     "$set": {"last_updated": now}},
                    upsert=True) for d in items], ordered=False)

            log.info("Stored %d document(s): %s", len(items), items[0]["_id"])
        await message.ack()
    except (ValueError, TypeError):  # JSONDecodeError and UnicodeDecodeError are ValueErrors
        log.warning("Bad message dropped: %r", message.body[:200])
        await message.reject(requeue=False)
        return
    except Exception:
        log.exception("DB error, requeueing")
        await asyncio.sleep(2)
        await message.nack(requeue=True)
        return

    # push the new records to every connected app (never affects the ack above)
    for d in items:
        data = {k: v for k, v in d.items() if k != "_id"}
        await hub.broadcast({"type": "update", "cow_id": d["cow_id"], "data": data})


async def ensure_index(cols: Cols):
    try:
        await cols.events.create_index([("cow_id", 1), ("date", -1)])
        await cols.realtime.create_index([("cow_id", 1), ("received_at", -1)])
        if settings.realtime_ttl_days > 0:
            # auto-delete raw readings after N days
            await cols.realtime.create_index(
                "received_at", expireAfterSeconds=settings.realtime_ttl_days * 86400)
        await cols.daily.create_index([("cow_id", 1), ("date", -1)])
    except Exception:
        log.exception("Could not create indexes (will continue)")


async def consume(cols: Cols):
    # Keep retrying: the Tailscale tunnel may need a few seconds, and the Pi may be offline
    while True:
        try:
            connection = await aio_pika.connect_robust(settings.amqp_url)
            async with connection:
                channel = await connection.channel()
                await channel.set_qos(prefetch_count=20)
                # passive: only checks the existing queue, never changes it
                queue = await channel.declare_queue(settings.queue_name, passive=True)
                log.info("Consuming from %s", settings.queue_name)
                async with queue.iterator() as it:
                    async for message in it:
                        await handle(message, cols)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("AMQP connection failed, retrying in 5 s")
            await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = AsyncMongoClient(settings.mongo_uri)
    cols = Cols(client[settings.mongo_db])
    app.state.cols = cols
    app.state.col = cols.events      # existing endpoints keep using this
    tasks = [asyncio.create_task(consume(cols)), asyncio.create_task(ensure_index(cols))]
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
    return {"status": "ok", "pi_ai_configured": bool(settings.pi_ai_url)}


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
# Real-time readings and daily consumption
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


# --------------------------------------------------------------------------
# Cow profiles (test collection)
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