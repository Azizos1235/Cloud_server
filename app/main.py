import asyncio, json, logging
from contextlib import asynccontextmanager

import aio_pika
from fastapi import FastAPI, Request
from pydantic_settings import BaseSettings, SettingsConfigDict
from pymongo import AsyncMongoClient
from pymongo.errors import DuplicateKeyError

log = logging.getLogger("ingest")
logging.basicConfig(level=logging.INFO)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    amqp_url: str
    mongo_uri: str
    mongo_db: str = "farm"
    mongo_collection: str = "feeding_events"
    queue_name: str = "test_queue"


settings = Settings()


async def handle(message, col):
    try:
        doc = json.loads(message.body)
        if not isinstance(doc, dict):
            raise TypeError("payload is not a JSON object")
        if "cow_id" in doc:
            doc["cow_id"] = str(doc["cow_id"]).strip().upper()
            if "date" in doc:
                doc["_id"] = f'{doc["cow_id"]}-{doc["date"]}'   # no duplicates
        await col.insert_one(doc)
        await message.ack()
    except DuplicateKeyError:
        await message.ack()
    except (json.JSONDecodeError, TypeError):
        log.warning("Bad message dropped: %r", message.body[:200])
        await message.reject(requeue=False)
    except Exception:
        log.exception("DB error, requeueing")
        await asyncio.sleep(2)
        await message.nack(requeue=True)


async def consume(col):
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
                        await handle(message, col)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("AMQP connection failed, retrying in 5 s")
            await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = AsyncMongoClient(settings.mongo_uri)
    col = client[settings.mongo_db][settings.mongo_collection]
    app.state.col = col
    try:
        await col.create_index([("cow_id", 1), ("date", 1)])
    except Exception:
        log.exception("Could not create index (will continue)")
    task = asyncio.create_task(consume(col))
    yield
    task.cancel()
    await client.close()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/events/{cow_id}")
async def events(cow_id: str, request: Request, limit: int = 30):
    cur = request.app.state.col.find(
        {"cow_id": cow_id.strip().upper()}, {"_id": 0}
    ).sort("date", -1).limit(limit)
    return await cur.to_list(length=limit)