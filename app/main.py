import asyncio
import contextlib
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pymongo import AsyncMongoClient
from starlette.exceptions import HTTPException as StarletteHTTPException

from .config import settings
from .database import Cols, ensure_index
from .routers import alerts, cows, health, profiles, websocket
from .services.ingestion import start_consumers

log = logging.getLogger("ingest")
log.info("AI service URL: %r (proxy: %r)", settings.pi_ai_url, settings.pi_proxy)


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = AsyncMongoClient(settings.mongo_uri)
    cols = Cols(client[settings.mongo_db])
    app.state.cols = cols
    tasks = start_consumers(cols)                       # one consumer per queue
    tasks.append(asyncio.create_task(ensure_index(cols)))
    yield
    for t in tasks:
        t.cancel()
    for t in tasks:
        with contextlib.suppress(asyncio.CancelledError):
            await t
    await client.close()


app = FastAPI(title="Mangeoire Intelligente API", lifespan=lifespan)


@app.exception_handler(StarletteHTTPException)
async def http_error(_: Request, exc: StarletteHTTPException):
    # keeps the {"error": ...} shape the Flutter app already reads, plus "detail"
    return JSONResponse({"error": exc.detail, "detail": exc.detail}, status_code=exc.status_code)


for r in (health.router, cows.router, profiles.router, alerts.router, websocket.router):
    app.include_router(r)
