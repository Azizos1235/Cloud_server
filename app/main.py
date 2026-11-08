import asyncio
import contextlib
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pymongo import AsyncMongoClient
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.config import settings
from app.db import Cols, ensure_index
from app.ingest.consumer import run_queue
from app.ingest.handlers import handle_daily, handle_dispatch, handle_realtime
from app.jobs.appetite import appetite_scheduler
from app.routers import alerts, cows, profiles, ws


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = AsyncMongoClient(settings.mongo_uri)
    cols = Cols(client[settings.mongo_db])
    app.state.cols = cols
    queues = [(settings.daily_queue, handle_daily),
              (settings.realtime_queue, handle_realtime),
              (settings.dispatch_queue, handle_dispatch)]
    tasks = [asyncio.create_task(run_queue(q, h, cols)) for q, h in queues if q]
    tasks.append(asyncio.create_task(ensure_index(cols)))
    if settings.eod_enabled:
        tasks.append(asyncio.create_task(appetite_scheduler(cols)))
    yield
    for t in tasks:
        t.cancel()
    for t in tasks:
        with contextlib.suppress(asyncio.CancelledError):
            await t
    await client.close()


app = FastAPI(lifespan=lifespan)


@app.exception_handler(StarletteHTTPException)
async def http_error(_: Request, exc: StarletteHTTPException):
    # keeps the {"error": ...} shape the Flutter app already reads, plus "detail"
    return JSONResponse({"error": exc.detail, "detail": exc.detail}, status_code=exc.status_code)


app.include_router(cows.router)
app.include_router(profiles.router)
app.include_router(alerts.router)
app.include_router(ws.router)
