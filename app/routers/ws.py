"""WebSocket: /ws/cows?token=<JWT>"""
import json
import logging

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect

from app.db import Cols
from app.deps import owner_filter, owns, resolve_actor
from app.hub import hub, reply
from app.services.ai import predict
from app.utils import latest

log = logging.getLogger("ingest")
router = APIRouter()


async def on_client_message(ws: WebSocket, cols: Cols, actor: dict, msg: dict):
    kind = msg.get("type")
    echo_id = msg.get("cow_id")
    cow_id = str(echo_id).strip().upper() if echo_id is not None else None
    try:
        if kind == "list":
            cur = cols.profiles.find(owner_filter(actor), {"_id": 1}).sort("_id", 1)
            await reply(ws, {"type": "cows", "ids": [p["_id"] async for p in cur]})

        elif kind in ("get", "ask") and cow_id:
            if not await owns(cols, actor, cow_id):
                raise LookupError(f"No cow {cow_id}")
            doc = await latest(cols.daily, cow_id)
            if doc is None:
                raise LookupError(f"No data for cow {cow_id}")
            if kind == "get":
                await reply(ws, {"type": "cow", "cow_id": echo_id, "data": doc})
            else:
                await reply(ws, {"type": "ai", "cow_id": echo_id, "result": await predict(doc)})

    except Exception as e:
        if not isinstance(e, (NotImplementedError, LookupError)):
            log.exception("WebSocket request failed: %r", msg)
        await reply(ws, {"type": "error", "cow_id": echo_id,
                         "where": "ai" if kind == "ask" else "data", "message": str(e)})


@router.websocket("/ws/cows")
async def ws_cows(ws: WebSocket, token: str = Query("")):
    cols = ws.app.state.cols
    actor = await resolve_actor(cols, token)
    await ws.accept()
    if actor is None:
        # Accept first so the client receives an actual WebSocket close code.
        await ws.close(code=1008)
        return
    hub.clients[ws] = actor
    try:
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except ValueError:
                continue
            if isinstance(msg, dict):
                await on_client_message(ws, cols, actor, msg)
    except WebSocketDisconnect:
        pass
    except Exception:
        log.exception("WebSocket crashed")
    finally:
        hub.clients.pop(ws, None)
