"""WebSocket hub: each client only receives the cows it owns."""
import json
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from fastapi import WebSocket
from fastapi.encoders import jsonable_encoder

from app.config import settings
from app.db import Cols

log = logging.getLogger("ingest")


def _is_today_event(msg: dict) -> bool:
    data = msg.get("data")
    timestamp = data.get("timestamp") if isinstance(data, dict) else None
    if isinstance(timestamp, str):
        try:
            timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            return False
    if not isinstance(timestamp, datetime):
        return False
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    tz = ZoneInfo(settings.timezone)
    return timestamp.astimezone(tz).date() == datetime.now(tz).date()


class Hub:
    def __init__(self):
        self.clients: dict[WebSocket, dict] = {}   # websocket -> actor

    async def broadcast(self, cols: Cols, cow_id: str, msg: dict):
        if not _is_today_event(msg):
            return
        owner = await cols.profiles.find_one({"_id": cow_id}, {"admin_id": 1})
        if not owner:
            return                                  # cow without owner: nobody gets it
        try:
            text = json.dumps(jsonable_encoder(msg))
        except Exception:
            log.exception("Could not encode broadcast message")
            return
        dead = []
        for ws, actor in list(self.clients.items()):
            if owner.get("admin_id") != actor["admin_id"]:
                continue
            try:
                await ws.send_text(text)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.pop(ws, None)


hub = Hub()


async def reply(ws: WebSocket, msg: dict):
    await ws.send_text(json.dumps(jsonable_encoder(msg)))
