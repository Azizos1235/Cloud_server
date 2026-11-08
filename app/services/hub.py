import json
import logging

from fastapi import WebSocket
from fastapi.encoders import jsonable_encoder

from ..database import Cols

log = logging.getLogger("ingest")


class Hub:
    """Keeps the connected clients. Each client only receives the cows it owns."""

    def __init__(self):
        self.clients: dict[WebSocket, dict] = {}   # websocket -> actor

    async def broadcast(self, cols: Cols, cow_id: str, msg: dict):
        owner = await cols.profiles.find_one({"_id": cow_id}, {"admin_id": 1, "eleveur_id": 1})
        if not owner:
            return                                  # cow without owner: nobody gets it
        try:
            text = json.dumps(jsonable_encoder(msg))
        except Exception:
            log.exception("Could not encode broadcast message")
            return
        dead = []
        for ws, actor in list(self.clients.items()):
            key = "admin_id" if actor["role"] == "admin" else "eleveur_id"
            if owner.get(key) != actor["id"]:
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
