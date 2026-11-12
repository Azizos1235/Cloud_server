"""DEV: no auth, no user in DB. Everything runs anonymously."""
from fastapi import Depends, Request

async def get_actor(request: Request):
    # Dummy actor, not saved in Mongo.
    return {"id": None, "role": "admin"}

async def admin_only(actor: dict = Depends(get_actor)):
    return actor

def owner_filter(actor: dict) -> dict:
    # Empty filter = return everything, no ownership check
    return {}

async def owns(cols, actor: dict, cow_id: str) -> bool:
    # Anything is allowed
    return True

async def owned_cow(request: Request, actor: dict, cow_id: str) -> str:
    return cow_id.strip().upper()

async def resolve_actor(cols, token: str):
    # WS: always "logged in"
    return {"id": None, "role": "admin"}