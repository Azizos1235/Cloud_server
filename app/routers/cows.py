"""Cows: what the Flutter CowService calls (all scoped to the caller)."""
import logging
from datetime import date as Date, datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.config import settings
from app.cow_ids import cow_data_filter
from app.deps import admin_only, get_actor, owned_cow, owner_filter
from app.jobs.appetite import run_for_day
from app.services.ai import predict
from app.utils import age_months, latest

log = logging.getLogger("ingest")
router = APIRouter()


@router.get("/health")
async def health():
    return {"status": "ok", "pi_ai_configured": bool(settings.pi_ai_url)}


@router.get("/cows")
async def cows(request: Request, actor: dict = Depends(get_actor)):
    cur = request.app.state.cols.profiles.find(owner_filter(actor), {"_id": 1}).sort("_id", 1)
    profiles = [p async for p in cur]
    return {
        "cows": [p["_id"] for p in profiles],
        "cow_profiles": [
            {"internal_id": str(p["_id"]), "cow_id": p.get("cow_id")}
            for p in profiles
        ],
    }


@router.get("/latest/{cow_id}")
async def latest_http(cow_id: str, request: Request, actor: dict = Depends(get_actor)):
    cid = await owned_cow(request, actor, cow_id)
    cow_filter = await cow_data_filter(request.app.state.cols, cid)
    doc = await latest(request.app.state.cols.daily, cow_filter)
    if doc is None:
        raise HTTPException(404, f"No data for cow {cid}")
    profile = await request.app.state.cols.profiles.find_one(
        {"$or": [{"_id": cid}, {"cow_id": cid}], **owner_filter(actor)},
        {"birth_date": 1},
    )
    doc["cow_id"] = cid
    if profile is not None and profile.get("birth_date") is not None:
        doc["age_months"] = age_months(profile["birth_date"].date())
    doc["food_consumed_g"] = doc.get(
        "food_consumed_g", doc.get("total_food_consumed_g", 0)
    )
    doc["feed_offered_g"] = doc.get(
        "feed_offered_g", doc.get("total_feed_offered_g", 0)
    )
    doc["eating_minutes"] = doc.get(
        "eating_minutes", doc.get("total_eating_minutes", 0)
    )
    doc.setdefault("visits", 0)
    doc.setdefault("no_food_visits", 0)
    doc.setdefault("avg_meal_duration_min", 0)
    return doc


@router.get("/predict/{cow_id}")
async def predict_http(cow_id: str, request: Request, actor: dict = Depends(get_actor)):
    cid = await owned_cow(request, actor, cow_id)
    cow_filter = await cow_data_filter(request.app.state.cols, cid)
    if await latest(request.app.state.cols.daily, cow_filter) is None:
        raise HTTPException(404, f"No data for cow {cid}")
    try:
        return await predict({"cow_id": cid})
    except NotImplementedError as e:
        raise HTTPException(501, str(e))
    except Exception as e:
        log.exception("AI service call failed")
        raise HTTPException(502, f"AI service unreachable: {type(e).__name__}: {e}")


@router.get("/realtime/{cow_id}")
async def realtime(cow_id: str, request: Request, limit: int = Query(50, ge=1, le=1000),
                   actor: dict = Depends(get_actor)):
    cid = await owned_cow(request, actor, cow_id)
    cow_filter = await cow_data_filter(request.app.state.cols, cid)
    cur = request.app.state.cols.realtime.find(cow_filter, {"_id": 0}) \
        .sort("timestamp", -1).limit(limit)
    return await cur.to_list(length=limit)


@router.get("/daily/{cow_id}")
async def daily(cow_id: str, request: Request, limit: int = Query(30, ge=1, le=366),
                actor: dict = Depends(get_actor)):
    cid = await owned_cow(request, actor, cow_id)
    cow_filter = await cow_data_filter(request.app.state.cols, cid)
    cur = request.app.state.cols.daily.find(cow_filter, {"_id": 0}).sort("date", -1).limit(limit)
    return await cur.to_list(length=limit)


@router.get("/dispatch/{cow_id}")
async def dispatches(cow_id: str, request: Request, limit: int = Query(30, ge=1, le=366),
                     actor: dict = Depends(get_actor)):
    cid = await owned_cow(request, actor, cow_id)
    cow_filter = await cow_data_filter(request.app.state.cols, cid)
    cur = request.app.state.cols.dispatch.find(cow_filter, {"_id": 0}).sort("date", -1).limit(limit)
    return await cur.to_list(length=limit)


@router.post("/appetite/run")
async def run_appetite_now(request: Request, day: Optional[Date] = None,
                           admin: dict = Depends(admin_only)):
    """Run the end-of-day appetite job by hand for the admin's own cows (default: today, UTC)."""
    cols = request.app.state.cols
    ids = [
        identifier
        async for p in cols.profiles.find(
            {"admin_id": admin["id"]}, {"_id": 1, "cow_id": 1}
        )
        for identifier in (p["_id"], p.get("cow_id"))
        if identifier
    ]
    day = day or datetime.now(timezone.utc).date()
    return {"date": day.isoformat(), "updated": await run_for_day(cols, day, ids)}
