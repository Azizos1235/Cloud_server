"""Cows: what the Flutter CowService calls (all scoped to the caller)."""
import logging
from datetime import date as Date, datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.config import settings
from app.cow_ids import cow_data_filter, normalize_cow_identifier
from app.deps import admin_only, get_actor, owned_cow, owner_filter
from app.jobs.appetite import run_for_day
from app.services.ai import predict
from app.utils import age_months, latest

log = logging.getLogger("ingest")
router = APIRouter()
MAX_HOME_APPETITE_COWS = 500
_APPETITE_CLASS_ALIASES = {
    "low": "decreasing",
    "decreasing": "decreasing",
    "normal": "stable",
    "stable": "stable",
    "high": "increasing",
    "increasing": "increasing",
}


def _home_appetite_class(value: object) -> Optional[str]:
    if not isinstance(value, str):
        return None
    return _APPETITE_CLASS_ALIASES.get(value.strip().lower())


def _home_appetite_date(value: object) -> Optional[str]:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, Date):
        return value.isoformat()
    if isinstance(value, str):
        try:
            return Date.fromisoformat(value[:10]).isoformat()
        except ValueError:
            return None
    return None


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


@router.get("/home/appetite")
async def home_appetite(request: Request, actor: dict = Depends(get_actor)):
    cols = request.app.state.cols
    profiles_cursor = (
        cols.profiles.find(
            owner_filter(actor),
            {"_id": 1, "cow_id": 1},
        )
        .sort("_id", 1)
        .limit(MAX_HOME_APPETITE_COWS + 1)
    )
    profiles = [profile async for profile in profiles_cursor]
    if len(profiles) > MAX_HOME_APPETITE_COWS:
        raise HTTPException(
            413,
            f"Home appetite is limited to {MAX_HOME_APPETITE_COWS} cows",
        )

    appetite_by_internal_id = {}
    identifiers_to_internal_ids = {}
    rfid_to_internal_id = {}
    for profile in profiles:
        if profile.get("_id") is None:
            continue
        internal_id = str(profile["_id"])
        rfid = normalize_cow_identifier(internal_id)
        raw_cow_id = profile.get("cow_id")
        cow_id = (
            normalize_cow_identifier(raw_cow_id)
            if isinstance(raw_cow_id, str) and raw_cow_id.strip()
            else None
        )
        appetite_by_internal_id[internal_id] = {
            "cow_id": cow_id,
            "appetite": None,
            "date": None,
        }
        rfid_to_internal_id[rfid] = internal_id
        for identifier in (rfid, cow_id):
            if identifier:
                identifiers_to_internal_ids.setdefault(
                    identifier, set()
                ).add(internal_id)

    if not appetite_by_internal_id:
        return {"appetite": appetite_by_internal_id}

    unambiguous_identifiers = {
        identifier: next(iter(internal_ids))
        for identifier, internal_ids in identifiers_to_internal_ids.items()
        if len(internal_ids) == 1
    }
    identifiers = sorted(identifiers_to_internal_ids)
    rfids = sorted(rfid_to_internal_id)
    pipeline = [
        {
            "$match": {
                "$or": [
                    {"cow_id": {"$in": identifiers}},
                    {"rfid": {"$in": rfids}},
                ]
            }
        },
        {"$sort": {"date": -1, "_id": 1}},
        {
            "$group": {
                "_id": {"$ifNull": ["$rfid", "$cow_id"]},
                "record": {"$first": "$$ROOT"},
            }
        },
        {"$sort": {"record.date": -1, "record._id": 1}},
        {
            "$project": {
                "_id": 1,
                "rfid": "$record.rfid",
                "date": "$record.date",
                "appetite_class": "$record.appetite_class",
            }
        },
    ]
    latest_by_internal_id = {}
    async for record in cols.daily.aggregate(pipeline):
        raw_rfid = record.get("rfid")
        internal_id = (
            rfid_to_internal_id.get(normalize_cow_identifier(raw_rfid))
            if isinstance(raw_rfid, str) and raw_rfid.strip()
            else None
        )
        if internal_id is None:
            raw_identifier = record.get("_id")
            if raw_identifier is not None:
                internal_id = unambiguous_identifiers.get(
                    normalize_cow_identifier(str(raw_identifier))
                )
        if internal_id is None or internal_id in latest_by_internal_id:
            continue
        latest_by_internal_id[internal_id] = record

    for internal_id, record in latest_by_internal_id.items():
        appetite_by_internal_id[internal_id].update(
            appetite=_home_appetite_class(record.get("appetite_class")),
            date=_home_appetite_date(record.get("date")),
        )

    return {"appetite": appetite_by_internal_id}


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
