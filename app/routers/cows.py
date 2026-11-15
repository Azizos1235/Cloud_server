"""Cows: what the Flutter CowService calls (all scoped to the caller)."""
import logging
import math
import os
import re
import traceback
from datetime import date as Date, datetime, timezone
from typing import Optional

from bson import ObjectId
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
MAX_HOME_APPETITE_VARIANTS = 25_000
_APPETITE_CLASS_ALIASES = {
    "low": "decreasing",
    "decreased": "decreasing",
    "decreasing": "decreasing",
    "decrease": "decreasing",
    "normal": "stable",
    "stable": "stable",
    "high": "increasing",
    "increased": "increasing",
    "increasing": "increasing",
    "increase": "increasing",
}


def _home_appetite_class(value: object) -> Optional[str]:
    if not isinstance(value, str):
        return None
    label = value.strip().casefold()
    if not label:
        return None
    exact_match = _APPETITE_CLASS_ALIASES.get(label)
    if exact_match is not None:
        return exact_match
    if any(alias in label for alias in ("low", "decreased", "decreasing", "decrease")):
        return "decreasing"
    if any(alias in label for alias in ("normal", "stable")):
        return "stable"
    if any(alias in label for alias in ("high", "increased", "increasing", "increase")):
        return "increasing"
    return None


def _home_cow_id(value: object) -> str:
    if value is None:
        return ""
    return re.sub(r"[-_]", "", str(value).strip()).upper()


def _home_identifier_variants(value: object) -> set[str]:
    raw = str(value).strip() if value is not None else ""
    compact = re.sub(r"[-_]", "", raw)
    variants = set()
    for candidate in (raw, compact):
        if candidate:
            variants.update((candidate, candidate.upper(), candidate.lower()))

    boundaries = [
        index
        for index in range(1, len(compact))
        if compact[index - 1].isalnum()
        and compact[index].isalnum()
        and compact[index - 1].isalpha() != compact[index].isalpha()
    ]
    for separator in ("-", "_"):
        separated = "".join(
            (separator if index in boundaries else "") + character
            for index, character in enumerate(compact)
        )
        if separated:
            variants.update(
                (separated, separated.upper(), separated.lower())
            )
    return variants


def _home_identifier_match(variants: list[str]) -> dict:
    return {
        "$or": [
            {"cow_id": {"$in": variants}},
            {"rfid": {"$in": variants}},
        ]
    }


def _home_appetite_match_conditions():
    return [
        {"appetite_class": {"$type": "string", "$ne": ""}},
        {"appetite.category": {"$type": "string", "$ne": ""}},
        {"appetite.status": {"$type": "string", "$ne": ""}},
        {"appetite_pct": {"$exists": True}},
        {"appetite.percentages": {"$exists": True}},
    ]


def _home_daily_pipeline(variants: list[str], require_appetite: bool) -> list[dict]:
    conditions = [_home_identifier_match(variants)]
    if require_appetite:
        conditions.append({"$or": _home_appetite_match_conditions()})
    return [
        {"$match": {"$and": conditions}},
        {"$sort": {"date": -1}},
        {
            "$group": {
                "_id": {"cow": "$cow_id", "rfid": "$rfid"},
                "doc": {"$first": "$$ROOT"},
            }
        },
    ]


def _home_profiles_index(profiles: list[dict]) -> tuple[dict, dict, list[str]]:
    by_internal_id = {}
    owners_by_identifier = {}
    variants = set()
    for profile in profiles:
        if profile.get("_id") is None:
            continue
        internal_id = str(profile["_id"])
        raw_cow_id = profile.get("cow_id")
        cow_id = (
            normalize_cow_identifier(raw_cow_id)
            if isinstance(raw_cow_id, str) and raw_cow_id.strip()
            else None
        )
        by_internal_id[internal_id] = {
            "cow_id": cow_id,
            "appetite": None,
            "source": None,
            "raw": None,
            "date": None,
        }
        for value in (
            profile["_id"],
            raw_cow_id,
            profile.get("rfid"),
            profile.get("nfc_uid"),
        ):
            identifier = _home_cow_id(value)
            if identifier:
                owners_by_identifier.setdefault(identifier, set()).add(internal_id)
                variants.update(_home_identifier_variants(value))
    internal_by_identifier = {
        identifier: next(iter(internal_ids))
        for identifier, internal_ids in owners_by_identifier.items()
        if len(internal_ids) == 1
    }
    return (
        by_internal_id,
        internal_by_identifier,
        sorted(variants)[:MAX_HOME_APPETITE_VARIANTS],
    )


def _home_profile_for_record(record: dict, internal_by_identifier: dict) -> Optional[str]:
    matches = {
        internal_by_identifier.get(_home_cow_id(record.get(field)))
        for field in ("cow_id", "rfid")
    }
    matches.discard(None)
    return next(iter(matches)) if len(matches) == 1 else None


def _home_record_datetime(value: object) -> Optional[datetime]:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, Date):
        parsed = datetime.combine(value, datetime.min.time())
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            try:
                parsed = datetime.combine(Date.fromisoformat(value[:10]), datetime.min.time())
            except ValueError:
                return None
    else:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _home_record_is_newer(candidate: dict, previous: Optional[dict]) -> bool:
    if previous is None:
        return True
    candidate_date = _home_record_datetime(candidate.get("date"))
    previous_date = _home_record_datetime(previous.get("date"))
    return (
        candidate_date is not None
        and (previous_date is None or candidate_date > previous_date)
    )


def _largest_appetite_percentage(value: object) -> Optional[str]:
    if not isinstance(value, dict):
        return None
    candidates = []
    for raw_label, raw_percent in value.items():
        appetite = _home_appetite_class(raw_label)
        if appetite is None or isinstance(raw_percent, bool):
            continue
        try:
            percent = float(raw_percent)
        except (TypeError, ValueError):
            continue
        if math.isfinite(percent):
            candidates.append((percent, appetite))
    return max(candidates, key=lambda item: item[0])[1] if candidates else None


def _resolve_home_appetite(record: dict) -> tuple[Optional[str], Optional[str], object]:
    appetite = record.get("appetite")
    nested = appetite if isinstance(appetite, dict) else {}
    for source, raw in (
        ("appetite_class", record.get("appetite_class")),
        ("appetite.category", nested.get("category")),
        ("appetite.status", nested.get("status")),
    ):
        if isinstance(raw, str) and raw.strip():
            return _home_appetite_class(raw), source, raw

    for source, raw in (
        ("appetite_pct", record.get("appetite_pct")),
        ("appetite.percentages", nested.get("percentages")),
    ):
        selected = _largest_appetite_percentage(raw)
        if selected is not None:
            return selected, source, raw
    return None, None, None


def _home_json_value(value: object):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Date):
        return value.isoformat()
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {str(key): _home_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_home_json_value(item) for item in value]
    return value


def _home_daily_appetite_fields(record: dict) -> dict:
    return {
        "date": _home_appetite_date(record.get("date")),
        "appetite_class": _home_json_value(record.get("appetite_class")),
        "appetite": _home_json_value(record.get("appetite")),
        "appetite_pct": _home_json_value(record.get("appetite_pct")),
        "appetite_computed_at": _home_json_value(
            record.get("appetite_computed_at")
        ),
    }


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
    return {
        "status": "ok",
        "pi_ai_configured": bool(settings.pi_ai_url),
        "build": os.getenv("RENDER_GIT_COMMIT", "unknown"),
    }


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
            {"_id": 1, "cow_id": 1, "rfid": 1, "nfc_uid": 1},
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

    appetite_by_internal_id, internal_by_identifier, variants = _home_profiles_index(
        profiles
    )
    if not variants:
        return _home_json_value({"appetite": appetite_by_internal_id})

    pipeline = _home_daily_pipeline(variants, require_appetite=True)
    cursor = await cols.daily.aggregate(pipeline)
    selected_by_internal_id = {}
    async for grouped in cursor:
        record = grouped.get("doc")
        if record is None:
            continue
        internal_id = _home_profile_for_record(record, internal_by_identifier)
        if internal_id is None or internal_id not in appetite_by_internal_id:
            continue
        _, source, _ = _resolve_home_appetite(record)
        if source is None or not _home_record_is_newer(
            record, selected_by_internal_id.get(internal_id)
        ):
            continue
        selected_by_internal_id[internal_id] = record

    for internal_id, record in selected_by_internal_id.items():
        result, source, raw = _resolve_home_appetite(record)
        appetite_by_internal_id[internal_id].update(
            appetite=result,
            source=source,
            raw=_home_json_value(raw),
            date=_home_appetite_date(record.get("date")),
        )

    return _home_json_value({"appetite": appetite_by_internal_id})


async def _home_appetite_debug_response(request: Request, actor: dict) -> dict:
    cols = request.app.state.cols
    cursor = (
        cols.profiles.find(
            owner_filter(actor),
            {"_id": 1, "cow_id": 1, "rfid": 1, "nfc_uid": 1},
        )
        .sort("_id", 1)
        .limit(MAX_HOME_APPETITE_COWS + 1)
    )
    owned_profiles = [profile async for profile in cursor]
    if len(owned_profiles) > MAX_HOME_APPETITE_COWS:
        raise HTTPException(
            413,
            f"Home appetite debug is limited to {MAX_HOME_APPETITE_COWS} cows",
        )
    profiles = owned_profiles[:10]
    _, internal_by_identifier, variants = _home_profiles_index(profiles)
    _, _, stats_variants = _home_profiles_index(owned_profiles)
    if not variants or not stats_variants:
        return {
            "cows": [
                {
                    "profile": {
                        "_id": str(profile["_id"]),
                        "cow_id": profile.get("cow_id"),
                    },
                    "newest_daily": None,
                    "chosen_doc": None,
                }
                for profile in profiles
                if profile.get("_id") is not None
            ],
            "total_cows": len(owned_profiles),
            "total_daily_docs_with_appetite_class": 0,
            "top_raw_labels": [],
        }

    newest_by_internal_id = {}
    newest_cursor = await cols.daily.aggregate(
        _home_daily_pipeline(variants, require_appetite=False)
    )
    async for grouped in newest_cursor:
        record = grouped.get("doc")
        if record is None:
            continue
        internal_id = _home_profile_for_record(record, internal_by_identifier)
        if internal_id is not None and _home_record_is_newer(
            record, newest_by_internal_id.get(internal_id)
        ):
            newest_by_internal_id[internal_id] = record

    chosen_by_internal_id = {}
    chosen_cursor = await cols.daily.aggregate(
        _home_daily_pipeline(variants, require_appetite=True)
    )
    async for grouped in chosen_cursor:
        record = grouped.get("doc")
        if record is None:
            continue
        internal_id = _home_profile_for_record(record, internal_by_identifier)
        if internal_id is None:
            continue
        _, source, _ = _resolve_home_appetite(record)
        if source is not None and _home_record_is_newer(
            record, chosen_by_internal_id.get(internal_id)
        ):
            chosen_by_internal_id[internal_id] = record

    appetite_class_match = {"appetite_class": {"$exists": True}}
    raw_label_match = {"appetite_class": {"$type": "string", "$ne": ""}}
    stats_pipeline = [
        {"$match": {"$and": [_home_identifier_match(stats_variants)]}},
        {
            "$facet": {
                "total": [
                    {"$match": appetite_class_match},
                    {"$count": "count"},
                ],
                "labels": [
                    {"$match": raw_label_match},
                    {"$group": {"_id": "$appetite_class", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1, "_id": 1}},
                    {"$limit": 10},
                    {"$project": {"_id": 0, "raw": "$_id", "count": 1}},
                ],
            }
        },
    ]
    stats_cursor = await cols.daily.aggregate(stats_pipeline)
    stats = {}
    async for item in stats_cursor:
        stats = item
        break
    total = stats.get("total", [])
    cows_debug = []
    for profile in profiles:
        if profile.get("_id") is None:
            continue
        internal_id = str(profile["_id"])
        newest = newest_by_internal_id.get(internal_id)
        chosen = chosen_by_internal_id.get(internal_id)
        chosen_fields = None
        if chosen is not None:
            result, source, raw = _resolve_home_appetite(chosen)
            chosen_fields = {
                **_home_daily_appetite_fields(chosen),
                "label": result,
                "source": source,
                "raw": _home_json_value(raw),
            }
        cows_debug.append(
            {
                "profile": {
                    "_id": internal_id,
                    "cow_id": profile.get("cow_id"),
                },
                "newest_daily": (
                    _home_daily_appetite_fields(newest) if newest is not None else None
                ),
                "chosen_doc": chosen_fields,
            }
        )

    return {
        "cows": cows_debug,
        "total_cows": len(owned_profiles),
        "total_daily_docs_with_appetite_class": (
            total[0]["count"] if total else 0
        ),
        "top_raw_labels": stats.get("labels", []),
    }


@router.get("/home/appetite/debug")
async def home_appetite_debug(request: Request, actor: dict = Depends(get_actor)):
    try:
        return _home_json_value(await _home_appetite_debug_response(request, actor))
    except HTTPException:
        raise
    except Exception as exc:
        log.exception("Home appetite debug endpoint failed")
        return {
            "error": type(exc).__name__,
            "message": str(exc)[:300],
            "traceback": traceback.format_exc().splitlines()[-10:],
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
