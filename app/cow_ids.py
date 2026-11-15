"""Resolve RFID tags to stable, human-readable cow IDs."""

from collections import OrderedDict

_CACHE_LIMIT = 512
_cow_id_cache: OrderedDict[str, str] = OrderedDict()


def normalize_cow_identifier(value: str) -> str:
    return str(value).strip().upper()


async def resolve_cow_id(cols, rfid: str) -> str | None:
    """Resolve an RFID tag through cow_profiles, caching successful lookups only."""
    tag = normalize_cow_identifier(rfid)
    if not tag:
        return None
    if tag in _cow_id_cache:
        _cow_id_cache.move_to_end(tag)
        return _cow_id_cache[tag]

    profile = await cols.profiles.find_one({"_id": tag}, {"cow_id": 1})
    cow_id = profile.get("cow_id") if profile else None
    if not isinstance(cow_id, str) or not cow_id.strip():
        return None

    cow_id = cow_id.strip().upper()
    if cow_id == tag:
        return None
    _cow_id_cache[tag] = cow_id
    _cow_id_cache.move_to_end(tag)
    if len(_cow_id_cache) > _CACHE_LIMIT:
        _cow_id_cache.popitem(last=False)
    return cow_id


async def cow_data_filter(cols, cow_id: str) -> dict:
    """Match current canonical documents and unmigrated RFID-keyed documents."""
    identifier = normalize_cow_identifier(cow_id)
    profile = await cols.profiles.find_one(
        {"$or": [{"cow_id": identifier}, {"_id": identifier}]},
        {"_id": 1, "cow_id": 1},
    )
    tag = normalize_cow_identifier(profile["_id"]) if profile else identifier
    canonical = (
        normalize_cow_identifier(profile["cow_id"])
        if profile and isinstance(profile.get("cow_id"), str) and profile["cow_id"].strip()
        else identifier
    )
    return {
        "$or": [
            {"cow_id": canonical},
            {"rfid": tag},
            {"cow_id": tag},
        ]
    }
