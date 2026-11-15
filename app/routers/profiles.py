"""Cow profiles: admins and éleveurs create and update; only admins delete."""
from datetime import date as Date, datetime
from typing import Literal, Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pymongo.errors import DuplicateKeyError

from app.config import BREED_TYPE
from app.cow_ids import cow_data_filter, normalize_cow_identifier
from app.deps import admin_only, admin_or_eleveur, get_actor, owned_cow, owner_filter
from app.utils import age_months, normalize_nfc_uid, profile_out

router = APIRouter(prefix="/profiles")


class CowProfile(BaseModel):
    model_config = ConfigDict(extra="ignore")

    birth_date: Date
    weight_kg: float = Field(gt=0, le=1500)
    type: Literal["beef", "dairy", "dual"]
    breed: Literal["Holstein", "Montbeliarde", "Brune", "Frisonne"]
    nfc_uid: Optional[str] = None

    @field_validator("nfc_uid")
    @classmethod
    def valid_nfc_uid(cls, v: Optional[str]):
        return normalize_nfc_uid(v) if v is not None else None

    @field_validator("birth_date")
    @classmethod
    def valid_birth_date(cls, v: Date):
        if not 0 < age_months(v) <= 240:
            raise ValueError("birth_date must give an age between 1 and 240 months")
        return v

    @model_validator(mode="after")
    def type_matches_breed(self):
        if BREED_TYPE[self.breed] != self.type:
            raise ValueError(f"{self.breed} is a {BREED_TYPE[self.breed]} breed, not {self.type}")
        return self


class CowProfileCreate(CowProfile):
    cow_id: str
    rfid: Optional[str] = None

    @field_validator("cow_id")
    @classmethod
    def normalize_cow_id(cls, value: str) -> str:
        cow_id = value.strip().upper()
        if not cow_id:
            raise ValueError("cow_id is required")
        return cow_id

    @field_validator("rfid")
    @classmethod
    def normalize_rfid(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        rfid = normalize_cow_identifier(value)
        if not rfid:
            raise ValueError("rfid cannot be empty")
        return rfid


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_profile(p: CowProfileCreate, request: Request, actor: dict = Depends(get_actor)):
    cols = request.app.state.cols
    cid = p.cow_id
    if actor["role"] == "eleveur" and p.nfc_uid is None:
        raise HTTPException(422, "nfc_uid is required for an éleveur")
    profile_id = p.rfid or cid
    if await cols.profiles.find_one(
        {"$or": [{"_id": profile_id}, {"cow_id": cid}]}, {"_id": 1}
    ):
        raise HTTPException(409, f"Cow {cid} already exists")
    if p.nfc_uid is not None and await cols.profiles.find_one(
        {"$or": [{"nfc_uid": p.nfc_uid}, {"_id": p.nfc_uid}]}, {"_id": 1}
    ):
        raise HTTPException(409, "NFC UID already assigned to another cow")

    doc = {"_id": profile_id, "admin_id": actor["admin_id"],
           "birth_date": datetime(p.birth_date.year, p.birth_date.month, p.birth_date.day),
           "weight_kg": p.weight_kg, "type": p.type, "breed": p.breed}
    if p.rfid:
        doc["cow_id"] = cid
    if p.nfc_uid is not None:
        doc["nfc_uid"] = p.nfc_uid
    try:
        await cols.profiles.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(409, "Cow ID or NFC UID already exists")
    return profile_out(doc)


@router.get("")
async def list_profiles(request: Request, actor: dict = Depends(get_actor)):
    cur = request.app.state.cols.profiles.find(owner_filter(actor)).sort("_id", 1)
    return [profile_out(p) async for p in cur]


@router.get("/by-nfc/{uid}")
async def get_profile_by_nfc(uid: str, request: Request, actor: dict = Depends(get_actor)):
    try:
        normalized = normalize_nfc_uid(uid)
    except ValueError:
        raise HTTPException(400, "Invalid NFC UID")
    tag_query = {"$or": [{"nfc_uid": normalized}, {"_id": normalized}]}
    profiles = request.app.state.cols.profiles
    profile = await profiles.find_one({**tag_query, **owner_filter(actor)})
    if profile is None:
        if await profiles.find_one(tag_query, {"_id": 1}):
            raise HTTPException(409, "Tag already registered elsewhere")
        raise HTTPException(404, "No profile for NFC UID")
    return profile_out(profile)


@router.get("/{cow_id}")
async def get_profile(cow_id: str, request: Request, actor: dict = Depends(get_actor)):
    await owned_cow(request, actor, cow_id)
    identifier = normalize_cow_identifier(cow_id)
    profile = await request.app.state.cols.profiles.find_one(
        {"$or": [{"_id": identifier}, {"cow_id": identifier}], **owner_filter(actor)}
    )
    return profile_out(profile)


@router.put("/{cow_id}")
async def put_profile(
    cow_id: str,
    p: CowProfile,
    request: Request,
    admin_id: ObjectId = Depends(admin_or_eleveur),
):
    cols = request.app.state.cols
    identifier = normalize_cow_identifier(cow_id)
    existing = await cols.profiles.find_one(
        {
            "$or": [{"_id": identifier}, {"cow_id": identifier}],
            "admin_id": admin_id,
        }
    )
    if existing is None:
        raise HTTPException(404, f"No profile for cow {identifier}")

    updates = {
        "birth_date": datetime(p.birth_date.year, p.birth_date.month, p.birth_date.day),
        "weight_kg": p.weight_kg,
        "type": p.type,
        "breed": p.breed,
    }
    stored_birth_date = existing.get("birth_date")
    if isinstance(stored_birth_date, datetime):
        stored_birth_date = stored_birth_date.date()
    if stored_birth_date != p.birth_date:
        updates["age_months"] = age_months(p.birth_date)

    unset = {}
    if p.nfc_uid is not None:
        duplicate = await cols.profiles.find_one(
            {"nfc_uid": p.nfc_uid, "_id": {"$ne": existing["_id"]}}, {"_id": 1}
        )
        if duplicate:
            raise HTTPException(409, "NFC UID already assigned to another cow")
        updates["nfc_uid"] = p.nfc_uid
    elif "nfc_uid" in p.model_fields_set:
        unset["nfc_uid"] = ""

    update = {"$set": updates}
    if unset:
        update["$unset"] = unset
    try:
        result = await cols.profiles.update_one(
            {"_id": existing["_id"], "admin_id": admin_id}, update
        )
    except DuplicateKeyError:
        raise HTTPException(409, "NFC UID already assigned to another cow")
    if result.matched_count == 0:
        raise HTTPException(404, f"No profile for cow {identifier}")

    doc = {**existing, **updates}
    if unset:
        doc.pop("nfc_uid", None)
    return profile_out(doc)


@router.delete("/{cow_id}")
async def delete_profile(cow_id: str, request: Request, admin: dict = Depends(admin_only)):
    cols = request.app.state.cols
    cid = await owned_cow(request, admin, cow_id)
    profile = await cols.profiles.find_one(
        {"$or": [{"_id": normalize_cow_identifier(cow_id)}, {"cow_id": cid}]},
        {"_id": 1},
    )
    cow_filter = await cow_data_filter(cols, cid)
    await cols.profiles.delete_one({"_id": profile["_id"]})
    # composition: readings and daily summaries do not outlive their cow
    await cols.realtime.delete_many(cow_filter)
    await cols.daily.delete_many(cow_filter)
    return {"deleted": cid}
