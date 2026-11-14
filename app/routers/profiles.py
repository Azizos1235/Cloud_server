"""Cow profiles: admins and éleveurs create; only admins edit and delete."""
from datetime import date as Date, datetime
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pymongo.errors import DuplicateKeyError

from app.config import BREED_TYPE
from app.deps import admin_only, get_actor, owned_cow, owner_filter
from app.utils import age_months, normalize_nfc_uid, profile_out

router = APIRouter(prefix="/profiles")


class CowProfile(BaseModel):
    model_config = ConfigDict(extra="ignore")

    birth_date: Date
    weight_kg: float = Field(gt=0, le=1500)
    type: Literal["beef", "dairy", "dual"]
    breed: Literal["Holstein", "Jersey", "Angus", "Limousin", "Charolais", "Simmental"]
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

    @field_validator("cow_id")
    @classmethod
    def normalize_cow_id(cls, value: str) -> str:
        cow_id = value.strip().upper()
        if not cow_id:
            raise ValueError("cow_id is required")
        return cow_id


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_profile(p: CowProfileCreate, request: Request, actor: dict = Depends(get_actor)):
    cols = request.app.state.cols
    cid = p.cow_id
    if actor["role"] == "eleveur" and p.nfc_uid is None:
        raise HTTPException(422, "nfc_uid is required for an éleveur")
    if await cols.profiles.find_one({"_id": cid}, {"_id": 1}):
        raise HTTPException(409, f"Cow {cid} already exists")
    if p.nfc_uid is not None and await cols.profiles.find_one(
        {"nfc_uid": p.nfc_uid}, {"_id": 1}
    ):
        raise HTTPException(409, "NFC UID already assigned to another cow")

    doc = {"_id": cid, "admin_id": actor["admin_id"],
           "birth_date": datetime(p.birth_date.year, p.birth_date.month, p.birth_date.day),
           "weight_kg": p.weight_kg, "type": p.type, "breed": p.breed}
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
    profile = await request.app.state.cols.profiles.find_one(
        {"nfc_uid": normalized, **owner_filter(actor)}
    )
    if profile is None:
        raise HTTPException(404, "No profile for NFC UID")
    return profile_out(profile)


@router.get("/{cow_id}")
async def get_profile(cow_id: str, request: Request, actor: dict = Depends(get_actor)):
    cid = await owned_cow(request, actor, cow_id)
    return profile_out(await request.app.state.cols.profiles.find_one({"_id": cid}))


@router.put("/{cow_id}")
async def put_profile(cow_id: str, p: CowProfile, request: Request, admin: dict = Depends(admin_only)):
    cols = request.app.state.cols
    cid = cow_id.strip().upper()
    existing = await cols.profiles.find_one({"_id": cid}, {"admin_id": 1, "nfc_uid": 1})
    if existing and existing.get("admin_id") != admin["admin_id"]:
        raise HTTPException(404, f"No profile for cow {cid}")      # another admin's cow
    nfc_uid = p.nfc_uid
    if "nfc_uid" not in p.model_fields_set and existing:
        nfc_uid = existing.get("nfc_uid")
    doc = {"_id": cid, "admin_id": admin["admin_id"],
           "birth_date": datetime(p.birth_date.year, p.birth_date.month, p.birth_date.day),
           "weight_kg": p.weight_kg, "type": p.type, "breed": p.breed}
    if nfc_uid is not None:
        duplicate = await cols.profiles.find_one({"nfc_uid": nfc_uid, "_id": {"$ne": cid}}, {"_id": 1})
        if duplicate:
            raise HTTPException(409, "NFC UID already assigned to another cow")
        doc["nfc_uid"] = nfc_uid
    try:
        await cols.profiles.replace_one({"_id": cid}, doc, upsert=True)
    except DuplicateKeyError:
        raise HTTPException(409, "NFC UID already assigned to another cow")
    return profile_out(doc)


@router.delete("/{cow_id}")
async def delete_profile(cow_id: str, request: Request, admin: dict = Depends(admin_only)):
    cols = request.app.state.cols
    cid = await owned_cow(request, admin, cow_id)
    await cols.profiles.delete_one({"_id": cid})
    # composition: readings and daily summaries do not outlive their cow
    await cols.realtime.delete_many({"cow_id": cid})
    await cols.daily.delete_many({"cow_id": cid})
    return {"deleted": cid}
