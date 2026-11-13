"""Vache (cow_profiles): only the admin creates, edits and deletes."""
from datetime import date as Date, datetime
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field, field_validator, model_validator
from pymongo.errors import DuplicateKeyError

from app.config import BREED_TYPE
from app.deps import admin_only, get_actor, owned_cow, owner_filter
from app.utils import age_months, normalize_nfc_uid, profile_out, to_oid

router = APIRouter(prefix="/profiles")


class CowProfile(BaseModel):
    birth_date: Date
    weight_kg: float = Field(gt=0, le=1500)
    type: Literal["beef", "dairy", "dual"]
    breed: Literal["Holstein", "Jersey", "Angus", "Limousin", "Charolais", "Simmental"]
    eleveur_id: Optional[str] = None      # the éleveur who looks after this cow
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


@router.put("/{cow_id}")
async def put_profile(cow_id: str, p: CowProfile, request: Request, admin: dict = Depends(admin_only)):
    cols = request.app.state.cols
    cid = cow_id.strip().upper()
    existing = await cols.profiles.find_one({"_id": cid}, {"admin_id": 1})
    if existing and existing["admin_id"] != admin["id"]:
        raise HTTPException(404, f"No profile for cow {cid}")      # another admin's cow
    eleveur_id = None
    if p.eleveur_id:
        eleveur_id = to_oid(p.eleveur_id)
        # an admin can only assign his own éleveurs (the auth backend must store admin_id)
        if not await cols.eleveurs.find_one({"_id": eleveur_id, "admin_id": admin["id"]}, {"_id": 1}):
            raise HTTPException(400, "This éleveur does not belong to you")
    doc = {"_id": cid, "admin_id": admin["id"], "eleveur_id": eleveur_id,
           "birth_date": datetime(p.birth_date.year, p.birth_date.month, p.birth_date.day),
           "weight_kg": p.weight_kg, "type": p.type, "breed": p.breed}
    if p.nfc_uid is not None:
        duplicate = await cols.profiles.find_one({"nfc_uid": p.nfc_uid, "_id": {"$ne": cid}}, {"_id": 1})
        if duplicate:
            raise HTTPException(409, "NFC UID already assigned to another cow")
        doc["nfc_uid"] = p.nfc_uid
    try:
        await cols.profiles.replace_one({"_id": cid}, doc, upsert=True)
    except DuplicateKeyError:
        raise HTTPException(409, "NFC UID already assigned to another cow")
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


@router.delete("/{cow_id}")
async def delete_profile(cow_id: str, request: Request, admin: dict = Depends(admin_only)):
    cols = request.app.state.cols
    cid = await owned_cow(request, admin, cow_id)
    await cols.profiles.delete_one({"_id": cid})
    # composition: readings and daily summaries do not outlive their cow
    await cols.realtime.delete_many({"cow_id": cid})
    await cols.daily.delete_many({"cow_id": cid})
    return {"deleted": cid}
