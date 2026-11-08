from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Request

from ..dependencies import admin_only, get_actor, owned_cow, to_oid
from ..schemas import CowProfile
from ..services.cow_service import owner_filter, profile_out

router = APIRouter(prefix="/profiles", tags=["profiles"])


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
    await cols.profiles.replace_one({"_id": cid}, doc, upsert=True)
    return profile_out(doc)


@router.get("")
async def list_profiles(request: Request, actor: dict = Depends(get_actor)):
    cur = request.app.state.cols.profiles.find(owner_filter(actor)).sort("_id", 1)
    return [profile_out(p) async for p in cur]


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
