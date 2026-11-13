from fastapi import APIRouter, Depends, HTTPException, Request

from app.deps import admin_only
from app.utils import sid, to_oid

router = APIRouter(prefix="/eleveurs")


@router.get("")
async def list_eleveurs(request: Request, admin: dict = Depends(admin_only)):
    cur = request.app.state.cols.eleveurs.find({"admin_id": admin["id"]}).sort("created_at", -1)
    result = []
    async for eleveur in cur:
        result.append({
            "id": sid(eleveur["_id"]),
            "name": eleveur.get("name"),
            "email": eleveur.get("email"),
            "phone": eleveur.get("phone"),
            "created_at": eleveur.get("created_at"),
            "cows_count": await request.app.state.cols.profiles.count_documents(
                {"eleveur_id": eleveur["_id"]}
            ),
        })
    return result


@router.delete("/{eleveur_id}")
async def delete_eleveur(eleveur_id: str, request: Request, admin: dict = Depends(admin_only)):
    cols = request.app.state.cols
    oid = to_oid(eleveur_id)
    result = await cols.eleveurs.delete_one({"_id": oid, "admin_id": admin["id"]})
    if result.deleted_count == 0:
        raise HTTPException(404, "Éleveur not found")
    await cols.profiles.update_many(
        {"eleveur_id": oid},
        {"$set": {"eleveur_id": None}},
    )
    return {"deleted": eleveur_id}
