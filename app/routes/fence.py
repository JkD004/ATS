from fastapi import APIRouter, HTTPException, Depends, status
from bson import ObjectId
from app.models import FenceCreate, FenceUpdate, FenceOut
from app.utils import get_current_user
from app.utils import find_point_in_radius
from app.database import db  # Assuming you have a global db instance from Motor
from datetime import datetime

fence_router = APIRouter()


# Create a fence
@fence_router.post("/fences", response_model=FenceOut)
async def create_fence_endpoint(fence_data: FenceCreate, current_user: dict = Depends(get_current_user)):
    # Check if a fence with the same name already exists for this user
    existing_fence = await db["fences"].find_one(
        {"userId": ObjectId(current_user["_id"]), "fenceName": fence_data.fenceName}
    )
    if existing_fence:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Fence with the same name already exists")

    # Prepare the fence data
    fence_data_dict = fence_data.dict()
    fence_data_dict["userId"] = ObjectId(current_user["_id"])
    fence_data_dict["createdAt"] = datetime.utcnow()
    fence_data_dict["updatedAt"] = datetime.utcnow()

    # Insert the new fence into MongoDB
    new_fence = await db["fences"].insert_one(fence_data_dict)

    # Determine whether to auto-select devices or use provided device_ids
    if fence_data.autoSelectDevices:
        # Fetch user's devices if auto-select is enabled
        user_device_ids = current_user["myDevices"]
    else:
        # Check if device_ids were provided
        if not hasattr(fence_data, "device_ids") or not fence_data.device_ids:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No devices provided")
        user_device_ids = fence_data.device_ids  # Assume device_ids is a list of device ObjectIds

    # Find devices within the radius and update them
    user_devices = await db["devices"].find({"_id": {"$in": user_device_ids}}).to_list(100)
    bulk_update_ops = []

    for device in user_devices:
        lat, lng = device.get("loc_data", {}).get("lat"), device.get("loc_data", {}).get("long")
        if lat is None or lng is None:
            continue

        # Check if the device is within the fence radius
        if find_point_in_radius(fence_data.center, {"lat": lat, "lng": lng}, fence_data.radius):
            bulk_update_ops.append({
                "updateOne": {
                    "filter": {"_id": device["_id"]},
                    "update": {"$set": {"fenceId": new_fence.inserted_id}}
                }
            })

    if bulk_update_ops:
        await db["devices"].bulk_write(bulk_update_ops)

    return {"message": "Fence created successfully", "id": str(new_fence.inserted_id)}


# Get all fences for the user
@fence_router.get("/fences", response_model=list)
async def get_user_fences(current_user: dict = Depends(get_current_user)):
    fences = await db["fences"].find({"userId": ObjectId(current_user["_id"])}).to_list(100)
    if not fences:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No fences found")
    return fences


# Edit a fence
@fence_router.put("/fences/{fence_id}", response_model=dict)
async def update_fence_endpoint(fence_id: str, fence_data: FenceUpdate, current_user: dict = Depends(get_current_user)):
    # Check if the fence exists
    fence = await db["fences"].find_one({"_id": ObjectId(fence_id), "userId": ObjectId(current_user["_id"])})
    if not fence:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Fence not found")

    # Prepare the update data
    fence_data_dict = fence_data.dict(exclude_unset=True)
    fence_data_dict["updatedAt"] = datetime.utcnow()

    # Update the fence in MongoDB
    await db["fences"].update_one({"_id": ObjectId(fence_id)}, {"$set": fence_data_dict})

    updated_fence = await db["fences"].find_one({"_id": ObjectId(fence_id)})
    return {"message": "Fence updated successfully", "data": updated_fence}


# Delete a fence
@fence_router.delete("/fences/{fence_id}", response_model=dict)
async def delete_fence_endpoint(fence_id: str, current_user: dict = Depends(get_current_user)):
    # Check if the fence exists
    fence = await db["fences"].find_one({"_id": ObjectId(fence_id), "userId": ObjectId(current_user["_id"])})
    if not fence:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Fence not found")

    # Delete the fence
    await db["fences"].delete_one({"_id": ObjectId(fence_id)})
    return {"message": "Fence deleted successfully"}
