from datetime import datetime, timezone
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from bson import ObjectId

from app.database import db
from app.models import DeviceData
from app.utils import get_current_user

import logging

device_data_router = APIRouter()


@device_data_router.post("/device-data", response_model=DeviceData)
async def create_device_data(device_data: DeviceData, current_user: str = Depends(get_current_user)):
    device_data_dict = {
        "device_id": device_data.device,
        "state": device_data.state,
        "reading_type": device_data.readingType,
        "reading": device_data.reading,
        "reading_unit": device_data.readingUnit,
        "created_at": datetime.now(timezone.utc),
    }
    await db["device_data"].insert_one(device_data_dict)
    return DeviceData(**device_data_dict)


# Get a list of devices (Fallback for older endpoints)
@device_data_router.get("/device-data", response_model=List[DeviceData])
async def get_device_data_list(current_user: str = Depends(get_current_user)):
    devices = await db["device_data"].find().to_list(length=None)
    if not devices:
        raise HTTPException(status_code=404, detail="No devices found")
    return [DeviceData(**device) for device in devices]

CATEGORY_LIMITS = {
    "A": 10,
    "B": 30,
    "C": 51
}


@device_data_router.get("/device-data/{device_id}/{list_category}", response_model=List[DeviceData])
async def get_device_data(device_id: str, list_category: str, current_user: str = Depends(get_current_user)):
    if device_id not in current_user.get("mydevices", []):
        raise HTTPException(status_code=404, detail="Device not part of user's devices")

    limit = CATEGORY_LIMITS.get(list_category.upper(), 10)

    # Query MongoDB directly — sorted by created_at descending, limited by category
    # Support both 'device_id' (new) and 'device' (legacy) for compatibility
    cursor = db["device_data"].find(
        {"$or": [{"device_id": device_id}, {"device": device_id}]}
    ).sort("created_at", -1).limit(limit)

    device_data = await cursor.to_list(length=limit)

    if not device_data:
        raise HTTPException(status_code=404, detail="Device data not found")

    return [DeviceData(**doc) for doc in device_data]


@device_data_router.get("/device-data/by-time-range", response_model=List[DeviceData])
async def get_device_data_by_time_range(
    device_id: str,
    from_datetime: datetime = Query(..., description="Start datetime in ISO format"),
    to_datetime: datetime = Query(..., description="End datetime in ISO format"),
    current_user: str = Depends(get_current_user)
):
    if device_id not in current_user.get("mydevices", []):
        raise HTTPException(status_code=403, detail="You do not have access to this device.")

    # Query MongoDB with a time range filter
    # Support both 'device_id' (new) and 'device' (legacy) for compatibility
    cursor = db["device_data"].find({
        "$or": [{"device_id": device_id}, {"device": device_id}],
        "created_at": {
            "$gte": from_datetime,
            "$lte": to_datetime
        }
    }).sort("created_at", 1)

    device_data = await cursor.to_list(length=None)

    if not device_data:
        raise HTTPException(status_code=404, detail="No data found in the given time range.")

    return [DeviceData(**doc) for doc in device_data]