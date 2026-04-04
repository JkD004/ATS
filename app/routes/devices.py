from datetime import datetime, timezone
from typing import List

from bson.objectid import ObjectId
from fastapi import APIRouter, Depends, HTTPException, status

from app.database import db
from app.models import DeviceCreate, DeviceOut, DeviceUpdate
from app.utils import get_current_user

device_router = APIRouter()

def format_device_data(device: dict) -> dict:
    if not device.get("device_data"):
        device["device_data"] = {}
    
    if "data_retrieve_time" not in device["device_data"]:
        updated_val = device.get("updated_at") or device.get("created_at") or datetime.utcnow()
        if not isinstance(updated_val, str):
            if hasattr(updated_val, "tzinfo") and not updated_val.tzinfo:
                updated_val = updated_val.replace(tzinfo=timezone.utc)
            updated_val = updated_val.isoformat()
            
        device["device_data"]["data_retrieve_time"] = {
            "updated_at": updated_val,
            "value": "last_seen"
        }
    return device


@device_router.post("/devices", response_model=DeviceOut)
async def create_device(device: DeviceCreate, current_user: str = Depends(get_current_user)):
    device_check = await db["devices"].find_one({"device_id": device.device_id})
    if device_check:
        raise HTTPException(status_code=404, detail="Device with this Id already exists")
    device_data = {
        "name": device.name,
        "parameters": device.parameters,
        "controls": device.controls,
        "device_id": device.device_id,
        "created_at": datetime.utcnow(),
        "updated_at": None,
        "status": "online",
        "last_status_update": None
    }
    result = await db["devices"].insert_one(device_data)

    updated_devices = list(set(current_user.get("mydevices", []) + [device.device_id]))

    # Update the user document in MongoDB
    await db["users"].update_one(
        {"_id": ObjectId(current_user["_id"])},
        {"$set": {"mydevices": updated_devices}},
    )
    return DeviceOut(**device_data)


# Get a list of devices
@device_router.get("/devices", response_model=List[DeviceOut])
async def get_device_list(current_user: str = Depends(get_current_user)):
    user_device_ids = current_user.get("mydevices", [])
    if not user_device_ids:
        raise HTTPException(status_code=404, detail="No devices found for this user")

    # Fetch devices that match the user's device_ids
    devices = await db["devices"].find({"device_id": {"$in": user_device_ids}}).to_list(length=None)

    if not devices:
        raise HTTPException(status_code=404, detail="No devices found")
    return [DeviceOut(**format_device_data(device)) for device in devices]


@device_router.get("/devices/{device_id}", response_model=DeviceOut)
async def get_device(device_id: str, current_user: str = Depends(get_current_user)):
    user_device_ids = current_user.get("mydevices", [])
    if device_id not in user_device_ids:
        raise HTTPException(status_code=404, detail="Devices does not belong to this user")

    device = await db["devices"].find_one({"device_id": device_id})
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")

    return DeviceOut(**format_device_data(device))

@device_router.get("/device-info/{device_id}", response_model=DeviceOut)
async def get_device_info(device_id: str, current_user: str = Depends(get_current_user)):

    device = await db["devices"].find_one({"device_id": device_id})
    if not device:
        raise HTTPException(status_code=404, detail="Device not found")

    return DeviceOut(**format_device_data(device))

@device_router.put("/devices/{device_id}", response_model=DeviceOut)
async def update_device(device_id: str, device: DeviceUpdate, current_user: dict = Depends(get_current_user)):
    user_device_ids = current_user.get("mydevices", [])
    if device_id not in user_device_ids:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Device does not belong to this user")

    # Fetch the device from the database
    device_check = await db["devices"].find_one({"device_id": device_id})
    if not device_check:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Device not found")

    update_data = device.model_dump(exclude_unset=True)

    # Enforce rules
    if "type_device" in update_data:
        update_data.pop("type_device")

    if ("lat" in update_data or "long" in update_data) and \
       device_check.get("type_device") not in ["SENSOR_PRESSURE_DEVICE_V1", "SENSOR_RADAR_DEVICE_V1"]:
        update_data.pop("lat", None)
        update_data.pop("long", None)

    # Merge with existing data
    update_data["device_settings"] = update_data.get("device_settings", device_check.get("device_settings", {}))
    update_data["name"] = update_data.get("name", device_check.get("name"))
    update_data["fixed_location"] = update_data.get("fixed_location", device_check.get("fixed_location"))
    update_data["radius"] = update_data.get("radius", device_check.get("radius"))
    update_data["updated_at"] = datetime.utcnow()

    await db["devices"].update_one({"device_id": device_id}, {"$set": update_data})
    updated_device = await db["devices"].find_one({"device_id": device_id})
    return DeviceOut(**updated_device)