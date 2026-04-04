from datetime import datetime
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from bson import ObjectId
from datetime import datetime

from app.database import db
from app.models import DeviceData
from app.utils import get_current_user

from dateutil.parser import isoparse
import logging

device_data_router = APIRouter()


@device_data_router.post("/device-data", response_model=DeviceData)
async def create_device_data(device_data: DeviceData, current_user: str = Depends(get_current_user)):
    device_data_dict = {
        "device": device_data.device,
        "state": device_data.state,
        "reading_type": device_data.readingType,
        "reading": device_data.reading,
        "reading_unit": device_data.readingUnit,
        "created_at": datetime.utcnow(),
    }
    await db["device_data"].insert_one(device_data_dict)
    return DeviceData(**device_data_dict)


# Get a list of devices
@device_data_router.get("/device-data", response_model=List[DeviceData])
async def get_device_data_list(current_user: str = Depends(get_current_user)):
    devices = await db["device_data"].find().to_list(length=None)  # Fetch all devices
    if not devices:
        raise HTTPException(status_code=404, detail="No devices found")
    return [DeviceData(**device) for device in devices]

CATEGORY_LIMITS = {
    "A": 10,
    "B": 30,
    "C": 51
}

@device_data_router.get("/device-data/{device_id}/{list_category}")
async def get_device_data(device_id: str, list_category: str, current_user: str = Depends(get_current_user)):
    if device_id not in current_user.get("mydevices", []):
        raise HTTPException(status_code=404, detail="Device not part of user's devices")
    
    limit = CATEGORY_LIMITS.get(list_category.upper(), 10)  # Default to 10 if invalid category
    
    device_data = (
        await db["device_data"]
        .find({"device_id": device_id, "device_data": {"$exists": True}})
        .sort("created_at", -1)
        .to_list(length=limit)
    )
    
    if not device_data:
        raise HTTPException(status_code=404, detail="Device data not found")

    
    return [DeviceData(**device) for device in device_data]


@device_data_router.get("/device-data/by-time-range", response_model=List[DeviceData])
async def get_device_data_by_time_range(
    device_id: str,
    from_datetime: datetime = Query(..., description="Start datetime in ISO format"),
    to_datetime: datetime = Query(..., description="End datetime in ISO format"),
    current_user: str = Depends(get_current_user)
):
    if device_id not in current_user.get("mydevices", []):
        raise HTTPException(status_code=403, detail="You do not have access to this device.")

    query = {
        "device_id": device_id,
        "device_data": {"$exists": True}
    }

    results = await db["device_data"].find(query).to_list(length=None)

    filtered_results = []
    for doc in results:
        try:
            data_retrieve = doc["device_data"].get("data_retrieve_time")

            # Handle various formats for data_retrieve_time
            raw_time = None
            if isinstance(data_retrieve, dict) and "value" in data_retrieve:
                raw_time = data_retrieve.get("value")
            elif isinstance(data_retrieve, str):
                raw_time = data_retrieve
            elif data_retrieve is None:
                # Skip documents where data_retrieve_time is None
                continue
            else:
                # Log unexpected formats for debugging
                logging.debug(f"Skipping doc {doc.get('_id')} - unexpected data_retrieve_time format: {type(data_retrieve)} - {data_retrieve}")
                continue

            if not raw_time:
                continue

            try:
                doc_time = isoparse(raw_time)
            except Exception as e:
                logging.debug(f"Skipping invalid document (id={doc.get('_id')}): Invalid ISO string '{raw_time}' - {e}")
                continue

            if from_datetime <= doc_time <= to_datetime:
                # Store the parsed datetime for sorting
                doc['_parsed_datetime'] = doc_time
                filtered_results.append(doc)

        except Exception as e:
            logging.debug(f"Skipping invalid document (id={doc.get('_id')}): {e}")
            continue
    
    print(f"Filtered results count: {len(filtered_results)}")
    
    if not filtered_results:
        raise HTTPException(status_code=404, detail="No data found in the given time range.")

    # Sort by the parsed datetime we stored earlier
    filtered_results.sort(key=lambda x: x['_parsed_datetime'])
    
    # Remove the temporary field before returning
    for doc in filtered_results:
        doc.pop('_parsed_datetime', None)

    print(filtered_results)

    return [DeviceData(**data) for data in filtered_results]