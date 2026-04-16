from bson import ObjectId
from fastapi import APIRouter, HTTPException, Depends, status

from app.database import db  # Assuming you have a global db instance from Motor
from app.models import AddDevicesRequest, UserOut, UserProfileUpdate, UserResetPassword
from app.utils import get_current_user, get_password_hash

user_router = APIRouter()


@user_router.post("/users/me/devices", response_model=UserOut)
async def update_my_devices(
        device_data: AddDevicesRequest, current_user: dict = Depends(get_current_user)
):

    # Validate the device IDs (you could add validation to check if these devices exist)
    device_ids = device_data.device_ids
    remove = device_data.remove
    
    existing_devices = await db["devices"].find({"device_id": {"$in": [device_id for device_id in device_ids]}}).to_list(length=None)
    existing_device_ids = [str(device["device_id"]) for device in existing_devices]

    # Raise an error if no devices are found
    if not existing_device_ids:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No valid device IDs provided.")
    
    # Update the user's myDevices list
    if remove:
        # Remove the specified devices from the user's myDevices list
        updated_devices = [device for device in current_user.get("mydevices", []) if device not in existing_device_ids]
    else:
        # Add the specified devices to the user's myDevices list, ensuring no duplicates
        updated_devices = list(set(current_user.get("mydevices", []) + existing_device_ids))

    # Update the user document in MongoDB
    await db["users"].update_one(
        {"_id": ObjectId(current_user["_id"])},
        {"$set": {"mydevices": updated_devices}},
    )

    # Fetch the updated user to return in the response
    updated_user = await db["users"].find_one({"_id": ObjectId(current_user["_id"])})

    # Return the updated user
    return UserOut(
        email=updated_user["email"],
        name=updated_user["name"],
        mobile_number=updated_user["mobile_number"],
        role=updated_user["role"],
        mydevices=updated_user["mydevices"],
        created_at=updated_user["created_at"],
        bin_threshold=updated_user.get("bin_threshold", None),
        notification_sent_at=updated_user.get("notification_sent_at", None),
    )


# Fetch the user profile
@user_router.get("/users/me", response_model=UserOut)
async def get_user_profile(current_user: dict = Depends(get_current_user)):
    # Return user profile
    return UserOut(
        email=current_user["email"],
        name=current_user["name"],
        mobile_number=current_user["mobile_number"],
        role=current_user["role"],
        mydevices=current_user.get("mydevices", []),
        created_at=current_user["created_at"],
        bin_threshold=current_user.get("bin_threshold", None),
        notification_sent_at=current_user.get("notification_sent_at", None),
    )


# Update user profile
@user_router.put("/users/me", response_model=UserOut)
async def update_user_profile(
        profile_data: UserProfileUpdate, current_user: dict = Depends(get_current_user)
):
    # Fetch the user from the database
    user = await db["users"].find_one({"_id": ObjectId(current_user["_id"])})
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    # Prepare the update data (only update fields that are provided)
    update_data = profile_data.model_dump(exclude_unset=True)

    # If no data provided to update, raise a bad request
    if not update_data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No data provided for update")

    # Perform the update
    await db["users"].update_one({"_id": ObjectId(current_user["_id"])}, {"$set": update_data})

    # Fetch the updated user data
    updated_user = await db["users"].find_one({"_id": ObjectId(current_user["_id"])})

    # Convert ObjectId fields to string and ensure the response follows UserOut
    updated_user["_id"] = str(updated_user["_id"])
    return UserOut(**updated_user)


@user_router.put("/users/me/reset-password", response_model=dict)
async def user_reset_password(
        password: UserResetPassword, current_user: dict = Depends(get_current_user)
):
    # Fetch the user from the database
    user = await db["users"].find_one({"_id": ObjectId(current_user["_id"])})
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    hashed_password = await get_password_hash(password.new_password)
    await db["users"].update_one({"email": user['email']}, {"$set": {"password": hashed_password}})

    return {"message": "Password reset successfully."}


@user_router.delete("/users/me", status_code=status.HTTP_204_NO_CONTENT)
async def delete_my_account(current_user: dict = Depends(get_current_user)):
    """
    Permanently delete the authenticated user's account.
    Removes user document. Devices remain available for other users.
    """
    user_id = ObjectId(current_user["_id"])
    
    # Delete user document only
    delete_result = await db["users"].delete_one({"_id": user_id})
    if delete_result.deleted_count == 0:
        raise HTTPException(status_code=status.HTTP_404_NOT_SHARED, detail="User not found")
    
    # Devices stay intact for shared/multi-user access
    return None
