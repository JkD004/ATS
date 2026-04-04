from datetime import datetime, timezone
from typing import List, Optional

from bson import ObjectId
from pydantic import BaseModel, EmailStr, Field, BeforeValidator
from typing_extensions import Annotated


# Helper function to serialize ObjectId to string
def to_str_id(obj_id: ObjectId) -> str:
    return str(obj_id)


# Define PyObjectId using Annotated and BeforeValidator
PyObjectId = Annotated[
    str, BeforeValidator(lambda v: str(ObjectId(v)) if ObjectId.is_valid(v) else ValueError("Invalid objectid"))]


# User models
class GoogleSignIn(BaseModel):
    id_token: str


class UserCreate(BaseModel):
    email: EmailStr
    password: Optional[str] = None  # Make password optional for Google Sign-In
    name: str
    mobile_number: str
    is_active: Optional[bool] = False
    google_id: Optional[str] = None  # Add Google ID field
    auth_provider: Optional[str] = "local"  # Add auth provider field


class UserOut(BaseModel):
    id: PyObjectId = Field(default_factory=lambda: str(ObjectId()), alias="_id")
    email: EmailStr
    name: str
    mobile_number: str
    role: str
    mydevices: Optional[List[str]] = []
    bin_threshold: Optional[float] = None
    notification_sent_at: Optional[dict] = None  # Changed to dict: {device_id: timestamp}
    created_at: datetime
    
    class Config:
        json_encoders = {ObjectId: to_str_id}  # Convert ObjectId to string for JSON output
        from_attributes = True
        arbitrary_types_allowed = True  # required for the _id


class UserAuth(BaseModel):
    email: Optional[EmailStr] = None  # Allow email to be optional
    username: Optional[str] = None  # Also allow login via username
    password: str


# User model for updating profile
class UserProfileUpdate(BaseModel):
    name: Optional[str] = None
    mobile_number: Optional[str] = None
    fcm_token: Optional[str] = Field(None, description="Firebase Cloud Messaging token for push notifications")
    bin_threshold: Optional[float] = None
    notification_sent_at: Optional[dict] = None  # Changed to dict: {device_id: timestamp}



class UserResetPassword(BaseModel):
    new_password: str


# User model for updating devices
class AddDevicesRequest(BaseModel):
    device_ids: List[str]  # List of device IDs to add
    remove: Optional[bool] = False

# Device models
class DeviceData(BaseModel):
    id: PyObjectId = Field(default_factory=lambda: str(ObjectId()), alias="_id")
    device_id: str
    device_data: Optional[dict] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True  # required for the _id
        json_encoders = {
            ObjectId: str,
            datetime: lambda v: v.astimezone(timezone.utc).isoformat()
        }


class DeviceCreate(BaseModel):
    device_id: str
    name: str = "New Device"
    device_data: Optional[dict] = None
    parameters: Optional[List[str]] = []
    controls: Optional[List[str]] = []
    created_at: datetime = Field(default_factory=datetime.utcnow)
    avatar_url: Optional[str] = None
    type_device: Optional[str] = None  # Add type_device field
    lat: Optional[float] = None
    long: Optional[float] = None
    
# Define a new model for DeviceSettings
class DeviceSettings(BaseModel):
    wake_up_time: dict = Field(..., example={"value": 1, "is_set": False})
    bin_height: Optional[float] = Field(..., example=5.0)
class DeviceUpdate(BaseModel):
    name: Optional[str] = None
    device_settings: Optional[DeviceSettings] = None
    fixed_location: Optional[dict] = None
    radius: Optional[int] = None
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    avatar_url: Optional[str] = None
    type_device: Optional[str] = None
    lat: Optional[float] = None
    long: Optional[float] = None


class DeviceOut(BaseModel):
    id: PyObjectId = Field(default_factory=lambda: str(ObjectId()), alias="_id")
    device_id: str
    app_id: Optional[str] = None
    name: str
    device_data: Optional[dict] = None  # loc object added here
    parameters: Optional[List[str]] = []
    controls: Optional[List[str]] = []
    device_settings: Optional[dict] = None
    created_at: datetime
    updated_at: Optional[datetime] = None
    fixed_location: Optional[dict] = None
    radius: Optional[int] = None
    avatar_url: Optional[str] = None
    type_device: Optional[str] = None
    lat: Optional[float] = None
    long: Optional[float] = None

    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True  # required for the _id
        json_encoders = {
            ObjectId: str,
            datetime: lambda v: v.astimezone(timezone.utc).isoformat()
        }


# Fence models
class FenceBase(BaseModel):
    fence_name: str
    center: dict  # lat and lng in a dict
    radius: float
    auto_select_devices: Optional[bool] = False
    user_id: Optional[str]  # String for the user ID to avoid ObjectId issues

    class Config:
        json_encoders = {ObjectId: to_str_id}  # Convert ObjectId to string for JSON representation
        arbitrary_types_allowed = True  # required for the _id


class FenceCreate(FenceBase):
    device_ids: Optional[List[str]] = None  # List of device IDs as strings
    createdAt: Optional[datetime] = Field(default_factory=datetime.utcnow)  # Auto-generate createdAt
    updatedAt: Optional[datetime] = Field(default_factory=datetime.utcnow)  # Auto-generate updatedAt


class FenceUpdate(BaseModel):
    fenceName: Optional[str]
    radius: Optional[float]

    class Config:
        json_encoders = {ObjectId: to_str_id}
        arbitrary_types_allowed = True  # required for the _id


class FenceOut(FenceBase):
    id: PyObjectId = Field(default_factory=lambda: str(ObjectId()), alias="_id")  # Automatically generate string ID
    createdAt: Optional[datetime] = Field(default_factory=datetime.utcnow)  # Auto-generate createdAt
    updatedAt: Optional[datetime] = Field(default_factory=datetime.utcnow)  # Auto-generate updatedAt

    class Config:
        json_encoders = {ObjectId: to_str_id}
        from_attributes = True
        arbitrary_types_allowed = True  # required for the _id
