import os
from datetime import datetime, timedelta
from typing import Union

from fastapi import Depends, status, HTTPException
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError
import bcrypt
from sendgrid import sendgrid, Mail

from app.constants import SENDGRID_API_KEY, SECRET_KEY, ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES, REFRESH_TOKEN_EXPIRE_DAYS
from app.database import db, sync_db
from geopy.distance import geodesic
import firebase_admin
from firebase_admin import messaging
from firebase_admin import credentials

# Initialize Firebase Admin SDK if not already initialized
firebase_cred_path = os.getenv("FIREBASE_CREDENTIALS_PATH")
if not firebase_cred_path:
    raise ValueError("FIREBASE_CREDENTIALS_PATH environment variable not set")
cred = credentials.Certificate(firebase_cred_path)
firebase_admin.initialize_app(cred)

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Fetch from environment variables
GMAIL_EMAIL = os.getenv("GMAIL_EMAIL")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")

sg = sendgrid.SendGridAPIClient(SENDGRID_API_KEY)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login")


def verify_password(plain_password, hashed_password):
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))


def get_password_hash(password):
    pwd_bytes = password.encode('utf-8')
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(pwd_bytes, salt)
    return hashed.decode('utf-8')


def create_access_token(data: dict, expires_delta: Union[timedelta, None] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def verify_access_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        return None


async def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("email")
        if email is None:
            raise HTTPException(status_code=400, detail="Invalid token")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    user = await db["users"].find_one({"email": email})
    if user is None or not user["is_active"]:
        raise HTTPException(status_code=400, detail="User not active or does not exist")

    return user


# Helper function to create or update device data in MongoDB
async def store_device_data(devicedataforuser):
    try: 
        device_id = devicedataforuser["device_id"]

        # Check if the device exists in the database
        device = await db["devices"].find_one({"device_id": device_id})
        try: 
            now = datetime.fromisoformat(devicedataforuser["device_data"]["data_retrieve_time"]) 
        except Exception as e:
            print(f"Error getting data_retrieve_time: {e}")
            now = datetime.utcnow()
        devicedataforuser['created_at'] = datetime.utcnow()
        device_data = {k: {"updated_at": now, "value": v} for k, v in devicedataforuser["device_data"].items()}
        if not device:
            # Create new device entry if it doesn't exist
            new_device = {
                "device_id": device_id,
                "app_id": devicedataforuser["app_id"],
                "name": "New Device",
                "device_data": device_data,  # Store location data in the new device
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "parameters": [],
                "controls": []
            }
            await db["devices"].insert_one(new_device)
        else:
            # Check location proximity
            await check_location_proximity(devicedataforuser, device)
            
            # Update existing device's location information
            existing_device_data = device.get("device_data", {})
            updated_device_data = {**existing_device_data, **{k: {"updated_at": now, "value": v} for k, v in devicedataforuser["device_data"].items() if v}}
            await db["devices"].update_one(
                {"device_id": device_id},
                {"$set": {"device_data": updated_device_data, "updated_at": datetime.utcnow()}}
            )

        # Insert the device data into the device_data collection
        await db["device_data"].insert_one(devicedataforuser)
        return True
    except Exception as e:
        print(f"Error storing device data: {e}")


async def check_location_proximity(devicedataforuser, device):
    """Checks if the incoming location is outside the defined radius."""
    try:
        # Check for necessary keys and convert if necessary
        if "lat" in devicedataforuser["device_data"] and devicedataforuser["device_data"]["lat"] and \
           "long" in devicedataforuser["device_data"] and devicedataforuser["device_data"]["long"] and \
           "fixed_location" in device and \
           "lat" in device["fixed_location"] and device["fixed_location"]["lat"] and \
           "long" in device["fixed_location"] and device["fixed_location"]["long"]:

            # Convert large integer coordinates to decimal if needed
            def normalize(coord):
                return coord / 1e7 if abs(coord) > 90 else coord

            incoming_location = (
                normalize(devicedataforuser["device_data"]["lat"]),
                normalize(devicedataforuser["device_data"]["long"])
            )
            fixed_location = (
                normalize(device["fixed_location"]["lat"]),
                normalize(device["fixed_location"]["long"])
            )

            # Calculate the distance in meters between incoming and fixed locations
            distance = geodesic(fixed_location, incoming_location).meters
            if distance > device["radius"]:
                await send_firebase_notification(device["device_id"], device["name"])

    except KeyError as e:
        print(f"Key error: {e} - Missing expected data in device data or location.")
    except TypeError as e:
        print(f"Type error: {e} - Invalid data type for latitude/longitude values.")
    except Exception as e:
        print(f"Unexpected error during proximity check: {e}")



async def send_firebase_notification(device_id, name):
    """Sends a notification via Firebase to all users with the specified device in their 'mydevices' array."""
    try:
        # Query to find all users who have this device ID in their 'mydevices' array
        users_with_device = await db["users"].find({"mydevices": {"$in": [device_id]}}).to_list(None)
        
        if not users_with_device:
            print(f"No users found with device {device_id} in their 'mydevices' array.")
            return

        # Send a notification to each user who has an FCM token
        for user in users_with_device:
            fcm_token = user.get("fcm_token")
            try:
                # Only proceed if the user has a valid FCM token
                if not fcm_token:
                    print(f"User {user['_id']} does not have an FCM token, skipping notification.")
                    continue
                
                # Define the notification message
                message = messaging.Message(
                    notification=messaging.Notification(
                        title="Alert",
                        body=f"Device: {name} has moved outside the designated area."
                    ),
                    token=fcm_token  # Target this user's device using their FCM token
                )

                # Send the notification
                response = messaging.send(message)
                print(f"Successfully sent notification to user {user['_id']}: {response}")
            except Exception as e:
                print(f"Error sending notification to user {user['_id']}: {e}")
    except Exception as e:
        print(f"Error sending notification to user {user['_id']}: {e}")
        
        
async def send_email(to_email: str, subject: str, body: str):
    msg = MIMEMultipart()
    msg['From'] = GMAIL_EMAIL
    msg['To'] = to_email
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'html'))

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(GMAIL_EMAIL, GMAIL_APP_PASSWORD)
            server.send_message(msg)
        print("Email sent successfully.")
    except Exception as e:
        print(f"Error sending email: {e}")
        raise HTTPException(status_code=500, detail="Failed to send email")


# Function to check if a point is within a radius
def find_point_in_radius(center: dict, point: dict, radius: float):
    center_coords = (center["lat"], center["lng"])
    point_coords = (point["lat"], point["lng"])

    distance = geodesic(center_coords, point_coords).meters
    return distance <= radius


def create_refresh_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt



def get_device_wakeup_time_sync(device_id: str):
    try:
        device = sync_db["devices"].find_one({"device_id": device_id})
        if not device:
            return None, None

        device_setting = device.get("device_setting")
        if not device_setting:
            return None, None

        wakeup_hour = device_setting.get("wakeup_hour")
        wakeup_minute = device_setting.get("wakeup_minute")

        if wakeup_hour is None or wakeup_minute is None:
            return None, None

        return wakeup_hour, wakeup_minute

    except Exception as e:
        print(f"[DFU] Exception in sync DB fetch: {e}")
        return None, None

