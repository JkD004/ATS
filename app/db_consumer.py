#!/usr/bin/python3
import asyncio
import json
import logging
import os
import redis
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
import firebase_admin
from firebase_admin import credentials, messaging
from dotenv import load_dotenv
from datetime import timezone
import requests


load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))


# Initialize Firebase Admin SDK
firebase_cred_path = os.path.join(os.path.dirname(__file__), "animal-tracker-35595-firebase-adminsdk-ee8x6-af16382a08.json")
if not firebase_admin._apps:
    cred = credentials.Certificate(firebase_cred_path)
    firebase_admin.initialize_app(cred)


# Configure logging
log_dir = os.getenv("LOG_DIR", "./logs")
if not os.path.exists(log_dir):
    os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, "database_worker.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Logs to console
        logging.FileHandler(log_file)  # Logs to file
    ]
)

# MongoDB connection
MONGODB_URI = os.getenv("MONGO_URL", 'mongodb://localhost:27017/TestDB')
db_name = os.getenv("DATABASE_NAME", "DB")
client = AsyncIOMotorClient(MONGODB_URI)
db = client[db_name]

# Redis connection
redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST', 'localhost'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=int(os.getenv('REDIS_DB', 0))
)



async def store_device_data(devicedataforuser):
    """
    Stores device data in MongoDB
    """
    try:
        device_id = devicedataforuser["device_id"]
        type_device = devicedataforuser.get("type_device")

        # Check if the device exists in the database
        device = await db["devices"].find_one({"device_id": device_id})

        try:
            now = datetime.fromisoformat(devicedataforuser["device_data"]["data_retrieve_time"])
            print(f"data_retrieve_time: {now}")
        except Exception as e:
            logging.error(f"Error getting data_retrieve_time: {e}")
            now = datetime.now(timezone.utc)
        

        
        devicedataforuser['created_at'] = datetime.now(timezone.utc)
        device_data = {k: {"updated_at": now, "value": v} for k, v in devicedataforuser["device_data"].items()}

        name = (
            device.get("name")
            if device else "New Device"
        )

        if type_device == "SENSOR_GPS_DEVICE_V1" or type_device == "MOTION_DETECTOR_DEVICE_V1":
            # Extract latitude and longitude from device_data
            lat_raw = devicedataforuser["device_data"].get("lat")
            long_raw = devicedataforuser["device_data"].get("long")

            lat = lat_raw / 1e7 if isinstance(lat_raw, int) else lat_raw
            long = long_raw / 1e7 if isinstance(long_raw, int) else long_raw
        else:
            lat = 0
            long = 0

        if type_device == "DISTANCE_SENSOR_DEVICE":
            distance = devicedataforuser["device_data"].get("distance_percentage")
            bin_level = devicedataforuser["device_data"].get("bin_level")
            url = "https://www.fast2sms.com/dev/bulkV2"

            payload = {
                "message": "Hello! This is a test SMS",
                "language": "english",
                "route": "q",
                "numbers": "8660541589, 9845858345"
            }

            headers = {
                "authorization": os.getenv("FAST2SMS")
            }

            response = requests.post(url, data=payload, headers=headers)
            
            print(response.json())
            print(f"distance: {distance}")
            print(f"bin_level: {bin_level}")
            logging.info(f"distance: {distance}")
            logging.info(f"bin_level: {bin_level}")
        else:
            distance = 1200
            bin_level = 2000

        
        if not device:
            # Create new device entry if it doesn't exist
            new_device = {
                "device_id": device_id,
                "app_id": devicedataforuser["app_id"],
                "name": name,
                "type_device": type_device,
                "device_data": device_data,  # Store location data in the new device
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
                "parameters": [],
                "controls": [],
                "avatar_url": None,  # Set default avatar_url to None for new devices,
                "lat": lat,
                "long": long,
            }
            await db["devices"].insert_one(new_device)
        else:
            if type_device == "SENSOR_GPS_DEVICE_V1":
                update_fields = {
                    "device_data": {
                        **device.get("device_data", {}),
                        **{k: {"updated_at": now, "value": v}
                        for k, v in devicedataforuser["device_data"].items() if v is not None}
                    },
                    "updated_at": datetime.now(timezone.utc),
                    "lat": lat,
                    "long": long,
                }
            elif type_device == "MOTION_DETECTOR_DEVICE_V1":
                update_fields = {
                    "device_data": {
                        **device.get("device_data", {}),
                        **{k: {"updated_at": now, "value": v}
                        for k, v in devicedataforuser["device_data"].items() if v is not None}
                    },
                    "updated_at": datetime.now(timezone.utc),
                    "lat": lat,
                    "long": long,
                }
            elif type_device == "SENSOR_RADAR_DEVICE_V1":
                current_utc_time = datetime.now(timezone.utc)

                update_fields = {
                    "device_data": {
                        **device.get("device_data", {}),
                        **{k: {"updated_at":  current_utc_time if k == "targets" else now, "value": v}
                        for k, v in devicedataforuser["device_data"].items() if v is not None}
                    },
                    "updated_at": datetime.now(timezone.utc)
                }
            
            else:
                update_fields = {
                    "device_data": {
                        **device.get("device_data", {}),
                        **{k: {"updated_at": now, "value": v}
                        for k, v in devicedataforuser["device_data"].items() if v is not None}
                    },
                    "updated_at": datetime.now(timezone.utc)
                }

            print(f"update_fields: {update_fields}")


            # if type_device is not None or missing:
            if not device.get("type_device"):
                # Update the device type if it is not set
                update_fields["type_device"] = type_device
                
            await db["devices"].update_one(
                {"device_id": device_id},
                {"$set": update_fields}
            )

        devicedatadb = {
            "device_id": device_id,
            "app_id": devicedataforuser["app_id"],
            "device_data": device_data,
            "payload": devicedataforuser["payload"],
            "created_at": datetime.now(timezone.utc),
        }

        if(type_device == "SENSOR_RADAR_DEVICE_V1"):
            targets = devicedataforuser["device_data"].get("targets", [])
            has_moving_target = any(target.get("speed", 0) != 0 for target in targets)
            
            if has_moving_target:
                await send_firebase_notification_radar_device(device_id, name)
                logging.info(f"Movement detected for radar device {device_id}, notification sent")
                        # Insert the device data into the device_data collection
                await db["device_data"].insert_one(devicedatadb)
                logging.info(f"Successfully stored data for device {device_id}")
            else:
                logging.info(f"No movement detected for radar device {device_id}, Data not stored")

        elif(type_device == "MOTION_DETECTOR_DEVICE_V1" and devicedataforuser["device_data"]["motion"] == 1):
                await send_firebase_notification_motion_device(device_id, name)
                logging.info(f"Motion detected for motion device {device_id}, notification sent")
                # Insert the device data into the device_data collection
                await db["device_data"].insert_one(devicedatadb)
        
        else:
            # Insert the device data into the device_data collection
            await db["device_data"].insert_one(devicedatadb)
            logging.info(f"Successfully stored data for device {device_id} {type_device}")

        if(type_device == "DISTANCE_SENSOR_DEVICE"):
            distance_pct = devicedataforuser["device_data"].get("distance_percentage", 0)
            logging.info(f"Attempting to send notification for bin device {device_id}, distance: {distance_pct}%")
            print(f"Attempting to send notification for bin device {device_id}, distance: {distance_pct}%")
            await send_firebase_notification_bin_device(device_id, name, distance_pct)
            logging.info(f"Bin threshold device {device_id}, notification check completed")

        return True
    

    except Exception as e:
        logging.error(f"Error storing device data: {e}")
        return False

async def process_queue_item(data):
    """
    Process a single queue item
    """
    try:
        devicedata = json.loads(data)
        await store_device_data(devicedata)
    except Exception as e:
        logging.error(f"Error processing queue item: {e}")

async def database_worker():
    """
    Main worker function that continuously processes items from Redis queue
    """
    logging.info("Database worker started")
    
    while True:
        try:
            # Check if there's data in the queue
            # Using BRPOP for reliable queue processing
            # This blocks until data is available
            logging.info("Checking queue...")
            queue_len = redis_client.llen("device_data_queue")
            logging.info(f"Queue length: {queue_len}")
            result = redis_client.blpop("device_data_queue", timeout=0)
            if result:
                queue_name, data = result
                # Process the data
                await process_queue_item(data)
            else:
                # No data in queue, wait a bit to avoid tight loop
                await asyncio.sleep(0.1)
                
        except redis.exceptions.ConnectionError as e:
            logging.error(f"Redis connection error: {e}")
            # Wait before retrying
            await asyncio.sleep(5)
            
        except Exception as e:
            logging.error(f"Unexpected error in database worker: {e}")
            await asyncio.sleep(1)

async def send_firebase_notification_radar_device(device_id, name):
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
                        body=f"Device {name} - Human Detected."
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


async def send_firebase_notification_motion_device(device_id, name):
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
                        body=f"Device {name} - Motion Detected."
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

isNotificationSent = False;

async def send_firebase_notification_bin_device(device_id, name, distance_percentage):
    """Sends a notification via Firebase to all users with the specified device in their 'mydevices' array."""
    try:
        logging.info(f"send_firebase_notification_bin_device called for device_id: {device_id}, name: {name}, distance: {distance_percentage}%")
        print(f"send_firebase_notification_bin_device called for device_id: {device_id}, name: {name}, distance: {distance_percentage}%")
        
        # Get device to verify it exists
        device = await db["devices"].find_one({"device_id": device_id})
        if not device:
            logging.error(f"Device {device_id} not found in database")
            return False
        
        current_time = datetime.now(timezone.utc)
        four_hours_ago = current_time - timedelta(hours=1)
        
        # Query to find users who:
        # 1. Have this device in their 'mydevices' array
        # 2. Have an FCM token
        # 3. Have bin_threshold < distance_percentage (or no threshold set, defaults to 80.0)
        # 4. Either no notification sent for this device OR notification sent more than 4 hours ago
        query = {
            "mydevices": {"$in": [device_id]},
            "fcm_token": {"$exists": True, "$ne": None, "$ne": ""},
            "bin_threshold": {"$lt": distance_percentage, "$ne": None},
            "$or": [
                {"notification_sent_at": {"$exists": False}},  # No notification_sent_at field
                {"notification_sent_at": None},  # notification_sent_at is None
                {f"notification_sent_at.{device_id}": {"$exists": False}},  # No notification for this device
                {f"notification_sent_at.{device_id}": None},  # Notification for this device is None
                {f"notification_sent_at.{device_id}": {"$lt": four_hours_ago}}  # Notification sent > 4 hours ago
            ]
        }
        
        users_with_device = await db["users"].find(query).to_list(None)

        logging.info(f"Found {len(users_with_device)} users eligible for notification for device {device_id}")
        print(f"Found {len(users_with_device)} users eligible for notification for device {device_id}")

        if not users_with_device:
            logging.info(f"No eligible users found for device {device_id}.")
            print(f"No eligible users found for device {device_id}.")
            return
        
        # Send notifications to eligible users (already filtered by query)
        for user in users_with_device:
            user_id = user['_id']
            fcm_token = user.get("fcm_token")
            user_bin_threshold = user.get("bin_threshold", 80.0)
            
            logging.info(f"Sending notification to user {user_id}, threshold: {user_bin_threshold}%")
            print(f"Sending notification to user {user_id}, threshold: {user_bin_threshold}%")
            
            try:
                # Define the notification message
                message = messaging.Message(
                    notification=messaging.Notification(
                        title="Alert",
                        body=f"{name} is filled {round(distance_percentage,0)}%"
                    ),
                    token=fcm_token
                )

                logging.info(f"Sending Firebase notification to user {user_id} with token {fcm_token[:20]}...")
                print(f"Sending Firebase notification to user {user_id} with token {fcm_token[:20]}...")
                
                # Send the notification
                response = messaging.send(message)
                logging.info(f"Successfully sent notification to user {user_id}: {response}")
                print(f"Successfully sent notification to user {user_id}: {response}")
                
                # Update notification timestamp for this specific device
                # Store as {device_id: timestamp} in notification_sent_at dict
                await db["users"].update_one(
                    {"_id": user_id},
                    {"$set": {f"notification_sent_at.{device_id}": current_time}}
                )
                logging.info(f"Updated notification_sent_at[{device_id}] timestamp for user {user_id}")
                
            except Exception as e:
                logging.error(f"Error sending notification to user {user_id}: {e}")
                print(f"Error sending notification to user {user_id}: {e}")
    except Exception as e:
        logging.error(f"Error in send_firebase_notification_bin_device: {e}")
        print(f"Error in send_firebase_notification_bin_device: {e}")
        return False


if __name__ == "__main__":
    # Run the database worker
    logging.info("Starting database worker process")
    asyncio.run(database_worker())