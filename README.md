# Animal Tracker System (ATS) Backend

This repository contains the FastAPI backend for the Animal Tracker System.

## Recent Backend Enhancements & Fixes

This section details the critical issues resolved to stabilize the backend and ensure reliable data ingestion and retrieval.

### 1. Device Cannot Be Claimed If It Connected Early
*   **What Was Happening**: When a new animal tracker was powered on, it connected to ChirpStack and sent its first data packet. The system automatically created a record in MongoDB with no owner assigned. Later, when the user tried to register that same tracker, the app displayed an error because the device already existed in the database.
*   **Where was the Problem**: `app/routes/devices.py` (`create_device` endpoint)
*   **Root Cause**: The endpoint rejected requests if the device was already in MongoDB. Since IoT trackers send data immediately, the MQTT consumer created the document before the user could claim it.
*   **Fix**: Modified the logic to allow users to claim an existing device if its `owner_id` is `None` (unowned).

### 2. Silent Data Loss in Kubernetes (Redis Localhost Fallback)
*   **What Was Happening**: In the K3s production environment, all incoming MQTT location packets were silently dropped with no error logs.
*   **Where was the Problem**: `app/mqtt_client.py` and `app/db_consumer.py`
*   **Root Cause**: The code used `os.getenv('REDIS_HOST', 'localhost')`. In a Kubernetes container, `localhost` refers to the container itself, not the Redis service pod. The connection failed silently, and data was lost without triggering a container restart.
*   **Fix**: Removed all fallback defaults for `REDIS_HOST`. Now, if it's not set, it throws a connection error ("Fail Fast, Fail Loud"), allowing Kubernetes to see the crash and alert DevOps.

### 3. Missing Environment Variables
*   **What Was Happening**: Running the backend locally crashed on startup due to missing environment variables like `INFLUXDB_URL` and `REDIS_HOST`.
*   **Where was the Problem**: `.env` and `app/database.py`
*   **Root Cause**: As features (like InfluxDB and Redis) were added, the corresponding environment variables were never added to the `.env` template.
*   **Fix**: Added all missing configuration blocks to the `.env` file template with placeholder values to ensure developers configure them.

### 4. History Always Empty (InfluxDB vs MongoDB Mismatch)
*   **What Was Happening**: Viewing device history consistently showed "No data found", even for active devices.
*   **Where was the Problem**: `app/routes/device_data.py`
*   **Root Cause**: While data ingestion wrote to MongoDB, the read endpoints were querying InfluxDB (which was empty due to an incomplete migration).
*   **Fix**: Reverted the read endpoints to query MongoDB using the `motor` async driver where the historical data actually resided.

### 5. Login Always Returns 401 Unauthorized
*   **What Was Happening**: Every login attempt failed with a 401 error.
*   **Where was the Problem**: `.env` and database configuration.
*   **Root Cause**: The `MONGO_URL` was pointing to a personal, empty database cluster from early development instead of the production database containing valid user data. The production URL also had a malformed `%22` character.
*   **Fix**: Replaced the `MONGO_URL` with the correct production database connection string and removed trailing URL-encoded artifacts.
