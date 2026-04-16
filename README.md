# Animal Tracker System (ATS) Backend 🐾

This repository contains the backend service for the **Animal Tracker System**, built to reliably ingest, process, and serve real-time IoT location data from remote animal tracking collars. It is designed to act as the bridge between LoRaWAN gateways (via ChirpStack), the underlying data stores (MongoDB & Redis), and the cross-platform frontend applications (Flutter app for end users and administrators).

---

## 🏗️ Architecture Overview

The system is built on a modern, asynchronous Python stack optimized for high-throughput IoT scenarios.

### Core Technologies
*   **Web Framework**: **FastAPI** (Python 3.10+) running on **Uvicorn**. Chosen for its asynchronous capabilities (`asyncio`) and automatic OpenAPI/Swagger documentation generation.
*   **Primary Database**: **MongoDB** (accessed asynchronously via the `motor` driver). Stores all stateful information including users, device metadata, real-time metrics, geographical fences, and historical location dots.
*   **Message Broker / Cache**: **Redis**. Acts as a reliable queue mechanism to decouple the rapid influx of MQTT messages from database write operations.
*   **IoT Messaging**: **MQTT** (Eclipse Paho / `aiomqtt`). The backend actively subscribes to an external MQTT broker (typically ChirpStack) where LoRaWAN gateways publish raw sensor data.

### Data Flow & Ingestion Pipeline
To prevent slow database inserts from blocking the event loop or causing dropped packets from fast-moving trackers, the backend utilizes a decoupled ingestion pipeline:

1.  **MQTT Client (`app/mqtt_client.py`)**: Runs persistently in the background. It subscribes to the ChirpStack MQTT broker. As soon as a tracker transmits geographical or physiological data, this client picks it up, parses the raw payload, and pushes a serialized JSON string to a **Redis Queue** (right-push, `RPUSH`).
2.  **Database Consumer Worker (`app/db_consumer.py`)**: Another background task that constantly polls the Redis queue (left-pop, `BLPOP`). It grabs data payloads one by one, applies business logic (e.g., matching device IDs, determining initial connections), and performs bulk or singular asynchronous inserts into **MongoDB**.
3.  **FastAPI Router (`app/routes/`)**: Simultaneously serves RESTful HTTP requests to the mobile application, fetching historical data, processing user logins, and handling device claims entirely separated from the heavy lifting of the MQTT ingestion pipeline.

---

## 🗂️ Project Structure

*   **`app/main.py`**: The entry point. Handles FastAPI initialization, CORS middleware, API route registration, and the `lifespan` context manager that strictly boots the MQTT listener and Redis consumer exactly when the web server starts.
*   **`app/database.py`**: Singleton connections. Establishes and manages async connection pools for both MongoDB (`motor.motor_asyncio.AsyncIOMotorClient`) and Redis (`redis.asyncio`).
*   **`app/models.py`**: Defines all `Pydantic` data shapes to validate incoming HTTP requests and format outgoing JSON responses. Models include `UserCreate`, `DeviceCreate`, `Fence`, and specialized nested structures for telemetry data.
*   **`app/routes/`**: Contains modular router files serving categorized endpoints:
    *   `auth.py`: JWT-based authorization, login verification, password hashing (`bcrypt`), and Google Identity platform integration.
    *   `devices.py`: CRUD operations for animal trackers. Handles device ownership transfers, metadata updates, and initial device pairing logic.
    *   `device_data.py`: Read-heavy endpoints designed to query MongoDB for historical paths, returning temporal datasets to plot animal routes on maps.
    *   `fence.py`: Geofencing logic. Allows users to draw polygons/radii on a map and assign specific tracking collars to them.
    *   `user.py`: Profile management, role assignments, and preference modifications.

---

## 🛠️ Key Technical Enhancements & Bug Fixes

The following critical issues were diagnosed and resolved to modernize the system and prepare it for production (Kubernetes/Edge deployment):

### 1. Device Cannot Be Claimed If It Connected Early
*   **The Problem**: When a new animal tracker was powered on, it connected to ChirpStack and sent its first data packet. The MQTT consumer automatically created a record in MongoDB with no owner assigned. Later, when the user tried to register that same tracker by entering its EUI, the app rejected the request because the device "already existed."
*   **The Fix**: Modified the `create_device` logic in `app/routes/devices.py`. If a device exists but its `owner_id` is null, the system allows the user to securely "claim" it.

### 2. Silent Data Loss in Kubernetes (Redis Localhost Fallback)
*   **The Problem**: In the K3s production environment, all incoming MQTT location packets were silently dropped with no error logs.
*   **The Root Cause**: The code relied on `os.getenv('REDIS_HOST', 'localhost')`. Inside a Kubernetes container, `localhost` points to the container itself, not the Redis service pod. The silent failure prevented K8s from detecting the broken connection.
*   **The Fix**: Removed all fallback defaults. The system now strictly requires the environment variable. If missing, the app crashes immediately ("Fail Fast, Fail Loud"), allowing Kubernetes to restart the pod and DevOps to spot the misconfiguration.

### 3. Missing Environment Variables
*   **The Problem**: Local development environments crashed abruptly on startup due to missing database pointers.
*   **The Fix**: Added comprehensive configuration blocks to the `.env` template for `REDIS_HOST`, `REDIS_PORT`, and InfluxDB parameters with placeholder documentation to guide new developers.

### 4. History Always Empty (InfluxDB vs MongoDB Mismatch)
*   **The Problem**: Viewing a device's history natively in the app always resulted in "No data found" despite active transmissions.
*   **The Root Cause**: An incomplete migration. The read API endpoints (`app/routes/device_data.py`) were querying InfluxDB (which had no data), while the MQTT consumer was successfully writing all live data precisely to MongoDB.
*   **The Fix**: Reverted the historical read endpoints to query the MongoDB `device_data` collections natively via motor async aggregation pipelines, immediately restoring the history map view for end users.

### 5. Login Always Returns 401 Unauthorized
*   **The Problem**: Every login attempt across all user accounts returned an HTTP 401 Unauthorized.
*   **The Root Cause**: The `MONGO_URL` string in the deployment configurations was still pointing to a personal, barren developer cluster, and contained a malformed `%22` encoding artifact.
*   **The Fix**: Pointed the environment to the correct staging/production database containing valid user seeds and stripped the URI artifacts to ensure successful connection pooling.
