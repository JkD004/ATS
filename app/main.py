import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from dotenv import load_dotenv

from app.mqtt_client import start_mqtt_listener
from app.routes.auth import auth_router
from app.routes.device_data import device_data_router
from app.routes.devices import device_router
from app.routes.fence import fence_router
from app.routes.user import user_router

load_dotenv()

env = os.getenv('ENV', '')
additional_prefix = ''
if env == 'Test':
    additional_prefix = '/test'

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the MQTT listener as a background task
    mqtt_task = asyncio.create_task(start_mqtt_listener())
    yield  # Yield control back to FastAPI for the application lifecycle
    # Optionally handle any shutdown logic here
    mqtt_task.cancel()  # Cancel the MQTT task if needed when FastAPI shuts down

from fastapi.middleware.cors import CORSMiddleware

# Create FastAPI app using the lifespan context
app = FastAPI(lifespan=lifespan, docs_url=f"/api/docs", root_path=additional_prefix)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router, prefix=f'/api/auth', tags=["auth"])
app.include_router(device_router, prefix=f'/api', tags=["devices"])
app.include_router(device_data_router, prefix=f'/api', tags=["device-data"])
app.include_router(fence_router, prefix=f'/api', tags=["fence-data"])
app.include_router(user_router, prefix=f'/api', tags=["user"])


@app.get("/")
def root():
    return {"message": "Welcome to the FastAPI and MongoDB IoT Project"}
