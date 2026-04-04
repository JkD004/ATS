import logging
import os

from bson import ObjectId
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient  # Use pymongo for synchronous operations


# Load environment variables from a .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load MongoDB URL from environment variable
MONGO_URL = os.getenv("MONGO_URL", 'mongodb://localhost:27017/DevDB')
DATABASE_NAME = os.getenv("DATABASE_NAME", 'DevDB')

if not MONGO_URL:
    logger.error("MONGO_URL environment variable not set.")
    raise ValueError("MONGO_URL environment variable not set.")

# Create an instance of AsyncIOMotorClient
try:
    client = AsyncIOMotorClient(MONGO_URL)
    db = client.get_database(DATABASE_NAME)
    logger.info("Successfully connected to MongoDB.")
except Exception as e:
    logger.error(f"Failed to connect to MongoDB: {e}")
    raise

# Create an instance of MongoClient
try:
    sync_client = MongoClient(MONGO_URL)  # Change to MongoClient for synchronous
    sync_db = sync_client[DATABASE_NAME]  # Access the database synchronously
    logger.info("Successfully connected to MongoDB.")
except Exception as e:
    logger.error(f"Failed to connect to MongoDB: {e}")
    raise

def serialize_id(id):
    if isinstance(id, ObjectId):
        return str(id)
    return id


def serialize_doc(doc):
    if not doc:
        return doc
    return {**doc, "_id": serialize_id(doc.get("_id"))}
