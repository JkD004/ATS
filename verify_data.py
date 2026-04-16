import asyncio
import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

async def check():
    load_dotenv()
    mongo_url = os.getenv('MONGO_URL')
    db_name = os.getenv('DATABASE_NAME')
    print(f"Connecting to {db_name}...")
    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]
    
    count = await db['device_data'].count_documents({})
    print(f"Device data count: {count}")
    
    if count > 0:
        last = await db['device_data'].find_one(sort=[('created_at', -1)])
        print(f"Last data device_id: {last.get('device_id')}")
        print(f"Last data created_at: {last.get('created_at')}")
    
    client.close()

if __name__ == "__main__":
    asyncio.run(check())
