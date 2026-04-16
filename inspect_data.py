import asyncio
import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

async def check():
    load_dotenv()
    mongo_url = os.getenv('MONGO_URL')
    db_name = os.getenv('DATABASE_NAME')
    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]
    
    docs = await db['device_data'].find().limit(5).to_list(None)
    for i, doc in enumerate(docs):
        print(f"Document {i}: {doc.get('device_id')} | {doc.get('device')} | {doc.get('created_at')}")
        print(f"Keys: {doc.keys()}")
    
    await client.close()

if __name__ == "__main__":
    asyncio.run(check())
