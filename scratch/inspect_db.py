import asyncio
import os
import json
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

async def inspect():
    mongo_url = os.getenv('MONGO_URL', 'mongodb://localhost:27017/TestDB')
    db_name = os.getenv('DATABASE_NAME', 'DB')
    
    print(f"Connecting to {mongo_url}, DB: {db_name}")
    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]
    
    # Check total counts
    device_count = await db['devices'].count_documents({})
    data_count = await db['device_data'].count_documents({})
    
    print(f"Total devices: {device_count}")
    print(f"Total historical data points: {data_count}")
    
    if data_count > 0:
        print("\nLast 5 data points:")
        cursor = db['device_data'].find().sort('created_at', -1).limit(5)
        async for doc in cursor:
            # Clean up ObjectId for printing
            doc['_id'] = str(doc['_id'])
            if 'created_at' in doc:
                doc['created_at'] = str(doc['created_at'])
            print(json.dumps(doc, indent=2))
    else:
        print("\nNo data points found in 'device_data' collection.")
        
    await client.close()

if __name__ == "__main__":
    asyncio.run(inspect())
