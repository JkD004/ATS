import asyncio
import os
import json
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

def inspect_sync():
    mongo_url = os.getenv('MONGO_URL', 'mongodb://localhost:27017/DevDB')
    db_name = os.getenv('DATABASE_NAME', 'DevDB')
    
    print(f"Connecting to {mongo_url}, DB: {db_name}")
    client = MongoClient(mongo_url)
    db = client[db_name]
    
    # Check total counts
    data_count = db['device_data'].count_documents({})
    print(f"Total historical data points: {data_count}")
    
    if data_count > 0:
        # Check field distribution
        with_device_id = db['device_data'].count_documents({"device_id": {"$exists": True}})
        with_device_legacy = db['device_data'].count_documents({"device": {"$exists": True}})
        
        print(f"Documents with 'device_id': {with_device_id}")
        print(f"Documents with 'device' (legacy): {with_device_legacy}")
        
        print("\nLatest 3 documents:")
        cursor = db['device_data'].find().sort('created_at', -1).limit(3)
        for doc in cursor:
            doc['_id'] = str(doc['_id'])
            if 'created_at' in doc:
                doc['created_at'] = str(doc['created_at'])
            print(json.dumps(doc, indent=2))
    else:
        print("\nNo data points found.")
        
    client.close()

if __name__ == "__main__":
    inspect_sync()
