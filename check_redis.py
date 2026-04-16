import redis
import os
from dotenv import load_dotenv

def check_redis():
    load_dotenv()
    host = os.getenv('REDIS_HOST', 'localhost')
    port = int(os.getenv('REDIS_PORT', 6379))
    db = int(os.getenv('REDIS_DB', 0))
    print(f"Connecting to Redis at {host}:{port} db {db}...")
    r = redis.Redis(host=host, port=port, db=db)
    try:
        length = r.llen("device_data_queue")
        print(f"Queue length: {length}")
    except Exception as e:
        print(f"Error connecting to Redis: {e}")

if __name__ == "__main__":
    check_redis()
