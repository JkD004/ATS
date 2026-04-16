import asyncio
from app.models import UserOut
from app.database import db

async def test():
    u = await db['users'].find_one({"email": "south@bgmcorp.com"})
    print("Fetched user:", u["email"])
    
    # Try parsing
    try:
        user_out = UserOut(**u)
        print("Parsed successfully!")
        
        # Try serializing (this simulates what fastapi does)
        json_str = user_out.json()
        print("Serialized:", json_str[:50] + "...")
    except Exception as e:
        print("Exception:", str(e))

asyncio.run(test())
