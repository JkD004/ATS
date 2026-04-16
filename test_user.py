import asyncio
from app.database import db

async def do():
    u = await db['users'].find_one({"email": "south@bgmcorp.com"})
    print(u)

asyncio.run(do())
