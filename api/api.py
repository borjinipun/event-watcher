# api.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncpg, os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
app = FastAPI()

class SubscriptionIn(BaseModel):
    channel: str
    target_query: str
    destination: str

@app.on_event("startup")
async def startup():
    app.state.pool = await asyncpg.create_pool(DATABASE_URL)

@app.on_event("shutdown")
async def shutdown():
    await app.state.pool.close()

@app.post("/subscribe")
async def subscribe(s: SubscriptionIn):
    if s.channel not in ("sms","webhook","email"):
        raise HTTPException(400, "invalid channel")
    q = "INSERT INTO subscriptions(channel,target_query,destination) VALUES($1,$2,$3) RETURNING id"
    row = await app.state.pool.fetchrow(q, s.channel, s.target_query, s.destination)
    return {"id": row['id']}
