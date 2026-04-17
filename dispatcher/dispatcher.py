# dispatcher.py
import asyncio, os, json
import aioredis, asyncpg, httpx
from twilio.rest import Client
from dotenv import load_dotenv

load_dotenv()
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
DATABASE_URL = os.getenv("DATABASE_URL")
TW_SID = os.getenv("TW_SID")
TW_TOKEN = os.getenv("TW_TOKEN")
TW_FROM = os.getenv("TW_FROM")  # DLT-approved sender

tw_client = Client(TW_SID, TW_TOKEN)

async def send_sms(to, body):
    # Twilio synchronous client used inside threadpool if needed; keep simple here
    msg = tw_client.messages.create(body=body, from_=TW_FROM, to=to)
    return {"sid": msg.sid, "status": msg.status}

async def send_webhook(url, payload):
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(url, json=payload)
        return {"status_code": r.status_code, "text": r.text}

async def process_event(pool, redis, stream_id, event):
    url = event[b'url'].decode()
    value = event[b'value'].decode()
    # find subscriptions matching this target_query (simple equality here)
    rows = await pool.fetch("SELECT id, channel, destination FROM subscriptions WHERE target_query=$1", url)
    for row in rows:
        sub_id = row['id']
        channel = row['channel']
        dest = row['destination']
        # idempotency: check deliveries table for same sub_id + url + value
        exists = await pool.fetchrow("""
          SELECT 1 FROM deliveries WHERE subscription_id=$1 AND target_url=$2 AND event_value=$3
        """, sub_id, url, value)
        if exists:
            continue
        # insert pending delivery
        await pool.execute("""
          INSERT INTO deliveries(subscription_id,target_url,event_value,status,created_at)
          VALUES($1,$2,$3,'pending',now())
        """, sub_id, url, value)
        try:
            if channel == 'sms':
                resp = await asyncio.get_event_loop().run_in_executor(None, send_sms, dest, f"Draw date published: {value}")
            elif channel == 'webhook':
                resp = await send_webhook(dest, {"url": url, "value": value})
            else:
                resp = {"error": "unsupported channel"}
            await pool.execute("UPDATE deliveries SET status=$1, provider_response=$2 WHERE subscription_id=$3 AND target_url=$4 AND event_value=$5",
                               'sent' if 'sid' in resp or resp.get('status_code',0) in (200,201) else 'failed',
                               json.dumps(resp), sub_id, url, value)
        except Exception as e:
            await pool.execute("UPDATE deliveries SET status='failed', provider_response=$1 WHERE subscription_id=$2 AND target_url=$3 AND event_value=$4",
                               str(e), sub_id, url, value)

async def consumer_loop():
    redis = await aioredis.from_url(REDIS_URL)
    pool = await asyncpg.create_pool(DATABASE_URL)
    last_id = "0-0"
    try:
        while True:
            res = await redis.xread({"events": last_id}, count=10, block=5000)
            if not res:
                continue
            # res is list of (stream, [(id, {k:v}), ...])
            for stream, entries in res:
                for entry_id, data in entries:
                    await process_event(pool, redis, entry_id, data)
                    last_id = entry_id
    finally:
        await redis.close()
        await pool.close()

if __name__ == "__main__":
    asyncio.run(consumer_loop())
