# watcher.py
import asyncio, hashlib, os
from playwright.async_api import async_playwright
import aioredis
import asyncpg
from dotenv import load_dotenv

load_dotenv()
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
DATABASE_URL = os.getenv("DATABASE_URL")

TARGET_URL = "https://example.com/draw-page"
SELECTOR = "#draw-date"  # change to actual selector

async def get_last_hash(pool, url):
    row = await pool.fetchrow("SELECT last_hash FROM last_seen WHERE target_url=$1", url)
    return row['last_hash'] if row else None

async def upsert_last_seen(pool, url, h, value):
    await pool.execute("""
      INSERT INTO last_seen(target_url,last_hash,last_value,updated_at)
      VALUES($1,$2,$3,now())
      ON CONFLICT (target_url) DO UPDATE
      SET last_hash = $2, last_value = $3, updated_at = now()
    """, url, h, value)

async def push_event(redis, url, value):
    # add event to Redis Stream 'events'
    await redis.xadd("events", {"url": url, "value": value})

async def run_once(pool, redis):
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        await page.goto(TARGET_URL, wait_until="networkidle")
        # extract text
        try:
            text = await page.locator(SELECTOR).inner_text()
        except Exception:
            text = await page.content()  # fallback: whole page
        await browser.close()

    h = hashlib.sha256(text.encode()).hexdigest()
    last = await get_last_hash(pool, TARGET_URL)
    if last != h:
        await upsert_last_seen(pool, TARGET_URL, h, text)
        await push_event(redis, TARGET_URL, text)

async def main():
    pool = await asyncpg.create_pool(DATABASE_URL)
    redis = await aioredis.from_url(REDIS_URL)
    try:
        while True:
            await run_once(pool, redis)
            await asyncio.sleep(60)  # polling cadence: 60s (adjustable)
    finally:
        await pool.close()
        await redis.close()

if __name__ == "__main__":
    asyncio.run(main())
