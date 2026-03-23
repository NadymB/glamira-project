import asyncio
import aiohttp
import json
from src.utils.config import *
import time
import random
from src.utils.logger import setup_logger
from aiohttp import ClientTimeout
from src.ingestion.job_processer import process_job
from src.utils.redis_client import get_redis_async
from src.ingestion.recover import recover_stuck_jobs

logger = setup_logger("worker")

# =========================
# 🔁 Worker loop chuẩn
# =========================
async def worker_loop(session, r):
    while True:
        await process_job(session, r, logger)
        await asyncio.sleep(random.uniform(0.1, 0.5))

async def recover_loop(r):
    while True:
        await recover_stuck_jobs(r)
        await asyncio.sleep(60)

async def handle_retry(r, data):
    await asyncio.sleep(60)

    await r.lpush("crawl_queue", json.dumps({
        "url": data["url"],
        "product_id": data.get("product_id")
    }))


async def retry_blocked_loop(r):
    while True:
        item = await r.brpop("result_blocked_queue", timeout=5)

        if not item:
            continue

        data = json.loads(item[1])

        # 🔥 không block loop chính
        asyncio.create_task(handle_retry(r, data))


# =========================
# 🚀 Main worker
# =========================
async def worker():
    r = get_redis_async()
    connector = aiohttp.TCPConnector(
        limit=0,               # unlimited connections
        limit_per_host=20,     # tránh spam 1 domain
        ttl_dns_cache=300,
        ssl=False
    )

    timeout = ClientTimeout(total=TIMEOUT) 

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector
    ) as session:

        tasks = [
            asyncio.create_task(worker_loop(session, r))
            for _ in range(CONCURRENCY)
        ]

        tasks.append(asyncio.create_task(recover_loop(r)))
        tasks.append(asyncio.create_task(recover_stuck_jobs(r)))

        await asyncio.gather(*tasks, return_exceptions=True)


# =========================
# ▶️ Run
# =========================
if __name__ == "__main__":
    asyncio.run(worker())