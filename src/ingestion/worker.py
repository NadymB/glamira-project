import asyncio
import aiohttp
import json
from config import *
import time
from utils.logger import setup_logger
from aiohttp import ClientTimeout
from job_processer import process_job
from utils.redis_client import get_redis
from recover import recover_stuck_jobs

logger = setup_logger("worker")

# =========================
# 🔁 Worker loop chuẩn
# =========================
async def worker_loop(session, r):
    while True:
        await process_job(session, r, logger)


# =========================
# 🚀 Main worker
# =========================
async def worker():
    r = get_redis()
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

        # spawn worker loops
        tasks = [
            asyncio.create_task(worker_loop(session, r))
            for _ in range(CONCURRENCY)
        ]

        last_recover = time.time()

        while True:
            # recover job mỗi 60s
            if time.time() - last_recover > 60:
                await recover_stuck_jobs(r, logger)
                last_recover = time.time()

            await asyncio.sleep(1)


# =========================
# ▶️ Run
# =========================
if __name__ == "__main__":
    asyncio.run(worker())