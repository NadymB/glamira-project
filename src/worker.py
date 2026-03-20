# worker_fetch.py
import asyncio
import aiohttp
import redis
import json
import random
from lxml import html
from config import *
import logging
import time 
from utils.logger import setup_logger
from aiohttp import ClientTimeout


r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD
)
CHECKPOINT_HASH = "worker_checkpoint"
PROCESSING_TS = "processing_timestamps"
PROCESSING_TIMEOUT = 300  # 5 phút
logger = logger = setup_logger('worker')

def recover_stuck_jobs():
    now = int(time.time())

    all_jobs = r.hgetall(PROCESSING_TS)

    for job_data, ts in all_jobs.items():
        ts = int(ts)
        if now - ts > PROCESSING_TIMEOUT:
            # job bị kẹt
            logger.warning(f"Recovering stuck job: {job_data.decode()}")

            # push lại crawl_queue
            r.lpush(CRAWL_QUEUE, job_data)

            # xoá khỏi processing_queue
            r.lrem(PROCESSING_QUEUE, 1, job_data)

            # xoá timestamp
            r.hdel(PROCESSING_TS, job_data)


async def fetch(session, url):
    """Fetch URL với retry và exponential backoff"""
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url, timeout=15) as resp:
                if resp.status == 404:
                    logger.info(f"Failed: {url}")
                    return {"url": url, "product_name": None, "status": "failed"}
                if resp.status in (403, 429):
                    logger.info(f"Blocked: {url}")
                    await asyncio.sleep(random.uniform(1, 5))
                    continue
                
                logger.info(f"Success: {url}")

                text = await resp.text()
                
                tree = html.fromstring(text)

                name = tree.xpath(
                    '//*[@data-ui-id="page-title-wrapper"]/text()'
                )

                return {
                    "url": url,
                    "product_name": name[0] if name else None
                }
            
        except Exception as e:
            # exponential backoff
            logger.warning(f"sleep exponential backoff: {2 ** attempt}s ")
            await asyncio.sleep(2 ** attempt)
    return {"url": url, "product_name": None, "status": "failed"}

async def process_job(session):
    data = r.brpoplpush(CRAWL_QUEUE, PROCESSING_QUEUE, timeout=5)
    if not data:
        return

    # save timestamp
    r.hset(PROCESSING_TS, data, int(time.time()))

    item = json.loads(data.decode())
    url = item["url"]
    product_id = item.get("product_id")

    logger.info(f"Processing: {url}")

    result = await fetch(session, url)
    result["product_id"] = product_id

    # 🔥 chọn queue theo status
    is_failed = result.get("status") == "failed"
    target_queue = RESULT_FAILED_QUEUE if is_failed else RESULT_SUCCESS_QUEUE

    # push result
    success = False
    for attempt in range(MAX_RETRIES):
        try:
            r.lpush(target_queue, json.dumps(result))
            success = True
            break
        except redis.exceptions.ConnectionError:
            logger.warning(f"sleep redis push result: 2s ")
            await asyncio.sleep(2)

    if success:
        r.hset(CHECKPOINT_HASH, url, json.dumps(result))
        r.lrem(PROCESSING_QUEUE, 1, data)
        r.hdel(PROCESSING_TS, data)

async def worker():
    connector = aiohttp.TCPConnector(limit=CONCURRENT)
    timeout = ClientTimeout(total=TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        last_recover = time.time()

        while True:
            # 🔁 recover mỗi 60s
            if time.time() - last_recover > 60:
                recover_stuck_jobs()
                last_recover = time.time()

            tasks = [
                process_job(session)
                for _ in range(CONCURRENT)
            ]

            await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(worker())