import random
import json
import time
import asyncio
from config import CRAWL_QUEUE, PROCESSING_QUEUE, PROCESSING_TS, RESULT_FAILED_QUEUE, RESULT_SUCCESS_QUEUE, RESULT_BLOCKED_QUEUE, CHECKPOINT_HASH, MAX_RETRIES
from fetcher import fetch
from utils.queue_core import brpoplpush, lpush

async def process_job(session, r, logger):
    data = await brpoplpush(r, CRAWL_QUEUE, PROCESSING_QUEUE)
    if not data:
        return

    await r.hset(PROCESSING_TS, data, int(time.time()))

    item = json.loads(data.decode())
    url = item["url"]
    product_id = item.get("product_id")

    # log nhẹ (1%)
    if random.random() < 0.01:
        logger.info(f"Processing: {url}")

    result = await fetch(session, url)
    result["product_id"] = product_id

    status = result.get("status")

    # route queue
    if status == "failed":
        target_queue = RESULT_FAILED_QUEUE
    elif status == "blocked":
        target_queue = RESULT_BLOCKED_QUEUE
    else:
        target_queue = RESULT_SUCCESS_QUEUE

    # push result với retry
    for attempt in range(MAX_RETRIES):
        try:
            await lpush(r, target_queue, json.dumps(result))
            break
        except Exception:
            await asyncio.sleep(2)

    # cleanup
    await r.hset(CHECKPOINT_HASH, url, json.dumps(result))
    await r.lrem(PROCESSING_QUEUE, 1, data)
    await r.hdel(PROCESSING_TS, data)
