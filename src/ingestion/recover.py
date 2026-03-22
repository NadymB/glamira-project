import time 
from utils.config import  PROCESSING_QUEUE, PROCESSING_TIMEOUT, PROCESSING_TS, CRAWL_QUEUE

async def recover_stuck_jobs(r, logger):
    now = int(time.time())

    all_jobs = await r.hgetall(PROCESSING_TS)

    for job_data, ts in all_jobs.items():
        ts = int(ts)
        if now - ts > PROCESSING_TIMEOUT:
            logger.warning(f"Recovering stuck job")

            await r.lpush(CRAWL_QUEUE, job_data)
            await r.lrem(PROCESSING_QUEUE, 1, job_data)
            await r.hdel(PROCESSING_TS, job_data)