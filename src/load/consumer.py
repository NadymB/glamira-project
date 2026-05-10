import time
from src.utils.config import RESULT_SUCCESS_QUEUE, UPLOAD_PROCESSING_QUEUE, BATCH_SIZE, CRAWL_QUEUE, PROCESSING_QUEUE
from src.utils.logger import setup_logger
from src.utils.redis_client import get_redis_sync
from src.load.recover import recover_on_start
from src.load.queue_ops import pop_safe, remove_processed_jobs
from src.load.gcs_loader import upload_batch 

logger = setup_logger("upload_consumer")
IDLE_TIMEOUT = 10 

def is_system_done(r):
    crawl = r.llen(CRAWL_QUEUE)
    processing = r.llen(PROCESSING_QUEUE)
    success = r.llen(RESULT_SUCCESS_QUEUE)

    return crawl == 0 and processing == 0 and success == 0


def run():
    logger.info("🚀 Upload consumer start")
    logger.info(f"BATCH_SIZE = {BATCH_SIZE}")
    r = get_redis_sync()
    recover_on_start(r, RESULT_SUCCESS_QUEUE, UPLOAD_PROCESSING_QUEUE, logger)

    batch = []
    total = 0

    while True:
        item = pop_safe(r, RESULT_SUCCESS_QUEUE, UPLOAD_PROCESSING_QUEUE)

        if item:
            batch.append(item)

        if len(batch) >= BATCH_SIZE:
            success = upload_batch(batch)

            if success:
                remove_processed_jobs(r, UPLOAD_PROCESSING_QUEUE, batch)
                total += len(batch)
                logger.info(f"📊 Total uploaded: {total}")
            batch.clear()

        if not item:
            if is_system_done(r):
                if batch:
                    logger.info(f"🏁 Final flush {len(batch)} items")

                    success = upload_batch(batch)
                    if success:
                        remove_processed_jobs(r, UPLOAD_PROCESSING_QUEUE, batch)
                        total += len(batch)

                    batch.clear()

                logger.info("😴 System idle... waiting for new data")
            time.sleep(5)
            continue

if __name__ == "__main__":
    run()