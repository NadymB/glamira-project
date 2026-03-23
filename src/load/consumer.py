import time
from src.utils.config import RESULT_SUCCESS_QUEUE, UPLOAD_PROCESSING_QUEUE, BATCH_SIZE, SLEEP_EMPTY
from src.utils.logger import setup_logger
from src.utils.redis_client import get_redis_sync
from src.load.recover import recover_on_start
from src.load.queue_ops import pop_safe, remove_processed_jobs
from src.load.gcs_loader import upload_batch 

logger = setup_logger("upload_consumer")

def run():
    logger.info("🚀 Upload consumer start")
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

        # queue rỗng → flush
        if not item:
            if batch:
                success = upload_batch(batch)
                if success:
                    remove_processed_jobs(r, UPLOAD_PROCESSING_QUEUE, batch)
                    total += len(batch)
                batch.clear()

            time.sleep(SLEEP_EMPTY)

if __name__ == "__main__":
    run()