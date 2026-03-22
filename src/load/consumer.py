import time
from src.utils.config import RESULT_SUCCESS_QUEUE, UPLOAD_PROCESSING_QUEUE, BATCH_SIZE, SLEEP_EMPTY
from src.utils.logger import setup_logger
from src.utils.redis_client import get_redis
from src.load.recover import recover_on_start
from src.utils.queue_core import brpoplpush, remove_batch
from src.load.gcs_loader import upload_bacth 

logger = setup_logger("upload_consumer")

def run():
    logger.info("🚀 Upload consumer start")
    r = get_redis()
    recover_on_start(r, RESULT_SUCCESS_QUEUE, UPLOAD_PROCESSING_QUEUE, logger)

    batch = []
    total = 0

    while True:
        item = brpoplpush(r, RESULT_SUCCESS_QUEUE, UPLOAD_PROCESSING_QUEUE)

        if item:
            batch.append(item)

        if len(batch) >= BATCH_SIZE:
            success = upload_bacth(batch)

            if success:
                remove_batch(r, UPLOAD_PROCESSING_QUEUE, batch)
                total += len(batch)
                logger.info(f"📊 Total uploaded: {total}")
                batch.clear()

        # queue rỗng → flush
        if not item:
            if batch:
                success = upload_bacth(batch)
                if success:
                    remove_batch(r, UPLOAD_PROCESSING_QUEUE, batch)
                    total += len(batch)
                batch.clear()

            time.sleep(SLEEP_EMPTY)

if __name__ == "__main__":
    run()