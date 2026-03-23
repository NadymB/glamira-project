import time
import json
from bson.objectid import ObjectId
from src.utils.config import *
from src.utils.logger import setup_logger
from src.utils.mongo_client import get_collection
from src.utils.redis_client import get_redis_sync
from src.extract.checkpoint import get_checkpoint, save_checkpoint
from src.extract.pipeline import build_pipeline
from src.extract.redis_loader import push_batch, build_message

logger = setup_logger('producer')

def extract(producer_id, start_id, end_id):
    r = get_redis_sync()
    collection = get_collection()
    logger.info(f"🚀 Producer {producer_id} start")
    logger.info(f"Range: {start_id} → {end_id}")

    last_id = get_checkpoint(r, producer_id)
    if last_id and last_id > start_id:
        logger.info(f"🔁 Resume from checkpoint: {last_id}")
        start_id = last_id
    
    pipeline = build_pipeline(start_id, end_id, [
        "view_product_detail",
        "select_product_option",
        "add_to_cart_action",
        "select_product_option_quality",
        "product_detail_recommendation_visible",
        "product_detail_recommendation_noticed"
    ])

    start_time = time.time()
    cursor = collection.aggregate(pipeline, allowDiskUse=True)

    logger.info("✅ Aggregation started, waiting for first result...")

    batch = []
    count = 0

    for i, doc in enumerate(cursor):
        if i == 0:
            logger.info(f"🔥 First doc after {time.time() - start_time:.2f}s")

        batch.append(build_message(doc))

        if len(batch) >= BATCH_SIZE:
            push_batch(r, batch)
            save_checkpoint(r, producer_id, doc["last_doc_id"])

            count += BATCH_SIZE
            logger.info(f"Worker {producer_id} pushed: {count}")
            batch.clear()

    if batch:
        push_batch(r, batch)
        count += len(batch)

    logger.info(f"Producer {producer_id} DONE. Total pushed: {count}")