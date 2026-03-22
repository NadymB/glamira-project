import time
import json
from bson.objectid import ObjectId
from src.utils.config import *
from src.utils.logger import setup_logger
from src.utils.mongo_client import get_collection
from src.utils.redis_client import get_redis
from src.extract.checkpoint import get_checkpoint, save_checkpoint
from src.extract.pipeline import build_pipeline
from src.extract.redis_loader import push_batch, build_message

logger = setup_logger('producer')

def extract(producer_id, start_id, end_id):
    collection = get_collection()
    logger.info(f"🚀 Producer {producer_id} start")
    logger.info(f"Range: {start_id} → {end_id}")

    last_id = get_checkpoint(producer_id)
    if last_id and last_id > start_id:
        logger.info(f"🔁 Resume from checkpoint: {last_id}")
        start_id = last_id
    
    pipeline = build_pipeline(start_id, end_id, ["product_detail_recommendation_clicked"])

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
            push_batch(batch)
            save_checkpoint(producer_id, doc["last_doc_id"])

            count += BATCH_SIZE
            logger.info(f"Worker {producer_id} pushed: {count}")
            batch.clear()

    if batch:
        push_batch(batch)
        count += len(batch)

    logger.info(f"Producer {producer_id} DONE. Total pushed: {count}")