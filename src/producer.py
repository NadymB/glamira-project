import time
from pymongo import MongoClient
import redis
import json
from bson.objectid import ObjectId
import logging
from config import *
from utils.logger import setup_logger

# ===== INIT =====
r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD
)
client = MongoClient(MONGO_URI)
collection = client[DB_NAME][COLLECTION_NAME]

logger = setup_logger('producer')

BATCH_SIZE = 1000
CHECKPOINT_PREFIX = "producer_checkpoint"


# ===== CHECKPOINT =====
def get_checkpoint(producer_id):
    key = f"{CHECKPOINT_PREFIX}:{producer_id}"
    cp = r.get(key)
    if cp:
        try:
            return ObjectId(cp.decode())
        except:
            pass
    return None


def save_checkpoint(producer_id, _id):
    key = f"{CHECKPOINT_PREFIX}:{producer_id}"
    r.set(key, str(_id))


# ===== MAIN =====
def run(producer_id, start_id, end_id):
    logger.info(f"🚀 Worker {producer_id} start")
    logger.info(f"Range: {start_id} → {end_id}")

    last_id = get_checkpoint(producer_id)
    if last_id and last_id > start_id:
        logger.info(f"🔁 Resume from checkpoint: {last_id}")
        start_id = last_id

    pipeline = [
        {
            "$match": {
                "_id": {"$gte": start_id, "$lt": end_id},
                "collection": { 
                    "$in": [
                        "view_product_detail",
                        "select_product_option",
                        "select_product_option_quality",
                        "product_detail_recommendation_visible",
                        "product_detail_recommendation_noticed"
                    ]
                }
            }
        },
        {
            "$project": {
                "_id": 1,
                "product_id": {
                    "$ifNull": ["$product_id", "$viewing_product_id"]
                },
                "url": "$current_url"
            }
        },
        {
            "$match": {
                "product_id": {"$ne": None},
                "url": {"$ne": None}
            }
        },
        {
            "$group": {
                "_id": "$product_id",
                "url": {"$first": "$url"},
                "last_doc_id": {"$max": "$_id"}
            }
        }
    ]
    
    start_time = time.time()
    cursor = collection.aggregate(pipeline, allowDiskUse=True)
    logger.info("✅ Aggregation started, waiting for first result...")

    batch = []
    count = 0

    for i, doc in enumerate(cursor):
        if i == 0:
            logger.info(f"🔥 First doc after {time.time() - start_time:.2f}s")

        product_id = str(doc["product_id"])
        url = doc["url"]

        batch.append(json.dumps({
            "product_id": product_id,
            "url": url
        }))

        if len(batch) >= BATCH_SIZE:
            r.lpush(CRAWL_QUEUE, *batch)
            batch.clear()

            save_checkpoint(producer_id, doc["last_doc_id"])

            count += BATCH_SIZE
            logger.info(f"Worker {producer_id} pushed: {count}")

    if batch:
        r.lpush(CRAWL_QUEUE, *batch)

    logger.info(f"✅ Worker {producer_id} DONE. Total pushed: {count}")


# ===== CLI =====
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--producer_id", required=True)
    parser.add_argument("--start_id", required=True)
    parser.add_argument("--end_id", required=True)

    args = parser.parse_args()

    run(
        producer_id=args.producer_id,
        start_id=ObjectId(args.start_id),
        end_id=ObjectId(args.end_id)
    )