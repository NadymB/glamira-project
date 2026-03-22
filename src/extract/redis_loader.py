import json
from src.utils.redis_client import get_redis
from src.utils.config import CRAWL_QUEUE

r = get_redis()

def push_batch(batch):
    if not batch:
        return
    r.lpush(CRAWL_QUEUE, *batch)


def build_message(doc):
    return json.dumps({
        "product_id": str(doc["_id"]),
        "url": doc["url"]
    })