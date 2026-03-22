import json
from utils.redis_client import get_redis
from utils.config import CRAWL_QUEUE

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