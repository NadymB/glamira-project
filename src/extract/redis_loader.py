import json
from src.utils.config import CRAWL_QUEUE

def push_batch(r, batch):
    if not batch:
        return
    r.lpush(CRAWL_QUEUE, *batch)


def build_message(doc):
    return json.dumps({
        "product_id": str(doc["_id"]),
        "url": doc["url"]
    })