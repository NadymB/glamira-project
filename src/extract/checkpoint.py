from utils.redis_client import get_redis
from config import CHECKPOINT_PREFIX
from bson.objectid import ObjectId

r = get_redis()
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
