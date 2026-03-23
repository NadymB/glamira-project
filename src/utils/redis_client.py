import redis
import redis.asyncio as redis_async
from src.utils.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

def get_redis_sync():
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD
    )

def get_redis_async():
    return redis_async.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=False  
    )