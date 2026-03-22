import redis
from src.utils.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

def get_redis():
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD
    )