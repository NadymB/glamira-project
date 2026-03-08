import redis
from config import *

r = redis.Redis(host=REDIS_HOST)

queue = r.llen(QUEUE_NAME)
failed = r.llen(FAILED_QUEUE)

print("queue remaining:", queue)
print("failed chunks:", failed)