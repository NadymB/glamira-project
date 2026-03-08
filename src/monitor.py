import redis
import time
from config import *

r = redis.Redis(host=REDIS_HOST)

QUEUE = QUEUE_NAME
PROCESSING = "processing_queue"
FAILED = "failed_queue"
DONE = "done_queue"

while True:

    queue = r.llen(QUEUE)
    processing = r.llen(PROCESSING)
    failed = r.llen(FAILED)
    done = r.llen(DONE)

    total = queue + processing + failed + done

    print("========== CRAWLER STATUS ==========")

    print("Total Jobs  :", total)
    print("Remaining   :", queue)
    print("Processing  :", processing)
    print("Done        :", done)
    print("Failed      :", failed)

    if total > 0:
        progress = done / total * 100
        print(f"Progress    : {progress:.2f}%")

    print("====================================")
    print()

    time.sleep(5)