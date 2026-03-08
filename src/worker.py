import redis
import json
import asyncio
import os
import socket

from google.cloud import storage
from async_crawler import crawl
from src.config import *

# Redis
r = redis.Redis(host=REDIS_HOST)

# queues
PROCESSING_QUEUE = "processing_queue"
FAILED_QUEUE = "failed_queue"
DONE_QUEUE = "done_queue"

# worker id
WORKER_ID = socket.gethostname()

# GCS
storage_client = storage.Client()
bucket = storage_client.bucket("glamira-data-lake")

PREFIX = "bronze/crawler-data"

# local folders
os.makedirs("chunks", exist_ok=True)
os.makedirs("/tmp/results", exist_ok=True)


async def process_chunk(file):

    print(f"{WORKER_ID} processing {file}")

    local_path = f"chunks/{file}"

    # download chunk từ GCS
    blob = bucket.blob(f"{PREFIX}/chunks/{file}")
    blob.download_to_filename(local_path)

    with open(local_path) as f:
        data = json.load(f)

    urls = [x["current_url"] for x in data]

    # crawl async
    results = await crawl(urls)

    # save local csv
    out = f"/tmp/results/{file}.csv"

    with open(out, "w") as f:
        for r_ in results:
            f.write(f"{r_['url']},{r_['product_name']}\n")

    # upload result lên GCS
    blob = bucket.blob(f"{PREFIX}/results/{file}.csv")
    blob.upload_from_filename(out)

    # remove processing
    r.lrem(PROCESSING_QUEUE, 1, file)

    # mark done
    r.lpush(DONE_QUEUE, file)

    print(f"{WORKER_ID} done {file}")


async def worker():

    while True:

        file = r.brpoplpush(QUEUE_NAME, PROCESSING_QUEUE)

        file = file.decode()

        try:

            await process_chunk(file)

        except Exception as e:

            print(f"{WORKER_ID} failed {file}", e)

            r.lrem(PROCESSING_QUEUE, 1, file)

            r.lpush(FAILED_QUEUE, file)


if __name__ == "__main__":

    asyncio.run(worker())