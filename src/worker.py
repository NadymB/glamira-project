import redis
import json
import asyncio
import os

from async_crawler import crawl
from config import *

r = redis.Redis(host=REDIS_HOST)

os.makedirs("results", exist_ok=True)


async def process_chunk(file):

    path = f"chunks/{file}"

    with open(path) as f:
        data = json.load(f)

    urls = [x["current_url"] for x in data]

    results = await crawl(urls)

    out = f"results/{file}.csv"

    with open(out, "w") as f:

        for r_ in results:
            f.write(f"{r_['url']},{r_['product_name']}\n")

    r.lrem("processing_queue", 1, file)


async def worker():

    while True:

        file = r.brpoplpush(QUEUE_NAME, "processing_queue")

        file = file.decode()

        print("processing", file)

        try:

            await process_chunk(file)

        except Exception as e:

            print("failed", file)

            r.lrem("processing_queue", 1, file)
            r.lpush(FAILED_QUEUE, file)


if __name__ == "__main__":

    asyncio.run(worker())