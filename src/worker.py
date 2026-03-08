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

        for r in results:
            f.write(f"{r['url']},{r['product_name']}\n")


async def worker():

    while True:

        file = r.rpop(QUEUE_NAME)

        if not file:
            print("queue empty")
            break

        file = file.decode()

        print("processing", file)

        try:

            await process_chunk(file)

        except Exception as e:

            print("failed", file)

            r.lpush(FAILED_QUEUE, file)


if __name__ == "__main__":

    asyncio.run(worker())