import os
import json
import asyncio
from src.async_crawler import crawl


def process_chunk(file):

    with open(file) as f:
        data = json.load(f)

    urls = [x["current_url"] for x in data]

    results = asyncio.run(crawl(urls))

    out = file.replace(".json", ".csv")

    with open(out, "w") as f:

        for r in results:
            f.write(f"{r['url']},{r['product_name']}\n")


if __name__ == "__main__":

    for file in os.listdir("chunks"):

        if file.endswith(".json"):

            process_chunk("chunks/" + file)