import aiohttp
import asyncio
import json
import csv
import argparse
from lxml import html

CONCURRENT_REQUESTS = 100


async def fetch_product_name(session, url):

    try:

        async with session.get(url, timeout=20) as response:

            text = await response.text()

            tree = html.fromstring(text)

            name = tree.xpath('//*[@data-ui-id="page-title-wrapper"]/text()')

            return {
                "current_url": url,
                "product_name": name[0].strip() if name else None
            }

    except Exception as e:

        return {
            "current_url": url,
            "product_name": None
        }


async def process_batch(session, batch):

    tasks = [fetch_product_name(session, url) for url in batch]

    return await asyncio.gather(*tasks)


async def crawl(input_file, output_file):

    with open(input_file, "r") as f:
        data = json.load(f)

    urls = [x["current_url"] for x in data if "current_url" in x]

    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession(connector=connector) as session:

        with open(output_file, "w", newline="", encoding="utf-8") as out:

            writer = csv.DictWriter(out, fieldnames=["current_url", "product_name"])
            writer.writeheader()

            batch = []

            for url in urls:

                batch.append(url)

                if len(batch) >= CONCURRENT_REQUESTS:

                    results = await process_batch(session, batch)

                    writer.writerows(results)

                    print(f"Processed {len(batch)}")

                    batch = []

            if batch:

                results = await process_batch(session, batch)

                writer.writerows(results)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True)
    parser.add_argument("--output", default="product_names.csv")

    args = parser.parse_args()

    asyncio.run(crawl(args.input, args.output))