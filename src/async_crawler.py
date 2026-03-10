import aiohttp
import asyncio
import random 
from lxml import html
from config import *

sem = asyncio.Semaphore(CONCURRENCY)


async def fetch(session, url):

    for attempt in range(MAX_RETRIES):

        try:

            async with sem:
                # random delay trước khi request
                await asyncio.sleep(random.uniform(0.2, 0.6))

                async with session.get(url, timeout=REQUEST_TIMEOUT) as r:
                    if r.status in [403, 429]:

                        print(f"[WARN] {url} blocked with status {r.status} (attempt {attempt+1})")

                        if attempt < MAX_RETRIES - 1:
                            wait = 2 ** attempt
                            await asyncio.sleep(wait)
                            continue
                        else:
                            return {
                                "url": url,
                                "product_name": None
                            }
                        
                    if r.status != 200:
                        raise Exception(f"status {r.status}")

                    text = await r.text()

                    tree = html.fromstring(text)

                    name = tree.xpath(
                        '//*[@data-ui-id="page-title-wrapper"]/text()'
                    )

                    return {
                        "url": url,
                        "product_name": name[0] if name else None
                    }

        except Exception:

            if attempt < MAX_RETRIES - 1:

                wait = 2 ** attempt
                await asyncio.sleep(wait)

            else:

                return {
                    "url": url,
                    "product_name": None
                }


async def crawl(urls):

    connector = aiohttp.TCPConnector(
        limit=CONCURRENCY,
        ttl_dns_cache=300,
        enable_cleanup_closed=True
    )

    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)

    results = []

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout
    ) as session:

        for i in range(0, len(urls), BATCH_SIZE):

            batch = urls[i:i+BATCH_SIZE]

            tasks = [fetch(session, u) for u in batch]

            batch_results = await asyncio.gather(*tasks)

            results.extend(batch_results)

            # delay tránh rate limit
            await asyncio.sleep(0.5)

    return results
    
if __name__ == "__main__":

    urls = [
        "https://www.glamira.de/glamira-ring-april.html?diamond=diamond-Brillant&itm_source=recommendation&itm_medium=sorting&alloy=white_red-375"
    ]

    results = asyncio.run(crawl(urls))

    print(results)