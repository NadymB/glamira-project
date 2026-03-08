import aiohttp
import asyncio
from lxml import html
from config import *

sem = asyncio.Semaphore(CONCURRENCY)


async def fetch(session, url):

    for attempt in range(MAX_RETRIES):

        try:

            async with sem:

                async with session.get(url, timeout=REQUEST_TIMEOUT) as r:

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

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout
    ) as session:

        tasks = [fetch(session, u) for u in urls]

        results = await asyncio.gather(*tasks)

        return results