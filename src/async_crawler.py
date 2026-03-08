import aiohttp
import asyncio
import json
from lxml import html

CONCURRENCY = 500


async def fetch(session, url):

    try:

        async with session.get(url, timeout=20) as r:

            text = await r.text()

            tree = html.fromstring(text)

            name = tree.xpath('//*[@data-ui-id="page-title-wrapper"]/text()')

            return {
                "url": url,
                "product_name": name[0] if name else None
            }

    except:
        return {"url": url, "product_name": None}


async def crawl(urls):

    connector = aiohttp.TCPConnector(limit=CONCURRENCY)

    async with aiohttp.ClientSession(connector=connector) as session:

        tasks = [fetch(session, u) for u in urls]

        return await asyncio.gather(*tasks)