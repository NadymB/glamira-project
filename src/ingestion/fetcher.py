import random
import aiohttp
from lxml import html
from config import MAX_RETRIES, USER_AGENTS

async def fetch(session, url):
    for attempt in range(MAX_RETRIES):
        try:
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept-Language": "en-US,en;q=0.9",
            }

            async with session.get(url, headers=headers) as resp:

                # ❌ chỉ 404 mới là failed
                if resp.status == 404:
                    return {
                        "url": url,
                        "product_name": None,
                        "status": "failed"
                    }

                # 🚫 block
                if resp.status in (403, 429):
                    return {
                        "url": url,
                        "product_name": None,
                        "status": "blocked"
                    }

                text = await resp.text()

                tree = html.fromstring(text)
                name = tree.xpath('//*[@data-ui-id="page-title-wrapper"]/text()')

                # 🚫 không parse được → block
                if not name:
                    return {
                        "url": url,
                        "product_name": None,
                        "status": "blocked"
                    }

                # ✅ success
                return {
                    "url": url,
                    "product_name": name[0].strip(),
                    "status": "success"
                }

        except Exception:
            await asyncio.sleep(2 ** attempt)

    # 🚫 retry hết → coi như blocked (thường do network / anti-bot)
    return {
        "url": url,
        "product_name": None,
        "status": "blocked"
    }