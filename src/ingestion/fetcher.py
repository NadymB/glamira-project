import random
import asyncio
from lxml import html
from src.utils.config import MAX_RETRIES
from src.utils.constants import XPATHS

async def fetch(session, url):
    for attempt in range(MAX_RETRIES):
        try:

            async with session.get(url) as resp:

                # ❌ chỉ 404 mới là failed
                if resp.status == 404:
                    return {
                        "url": url,
                        "product_name": None,
                        "status": "failed"
                    }

                # 🚫 block
                if resp.status in (403, 429):
                    await asyncio.sleep(random.uniform(3, 10))  # cooldown
                    continue

                text = await resp.text()

                tree = html.fromstring(text)

                 # 🔥 tìm product name với fallback
                name = []
                for xp in XPATHS:
                    name = tree.xpath(xp)
                    if name:
                        break

                # clean text
                name = [n.strip() for n in name if isinstance(n, str) and n.strip()]

                # 🚫 không parse được → block
                if not name:
                    return {
                        "url": url,
                        "product_name": None,
                        "status": "parse_error"
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