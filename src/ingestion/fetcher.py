import random
import asyncio
import re 
import json
from lxml import html
from src.utils.config import MAX_RETRIES
from src.utils.constants import XPATHS

def extract_react_data(text):
    match = re.search(r'var\s+react_data\s*=\s*(\{.*?\});', text, re.DOTALL)
    if not match:
        return None
    
    json_str = match.group(1)
    
    try:
        return json.loads(json_str)
    except Exception:
        return None

async def fetch(session, product_id):
    url = f"https://www.glamira.com/catalog/product/view/id/{product_id}"

    for attempt in range(MAX_RETRIES):
        try:

            async with session.get(url) as resp:

                # ❌ chỉ 404 mới là failed
                if resp.status == 404:
                    return {
                        "product_id": product_id,
                        "status": "failed"
                    }

                # 🚫 block
                if resp.status in (403, 429):
                    await asyncio.sleep(random.uniform(3, 10))  # cooldown
                    continue

                text = await resp.text()

                # 🔥 ưu tiên lấy từ script
                react_data = extract_react_data(text)
                price = react_data.get("price")
                price = float(price) if price else None

                if react_data:
                    return {
                        "product_id": product_id,
                        "product_name": react_data.get("name"),
                        "price": price,
                        "sku": react_data.get("sku"),
                        "product_type": react_data.get("product_type"),
                        "category_name": react_data.get("category_name"),
                        "status": "success"
                    }
                else:
                    print("⚠️  react_data not found")
                    return {
                        "product_id": product_id,
                        "product_name": None,
                        "status": "not_found"                             
                    }
        except Exception:
            await asyncio.sleep(2 ** attempt)

    # 🚫 retry hết → coi như blocked (thường do network / anti-bot)
    return {
        "product_id": product_id,
        "status": "blocked"
    }