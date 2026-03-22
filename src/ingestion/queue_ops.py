# from config import CRAWL_QUEUE, PROCESSING_QUEUE

# async def pop_job(r):
#     return await r.brpoplpush(CRAWL_QUEUE, PROCESSING_QUEUE, timeout=5)


# async def push_result(r, queue, data):
#     await r.lpush(queue, data)