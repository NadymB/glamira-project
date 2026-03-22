async def brpoplpush(r, source, dest, timeout=5):
    return await r.brpoplpush(source, dest, timeout=timeout)


async def lpush(r, queue, data):
    await r.lpush(queue, data)

def remove_batch(r, processing_queue, batch):
    pipe = r.pipeline()
    for item in batch:
        pipe.lrem(processing_queue, 1, item)
    pipe.execute()