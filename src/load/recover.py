async def recover_on_start(r, source_queue , processing_queue, logger):
    stuck = await r.lrange(processing_queue, 0, -1)

    if stuck:
        logger.warning(f"♻️ Recover {len(stuck)} items")

        pipe = r.pipeline()
        for item in stuck:
            pipe.lpush(source_queue, item)
        pipe.delete(processing_queue)
        pipe.execute()