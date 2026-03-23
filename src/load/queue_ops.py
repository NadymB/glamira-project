
def pop_safe(r, source, dest, timeout=5):
    return r.brpoplpush(source, dest, timeout=timeout)

def remove_processed_jobs(r, processing_queue, batch):
    pipe = r.pipeline()
    for item in batch:
        pipe.lrem(processing_queue, 1, item)
    pipe.execute()