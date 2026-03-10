import json
import os
import redis
import argparse
import sys
import uuid
import time

from google.cloud import storage
from json import JSONDecoder
from config import *
from urllib.parse import urlparse

# Redis
r = redis.Redis(host=REDIS_HOST)

# GCS
storage_client = storage.Client()
bucket = storage_client.bucket("glamira-data-lake")

PREFIX = "bronze/crawler-data"

os.makedirs("/tmp/chunks", exist_ok=True)

def normalize_url(url):

    try:
        p = urlparse(url)

        path = p.path.rstrip("/")
        # bỏ query params
        return f"{p.scheme}://{p.netloc}{path}"

    except:
        return url
    
def upload_with_verify(blob, local_path, retries=5):

    for attempt in range(retries):

        blob.upload_from_filename(local_path)

        # verify object exist in GCS
        if blob.exists():
            return True

        time.sleep(2 ** attempt)

    return False


def save_chunk(chunk):
    chunk_id = str(uuid.uuid4())

    filename = f"chunk_{chunk_id}.json"

    local_path = f"/tmp/chunks/{filename}"

    with open(local_path, "w") as f:
        json.dump(chunk, f)

    # upload to GCS
    blob = bucket.blob(f"{PREFIX}/chunks/{filename}")
    ok = upload_with_verify(blob, local_path)

    if not ok:
        print("upload failed:", filename)
        return

    # push job vào redis queue
    r.lpush(QUEUE_NAME, filename)

    print("uploaded and queued:", filename)


def stream_split(input_file, chunk_size):

    chunk = []

    for line in input_file:

        line = line.strip()

        if not line:
            continue

        try:
            obj = json.loads(line)
        except:
            continue

        url = obj.get("current_url")

        if not url:
            continue

        url = normalize_url(url)

        # redis deduplicate
        if r.sadd("seen_urls", url) == 0:
            continue

        obj["current_url"] = url

        chunk.append(obj)

        if len(chunk) >= chunk_size:

            save_chunk(chunk)
            chunk = []

    if chunk:
        save_chunk(chunk)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True)
    parser.add_argument("--chunk-size", type=int, default=10000)

    args = parser.parse_args()

    # read from STDIN
    if args.input == "-":
        stream_split(sys.stdin, args.chunk_size)

    else:
        with open(args.input, "r") as f:
            stream_split(f, args.chunk_size)