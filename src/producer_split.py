import json
import os
import redis
import argparse
import sys
import uuid

from google.cloud import storage
from json import JSONDecoder
from config import *

# Redis
r = redis.Redis(host=REDIS_HOST)

# GCS
storage_client = storage.Client()
bucket = storage_client.bucket("glamira-data-lake")

PREFIX = "bronze/crawler-data"

os.makedirs("/tmp/chunks", exist_ok=True)


def save_chunk(chunk):
    chunk_id = str(uuid.uuid4())

    filename = f"chunk_{chunk_id}.json"

    local_path = f"/tmp/chunks/{filename}"

    with open(local_path, "w") as f:
        json.dump(chunk, f)

    # upload to GCS
    blob = bucket.blob(f"{PREFIX}/chunks/{filename}")
    blob.upload_from_filename(local_path)

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