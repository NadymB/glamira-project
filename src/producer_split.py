import json
import os
import redis
import argparse

from google.cloud import storage
from json import JSONDecoder
from src.config import *

# Redis
r = redis.Redis(host=REDIS_HOST)

# GCS
storage_client = storage.Client()
bucket = storage_client.bucket("glamira-data-lake")

PREFIX = "bronze/crawler-data"

os.makedirs("/tmp/chunks", exist_ok=True)


def save_chunk(chunk, index):

    filename = f"chunk_{index}.json"

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

    decoder = JSONDecoder()

    with open(input_file, "r") as f:

        buffer = ""
        chunk = []
        index = 0

        for line in f:

            buffer += line.strip()

            while buffer:

                try:

                    obj, idx = decoder.raw_decode(buffer)

                    chunk.append(obj)

                    buffer = buffer[idx:].strip()

                    if len(chunk) >= chunk_size:

                        save_chunk(chunk, index)

                        chunk = []
                        index += 1

                except json.JSONDecodeError:
                    break

        if chunk:
            save_chunk(chunk, index)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True)
    parser.add_argument("--chunk-size", type=int, default=10000)

    args = parser.parse_args()

    stream_split(args.input, args.chunk_size)