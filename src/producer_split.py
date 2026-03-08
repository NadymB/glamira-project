import json
import os
import redis
import ijson
from config import *

r = redis.Redis(host=REDIS_HOST)

os.makedirs("chunks", exist_ok=True)


def save_chunk(chunk, index):

    filename = f"chunk_{index}.json"
    path = f"chunks/{filename}"

    with open(path, "w") as f:
        json.dump(chunk, f)

    r.lpush(QUEUE_NAME, filename)

    print("queued", filename)


def stream_split(input_file):

    with open(input_file, "rb") as f:

        parser = ijson.items(f, "item")

        chunk = []
        index = 0

        for item in parser:

            chunk.append(item)

            if len(chunk) >= CHUNK_SIZE:

                save_chunk(chunk, index)

                chunk = []
                index += 1

        if chunk:
            save_chunk(chunk, index)


if __name__ == "__main__":

    stream_split("all_products.json")