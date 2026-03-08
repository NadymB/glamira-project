import ijson
import json
import os
import argparse


def stream_split(input_file, chunk_size, output_dir):

    os.makedirs(output_dir, exist_ok=True)

    with open(input_file, "rb") as f:

        parser = ijson.items(f, "item")

        chunk = []
        index = 0

        for item in parser:

            chunk.append(item)

            if len(chunk) >= chunk_size:

                save_chunk(chunk, index, output_dir)
                chunk = []
                index += 1

        if chunk:
            save_chunk(chunk, index, output_dir)


def save_chunk(chunk, index, output_dir):

    file = f"{output_dir}/chunk_{index}.json"

    with open(file, "w") as f:
        json.dump(chunk, f)

    print("Saved", file)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True)
    parser.add_argument("--chunk-size", type=int, default=10000)
    parser.add_argument("--output-dir", default="chunks")

    args = parser.parse_args()

    stream_split(args.input, args.chunk_size, args.output_dir)