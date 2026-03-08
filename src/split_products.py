import json
import math
import argparse


def split_products(input_file, workers, output_prefix="products_part"):

    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    total = len(data)
    chunk_size = math.ceil(total / workers)

    print(f"Total products: {total}")
    print(f"Workers: {workers}")
    print(f"Chunk size: {chunk_size}")

    for i in range(workers):

        chunk = data[i * chunk_size:(i + 1) * chunk_size]

        output_file = f"{output_prefix}_{i}.json"

        with open(output_file, "w", encoding="utf-8") as out:
            json.dump(chunk, out)

        print(f"Saved: {output_file} ({len(chunk)} records)")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--output", default="products_part")

    args = parser.parse_args()

    split_products(args.input, args.workers, args.output)