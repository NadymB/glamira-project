#!/bin/bash
set -e

INPUT=${1:-data/raw/all_products.json}
CHUNK_SIZE=${2:-10000}
CHUNK_DIR=${3:-chunks}
RESULT_DIR=${4:-results}

echo "=================================="
echo "Input file: $INPUT"
echo "Chunk size: $CHUNK_SIZE"
echo "Chunk dir: $CHUNK_DIR"
echo "Result dir: $RESULT_DIR"
echo "=================================="

mkdir -p "$CHUNK_DIR"
mkdir -p "$RESULT_DIR"

echo "STEP 1: Streaming split..."

poetry run python3 -m src.stream_split \
  --input "$INPUT" \
  --chunk-size "$CHUNK_SIZE" \
  --output-dir "$CHUNK_DIR"

echo "STEP 2: Crawling chunks..."

for file in $CHUNK_DIR/*.json
do
    echo "Processing $file"

    poetry run python3 -m src.worker "$file" \
        > "$RESULT_DIR/$(basename $file .json).csv"
done

echo "STEP 3: Merging results..."

cat $RESULT_DIR/*.csv > $RESULT_DIR/final_results.csv

echo "✅ Pipeline completed"