#!/bin/bash
set -e

INPUT=${1:-data/products.json}
WORKERS=${2:-8}
PREFIX=${3:-products_part}
OUTPUT=${4:-product_names.csv}

echo "=================================="
echo "Input file: $INPUT"
echo "Workers: $WORKERS"
echo "Prefix: $PREFIX"
echo "Output file: $OUTPUT"
echo "=================================="

echo "Running split products..."

python3 -m src.split_products \
  --input "$INPUT" \
  --workers "$WORKERS" \
  --output "$PREFIX"

echo "Running crawler..."

for file in ${PREFIX}_*.json
do
  echo "Processing $file"
  python3 -m src.crawl_product_name \
    --input "$file" \
    --output "result_${file%.json}.csv"
done

echo "✅ Pipeline completed"