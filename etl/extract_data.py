import argparse
from bson import ObjectId
from src.extract.mongo_extract import extract

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--producer_id", required=True)
    parser.add_argument("--start_id", required=True)
    parser.add_argument("--end_id", required=True)

    args = parser.parse_args()

    extract(
        producer_id=args.producer_id,
        start_id=ObjectId(args.start_id),
        end_id=ObjectId(args.end_id)
    )

if __name__ == "__main__":
    main()