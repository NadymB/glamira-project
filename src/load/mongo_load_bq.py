import json
import gzip
import time
from bson import ObjectId
from src.utils.mongo_client import get_collection
from google.cloud import storage, bigquery

OUTPUT_FILE = f"summary_part_2.jsonl"

BUCKET_NAME = "glamira-data-lake"
GCS_PATH = "bronze/summary/summary_part_2.jsonl"

START_ID = "5ebc7a9d0000000000000000"
END_ID   = "5ed8cb2c0000000000000000"

BQ_DATASET = "glamira_raw"
BQ_TABLE = "summary"

# INIT CLIENT
storage_client = storage.Client()
bq_client = bigquery.Client()


def build_pipeline(start_id, end_id):
    return [
        {
            "$match": {
                "_id": {
                    "$gte": ObjectId(start_id),
                    "$lt": ObjectId(end_id)
                }
            }
        },
        {
            "$project": {
                "_id": 0  # 🔥 loại bỏ _id
            }
        }
    ]

def normalize_option(doc):
    opt = doc.get("option")

    if isinstance(opt, dict):
        # convert object -> array
        doc["option"] = [{
            "alloy": opt.get("alloy"),
            "diamond": opt.get("diamond"),
            "shapediamond": opt.get("shapediamond")
        }]

    elif isinstance(opt, list):
        # giữ nguyên
        doc["option"] = opt

    else:
        doc["option"] = []

    return doc

def normalize_cart_products(doc):
    products = doc.get("cart_products")

    if isinstance(products, list):
        for p in products:
            opt = p.get("option")

            if isinstance(opt, dict):
                p["option"] = [opt]

            elif isinstance(opt, list):
                p["option"] = opt

            else:
                # ❗ fix case option = "" hoặc null
                p["option"] = []

    return doc

def extract_to_jsonl():
    collection = get_collection()

    pipeline = build_pipeline(START_ID, END_ID)

    cursor = collection.aggregate(
        pipeline,
        allowDiskUse=True,
        batchSize=1000
    )

    count = 0
    error = 0
    start_time = time.time()

    with open(OUTPUT_FILE, "wt", encoding="utf-8") as fout:
        for doc in cursor:
            try:
                doc = normalize_option(doc)
                doc = normalize_cart_products(doc)
                line = json.dumps(doc, ensure_ascii=False, )

                json.loads(line)

                fout.write(line + "\n")

                count += 1

            except Exception as e:
                error += 1
                print("❌", e)

    print(f"🎉 Done: {count} records | {error} errors | {time.time()-start_time:.2f}s")

# 🚀 STEP 2: UPLOAD GCS
def upload_to_gcs():
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(GCS_PATH)

    blob.upload_from_filename(OUTPUT_FILE)

    print(f"✅ Uploaded to gs://{BUCKET_NAME}/{GCS_PATH}")

    return f"gs://{BUCKET_NAME}/{GCS_PATH}"


def load_to_bq(gcs_uri):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_APPEND"
    )

    job = bq_client.load_table_from_uri(
        gcs_uri,
        f"{BQ_DATASET}.{BQ_TABLE}",
        job_config=job_config
    )

    job.result()

    print("✅ Loaded to BigQuery")


# MAIN
if __name__ == "__main__":
    extract_to_jsonl()
    uri = upload_to_gcs()
    load_to_bq(uri)