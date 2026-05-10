import json
import gzip
import time
from bson import ObjectId
from src.utils.mongo_client import get_collection
from google.cloud import storage, bigquery

OUTPUT_FILE = f"ip_location.jsonl"

BUCKET_NAME = "glamira-data-lake"
GCS_PATH = "bronze/summary/ip_location.jsonl"

START_ID = "5ebc7a9d0000000000000000"
END_ID   = "5ed8cb2c0000000000000000"

BQ_DATASET = "glamira_raw"
BQ_TABLE = "ip_location"

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
                json.loads(doc)

                fout.write(doc + "\n")

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