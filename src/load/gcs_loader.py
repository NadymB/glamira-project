import uuid 
import time 
from google.cloud import storage
from utils.config import *
from utils.logger import setup_logger

logger = setup_logger("gcs_loader")

storage_client = storage.Client()
bucket = storage_client.bucket("glamira-data-lake")

def upload_bacth(batch):
    file_name = f"bronze/crawl_{uuid.uuid4()}.jsonl"

    data = "\n".join(batch)

    for attempt in range(MAX_RETRIES):
        try:
            blob = bucket.blob(file_name)

            blob.upload_from_string(
                data,
                content_type="application/json"
            )

            logger.info(f"☁️ Uploaded {len(batch)} → {file_name}")
            return True

        except Exception as e:
            logger.error(f"❌ Upload failed {attempt+1}: {e}")
            time.sleep(2 ** attempt)

    return False