from urllib.parse import quote_plus
from pymongo import MongoClient

username = "admin"
password = quote_plus("your_password")

REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_PASSWORD = "your_strong_password"

MONGO_URI = f"mongodb://{username}:{password}@localhost:27017/?authSource=admin"
DB_NAME = "glamira"
COLLECTION_NAME = "summary"

CRAWL_QUEUE = "crawl_queue"
PROCESSING_QUEUE = "processing_queue"
RESULT_QUEUE = "result_queue"

CONCURRENCY = 3
MAX_RETRIES = 3
BATCH_SIZE = 50

CONCURRENT=5
TIMEOUT=10