from mongo_client import MongoClient
from utils.config import MONGO_URI, DB_NAME, COLLECTION_NAME

def get_collection():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME][COLLECTION_NAME]
