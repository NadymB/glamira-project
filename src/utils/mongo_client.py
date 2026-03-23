from pymongo import MongoClient
from src.utils.config import MONGO_URI, DB_NAME, COLLECTION_NAME

_client = None

def get_client():
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI)
    return _client


def get_db():
    return get_client()[DB_NAME]


def get_collection():
    return get_db()[COLLECTION_NAME]
