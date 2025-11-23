# core/mongo.py
import os
from pymongo import MongoClient, ASCENDING

_mongo = None
_db = None

def db():
    global _mongo, _db
    if _db is not None:
        return _db

    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    name = os.getenv("MONGO_DB", "spinify_ads")

    _mongo = MongoClient(uri)
    _db = _mongo[name]
    return _db


def ensure_indexes():
    d = db()

    # users
    d.users.create_index([("user_id", ASCENDING)], unique=True)

    # sessions
    d.sessions.create_index([("user_id", ASCENDING)])
    d.sessions.create_index([("slot", ASCENDING)])

    # settings
    d.settings.create_index([("key", ASCENDING)], unique=True)

    # groups
    d.groups.create_index([("user_id", ASCENDING)])

    # stats
    d.stats.create_index([("user_id", ASCENDING)])
