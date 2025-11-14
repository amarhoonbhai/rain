# core/mongo.py — Mongo connection + indexes
import os
from functools import lru_cache
from pymongo import MongoClient, ASCENDING, DESCENDING

MONGO_URI = os.getenv("MONGO_URI") or os.getenv("MONGODB_URI")
DB_NAME   = os.getenv("MONGO_DB_NAME", "rain")

if not MONGO_URI:
    raise RuntimeError("MONGO_URI missing in environment")

@lru_cache(maxsize=1)
def _client() -> MongoClient:
    # SRV/standard URIs supported; appName can be embedded in the URI
    return MongoClient(MONGO_URI)

@lru_cache(maxsize=1)
def db():
    return _client()[DB_NAME]

def ensure_indexes():
    d = db()
    # Users
    d.users.create_index([("user_id", ASCENDING)], unique=True, name="u_user_id")
    # Sessions
    d.sessions.create_index([("user_id", ASCENDING), ("slot", ASCENDING)], unique=True, name="u_user_slot")
    d.sessions.create_index([("user_id", ASCENDING), ("updated_at", ASCENDING)], name="i_sessions_updated")
    # Groups
    d.groups.create_index([("user_id", ASCENDING)], unique=True, name="u_groups_uid")
    # Settings
    d.settings.create_index([("key", ASCENDING)], unique=True, name="u_settings_key")
    # Stats
    d.stats.create_index([("user_id", ASCENDING)], unique=True, name="u_stats_uid")
    d.stats.create_index([("sent_ok", DESCENDING)], name="i_stats_sentok_desc")
