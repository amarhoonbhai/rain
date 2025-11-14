# core/mongo.py — Mongo connection + indexes
import os
from functools import lru_cache

from pymongo import MongoClient, ASCENDING

# Load .env here so that MONGO_URI is available
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    # If dotenv is not installed, just ignore; env can still come from systemd/export
    pass

# Read URI *after* load_dotenv()
MONGO_URI = os.getenv("MONGO_URI") or os.getenv("MONGODB_URI")
DB_NAME   = os.getenv("MONGO_DB_NAME", "rain")

if not MONGO_URI:
    raise RuntimeError("MONGO_URI missing in environment")

@lru_cache(maxsize=1)
def _client() -> MongoClient:
    # Let the URI carry appName; avoid extra kwargs for widest compatibility
    return MongoClient(MONGO_URI)

@lru_cache(maxsize=1)
def db():
    return _client()[DB_NAME]

def ensure_indexes():
    d = db()
    d.users.create_index([("user_id", ASCENDING)], unique=True, name="u_user_id")
    d.sessions.create_index([("user_id", ASCENDING), ("slot", ASCENDING)], unique=True, name="u_user_slot")
    d.groups.create_index([("user_id", ASCENDING)], unique=True, name="u_groups_uid")
    d.settings.create_index([("key", ASCENDING)], unique=True, name="u_settings_key")
    d.stats.create_index([("user_id", ASCENDING)], unique=True, name="u_stats_uid")
