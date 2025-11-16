# core/mongo.py — Mongo connection + indexes (robust env handling)
# - Loads .env before reading env vars
# - Sanitizes DB name (no blanks/spaces/nulls)
# - Provides cached client/db getters
# - Creates all needed indexes

import os
from functools import lru_cache
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConfigurationError

# Load .env early so imports that depend on env won't break
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

def _clean_db_name(raw: str | None, default: str = "rain") -> str:
    """Return a safe Mongo database name (fallback to default if invalid)."""
    if raw is None:
        return default
    name = str(raw).strip()
    # Reject empty, spaces-only, names containing spaces or nulls
    if not name or name == " " or " " in name or "\x00" in name:
        return default
    return name

# Read env after load_dotenv()
MONGO_URI = (os.getenv("MONGO_URI") or os.getenv("MONGODB_URI") or "").strip()
DB_NAME   = _clean_db_name(os.getenv("MONGO_DB_NAME"), default="rain")

if not MONGO_URI:
    raise RuntimeError("MONGO_URI missing in environment (.env)")

@lru_cache(maxsize=1)
def _client() -> MongoClient:
    """
    Cached MongoClient. The URI may already include appName/params.
    Keep it simple for widest Atlas compatibility.
    """
    try:
        return MongoClient(MONGO_URI)
    except ConfigurationError as e:
        # Common when the URI has a trailing space or malformed query string
        raise RuntimeError(f"Invalid MONGO_URI configuration: {e}") from e

@lru_cache(maxsize=1)
def db():
    """Cached DB handle."""
    return _client()[DB_NAME]

def ensure_indexes():
    """Create all required indexes idempotently."""
    d = db()

    # Users: unique user_id
    d.users.create_index([("user_id", ASCENDING)], unique=True, name="u_user_id")

    # Sessions: unique (user_id, slot)
    d.sessions.create_index(
        [("user_id", ASCENDING), ("slot", ASCENDING)],
        unique=True, name="u_user_slot"
    )

    # Groups: one doc per user_id
    d.groups.create_index([("user_id", ASCENDING)], unique=True, name="u_groups_uid")

    # Settings: unique key
    d.settings.create_index([("key", ASCENDING)], unique=True, name="u_settings_key")

    # Stats: one row per user
    d.stats.create_index([("user_id", ASCENDING)], unique=True, name="u_stats_uid")
