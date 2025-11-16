# core/mongo.py — resilient Mongo connection + indexes + .env loader
import os
from functools import lru_cache
from urllib.parse import urlparse

from pymongo import MongoClient, ASCENDING


# ---------- best-effort .env loading ----------
def _load_dotenv_best_effort() -> None:
    """
    Try multiple locations so all processes (run_all, bots, worker) see the same env.
    Order:
      1) CWD .env
      2) repo root .env (folder of run_all.py / this package)
      3) .env.local at repo root (optional)
    """
    try:
        from dotenv import load_dotenv, find_dotenv
    except Exception:
        return

    # 1) CWD
    found = find_dotenv(usecwd=True)
    if found:
        load_dotenv(found, override=False)

    # 2) repo root (two parents up from this file)
    here = os.path.abspath(__file__)
    repo = os.path.dirname(os.path.dirname(here))
    env_repo = os.path.join(repo, ".env")
    if os.path.isfile(env_repo):
        load_dotenv(env_repo, override=False)

    # 3) optional .env.local
    env_local = os.path.join(repo, ".env.local")
    if os.path.isfile(env_local):
        load_dotenv(env_local, override=False)


_load_dotenv_best_effort()


# ---------- helpers ----------
def _clean(s: str | None) -> str | None:
    if s is None:
        return None
    s = s.strip()
    if not s or s in ('""', "''"):
        return None
    return s


def _die_missing_uri(db_name: str | None):
    cwd = os.getcwd()
    msg = (
        "MONGO_URI missing in environment.\n"
        f"- CWD: {cwd}\n"
        f"- MONGO_DB_NAME seen: {db_name!r}\n"
        "- Ensure a .env is present in repo root (same folder as run_all.py) OR export variables.\n"
        "- Required keys:\n"
        "    MONGO_URI=mongodb+srv://USER:PASS@HOST/?retryWrites=true&w=majority&appName=Rain\n"
        "    MONGO_DB_NAME=rain\n"
        "- If password contains @:/?&#=+, URL-encode it.\n"
        "- Quick test:\n"
        "    python3 - <<'PY'\n"
        "    from dotenv import load_dotenv; load_dotenv(); import os\n"
        "    print('MONGO_URI=', os.getenv('MONGO_URI'))\n"
        "    print('MONGODB_URI=', os.getenv('MONGODB_URI'))\n"
        "    print('MONGO_DB_NAME=', os.getenv('MONGO_DB_NAME'))\n"
        "    PY\n"
    )
    raise RuntimeError(msg)


# ---------- read env ----------
MONGO_URI = _clean(os.getenv("MONGO_URI")) or _clean(os.getenv("MONGODB_URI"))
MONGO_DB_NAME = _clean(os.getenv("MONGO_DB_NAME")) or "rain"

if not MONGO_URI:
    _die_missing_uri(MONGO_DB_NAME)

# Validate shape early (helps catch stray spaces / wrong scheme)
try:
    parsed = urlparse(MONGO_URI)
    if not parsed.scheme or "mongodb" not in parsed.scheme:
        raise ValueError("Invalid scheme")
except Exception:
    raise RuntimeError(f"Invalid MONGO_URI format: {MONGO_URI!r}")


# ---------- client / db ----------
@lru_cache(maxsize=1)
def _client() -> MongoClient:
    # Let the SRV URI carry appName etc. Avoid extra kwargs for compatibility.
    return MongoClient(MONGO_URI)


@lru_cache(maxsize=1)
def db():
    """Return the selected database and ensure the name is valid."""
    d = _client()[MONGO_DB_NAME]
    # Force a touch to trigger potential "Bad database name" errors now.
    _ = d.settings.name
    return d


def ensure_indexes() -> None:
    d = db()
    d.users.create_index([("user_id", ASCENDING)], unique=True, name="u_user_id")
    d.sessions.create_index([("user_id", ASCENDING), ("slot", ASCENDING)], unique=True, name="u_user_slot")
    d.groups.create_index([("user_id", ASCENDING)], unique=True, name="u_groups_uid")
    d.settings.create_index([("key", ASCENDING)], unique=True, name="u_settings_key")
    d.stats.create_index([("user_id", ASCENDING)], unique=True, name="u_stats_uid")


# ---------- optional: small debug helper ----------
def env_debug_string() -> str:
    return (
        f"MONGO_URI set? {bool(MONGO_URI)} | "
        f"MONGO_DB_NAME={MONGO_DB_NAME!r}"
    )
