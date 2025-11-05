# profile_enforcer.py — robust bio/name enforcer (Pyrogram 2.x)
# - Concurrent, rate-limited enforcement across sessions
# - FloodWait backoff + jitter; skips no-op updates
# - ENV-configurable BIO/NAME_SUFFIX/intervals/concurrency
# - If core.db exposes name_lock_targets(): enforce exact premium name per user
#   else: enforce global BIO + NAME_SUFFIX for everyone (like previous version)

from __future__ import annotations
import os, asyncio, logging, random
from datetime import datetime
from zoneinfo import ZoneInfo

from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError, SessionExpired, AuthKeyUnregistered, Unauthorized

import core.db as db  # single import to probe for optional helpers

# ---------------- env / config ----------------
IST = ZoneInfo("Asia/Kolkata")

BIO               = os.getenv("ENFORCER_BIO", "#1 Free Ads Bot — Join @PhiloBots")
NAME_SUFFIX       = os.getenv("ENFORCER_NAME_SUFFIX", " — via @SpinifyAdsBot")
LOOP_EVERY_SEC    = int(os.getenv("ENFORCER_LOOP_SEC", "300"))     # main sweep interval (default 5min)
SESSION_GAP_SEC   = float(os.getenv("ENFORCER_SESSION_GAP", "0.8")) # gap between sessions of same user
MAX_CONCURRENCY   = int(os.getenv("ENFORCER_CONCURRENCY", "4"))     # parallel session updates across users
FW_SAFETY_PAD     = int(os.getenv("ENFORCER_FW_PAD_SEC", "2"))      # extra seconds after FloodWait
CONNECT_TIMEOUT   = int(os.getenv("ENFORCER_CONNECT_TIMEOUT", "10"))

# toggle: also enforce BIO/NAME_SUFFIX when premium name-lock exists (usually yes)
ENFORCE_BIO_ALWAYS = (os.getenv("ENFORCER_BIO_ALWAYS", "1") == "1")
ENFORCE_SUFFIX_WHEN_LOCKED = (os.getenv("ENFORCER_SUFFIX_WITH_LOCK", "1") == "1")

HAS_NAME_LOCK = all(hasattr(db, fn) for fn in ("name_lock_targets", "sessions_strings"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [enforcer] %(levelname)s %(message)s")
log = logging.getLogger("enforcer")

# ---------------- helpers ----------------
def now_ist() -> datetime:
    return datetime.now(IST)

async def _connect_client(session_string: str, api_id: int, api_hash: str) -> Client | None:
    app = Client(
        name=f"prof-{id(session_string)}",
        api_id=api_id, api_hash=api_hash,
        session_string=session_string,
        in_memory=True,
        no_updates=True,
    )
    try:
        # pyrogram start() runs updates runner; connect() is lighter for simple methods
        await asyncio.wait_for(app.connect(), timeout=CONNECT_TIMEOUT)
        return app
    except Exception:
        try:
