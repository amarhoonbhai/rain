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
            await app.disconnect()
        except Exception:
            pass
        return None

async def _safe_update_profile(app: Client, *, first_name: str | None = None, bio: str | None = None) -> bool:
    """Update first_name and/or bio only if provided; handle FloodWait. Return True if changed."""
    changed = False
    try:
        if first_name is not None:
            try:
                await app.update_profile(first_name=first_name)
                changed = True
            except FloodWait as fw:
                await asyncio.sleep(int(getattr(fw, "value", 3)) + FW_SAFETY_PAD)
            except RPCError:
                pass
        if bio is not None:
            try:
                await app.update_profile(bio=bio)
                changed = True
            except FloodWait as fw:
                await asyncio.sleep(int(getattr(fw, "value", 3)) + FW_SAFETY_PAD)
            except RPCError:
                pass
    except Exception:
        pass
    return changed

async def _enforce_for_session(api_id: int, api_hash: str, session_string: str,
                               desired_name: str | None, use_suffix: bool, set_bio: bool) -> bool:
    """
    If desired_name is provided => enforce exact first_name.
    Else if use_suffix => append NAME_SUFFIX when missing.
    Also set BIO if set_bio.
    Returns True if any change happened.
    """
    app = await _connect_client(session_string, api_id, api_hash)
    if not app:
        return False

    changed = False
    try:
        me = await app.get_me()
        current_first = (getattr(me, "first_name", "") or "").strip()

        next_name = None
        if desired_name:
            # premium lock: exact match
            if current_first != desired_name:
                next_name = desired_name
        elif use_suffix:
            base = (current_first or "User").split(" — ")[0]
            if not current_first.endswith(NAME_SUFFIX):
                next_name = (base + NAME_SUFFIX)[:64]  # Telegram cap

        next_bio = BIO if set_bio else None

        if next_name is None and next_bio is None:
            return False

        changed = await _safe_update_profile(app, first_name=next_name, bio=next_bio)
        await asyncio.sleep(SESSION_GAP_SEC)
    except (SessionExpired, AuthKeyUnregistered, Unauthorized):
        return False
    except FloodWait as fw:
        await asyncio.sleep(int(getattr(fw, "value", 3)) + FW_SAFETY_PAD)
    except Exception:
        pass
    finally:
        try:
            await app.disconnect()
        except Exception:
            pass
    return changed

# ---------------- sweep strategies ----------------
async def _targets_from_name_lock() -> list[tuple[int, list[dict], dict]]:
    """
    Returns [(user_id, sessions, cfg_dict), ...]
    cfg: {'enabled': True, 'name': 'Exact Name', 'expires_at': 'ISO'|None}
    """
    out = []
    for row in db.name_lock_targets():
        uid = row["user_id"]
        cfg = row["cfg"] or {}
        sessions = db.sessions_strings(uid)
        if not sessions:
            continue
        out.append((uid, sessions, cfg))
    return out

async def _targets_all_users() -> list[tuple[int, list[dict]]]:
    """Fallback: all users/sessions (no premium locks)."""
    con = db.get_conn()
    uids = [r["user_id"] for r in con.execute("SELECT user_id FROM users").fetchall()]
    con.close()
    res = []
    for uid in uids:
        sessions = db.sessions_strings(uid)
        if sessions:
            res.append((uid, sessions))
    return res

# ---------------- main loop ----------------
async def sweep_once(sema: asyncio.Semaphore):
    tasks = []

    if HAS_NAME_LOCK:
        # Premium flow: enforce exact names for locked users;
        # optionally still enforce BIO and suffix (config).
        targets = await _targets_from_name_lock()
        for uid, sessions, cfg in targets:
            desired = (cfg.get("name") or "").strip() if cfg else ""
            set_bio = ENFORCE_BIO_ALWAYS
            use_suffix = ENFORCE_SUFFIX_WHEN_LOCKED and not desired
            for s in sessions:
                async def runner(api_id=s["api_id"], api_hash=s["api_hash"], ss=s["session_string"],
                                 want=desired, suffix=use_suffix, bio=set_bio, user=uid):
                    async with sema:
                        changed = await _enforce_for_session(api_id, api_hash, ss, want, suffix, bio)
                        if changed:
                            log.info(f"u{user}: enforced (lock={'yes' if want else 'no'})")
                tasks.append(asyncio.create_task(runner()))
    else:
        # Legacy/global flow: enforce BIO + suffix for everyone
        targets = await _targets_all_users()
        for uid, sessions in targets:
            for s in sessions:
                async def runner(api_id=s["api_id"], api_hash=s["api_hash"], ss=s["session_string"], user=uid):
                    async with sema:
                        changed = await _enforce_for_session(api_id, api_hash, ss, None, True, True)
                        if changed:
                            log.info(f"u{user}: enforced (global)")
                tasks.append(asyncio.create_task(runner()))

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

async def main():
    db.init_db()
    sema = asyncio.Semaphore(max(1, MAX_CONCURRENCY))
    # random small delay on boot to desync from other loops
    await asyncio.sleep(random.uniform(0.5, 1.5))
    while True:
        try:
            await sweep_once(sema)
        except Exception as e:
            log.error(f"sweep error: {e}")
            await asyncio.sleep(2)
        await asyncio.sleep(LOOP_EVERY_SEC)

if __name__ == "__main__":
    asyncio.run(main())
