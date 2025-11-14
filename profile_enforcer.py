# profile_enforcer.py — periodically enforces bio/name with optional name-lock
# Improvements:
# • FloodWait-aware (backs off per account)
# • Per-user disable via settings key: enforce:disable:{user_id} = 1
# • Last-run throttle per slot (skip if enforced recently)
# • Name/Bio length-safe trimming (first_name<=64, bio<=70 by default)
# • Concurrency limited via ENFORCE_CONCURRENCY
# • Env-driven cadence via ENFORCE_EVERY_SEC (default 300s)

import os, asyncio, logging, time, random
from contextlib import suppress
from dotenv import load_dotenv
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError

from core.db import (
    init_db, users_with_sessions, sessions_list,
    get_setting, set_setting
)

load_dotenv()

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("enforcer")

# -------- ENV / Defaults --------
DEF_BIO     = os.getenv("ENFORCE_BIO", "#1 Free Ads Bot — Join @PhiloBots")
DEF_SUFFIX  = os.getenv("ENFORCE_NAME_SUFFIX", " — via @SpinifyAdsBot")
EVERY_SEC   = int(os.getenv("ENFORCE_EVERY_SEC", "300"))         # full sweep cadence
MIN_GAP_SEC = int(os.getenv("ENFORCE_MIN_GAP_SEC", "600"))       # per-slot throttle
CONC        = int(os.getenv("ENFORCE_CONCURRENCY", "3"))         # parallel sessions
NAME_MAX    = int(os.getenv("ENFORCE_NAME_MAX", "64"))           # Telegram first_name soft limit
BIO_MAX     = int(os.getenv("ENFORCE_BIO_MAX", "70"))            # Bio soft limit

def _name_lock_for(user_id: int) -> tuple[bool, str | None]:
    on  = str(get_setting(f"name_lock:{user_id}", 0)).lower() in ("1","true","yes")
    val = get_setting(f"name_lock:name:{user_id}", None)
    if val is not None:
        try: val = str(val)
        except Exception: val = None
    return on, val

def _disabled_for(user_id: int) -> bool:
    return str(get_setting(f"enforce:disable:{user_id}", 0)).lower() in ("1","true","yes")

def _last_key(user_id: int, slot: int) -> str:
    return f"enforce:last:{user_id}:{slot}"

def _trim(s: str, n: int) -> str:
    return s if len(s) <= n else s[: max(0, n)].rstrip()

async def enforce_once(user_id: int, slot: int, api_id: int, api_hash: str, session_string: str):
    # throttle per slot
    lk = _last_key(user_id, slot)
    last = int(get_setting(lk, 0) or 0)
    now  = int(time.time())
    if now - last < MIN_GAP_SEC:
        return

    app = Client(
        name=f"enf-{user_id}-s{slot}",
        api_id=int(api_id),
        api_hash=str(api_hash),
        session_string=str(session_string)
    )

    try:
        await app.start()
        me = await app.get_me()

        # Desired bio
        desired_bio = _trim(DEF_BIO or "", BIO_MAX)

        # Desired name (respect lock)
        lock_on, lock_val = _name_lock_for(user_id)
        if lock_on and lock_val:
            desired_name = _trim(lock_val, NAME_MAX)
        else:
            base = (getattr(me, "first_name", None) or "User").split(" — ")[0]
            desired_name = _trim(base + (DEF_SUFFIX or ""), NAME_MAX)

        # Apply bio
        try:
            cur_bio = getattr(me, "bio", "") or ""
            if desired_bio and cur_bio != desired_bio:
                await app.update_profile(bio=desired_bio)
        except FloodWait as fw:
            sleep_s = fw.value + 1
            log.warning("[u%s s%s] Bio FloodWait %ss", user_id, slot, sleep_s)
            await asyncio.sleep(sleep_s)
        except Exception as e:
            log.debug("[u%s s%s] bio update skipped: %s", user_id, slot, e)

        # Apply name
        try:
            cur_name = getattr(me, "first_name", "") or ""
            if desired_name and cur_name != desired_name:
                await app.update_profile(first_name=desired_name)
        except FloodWait as fw:
            sleep_s = fw.value + 1
            log.warning("[u%s s%s] Name FloodWait %ss", user_id, slot, sleep_s)
            await asyncio.sleep(sleep_s)
        except Exception as e:
            log.debug("[u%s s%s] name update skipped: %s", user_id, slot, e)

        # Mark success time
        set_setting(lk, int(time.time()))
        log.info("[u%s s%s] enforced", user_id, slot)

    except FloodWait as fw:
        sleep_s = fw.value + 1
        log.warning("[u%s s%s] Start FloodWait %ss", user_id, slot, sleep_s)
        await asyncio.sleep(sleep_s)
    except RPCError as e:
        log.info("[u%s s%s] RPCError: %s", user_id, slot, e)
    except Exception as e:
        log.info("[u%s s%s] enforce failed: %s", user_id, slot, e)
    finally:
        with suppress(Exception):
            await app.stop()

async def sweep_once(sema: asyncio.Semaphore):
    tasks = []
    for uid in users_with_sessions():
        if _disabled_for(uid):
            continue
        for s in sessions_list(uid):
            api_id  = int(s["api_id"])
            api_hash= str(s["api_hash"])
            sess    = str(s["session_string"])
            slot    = int(s["slot"])

            async def _runner(u=uid, sl=slot, a=api_id, h=api_hash, ss=sess):
                async with sema:
                    try:
                        await enforce_once(u, sl, a, h, ss)
                        # tiny jitter to spread calls
                        await asyncio.sleep(0.1 + random.random()*0.2)
                    except Exception as e:
                        log.debug("[u%s s%s] runner err: %s", u, sl, e)

            tasks.append(asyncio.create_task(_runner()))
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

async def main():
    init_db()
    sema = asyncio.Semaphore(CONC)
    log.info("enforcer started: every=%ss, min_gap=%ss, conc=%s", EVERY_SEC, MIN_GAP_SEC, CONC)
    while True:
        try:
            await sweep_once(sema)
        except Exception as e:
            log.error("sweep error: %s", e)
        await asyncio.sleep(EVERY_SEC)

if __name__ == "__main__":
    asyncio.run(main())
