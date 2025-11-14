# profile_enforcer.py — periodically enforces bio/name with optional name-lock
import os, asyncio, logging
from pyrogram import Client
from core.db import init_db, users_with_sessions, sessions_list, get_setting

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"))
log = logging.getLogger("enforcer")

# Global defaults (can be overridden per user via settings)
DEF_BIO = os.getenv("ENFORCE_BIO", "#1 Free Ads Bot — Join @PhiloBots")
DEF_SUFFIX = os.getenv("ENFORCE_NAME_SUFFIX", " — via @SpinifyAdsBot")

SLEEP_SEC = 300  # every 5 minutes

def _name_lock_for(user_id: int):
    on  = str(get_setting(f"name_lock:{user_id}", 0)) in ("1", "true", "True")
    val = get_setting(f"name_lock:name:{user_id}", None)
    return on, val

async def enforce_once(user_id: int, slot: int, api_id: int, api_hash: str, session_string: str):
    app = Client(name=f"enf-{user_id}-s{slot}", api_id=api_id, api_hash=api_hash, session_string=session_string)
    try:
        await app.start()
        me = await app.get_me()
        # BIO
        try:
            await app.update_profile(bio=DEF_BIO)
        except Exception:
            pass
        # Name
        lock_on, lock_val = _name_lock_for(user_id)
        desired = lock_val if (lock_on and lock_val) else ( (me.first_name or "User").split(" — ")[0] + DEF_SUFFIX )
        try:
            if (me.first_name or "") != desired:
                await app.update_profile(first_name=desired)
        except Exception:
            pass
        log.info("enforced u%s: slot %s", user_id, slot)
    except Exception as e:
        log.info("u%s s%s enforce failed: %s", user_id, slot, e)
    finally:
        try: await app.stop()
        except Exception: pass

async def main():
    init_db()
    while True:
        for uid in users_with_sessions():
            for s in sessions_list(uid):
                await enforce_once(uid, s["slot"], int(s["api_id"]), str(s["api_hash"]), str(s["session_string"]))
                await asyncio.sleep(0.3)
        await asyncio.sleep(SLEEP_SEC)

if __name__ == "__main__":
    asyncio.run(main())
