# profile_enforcer.py — optional: enforce bio/name across sessions
import asyncio, logging
from core.db import init_db, sessions_list
from pyrogram import Client

logging.basicConfig(level="INFO")
log = logging.getLogger("enforcer")

BIO = "#1 Free Ads Bot — Join @PhiloBots"
NAME_SUFFIX = " — via @SpinifyAdsBot"

async def enforce_user(uid: int, slot: int, api_id: int, api_hash: str, session_string: str):
    app = Client(name=f"enf-{uid}-s{slot}", api_id=api_id, api_hash=api_hash, session_string=session_string)
    try:
        await app.start()
        me = await app.get_me()
        try:
            await app.update_profile(bio=BIO)
        except Exception:
            pass
        try:
            base = (me.first_name or "User").split(" — ")[0]
            if not (me.first_name or "").endswith(NAME_SUFFIX):
                await app.update_profile(first_name=base + NAME_SUFFIX)
        except Exception:
            pass
    except Exception as e:
        log.info(f"[u{uid}s{slot}] enforce failed: {e}")
    finally:
        try:
            await app.stop()
        except Exception:
            pass

async def main():
    init_db()
    while True:
        try:
            # iterate all sessions
            # powered by db only (no cache)
            from core.db import users_with_sessions
            for uid in users_with_sessions():
                for r in sessions_list(uid):
                    await enforce_user(uid, r["slot"], r["api_id"], r["api_hash"], r["session_string"])
                    await asyncio.sleep(0.3)
        except Exception as e:
            log.error(f"loop: {e}")
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())
