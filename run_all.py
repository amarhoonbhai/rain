# profile_enforcer.py — enforce name/bio unless premium
import asyncio, logging
from pyrogram import Client
from core.db import get_conn, init_db, is_premium

logging.basicConfig(level="INFO")
log = logging.getLogger("enforcer")

BIO = "#1 Free Ads Bot — Join @PhiloBots"
NAME_SUFFIX = " — via @SpinifyAdsBot"

def load_accounts():
    conn = get_conn()
    rows = conn.execute("SELECT user_id, api_id, api_hash, session_string FROM sessions").fetchall()
    conn.close()
    return rows

async def enforce_once(user_id, api_id, api_hash, session_string):
    if is_premium(user_id):
        log.info(f"u{user_id}: premium — skip enforce")
        return
    app = Client(name=f"enf-{user_id}", api_id=api_id, api_hash=api_hash, session_string=session_string)
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
        log.error(f"u{user_id}: enforce failed: {e}")
    finally:
        try: await app.stop()
        except Exception: pass

async def main():
    init_db()
    while True:
        for r in load_accounts():
            await enforce_once(r["user_id"], r["api_id"], r["api_hash"], r["session_string"])
            await asyncio.sleep(0.5)
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())
