import asyncio
from datetime import timedelta, timezone
from pyrogram import Client
from core.db import get_conn

BIO_TEXT = "#1 Free Ads Bot — Join @PhiloBots"
NAME_TAG = " — via @SpinifyAdsBot"
CHECK_SEC = 1800  # 30 min

async def enforce_once():
    conn = get_conn()
    cur = conn.execute("SELECT user_id, api_id, api_hash, session_string FROM user_sessions")
    rows = cur.fetchall()
    conn.close()

    for r in rows:
        uid = r["user_id"]
        try:
            app = Client(
                name=f"enf-{uid}",
                api_id=r["api_id"],
                api_hash=r["api_hash"],
                session_string=r["session_string"],
                in_memory=True
            )
            await app.connect()
            me = await app.get_me()

            # bio
            if (me.bio or "") != BIO_TEXT:
                await app.update_profile(bio=BIO_TEXT)

            # name
            base = me.first_name.split(" — ")[0]
            wanted = base + NAME_TAG
            if me.first_name != wanted:
                await app.update_profile(first_name=wanted)

            await app.disconnect()
        except Exception:
            continue

async def main():
    while True:
        await enforce_once()
        await asyncio.sleep(CHECK_SEC)

if __name__ == "__main__":
    asyncio.run(main())
  
