import asyncio, sqlite3
from datetime import datetime, timedelta, timezone
from pyrogram import Client
from core.db import get_conn
from core.utils import is_due, now_ist_iso

IST = timezone(timedelta(hours=5, minutes=30))

async def send_for_user(row, groups):
    user_id = row["user_id"]
    api_id = row["api_id"]
    api_hash = row["api_hash"]
    session_string = row["session_string"]
    ad_message = row["ad_message"]

    app = Client(
        name=f"wrk-{user_id}",
        api_id=api_id,
        api_hash=api_hash,
        session_string=session_string,
        in_memory=True
    )
    await app.connect()
    for g in groups:
        link = g["group_link"]
        try:
            # join/resolve
            chat = await app.resolve_peer(link)
            await app.send_message(chat_id=chat, text=ad_message)
        except Exception as e:
            # you can log
            continue
    await app.disconnect()

async def loop_worker():
    while True:
        conn = get_conn()
        # join users + sessions
        cur = conn.execute("""
            SELECT u.user_id, u.ad_message, u.interval_minutes, u.last_sent_at,
                   s.api_id, s.api_hash, s.session_string
            FROM users u
            JOIN user_sessions s ON u.user_id = s.user_id
            WHERE u.ad_message IS NOT NULL
        """)
        users = cur.fetchall()
        for u in users:
            if not is_due(u["last_sent_at"], u["interval_minutes"]):
                continue
            # get groups
            cur2 = conn.execute("SELECT group_link FROM user_groups WHERE user_id = ?", (u["user_id"],))
            groups = cur2.fetchall()
            if not groups:
                continue
            await send_for_user(u, groups)
            # update last_sent_at
            conn.execute("UPDATE users SET last_sent_at = ? WHERE user_id = ?", (now_ist_iso(), u["user_id"]))
            conn.commit()
        conn.close()
        await asyncio.sleep(30)   # check every 30s

if __name__ == "__main__":
    asyncio.run(loop_worker())
  
