# profile_enforcer.py
# Spinify – Profile Guard (Name + Bio Enforcer)
# Ensures every logged-in session keeps required bio + suffix name.

import asyncio
import logging
from datetime import datetime, timezone

from telethon import TelegramClient, errors
from telethon.sessions import StringSession

from core.db import (
    init_db,
    sessions_list,
    get_setting,
)

log = logging.getLogger("enforcer")


# ================================
# REQUIRED BI0 + NAME SUFFIX
# ================================
BIO = get_setting("enforce_bio", "#1 Free Ads Bot — Managed By @PhiloBots")
NAME_SUFFIX = get_setting("enforce_suffix", " Hosted By — @SpinifyAdsBot")


def now_ts():
    return int(datetime.now(timezone.utc).timestamp())


# ================================
# Per-user enforcing loop
# ================================
async def enforce_profile(uid: int, sess: dict):
    api_id = int(sess["api_id"])
    api_hash = sess["api_hash"]
    session_str = sess["session_string"]

    client = TelegramClient(
        session=StringSession(session_str),
        api_id=api_id,
        api_hash=api_hash,
        system_version="Android",
    )

    await client.start()
    log.info(f"[enforcer] started user={uid} slot={sess['slot']}")

    try:
        while True:
            try:
                me = await client.get_me()

                # --- Enforce BIO --------------------------------------------------
                try:
                    if me.bot is False:
                        await client.update_profile(bio=BIO)
                except Exception:
                    pass

                # --- Enforce NAME SUFFIX -----------------------------------------
                try:
                    base = (me.first_name or "User").split(" Hosted By — ")[0]
                    desired = base + NAME_SUFFIX
                    if me.first_name != desired:
                        await client.update_profile(first_name=desired)
                except Exception:
                    pass

            except errors.FloodWaitError as e:
                log.warning(f"[enforcer {uid}] flood wait {e.seconds}s")
                await asyncio.sleep(e.seconds)

            except Exception as e:
                log.error(f"[enforcer {uid}] error: {e}")

            await asyncio.sleep(180)  # every 3 minutes

    finally:
        await client.disconnect()
        log.info(f"[enforcer] stopped user={uid}")


# ================================
# Main entry for run_all.py
# ================================
async def start_enforcer():
    init_db()

    from core.db import get_conn
    rows = get_conn().execute("SELECT DISTINCT user_id FROM users").fetchall()

    if not rows:
        await asyncio.sleep(10)
        return

    tasks = []

    for r in rows:
        uid = r["user_id"]
        for sess in sessions_list(uid):
            tasks.append(asyncio.create_task(enforce_profile(uid, sess)))

    if tasks:
        await asyncio.gather(*tasks)
