# profile_enforcer.py — A1 Compact Version
# Keeps user profile BIO + NAME_SUFFIX enforced across all connected sessions.

import asyncio
import logging
from pyrogram import Client
from core.db import init_db, users_with_sessions, sessions_list
import os

log = logging.getLogger("enforcer")

BIO = os.getenv("ENFORCE_BIO", "Managed by @PhiloBots")
NAME_SUFFIX = os.getenv("ENFORCE_NAME_SUFFIX", " — By @SpinifyAdsBot")


async def enforce_user(uid: int):
    """Enforces name suffix + bio on all sessions of a user."""
    sessions = sessions_list(uid)
    if not sessions:
        return

    for sess in sessions:
        try:
            api_id = int(sess["api_id"])
            api_hash = sess["api_hash"]
            ss = sess["session_string"]

            app = Client(
                f"e{uid}_{sess['slot']}",
                api_id=api_id,
                api_hash=api_hash,
                session_string=ss,
                in_memory=True
            )
            await app.start()

            # Apply BIO
            try:
                await app.update_profile(bio=BIO)
            except Exception:
                pass

            # Apply Name Suffix
            try:
                me = await app.get_me()
                base = (me.first_name or "User").split(" — ")[0]
                desired = base + NAME_SUFFIX

                if me.first_name != desired:
                    await app.update_profile(first_name=desired)
            except Exception:
                pass

            await app.stop()

        except Exception as e:
            log.error(f"[Enforcer] Error user {uid}: {e}")


async def start():
    """Entry function called from run_all.py"""
    init_db()
    log.info("Enforcer: started.")

    while True:
        try:
            users = users_with_sessions()  # from db.py
            for uid in users:
                await enforce_user(uid)
        except Exception as e:
            log.error(f"Enforcer main loop: {e}")

        await asyncio.sleep(3600)  # run every 1 hour
