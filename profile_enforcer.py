# profile_enforcer.py
# Spinify Ads — Profile/Bio Enforcer (Stable B2 Version)
# Runs in background via run_all.py
# Keeps name suffix + bio always correct.

import asyncio
import logging
from telethon import TelegramClient, errors
from telethon.sessions import StringSession
from datetime import datetime, timezone

from core.db import (
    init_db,
    sessions_list,
    get_conn,
    get_setting,
)

log = logging.getLogger("enforcer")

# Defaults (Login bot also sets these)
DEFAULT_BIO = "#1 Free Ads Bot — Managed By @PhiloBots"
DEFAULT_SUFFIX = " Hosted By — @SpinifyAdsBot"


# ------------------------------
# Helpers
# ------------------------------
def now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


async def enforce_for_user(uid: int, sess: dict):
    """Connect the user's session → fix bio + fix name suffix."""
    api_id = int(sess["api_id"])
    api_hash = sess["api_hash"]
    string_sess = sess["session_string"]

    client = TelegramClient(
        StringSession(string_sess),
        api_id=api_id,
        api_hash=api_hash,
        system_version="Android"
    )

    try:
        await client.start()
    except Exception as e:
        log.error(f"[ENF] Cannot start user {uid}: {e}")
        return

    try:
        me = await client.get_me()

        # -------------------
        # enforce BIO
        # -------------------
        try:
            current_bio = me.bot_info_description or ""
        except:
            current_bio = ""

        bio = get_setting(f"bio:{uid}", DEFAULT_BIO)

        if current_bio != bio:
            try:
                await client.update_profile(bio=bio)
                log.info(f"[ENF] Bio updated for {uid}")
            except Exception:
                pass

        # -------------------
        # enforce NAME SUFFIX
        # -------------------
        try:
            fname = me.first_name or "User"
        except:
            fname = "User"

        suffix = get_setting(f"name_suffix:{uid}", DEFAULT_SUFFIX)
        base = fname.split(" Hosted By — ")[0].strip()
        desired = base + suffix

        if fname != desired:
            try:
                await client.update_profile(first_name=desired)
                log.info(f"[ENF] Name updated for {uid}")
            except Exception:
                pass

    except Exception as e:
        log.error(f"[ENF] Error enforcing for {uid}: {e}")

    finally:
        await client.disconnect()


# ------------------------------
# Infinite loop
# ------------------------------
async def start():
    """Entry for run_all.py"""
    init_db()
    log.info("Enforcer: started")

    while True:
        try:
            rows = get_conn().execute("SELECT user_id FROM users").fetchall()
            if not rows:
                await asyncio.sleep(10)
                continue

            for r in rows:
                uid = r["user_id"]

                for sess in sessions_list(uid):
                    await enforce_for_user(uid, sess)
                    await asyncio.sleep(2)  # small delay

        except Exception as e:
            log.error(f"[ENF] Loop crash: {e}")

        await asyncio.sleep(30)  # run every 30s
