# profile_enforcer.py
# Spinify Ads — Profile Enforcer (B-2 Stable Version)
# Automatically fixes:
#   ✔ BIO
#   ✔ NAME SUFFIX
#   ✔ Username stays original (never touches)
#
# Runs every 20 minutes from run_all.py

import asyncio
import logging
from telethon import TelegramClient
from telethon.sessions import StringSession

from core.db import (
    init_db,
    sessions_list,
    ensure_user,
    get_conn
)

log = logging.getLogger("enforcer")

# Required branding
BIO = "#1 Free Ads Bot — Managed By @PhiloBots"
NAME_SUFFIX = " Hosted By — @SpinifyAdsBot"


async def enforce_for_user(uid: int, sess: dict):
    """Fix BIO and Name Suffix for a single logged account"""

    api_id = sess["api_id"]
    api_hash = sess["api_hash"]
    s = sess["session_string"]

    client = TelegramClient(StringSession(s), api_id, api_hash)

    try:
        await client.start()
        me = await client.get_me()

        base = (me.first_name or "User").split(" Hosted By — ")[0]
        desired_name = base + NAME_SUFFIX

        # Enforce name
        try:
            if (me.first_name or "") != desired_name:
                await client.update_profile(first_name=desired_name)
                log.info(f"[{uid}] name enforced → {desired_name}")
        except Exception as e:
            log.warning(f"[{uid}] name update failed: {e}")

        # Enforce bio
        try:
            await client.update_profile(bio=BIO)
        except Exception as e:
            log.warning(f"[{uid}] bio update failed: {e}")

    except Exception as e:
        log.error(f"[{uid}] enforcer failed: {e}")

    finally:
        await client.disconnect()


async def start():
    """Called by run_all.py — runs infinite loop"""

    init_db()
    log.info("Enforcer started.")

    while True:
        try:
            rows = get_conn().execute("SELECT user_id FROM users").fetchall()

            for r in rows:
                uid = r["user_id"]
                sessions = sessions_list(uid)

                for sess in sessions:
                    await enforce_for_user(uid, sess)

            log.info("Enforcer cycle completed.")

        except Exception as e:
            log.error(f"Enforcer fatal error: {e}")

        await asyncio.sleep(1200)  # 20 minutes
