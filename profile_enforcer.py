import os
import asyncio
import logging
from typing import Dict

from pyrogram import Client
from pyrogram.errors import RPCError, SessionRevoked, AuthKeyUnregistered, FloodWait

from core.db import init_db, users_with_sessions, sessions_list

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("enforcer")

BIO_DEFAULT = "#1 Free Ads Bot — Managed By @PhiloBots"
NAME_SUFFIX = " Hosted By — @SpinifyAdsBot"

SLEEP_SCAN = 300       # full scan interval
MIN_GAP_PER_USER = 600 # minimum gap per user (seconds)
MAX_CONCURRENCY = 3

_last_run: Dict[int, float] = {}


async def enforce_one(uid: int, slot: int, api_id: int, api_hash: str, session_string: str):
    app = Client(
        name=f"enf-{uid}-s{slot}",
        api_id=api_id,
        api_hash=api_hash,
        session_string=session_string
    )
    try:
        await app.start()
    except (SessionRevoked, AuthKeyUnregistered):
        log.info("enforcer: session revoked for u%s s%s", uid, slot)
        return
    except RPCError as e:
        log.info("enforcer: RPC error u%s s%s: %s", uid, slot, e)
        return
    except Exception as e:
        log.info("enforcer: start fail u%s s%s: %s", uid, slot, e)
        return

    try:
        me = await app.get_me()
        # Enforce first name suffix
        base = (me.first_name or "User").strip()
        if NAME_SUFFIX not in base:
            new_name = f"{base}{NAME_SUFFIX}"
        else:
            new_name = base

        try:
            await app.update_profile(first_name=new_name)
        except Exception as e:
            log.debug("enforcer: name update fail u%s s%s: %s", uid, slot, e)

        # Enforce bio
        try:
            await app.update_profile(bio=BIO_DEFAULT)
        except Exception as e:
            log.debug("enforcer: bio update fail u%s s%s: %s", uid, slot, e)

        log.info("enforcer: enforced u%s s%s", uid, slot)

    finally:
        try:
            await app.stop()
        except Exception:
            pass


async def main():
    init_db()
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    loop = asyncio.get_running_loop()

    log.info("profile_enforcer started | scan=%ss gap=%ss conc=%s",
             SLEEP_SCAN, MIN_GAP_PER_USER, MAX_CONCURRENCY)

    while True:
        now = loop.time()
        for uid in users_with_sessions():
            last = _last_run.get(uid, 0.0)
            if now - last < MIN_GAP_PER_USER:
                continue
            _last_run[uid] = now

            sessions = sessions_list(uid)
            for s in sessions:
                api_id = int(s["api_id"])
                api_hash = str(s["api_hash"])
                session_string = str(s["session_string"])
                slot = int(s["slot"])

                async with sem:
                    asyncio.create_task(enforce_one(uid, slot, api_id, api_hash, session_string))
                await asyncio.sleep(0.2)

        await asyncio.sleep(SLEEP_SCAN)


if __name__ == "__main__":
    asyncio.run(main())
