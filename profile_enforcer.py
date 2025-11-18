import os
import asyncio
import logging

from pyrogram import Client
from core.db import (
    init_db,
    users_with_sessions,
    sessions_list,
    get_setting_name_lock,
)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL)
log = logging.getLogger("enforcer")

# Defaults can be overridden from .env
DEF_BIO = os.getenv("ENFORCE_BIO", "#1 Free Ads Bot — Join @PhiloBots")
DEF_SUFFIX = os.getenv("ENFORCE_NAME_SUFFIX", " — By @SpinifyAdsBot")

# How often we scan all users (seconds)
SLEEP_SEC = int(os.getenv("ENFORCER_SCAN_SEC", "300"))
# Per-user minimum gap between enforcements (seconds)
MIN_GAP = int(os.getenv("ENFORCER_MIN_GAP_SEC", "600"))
# Max concurrent Pyrogram sessions
_CONC = int(os.getenv("ENFORCER_CONCURRENCY", "3"))

# user_id -> monotonic timestamp of last enforcement
_last_run: dict[int, float] = {}


async def _enforce_one(
    uid: int,
    slot: int,
    api_id: int,
    api_hash: str,
    session_string: str,
) -> None:
    """
    Enforce bio + (optional) name-lock for a single slot.
    """
    app = Client(
        name=f"enf-{uid}-s{slot}",
        api_id=api_id,
        api_hash=api_hash,
        session_string=session_string,
    )
    try:
        await app.start()

        # 1) Bio
        try:
            await app.update_profile(bio=DEF_BIO)
        except Exception:
            # some accounts may be restricted; just continue
            pass

        # 2) Name / name-lock
        me = await app.get_me()
        lock_on, lock_name = get_setting_name_lock(uid)

        if lock_on and lock_name:
            desired = lock_name
        else:
            base = (me.first_name or "User").split(" — ")[0]
            desired = base + DEF_SUFFIX

        try:
            if (me.first_name or "") != desired:
                await app.update_profile(first_name=desired)
        except Exception:
            # ignore 400-style errors (e.g., invalid names)
            pass

        log.info("enforced u%s s%s", uid, slot)

    except Exception as e:
        log.info("enforce fail u%s s%s: %s", uid, slot, e)
    finally:
        try:
            await app.stop()
        except Exception:
            pass


async def _enforce_wrapped(
    uid: int,
    slot: int,
    api_id: int,
    api_hash: str,
    session_string: str,
    sem: asyncio.Semaphore,
) -> None:
    """
    Wrapper to apply semaphore to the whole client lifetime.
    """
    async with sem:
        await _enforce_one(uid, slot, api_id, api_hash, session_string)


async def main() -> None:
    """
    Periodically loop over all panel users that have sessions and enforce:
      - bio (DEF_BIO)
      - first_name (locked or base+suffix)
    """
    init_db()
    sem = asyncio.Semaphore(_CONC)
    loop = asyncio.get_running_loop()

    log.info(
        "profile_enforcer started | scan=%ss min_gap=%ss conc=%s",
        SLEEP_SEC,
        MIN_GAP,
        _CONC,
    )

    while True:
        try:
            uids = users_with_sessions()
            for uid in uids:
                last = _last_run.get(uid, 0.0)
                if last and (loop.time() - last < MIN_GAP):
                    continue

                _last_run[uid] = loop.time()
                for s in sessions_list(uid):
                    try:
                        api_id = int(s["api_id"])
                        api_hash = str(s["api_hash"])
                        session_string = str(s["session_string"])
                        slot = int(s["slot"])
                    except Exception as e:
                        log.warning("bad session record for u%s: %s", uid, e)
                        continue

                    asyncio.create_task(
                        _enforce_wrapped(
                            uid,
                            slot,
                            api_id,
                            api_hash,
                            session_string,
                            sem,
                        )
                    )
                    # small spread to avoid burst
                    await asyncio.sleep(0.2)

            await asyncio.sleep(SLEEP_SEC)

        except asyncio.CancelledError:
            log.info("profile_enforcer cancelled, exiting loop")
            break
        except Exception as e:
            log.exception("profile_enforcer loop error: %s", e)
            # if something weird happens, wait a bit then continue
            await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(main())
