# profile_enforcer.py — keep bio/name consistent across all user sessions
# Rules:
#  • Non-premium: enforce BIO + add NAME_SUFFIX to first_name (once).
#  • Premium (no lock): DO NOT touch name/bio.
#  • Premium with lock: force locked name (no suffix), leave bio untouched.
#
# Env overrides:
#  ENFORCER_ENABLED=1|0 (default 1)
#  ENFORCER_BIO="#1 Free Ads Bot — Join @PhiloBots"
#  ENFORCER_NAME_SUFFIX=" — via @SpinifyAdsBot"
#  ENFORCER_INTERVAL_SEC=300

import os, asyncio, logging
from datetime import datetime
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError, Unauthorized, AuthKeyUnregistered
try:
    from pyrogram.errors import SessionRevoked, SessionExpired, UserDeactivated, UserDeactivatedBan
except Exception:  # best-effort compat
    class _E(Exception): pass
    SessionRevoked = SessionExpired = UserDeactivated = UserDeactivatedBan = _E

from core.db import init_db, get_conn, is_premium, get_setting

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("enforcer")

ENABLED = os.getenv("ENFORCER_ENABLED", "1").strip() not in ("0", "false", "False", "")
BIO_DEFAULT = os.getenv("ENFORCER_BIO", "#1 Free Ads Bot — Join @PhiloBots")
NAME_SUFFIX = os.getenv("ENFORCER_NAME_SUFFIX", " — via @SpinifyAdsBot")
INTERVAL = int(os.getenv("ENFORCER_INTERVAL_SEC", "300"))

# keys used by set_name_lock() in db.py
def _lock_enabled_key(uid:int) -> str: return f"premium:lock:enabled:{uid}"
def _lock_name_key(uid:int)    -> str: return f"premium:lock:name:{uid}"

def _rows_sessions():
    conn = get_conn()
    rows = conn.execute(
        "SELECT user_id, slot, api_id, api_hash, session_string FROM user_sessions ORDER BY user_id, slot"
    ).fetchall()
    conn.close()
    return rows

async def _enforce_for_session(row):
    uid  = row["user_id"]
    slot = row["slot"]
    api_id, api_hash, sess = int(row["api_id"]), str(row["api_hash"]), str(row["session_string"])

    prem = bool(is_premium(uid))
    lock_on  = bool(int(get_setting(_lock_enabled_key(uid), 0) or 0))
    lock_name = get_setting(_lock_name_key(uid), None)

    app = Client(name=f"enf-u{uid}-s{slot}", api_id=api_id, api_hash=api_hash, session_string=sess)
    try:
        await app.start()
        me = await app.get_me()

        if prem:
            if lock_on and (lock_name or "").strip():
                # Force exact name; leave bio alone for premium
                current = (me.first_name or "") + ((" " + me.last_name) if me.last_name else "")
                if current != lock_name:
                    try:
                        await app.update_profile(first_name=lock_name, last_name=None)
                        log.info(f"u{uid}s{slot}: set locked name -> {lock_name!r}")
                    except Exception as e:
                        log.warning(f"u{uid}s{slot}: set locked name failed: {e}")
            # else premium, no lock: do nothing
        else:
            # Non-premium: set BIO and suffix first_name once
            try:
                if (me.bio or "") != BIO_DEFAULT:
                    await app.update_profile(bio=BIO_DEFAULT)
                    log.info(f"u{uid}s{slot}: bio enforced")
            except Exception as e:
                log.debug(f"u{uid}s{slot}: bio set failed: {e}")

            try:
                base = (me.first_name or "User").split(" — ")[0]
                target = base + NAME_SUFFIX
                current = (me.first_name or "")
                if current != target:
                    await app.update_profile(first_name=target)
                    log.info(f"u{uid}s{slot}: name enforced -> {target!r}")
            except Exception as e:
                log.debug(f"u{uid}s{slot}: name set failed: {e}")

    except (Unauthorized, AuthKeyUnregistered, SessionRevoked, SessionExpired, UserDeactivated, UserDeactivatedBan) as e:
        log.warning(f"u{uid}s{slot}: auth error, skipping: {e}")
    except FloodWait as fw:
        log.info(f"u{uid}s{slot}: FloodWait {fw.value}s"); await asyncio.sleep(fw.value + 1)
    except RPCError as e:
        log.debug(f"u{uid}s{slot}: RPC error: {e}")
    except Exception as e:
        log.debug(f"u{uid}s{slot}: start/update failed: {e}")
    finally:
        try: await app.stop()
        except Exception: pass

async def main():
    if not ENABLED:
        log.info("Enforcer disabled by ENFORCER_ENABLED=0")
        # sleep forever so supervisor stays happy
        while True:
            await asyncio.sleep(3600)

    init_db()
    log.info("Enforcer started")
    while True:
        try:
            rows = _rows_sessions()
            # light pacing to be gentle
            for r in rows:
                await _enforce_for_session(r)
                await asyncio.sleep(0.4)
        except Exception as e:
            log.error(f"loop error: {e}")
        await asyncio.sleep(INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
