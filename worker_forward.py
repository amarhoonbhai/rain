# worker_forward.py — forwards saved ad text to user groups on schedule
# • Per-user interval: 30/45/60 minutes (default 30)
# • Up to 3 sessions (round-robin)
# • Global Night Mode (00:00–07:00 IST)
# • Tracks last_sent_at and sent_ok

import asyncio
import logging
from datetime import datetime, time
from zoneinfo import ZoneInfo

from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError

from core.db import (
    init_db,
    users_with_sessions, sessions_strings,
    list_groups, get_ad, get_interval, get_last_sent_at, mark_sent_now,
    night_enabled, set_setting, get_setting, inc_sent_ok
)

__all__ = ["main", "main_loop"]

LOG_LEVEL = "INFO"
logging.basicConfig(level=LOG_LEVEL)
log = logging.getLogger("worker")

IST = ZoneInfo("Asia/Kolkata")
NIGHT_START = time(0, 0)   # 00:00
NIGHT_END   = time(7, 0)   # 07:00

def is_night_now_ist() -> bool:
    now = datetime.now(IST).time()
    return NIGHT_START <= now < NIGHT_END

def _parse_mode_string(s: str | None):
    if not s: return None
    s = s.strip().lower()
    if s in ("markdown","md"): return "Markdown"
    if s in ("html","htm"): return "HTML"
    return None

def _next_slot_index(user_id: int, total_slots: int) -> int:
    key = f"worker:last_session:{user_id}"
    cur = int(get_setting(key, -1) or -1)
    nxt = (cur + 1) % max(1, total_slots)
    set_setting(key, nxt)
    return nxt

async def _send_via_session(sess: dict, groups: list[str], text: str, parse_mode: str | None) -> int:
    ok = 0
    app = Client(
        name=f"user-{sess['user_id']}-s{sess['slot']}",
        api_id=int(sess["api_id"]),
        api_hash=str(sess["api_hash"]),
        session_string=str(sess["session_string"])
    )
    try:
        await app.start()
    except Exception as e:
        log.error(f"[u{sess['user_id']} s{sess['slot']}] start failed: {e}")
        return 0

    for g in groups:
        try:
            await app.send_message(chat_id=g, text=text, parse_mode=parse_mode)
            ok += 1
            await asyncio.sleep(0.4)  # gentle throttle
        except FloodWait as fw:
            log.warning(f"[u{sess['user_id']} s{sess['slot']}] FloodWait {fw.value}s on {g}")
            await asyncio.sleep(fw.value + 1)
        except RPCError as e:
            log.warning(f"[u{sess['user_id']} s{sess['slot']}] RPCError on {g}: {e}")
        except Exception as e:
            log.warning(f"[u{sess['user_id']} s{sess['slot']}] send failed on {g}: {e}")

    try:
        await app.stop()
    except Exception:
        pass
    return ok

async def process_user(user_id: int):
    if night_enabled() and is_night_now_ist():
        return

    text, mode = get_ad(user_id)
    if not text:
        return
    groups = list_groups(user_id)
    if not groups:
        return
    sessions = sessions_strings(user_id)
    if not sessions:
        return

    interval = get_interval(user_id) or 30
    last_ts = get_last_sent_at(user_id)
    now = int(datetime.utcnow().timestamp())
    if last_ts is not None and now - last_ts < interval * 60:
        return  # not due yet

    idx = _next_slot_index(user_id, len(sessions))
    sess = sessions[idx]

    sent = await _send_via_session(sess, groups, text, _parse_mode_string(mode))
    if sent > 0:
        mark_sent_now(user_id)
        inc_sent_ok(user_id, sent)

async def main_loop():
    init_db()
    while True:
        try:
            for uid in users_with_sessions():
                try:
                    await process_user(uid)
                except Exception as e:
                    log.error(f"[u{uid}] process error: {e}")
                await asyncio.sleep(0.2)  # spread load
        except Exception as e:
            log.error(f"loop error: {e}")
        await asyncio.sleep(15)  # tick

# <-- this is what run_all.py awaits
async def main():
    await main_loop()

if __name__ == "__main__":
    asyncio.run(main())
