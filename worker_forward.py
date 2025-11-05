# worker_forward.py — forwards per interval (30/45/60) to user's added groups; counts messages
import asyncio, logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError, ChatWriteForbidden, ChannelPrivate, UsernameInvalid
from core.db import init_db, list_active_sessions, get_ad, list_groups, get_interval, update_last_sent, inc_sent

log = logging.getLogger("worker"); logging.basicConfig(level=logging.INFO)
FORWARD_TASKS: Dict[int, asyncio.Task] = {}
IST = timezone(timedelta(hours=5, minutes=30))
VALID_INTERVALS = {30, 45, 60}

def now_ist_iso() -> str:
    return datetime.now(IST).isoformat(timespec="seconds")

def normalize_groups(groups: List[str]) -> List[str]:
    out = []
    for raw in groups:
        g = (raw or "").strip()
        if not g: continue
        if g.startswith("https://t.me/"): g = g.split("https://t.me/", 1)[1].strip("/")
        if g.startswith("@"): g = g[1:]
        if g and g not in out: out.append(g)
    return out

async def _send_round(user_id: int, app: Client, ad_text: str, groups: List[str]) -> tuple[int,int]:
    ok = fail = 0
    for g in groups:
        try:
            await app.send_message(g, ad_text, disable_web_page_preview=True)
            ok += 1
            await asyncio.sleep(3)  # gentle pacing
        except FloodWait as fw:
            wait_s = int(getattr(fw, "value", 5)) if hasattr(fw, "value") else 5
            log.warning(f"[{user_id}] FloodWait {wait_s}s on {g}"); await asyncio.sleep(wait_s)
        except (ChatWriteForbidden, ChannelPrivate, UsernameInvalid) as e:
            log.error(f"[{user_id}] cannot post to {g}: {e.__class__.__name__}"); fail += 1; await asyncio.sleep(1)
        except RPCError as e:
            log.error(f"[{user_id}] RPCError on {g}: {e}"); fail += 1; await asyncio.sleep(2)
        except Exception as e:
            log.exception(f"[{user_id}] unexpected error on {g}: {e}"); fail += 1; await asyncio.sleep(1)
    return ok, fail

async def _user_task(user_id: int, api_id: int, api_hash: str, session_string: str):
    log.info(f"[worker] starting user task {user_id}")
    async with Client(name=f"user-{user_id}", api_id=api_id, api_hash=api_hash, session_string=session_string, workdir=":memory:") as app:
        me = await app.get_me(); log.info(f"[worker] logged in as @{getattr(me, 'username', 'user')} for {user_id}")
        while True:
            ad_text = get_ad(user_id)
            groups = normalize_groups(list_groups(user_id))
            interval = int(get_interval(user_id) or 30)
            if interval not in VALID_INTERVALS: interval = 30

            if not ad_text or not groups:
                await asyncio.sleep(10)
                continue

            ok, fail = await _send_round(user_id, app, ad_text, groups)
            inc_sent(user_id, ok, fail)
            update_last_sent(user_id, now_ist_iso())
            log.info(f"[worker] user {user_id} round complete: ok={ok} fail={fail} next in {interval}m")
            await asyncio.sleep(interval * 60)

async def loop_worker():
    init_db()
    try:
        while True:
            sessions = {row["user_id"]: row for row in list_active_sessions()}
            # spawn tasks for active sessions
            for uid, row in sessions.items():
                if uid not in FORWARD_TASKS:
                    FORWARD_TASKS[uid] = asyncio.create_task(
                        _user_task(uid, row["api_id"], row["api_hash"], row["session_string"])
                    )
                    log.info(f"[worker] spawned task for user {uid}")
            # stop tasks whose sessions disappeared
            for uid in list(FORWARD_TASKS.keys()):
                if uid not in sessions:
                    t = FORWARD_TASKS.pop(uid)
                    t.cancel()
                    log.info(f"[worker] stopped task for {uid}")
            await asyncio.sleep(10)
    except asyncio.CancelledError:
        pass
    finally:
        for t in FORWARD_TASKS.values(): t.cancel()
        await asyncio.gather(*FORWARD_TASKS.values(), return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(loop_worker())
