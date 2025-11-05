# worker_forward.py — Spinify Forwarder
# - Per-user interval (30/45/60)
# - Multi-sessions (up to 3)
# - Groups cap (5) respected (db)
# - Global Night Mode 00:00–07:00 IST
# - Stats bump + resilient sending with FloodWait handling
# - Minimal deps: stdlib + pyrogram

from __future__ import annotations
import asyncio
import random
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo

from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError, SessionExpired, AuthKeyUnregistered, Unauthorized

from core.db import (
    get_conn, list_groups, get_ad, get_interval,
    sessions_strings, night_enabled, bump_sent
)

IST = ZoneInfo("Asia/Kolkata")
ALLOWED_INTERVALS = {30, 45, 60}
WORKER_POLL_SEC = 5.0                 # main loop tick
MSG_GAP_SEC_MIN, MSG_GAP_SEC_MAX = 0.6, 1.2   # between group posts

# ---------------- time helpers ----------------
def now_ist() -> datetime:
    return datetime.now(IST)

def in_night_window(dt: datetime) -> bool:
    t = dt.timetz()
    return time(0, 0, tzinfo=IST) <= t < time(7, 0, tzinfo=IST)

# ---------------- schedule helpers (settings table) ----------------
def _get_next(uid: int) -> datetime | None:
    try:
        conn = get_conn()
        row = conn.execute("SELECT val FROM settings WHERE key=?", (f"next:{uid}",)).fetchone()
        conn.close()
        if not row or not row["val"]:
            return None
        return datetime.fromisoformat(row["val"])
    except Exception:
        return None

def _set_next(uid: int, dt: datetime) -> None:
    try:
        conn = get_conn()
        iso = dt.isoformat()
        conn.execute(
            "INSERT INTO settings(key,val) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET val=excluded.val",
            (f"next:{uid}", iso),
        )
        conn.commit(); conn.close()
    except Exception:
        pass

# ---------------- send engine ----------------
async def _send_with_session(api_id: int, api_hash: str, session_string: str, groups: list[str], text: str) -> int:
    """Send `text` to all `groups` using a single user session. Returns number of successful sends."""
    sent = 0
    app = Client(
        name=f"fwd-{id(session_string)}",
        api_id=api_id, api_hash=api_hash,
        session_string=session_string,
        in_memory=True,
        # NOTE: you can set no_updates=True, but connect() is enough for simple sends
    )
    try:
        await app.connect()
    except (SessionExpired, AuthKeyUnregistered, Unauthorized, RPCError):
        return 0
    except Exception:
        return 0

    try:
        for g in groups:
            try:
                await app.send_message(g, text)
                sent += 1
            except FloodWait as fw:
                await asyncio.sleep(int(getattr(fw, "value", 5)) + 1)
            except RPCError:
                # skip bad group, privacy, or link
                pass
            except Exception:
                pass
            # jitter between sends
            await asyncio.sleep(random.uniform(MSG_GAP_SEC_MIN, MSG_GAP_SEC_MAX))
    finally:
        try:
            await app.disconnect()
        except Exception:
            pass
    return sent

# ---------------- main loop ----------------
_inflight_users: dict[int, asyncio.Task] = {}

async def _process_user(uid: int):
    """Runs one full send cycle for a user (fan-out on all sessions)."""
    sessions = sessions_strings(uid)
    if not sessions:
        return

    groups = list_groups(uid)
    if not groups:
        return

    ad = get_ad(uid)
    if not ad:
        return

    # enforce allowed intervals
    interval = get_interval(uid) or 30
    if interval not in ALLOWED_INTERVALS:
        interval = 30

    # fan out over sessions concurrently
    tasks = []
    for s in sessions:
        tasks.append(_send_with_session(
            api_id=s["api_id"], api_hash=s["api_hash"],
            session_string=s["session_string"], groups=groups, text=ad
        ))
    results = await asyncio.gather(*tasks, return_exceptions=True)
    total_sent = 0
    for r in results:
        if isinstance(r, Exception):
            continue
        total_sent += int(r or 0)

    # bump stats + schedule next regardless (even if 0 sent, to avoid tight loops)
    now = now_ist()
    if total_sent > 0:
        bump_sent(uid, inc=total_sent, last_at_iso=now.isoformat())
    _set_next(uid, now + timedelta(minutes=interval))

async def loop_worker(poll_every: float = WORKER_POLL_SEC):
    # small random stagger on boot
    await asyncio.sleep(random.uniform(0.5, 1.5))
    while True:
        try:
            now = now_ist()
            if night_enabled() and in_night_window(now):
                # sleep a minute while night mode is active
                await asyncio.sleep(60)
                continue

            # iterate known users
            conn = get_conn()
            uids = [r["user_id"] for r in conn.execute("SELECT user_id FROM users").fetchall()]
            conn.close()

            for uid in uids:
                # debounce if task already running
                t = _inflight_users.get(uid)
                if t and not t.done():
                    continue

                nxt = _get_next(uid)
                if nxt and now < nxt:
                    continue

                # schedule this user
                _inflight_users[uid] = asyncio.create_task(_process_user(uid))

        except Exception:
            # keep alive on any unexpected error
            await asyncio.sleep(2)

        await asyncio.sleep(poll_every)

async def main():
    await loop_worker()

__all__ = ["main", "loop_worker"]
if __name__ == "__main__":
    asyncio.run(main())
    
