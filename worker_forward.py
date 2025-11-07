# worker_forward.py — robust forwarder with auto-join, target normalization, and AUTO-REHYDRATE
# • Auto-joins @usernames / invite links before sending
# • Normalizes https://t.me/... / tg://join?invite=...
# • Skips channels (requires admin) with clear logs
# • Per-user interval 30/45/60 (default 30)
# • Round-robin sessions (<=3)
# • Night Mode (00:00–07:00 IST)
# • Verbose logs
# • AUTO-REHYDRATE: detect expired/unauthorized sessions and DM user to re-login via @SpinifyLoginBot

import os
import asyncio
import logging
import re
import time as _time
from urllib.parse import urlparse, parse_qs
from datetime import datetime, time
from zoneinfo import ZoneInfo

from aiogram import Bot  # used only for notifying users to re-login

from pyrogram import Client
from pyrogram.errors import (
    FloodWait, RPCError, ChannelPrivate, UsernameInvalid, UsernameNotOccupied,
    Unauthorized, AuthKeyUnregistered
)
# Some installs may not expose these explicitly; we’ll guard with isinstance/str
try:
    from pyrogram.errors import SessionRevoked, SessionExpired, UserDeactivated, UserDeactivatedBan
except Exception:
    SessionRevoked = SessionExpired = UserDeactivated = UserDeactivatedBan = tuple()

from pyrogram.types import Chat

from core.db import (
    init_db,
    users_with_sessions, sessions_strings,
    list_groups, get_ad, get_interval, get_last_sent_at, mark_sent_now,
    night_enabled, set_setting, get_setting, inc_sent_ok
)

__all__ = ["main", "main_loop"]

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("worker")

IST = ZoneInfo("Asia/Kolkata")
NIGHT_START = time(0, 0)   # 00:00
NIGHT_END   = time(7, 0)   # 07:00

USERNAME_RE = re.compile(r"^@?([A-Za-z0-9_]{5,})$")

# --- Bot notifier for auto-rehydrate ---
BOT_TOKEN = (os.getenv("MAIN_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
BOT_NOTIFIER: Bot | None = Bot(BOT_TOKEN) if BOT_TOKEN and ":" in BOT_TOKEN else None
AUTH_PING_COOLDOWN_SEC = 6 * 3600  # 6 hours

def is_night_now_ist() -> bool:
    now = datetime.now(IST).time()
    return NIGHT_START <= now < NIGHT_END

def _parse_mode_string(s: str | None):
    if not s: return None
    s = s.strip().lower()
    if s in ("markdown","md"): return "markdown"
    if s in ("html","htm"): return "html"
    return None

def _next_slot_index(user_id: int, total_slots: int) -> int:
    key = f"worker:last_session:{user_id}"
    cur = int(get_setting(key, -1) or -1)
    nxt = (cur + 1) % max(1, total_slots)
    set_setting(key, nxt)
    return nxt

async def _notify_rehydrate(user_id: int, slot: int, reason: str):
    """DM user to re-login via @SpinifyLoginBot with cooldown."""
    if BOT_NOTIFIER is None:
        log.info(f"[rehydrate u{user_id} s{slot}] notifier disabled (no MAIN_BOT_TOKEN).")
        return
    key = f"authping:{user_id}:{slot}"
    last = int(get_setting(key, 0) or 0)
    now = int(_time.time())
    if now - last < AUTH_PING_COOLDOWN_SEC:
        return
    set_setting(key, now)
    msg = (
        "✇ Session issue detected\n"
        f"✇ Your account (slot {slot}) looks <b>expired or unauthorized</b>.\n"
        "✇ Please log in again via <b>@SpinifyLoginBot</b>.\n"
        f"✇ Reason: <code>{reason}</code>"
    )
    try:
        await BOT_NOTIFIER.send_message(user_id, msg)
        log.info(f"[rehydrate u{user_id} s{slot}] notified user")
    except Exception as e:
        log.info(f"[rehydrate u{user_id} s{slot}] notify failed: {e}")

def normalize_target(raw: str) -> tuple[str, str]:
    """
    Return (kind, value)
    kind: 'username' | 'invite' | 'id'
    value:
      - username without @ (e.g., 'MyGroup')
      - invite link string (e.g., 'https://t.me/+abcdEF...')
      - numeric id string for -100 chats (if provided)
    """
    s = (raw or "").strip()
    if not s:
        return ("", "")

    # Numeric id?
    if s.lstrip("-").isdigit():
        return ("id", s)

    # Username like @name
    m = USERNAME_RE.match(s.lstrip("@"))
    if m and not s.startswith("https://") and not s.startswith("tg://"):
        return ("username", m.group(1))

    # Telegram links
    if s.startswith("https://") or s.startswith("http://"):
        u = urlparse(s)
        if u.netloc.lower() == "t.me":
            path = u.path.strip("/")
            if path.startswith("+") or path.startswith("joinchat"):
                return ("invite", s)
            seg = path.split("/")[0]
            if USERNAME_RE.match(seg):
                return ("username", seg)
            return ("invite", s)  # fallback
        return ("invite", s)

    # tg://join?invite=...
    if s.startswith("tg://"):
        u = urlparse(s)
        q = parse_qs(u.query)
        inv = q.get("invite", [None])[0]
        if inv:
            return ("invite", f"https://t.me/+{inv}")

    # Last resort: try as username
    return ("username", s.lstrip("@"))

async def ensure_joined(app: Client, kind: str, value: str) -> Chat | None:
    """Ensure the session is in the target chat. Returns Chat or None."""
    try:
        if kind == "id":
            chat = await app.get_chat(int(value))
            return chat
        if kind == "username":
            uname = value
            chat = None
            try:
                chat = await app.get_chat(uname)
            except (UsernameInvalid, UsernameNotOccupied):
                chat = None
            try:
                await app.join_chat(uname)
            except Exception:
                pass  # already joined or not required
            if chat is None:
                chat = await app.get_chat(uname)
            return chat
        if kind == "invite":
            try:
                chat = await app.join_chat(value)
            except Exception as e:
                log.info(f"[join] invite join failed: {e} — trying resolve")
                try:
                    k2, v2 = normalize_target(value.replace("joinchat/", "").replace("+", ""))
                    if k2 == "username":
                        try: await app.join_chat(v2)
                        except Exception: pass
                        chat = await app.get_chat(v2)
                        return chat
                except Exception:
                    pass
                return None
            return chat
    except ChannelPrivate:
        log.info("[join] channel is private (need admin/invite)")
        return None
    except Unauthorized as e:
        # session invalid during join step
        raise e
    except Exception as e:
        log.info(f"[join] unexpected: {e}")
        return None

async def _send_via_session(sess: dict, raw_targets: list[str], text: str, parse_mode: str | None) -> int:
    ok = 0
    app = Client(
        name=f"user-{sess['user_id']}-s{sess['slot']}",
        api_id=int(sess["api_id"]),
        api_hash=str(sess["api_hash"]),
        session_string=str(sess["session_string"])
    )
    try:
        await app.start()
    except (Unauthorized, AuthKeyUnregistered, SessionRevoked, SessionExpired, UserDeactivated, UserDeactivatedBan) as e:
        # auto-rehydrate notify
        await _notify_rehydrate(sess["user_id"], sess["slot"], e.__class__.__name__)
        log.error(f"[u{sess['user_id']} s{sess['slot']}] start auth error: {e}")
        return 0
    except Exception as e:
        log.error(f"[u{sess['user_id']} s{sess['slot']}] start failed: {e}")
        # unknown error; don't notify
        return 0

    for raw in raw_targets:
        kind, val = normalize_target(raw)
        if not val:
            log.info(f"[u{sess['user_id']}] skip empty target")
            continue

        try:
            chat = await ensure_joined(app, kind, val)
        except (Unauthorized, AuthKeyUnregistered, SessionRevoked, SessionExpired, UserDeactivated, UserDeactivatedBan) as e:
            await _notify_rehydrate(sess["user_id"], sess["slot"], e.__class__.__name__)
            log.error(f"[u{sess['user_id']} s{sess['slot']}] auth error while joining: {e}")
            chat = None

        if chat is None:
            log.info(f"[u{sess['user_id']}] could not join/resolve: {raw}")
            continue

        # Skip channels unless admin (users can't post to channels)
        if getattr(chat, "type", "") == "channel":
            log.info(f"[u{sess['user_id']}] skipping channel (need admin): {getattr(chat,'username',None) or chat.id}")
            continue

        target_id = chat.id
        try:
            await app.send_message(chat_id=target_id, text=text, parse_mode=parse_mode)
            ok += 1
            log.info(f"[u{sess['user_id']} s{sess['slot']}] sent to {getattr(chat,'username',None) or chat.id}")
            await asyncio.sleep(0.5)
        except FloodWait as fw:
            log.warning(f"[u{sess['user_id']} s{sess['slot']}] FloodWait {fw.value}s on {chat.id}")
            await asyncio.sleep(fw.value + 1)
        except (Unauthorized, AuthKeyUnregistered, SessionRevoked, SessionExpired, UserDeactivated, UserDeactivatedBan) as e:
            await _notify_rehydrate(sess["user_id"], sess["slot"], e.__class__.__name__)
            log.error(f"[u{sess['user_id']} s{sess['slot']}] auth error on send: {e}")
        except RPCError as e:
            log.warning(f"[u{sess['user_id']} s{sess['slot']}] RPCError on {chat.id}: {e}")
        except Exception as e:
            log.warning(f"[u{sess['user_id']} s{sess['slot']}] send failed on {chat.id}: {e}")

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
        log.info(f"[u{user_id}] no ad text set")
        return
    groups = list_groups(user_id)
    if not groups:
        log.info(f"[u{user_id}] no groups configured")
        return
    sessions = sessions_strings(user_id)
    if not sessions:
        log.info(f"[u{user_id}] no sessions")
        return

    interval = get_interval(user_id) or 30
    last_ts = get_last_sent_at(user_id)
    now = int(datetime.utcnow().timestamp())
    if last_ts is not None and now - last_ts < interval * 60:
        remain = interval*60 - (now - last_ts)
        log.info(f"[u{user_id}] not due yet ({remain}s left)")
        return

    idx = _next_slot_index(user_id, len(sessions))
    sess = sessions[idx]

    sent = await _send_via_session(sess, groups, text, _parse_mode_string(mode))
    if sent > 0:
        mark_sent_now(user_id)
        inc_sent_ok(user_id, sent)
        log.info(f"[u{user_id}] sent_ok+={sent}")
    else:
        log.info(f"[u{user_id}] nothing sent this tick")

async def main_loop():
    init_db()
    while True:
        try:
            for uid in users_with_sessions():
                try:
                    await process_user(uid)
                except Exception as e:
                    log.error(f"[u{uid}] process error: {e}")
                await asyncio.sleep(0.2)
        except Exception as e:
            log.error(f"loop error: {e}")
        await asyncio.sleep(15)

async def main():
    await main_loop()

if __name__ == "__main__":
    asyncio.run(main())
