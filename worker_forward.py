# worker_forward.py — Pinned-Saved forwarder (NO-JOIN)
# Sends the PINNED message from Saved Messages of each session to saved groups.
# Respects Pause, Night Mode, and user intervals. No per-group cooldown.

import os, asyncio, logging, re, time as _time
from urllib.parse import urlparse
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from aiogram import Bot  # only for optional DM notifications

from pyrogram import Client
from pyrogram.errors import (
    FloodWait, RPCError, UsernameInvalid, UsernameNotOccupied,
    Unauthorized, AuthKeyUnregistered, UserDeactivated, UserDeactivatedBan
)
try:
    from pyrogram.errors import SessionRevoked, SessionExpired, UserNotParticipant
except Exception:
    class _Dummy(Exception): pass
    SessionRevoked = SessionExpired = UserNotParticipant = _Dummy  # best-effort

from core.db import (
    init_db,
    users_with_sessions, sessions_strings,
    list_groups,
    get_interval, get_last_sent_at, mark_sent_now,
    night_enabled, set_setting, get_setting, inc_sent_ok,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"))
log = logging.getLogger("worker")

IST = ZoneInfo("Asia/Kolkata")
NIGHT_START = time(0, 0)
NIGHT_END   = time(7, 0)

# --- Bot notifier for auth / session issues (optional) ---
BOT_TOKEN = (os.getenv("MAIN_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
BOT_NOTIFIER = Bot(BOT_TOKEN) if BOT_TOKEN and ":" in BOT_TOKEN else None
AUTH_PING_COOLDOWN_SEC = 6 * 3600  # 6 hours

# --- parsing/normalizing targets ---
SPLIT_RE = re.compile(r"[,\s]+")
USERNAME_RE = re.compile(r"^@?([A-Za-z0-9_]{5,})$")

def expand_targets(raw_targets: list[str]) -> list[str]:
    out, seen = [], set()
    for entry in raw_targets or []:
        if not entry: continue
        for tok in SPLIT_RE.split(entry.strip()):
            t = tok.strip().rstrip("/.,")
            if not t: continue
            if t not in seen:
                seen.add(t); out.append(t)
    return out

def extract_username_from_link(s: str) -> str | None:
    # Only convert t.me/username -> username ; do NOT touch t.me/+invite
    if not s.startswith("http"): return None
    u = urlparse(s)
    if u.netloc.lower() != "t.me": return None
    path = u.path.strip("/")
    if not path or path.startswith("+") or path.startswith("joinchat"):
        return None
    uname = path.split("/")[0]
    return uname if USERNAME_RE.match(uname) else None

def normalize_tokens(tokens: list[str]) -> list[str]:
    norm = []
    for t in tokens:
        if t.lstrip("-").isdigit():            # numeric id
            norm.append(t); continue
        m = USERNAME_RE.match(t.lstrip("@"))   # @username
        if m: norm.append(m.group(1)); continue
        u = extract_username_from_link(t)      # t.me/username
        if u: norm.append(u); continue
        # invite links or unknown → keep as-is (we'll log reminder)
        norm.append(t)
    # de-dup preserve order
    seen, out = set(), []
    for x in norm:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

def _parse_mode_string(s: str | None):
    if not s: return None
    s = s.strip().lower()
    if s in ("markdown","md"): return "markdown"
    if s in ("html","htm"): return "html"
    return None

def is_night_now_ist() -> bool:
    now = datetime.now(IST).time()
    return NIGHT_START <= now < NIGHT_END

def _is_paused(uid: int) -> bool:
    v = str(get_setting(f"user:{uid}:paused", "0")).lower()
    return v in ("1","true","yes","on")

def _next_slot_index(user_id: int, total_slots: int) -> int:
    key = f"worker:last_session:{user_id}"
    cur = int(get_setting(key, -1) or -1)
    nxt = (cur + 1) % max(1, total_slots)
    set_setting(key, nxt)
    return nxt

async def _notify_rehydrate(user_id: int, slot: int, reason: str):
    if BOT_NOTIFIER is None: return
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
    except Exception:
        pass

async def resolve_target_chat(app: Client, target: str):
    """
    NO-JOIN: only resolve; never join.
    - username -> get_chat('username')
    - numeric id -> get_chat(int)
    - public link t.me/username -> same as username
    - private invite links: return None (user must join manually); we log a reminder
    """
    # numeric id
    if target.lstrip("-").isdigit():
        try:
            return await app.get_chat(int(target))
        except Exception as e:
            log.info(f"[resolve] id {target} failed: {e}")
            return None
    # pure username
    m = USERNAME_RE.match(target.lstrip("@"))
    if m:
        uname = m.group(1)
        try:
            return await app.get_chat(uname)
        except (UsernameInvalid, UsernameNotOccupied):
            log.info(f"[resolve] @{uname} invalid/not occupied")
            return None
        except Exception as e:
            log.info(f"[resolve] @{uname} failed: {e}")
            return None
    # links
    if target.startswith("http"):
        u = extract_username_from_link(target)
        if u:
            try:
                return await app.get_chat(u)
            except Exception as e:
                log.info(f"[resolve] link→@{u} failed: {e}")
                return None
        # invite link: skip in NO-JOIN mode
        log.info(f"[resolve] private invite link saved; requires manual join: {target}")
        return None
    # unknown
    return None

async def _copy_pinned_from_saved(app: Client, to_chat_id: int) -> bool:
    """
    Copy the pinned message from 'Saved Messages' (me) to target chat.
    Works for text or media+caption (premium emoji OK).
    """
    try:
        me_chat = await app.get_chat("me")
        pin = getattr(me_chat, "pinned_message", None)
        if not pin:
            log.info("[pinned] no pinned message in Saved Messages")
            return False
        await app.copy_message(chat_id=to_chat_id, from_chat_id="me", message_id=pin.id)
        return True
    except FloodWait as fw:
        log.warning(f"[pinned] FloodWait {fw.value}s on copy → sleep")
        await asyncio.sleep(fw.value + 1)
    except RPCError as e:
        log.warning(f"[pinned] RPCError on copy: {e}")
    except Exception as e:
        log.warning(f"[pinned] copy failed: {e}")
    return False

async def _send_via_session(sess: dict, targets: list[str]) -> int:
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
        await _notify_rehydrate(sess["user_id"], sess["slot"], e.__class__.__name__)
        log.error(f"[u{sess['user_id']} s{sess['slot']}] start auth error: {e}")
        return 0
    except Exception as e:
        log.error(f"[u{sess['user_id']} s{sess['slot']}] start failed: {e}")
        return 0

    for tgt in targets:
        chat = await resolve_target_chat(app, tgt)
        if chat is None:
            # likely private invite or bad token; we log and move on
            log.info(f"[u{sess['user_id']}] unresolved/needs manual join: {tgt}")
            continue
        try:
            sent = await _copy_pinned_from_saved(app, chat.id)
            if sent:
                ok += 1
                log.info(f"[u{sess['user_id']} s{sess['slot']}] sent to {getattr(chat,'username',None) or chat.id}")
                await asyncio.sleep(0.5)
            else:
                log.info(f"[u{sess['user_id']} s{sess['slot']}] no pinned content to send")
        except FloodWait as fw:
            log.warning(f"[u{sess['user_id']} s{sess['slot']}] FloodWait {fw.value}s on {chat.id}")
            await asyncio.sleep(fw.value + 1)
        except UserNotParticipant:
            log.info(f"[u{sess['user_id']}] not a participant in {getattr(chat,'username',None) or chat.id} (private invite)")
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
    # honor Pause
    if _is_paused(user_id):
        return
    # honor Night Mode
    if night_enabled() and is_night_now_ist():
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

    # expand + normalize once per tick
    targets = normalize_tokens(expand_targets(groups))
    if not targets:
        log.info(f"[u{user_id}] no valid targets after expansion")
        return

    sent = await _send_via_session(sess, targets)
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
