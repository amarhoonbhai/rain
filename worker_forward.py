# worker_forward.py — Saved-All (no pinned), clean scheduler
# - Sends the next NEW message from Saved Messages (“me”) each interval
# - Default interval = 30 minutes (auto-applied if unset)
# - Scan loop every 15s
# - 10s delay between groups
# - Accepts any tokens (username / id / t.me link); NO auto-join
#   • t.me invite links are saved but skipped; user gets a DM reminder (cooldown)
# - Auto-rehydrate DM for expired/revoked sessions (cooldown)
# - Honors Night Mode (00:00–07:00 IST) via db.night_enabled()

import os, asyncio, logging, re, time as _time
from urllib.parse import urlparse
from datetime import datetime, time
from zoneinfo import ZoneInfo

from pyrogram import Client
from pyrogram.errors import (
    FloodWait, RPCError, UsernameInvalid, UsernameNotOccupied,
    Unauthorized, AuthKeyUnregistered,
)
try:
    from pyrogram.errors import SessionRevoked, SessionExpired, UserDeactivated, UserDeactivatedBan, UserNotParticipant
except Exception:
    class _E(Exception): pass
    SessionRevoked = SessionExpired = UserDeactivated = UserDeactivatedBan = UserNotParticipant = _E

from aiogram import Bot as AioBot

from core.db import (
    init_db,
    users_with_sessions, sessions_strings,
    list_groups, get_interval, night_enabled,
    mark_sent_now, inc_sent_ok,
    get_setting, set_setting, set_interval,
)

# ---------- logging ----------
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("worker")

# ---------- const ----------
IST = ZoneInfo("Asia/Kolkata")
NIGHT_START = time(0, 0)
NIGHT_END   = time(7, 0)

SEND_BETWEEN_GROUPS_SEC = 10
SCAN_PERIOD_SEC = 15
HISTORY_FETCH_LIMIT = 200
DEFAULT_INTERVAL_MIN = 30

# Main bot for DMs (rehydrate + join-manual nudges)
BOT_TOKEN = (os.getenv("MAIN_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
BOT_NOTIFIER = AioBot(BOT_TOKEN) if BOT_TOKEN and ":" in BOT_TOKEN else None

AUTH_PING_ENABLED = str(os.getenv("AUTH_PING_ENABLED", "1")).strip().lower() in ("1","true","yes")
AUTH_PING_COOLDOWN_SEC = int(os.getenv("AUTH_PING_COOLDOWN_SEC", "21600"))  # 6h

JOIN_NUDGE_COOLDOWN_SEC = 6 * 3600  # DM “join manually” at most every 6h per (user,token)

NO_AUTO_JOIN = True  # never join on our own

USERNAME_RE = re.compile(r"^@?([A-Za-z0-9_]{5,})$")
SPLIT_RE = re.compile(r"[,\s]+")

# ---------- helpers: targets ----------
def expand_tokens(raw_list: list[str]) -> list[str]:
    out, seen = [], set()
    for entry in raw_list or []:
        if not entry: continue
        for tok in SPLIT_RE.split(entry.strip()):
            t = tok.strip().rstrip("/.,")
            if not t: continue
            if t not in seen:
                seen.add(t); out.append(t)
    return out

def extract_username_from_link(s: str) -> str | None:
    if not s.startswith("http"): return None
    u = urlparse(s)
    if u.netloc.lower() != "t.me": return None
    path = u.path.strip("/")
    if not path or path.startswith("+") or path.startswith("joinchat"):
        return None  # that’s an invite link; handled elsewhere
    uname = path.split("/")[0]
    return uname if USERNAME_RE.match(uname) else None

def normalize_tokens(tokens: list[str]) -> list[str]:
    norm, seen = [], set()
    for t in tokens:
        if not t: continue
        if t.lstrip("-").isdigit():
            x = t  # numeric id
        else:
            m = USERNAME_RE.match(t.lstrip("@"))
            if m: x = m.group(1)   # username
            else:
                u = extract_username_from_link(t)
                x = u if u else t  # keep invite links as-is (for reminder)
        if x not in seen:
            seen.add(x); norm.append(x)
    return norm

def is_invite_link(token: str) -> bool:
    if not token.startswith("http"): return False
    u = urlparse(token)
    if u.netloc.lower() != "t.me": return False
    path = u.path.strip("/")
    return bool(path.startswith("+") or path.startswith("joinchat"))

# ---------- helpers: time / state ----------
def is_night_now_ist() -> bool:
    now = datetime.now(IST).time()
    return NIGHT_START <= now < NIGHT_END

def _choose_session_index(user_id: int, total: int) -> int:
    key = f"worker:last_session:{user_id}"
    cur = int(get_setting(key, -1) or -1)
    nxt = (cur + 1) % max(1, total)
    set_setting(key, nxt)
    return nxt

# ---------- DM notifiers ----------
async def _notify_rehydrate(user_id: int, slot: int, reason: str):
    if not AUTH_PING_ENABLED or BOT_NOTIFIER is None:
        return
    key = f"authping:{user_id}:{slot}"
    last = int(get_setting(key, 0) or 0)
    now = int(_time.time())
    if now - last < AUTH_PING_COOLDOWN_SEC:
        return
    set_setting(key, now)
    txt = (
        "✇ Session issue detected\n"
        f"✇ Your account (slot {slot}) looks <b>expired / unauthorized</b>.\n"
        "✇ Please log in again via <b>@SpinifyLoginBot</b>.\n"
        f"✇ Reason: <code>{reason}</code>\n"
        "✇ This check runs every ≤15s."
    )
    try:
        await BOT_NOTIFIER.send_message(user_id, txt)
    except Exception:
        pass

async def _nudge_join_manually(user_id: int, token: str):
    if BOT_NOTIFIER is None:  # optional
        return
    key = f"join-nudge:{user_id}:{token}"
    last = int(get_setting(key, 0) or 0)
    now = int(_time.time())
    if now - last < JOIN_NUDGE_COOLDOWN_SEC:
        return
    set_setting(key, now)
    txt = (
        "✇ Manual join required\n"
        "✇ You added a private invite link as a group target.\n"
        "✇ I cannot auto-join; please join it manually with your account, then keep the link saved here.\n"
        f"✇ Link: {token}"
    )
    try:
        await BOT_NOTIFIER.send_message(user_id, txt)
    except Exception:
        pass

# ---------- resolving ----------
async def resolve_chat(app: Client, token: str):
    if token.lstrip("-").isdigit():
        try: return await app.get_chat(int(token))
        except Exception as e:
            log.info(f"[resolve] id {token} failed: {e}"); return None
    m = USERNAME_RE.match(token.lstrip("@"))
    if m:
        uname = m.group(1)
        try: return await app.get_chat(uname)
        except (UsernameInvalid, UsernameNotOccupied):
            log.info(f"[resolve] @{uname} invalid/not occupied"); return None
        except Exception as e:
            log.info(f"[resolve] @{uname} failed: {e}"); return None
    if token.startswith("http"):
        u = extract_username_from_link(token)
        if u:
            try: return await app.get_chat(u)
            except Exception as e:
                log.info(f"[resolve] link→@{u} failed: {e}"); return None
        # invite link: keep for reminder, but we can’t resolve
        return None
    return None

# ---------- Saved-All cursor ----------
def _cursor_key(user_id: int, slot: int) -> str:
    return f"sall:last:{user_id}:{slot}"

async def _get_next_saved_message(app: Client, user_id: int, slot: int):
    last_id = int(get_setting(_cursor_key(user_id, slot), 0) or 0)
    msgs = await app.get_history("me", limit=HISTORY_FETCH_LIMIT)
    if not msgs: return None, last_id
    candidates = [m for m in msgs if getattr(m, "id", 0) and m.id > last_id]
    if not candidates: return None, last_id
    next_msg = min(candidates, key=lambda m: m.id)
    return next_msg, last_id

def _save_cursor(user_id: int, slot: int, msg_id: int):
    set_setting(_cursor_key(user_id, slot), int(msg_id))

# ---------- sending ----------
async def _send_saved_to_targets(app: Client, msg, targets: list[str], user_id: int, slot: int) -> int:
    ok = 0
    for token in targets:
        if is_invite_link(token):
            log.info(f"[u{user_id}] invite link saved; skipping send, nudging user: {token}")
            await _nudge_join_manually(user_id, token)
            continue

        chat = await resolve_chat(app, token)
        if chat is None:
            log.info(f"[u{user_id}] unresolved/unsupported token: {token}")
            continue

        try:
            await app.copy_message(chat_id=chat.id, from_chat_id="me", message_id=msg.id)
            ok += 1
            log.info(f"[u{user_id} s{slot}] sent to {getattr(chat,'username',None) or chat.id} (msg {msg.id})")
            await asyncio.sleep(SEND_BETWEEN_GROUPS_SEC)
        except FloodWait as fw:
            log.warning(f"[u{user_id} s{slot}] FloodWait {fw.value}s on {chat.id}")
            await asyncio.sleep(fw.value + 1)
        except UserNotParticipant:
            log.info(f"[u{user_id}] not a participant in {getattr(chat,'username',None) or chat.id} (NO-JOIN)")
        except (Unauthorized, AuthKeyUnregistered, SessionRevoked, SessionExpired) as e:
            await _notify_rehydrate(user_id, slot, e.__class__.__name__)
            log.error(f"[u{user_id} s{slot}] auth error on send: {e}")
        except RPCError as e:
            log.warning(f"[u{user_id} s{slot}] RPCError on {chat.id}: {e}")
        except Exception as e:
            log.warning(f"[u{user_id} s{slot}] send failed on {chat.id}: {e}")
    return ok

# ---------- per-user cycle ----------
async def process_user(user_id: int):
    # Night / pause
    if night_enabled() and is_night_now_ist():
        return
    if str(get_setting(f"paused:{user_id}", "0")).lower() in ("1","true","yes"):
        return

    # groups
    groups_raw = list_groups(user_id)
    targets = normalize_tokens(expand_tokens(groups_raw))
    if not targets:
        log.info(f"[u{user_id}] no groups configured"); return

    # interval
    minutes = get_interval(user_id)
    if not minutes or int(minutes) < 1:
        minutes = DEFAULT_INTERVAL_MIN
        set_interval(user_id, minutes)
        log.info(f"[u{user_id}] interval defaulted to {minutes}m")
    minutes = int(minutes)

    # schedule gate
    last_ts = int(get_setting(f"last_sent_at:{user_id}", 0) or 0)
    now = int(datetime.utcnow().timestamp())
    if last_ts and (now - last_ts) < minutes * 60:
        remain = minutes * 60 - (now - last_ts)
        log.info(f"[u{user_id}] not due yet ({remain}s left)")
        return

    # session
    sessions = sessions_strings(user_id)
    if not sessions:
        log.info(f"[u{user_id}] no sessions"); return
    idx = _choose_session_index(user_id, len(sessions))
    sess = sessions[idx]; slot = int(sess["slot"])

    app = Client(
        name=f"user-{user_id}-s{slot}",
        api_id=int(sess["api_id"]), api_hash=str(sess["api_hash"]),
        session_string=str(sess["session_string"])
    )
    try:
        await app.start()
    except (Unauthorized, AuthKeyUnregistered, SessionRevoked, SessionExpired) as e:
        await _notify_rehydrate(user_id, slot, e.__class__.__name__)
        log.error(f"[u{user_id} s{slot}] start auth error: {e}")
        return
    except Exception as e:
        log.error(f"[u{user_id} s{slot}] start failed: {e}")
        return

    try:
        next_msg, _ = await _get_next_saved_message(app, user_id, slot)
        if not next_msg:
            log.info(f"[u{user_id} s{slot}] no new Saved Messages to send")
            return

        sent = await _send_saved_to_targets(app, next_msg, targets, user_id, slot)
        if sent > 0:
            _save_cursor(user_id, slot, next_msg.id)
            set_setting(f"last_sent_at:{user_id}", int(datetime.utcnow().timestamp()))
            mark_sent_now(user_id); inc_sent_ok(user_id, sent)
            log.info(f"[u{user_id}] sent_ok += {sent}")
        else:
            log.info(f"[u{user_id}] nothing sent this tick")
    finally:
        try: await app.stop()
        except Exception: pass

# ---------- loop ----------
async def main_loop():
    init_db()
    log.info("[worker] started (Saved-All mode, 30m default, 15s scan)")
    while True:
        try:
            for uid in users_with_sessions():
                try:
                    await process_user(uid)
                except Exception as e:
                    log.error(f"[u{uid}] process error: {e}")
                await asyncio.sleep(0.2)
        except Exception as e:
            log.error(f"[loop] error: {e}")
        await asyncio.sleep(SCAN_PERIOD_SEC)

async def main():
    await main_loop()

if __name__ == "__main__":
    asyncio.run(main())
