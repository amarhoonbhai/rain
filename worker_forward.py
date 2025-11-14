# worker_forward.py
# ─────────────────────────────────────────────────────────────────────────────
# Spinify Forward Worker (multi-user, multi-session)
# • Schedules per-user sends using each user's saved messages (Saved-All mode)
# • Traverses Saved Messages strictly oldest → newest (per session cursor)
# • Sends to all configured groups with 30s gap between groups
# • No auto-join; accepts usernames, numeric IDs, and links (invite links saved but skipped)
# • Session command handlers (from the logged-in account itself):
#     .help            → show commands
#     .listgc          → list saved groups
#     .cleargc         → clear all groups
#     .addgc <lines>   → add up to cap groups (5 default, 10 if unlocked)
#     .time 30m|45m|60m→ set send interval (minutes)
#     .fstats          → show interval, groups, next-send ETA
# • Auto-rehydrate DM (optional): notifies user via main bot if session is revoked/expired
# • Tick frequency: ~15s; default interval: 30m
# • Compatible with multiple db.py variants via safe wrappers (prefers core.db APIs)
# ─────────────────────────────────────────────────────────────────────────────

import os
import re
import asyncio
import logging
from typing import Dict, Tuple, Optional, List
from datetime import datetime, time as dtime, timezone

from urllib.parse import urlparse

from aiogram import Bot  # for optional DM notifications
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import (
    FloodWait, RPCError, UsernameInvalid, UsernameNotOccupied,
    Unauthorized, AuthKeyUnregistered, UserDeactivated, UserDeactivatedBan
)

# Optional imports for finer auth states
try:
    from pyrogram.errors import SessionRevoked, SessionExpired, UserNotParticipant
except Exception:
    class _Dummy(Exception): ...
    SessionRevoked = SessionExpired = UserNotParticipant = _Dummy

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("worker")

# ── DB (import as module for adaptive wrappers) ───────────────────────────────
try:
    import core.db as db
except Exception as e:
    raise RuntimeError(f"core.db import failed: {e}")

# ── Constants / Env ──────────────────────────────────────────────────────────
TICK_SEC = 15
PER_GROUP_DELAY_SEC = 30
DEFAULT_INTERVAL_MIN = 30

IST_OFFSET = 5*3600 + 1800  # +05:30 in seconds

MAIN_BOT_TOKEN = (os.getenv("MAIN_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
BOT_NOTIFIER = Bot(MAIN_BOT_TOKEN) if MAIN_BOT_TOKEN and ":" in MAIN_BOT_TOKEN else None
AUTH_PING_COOLDOWN_SEC = 6 * 3600  # 6 hours

# ── Helpers: Time / Night ────────────────────────────────────────────────────
NIGHT_START = dtime(0, 0)
NIGHT_END = dtime(7, 0)

def is_night_now_ist() -> bool:
    now_utc = datetime.utcnow()
    secs = (now_utc.timestamp() + IST_OFFSET) % 86400
    hh = int(secs // 3600)
    mm = int((secs % 3600) // 60)
    tt = dtime(hh, mm)
    return NIGHT_START <= tt < NIGHT_END

# ── Helpers: DB-safe wrappers (work across variants) ──────────────────────────
def db_get_setting(key: str, default=None):
    if hasattr(db, "get_setting"):
        try:
            return db.get_setting(key, default)
        except TypeError:
            val = db.get_setting(key)
            return default if val is None else val
    return default

def db_set_setting(key: str, val):
    if hasattr(db, "set_setting"):
        db.set_setting(key, val)

def db_users_with_sessions() -> List[int]:
    if hasattr(db, "users_with_sessions"):
        return list(db.users_with_sessions())
    # fallback: all users that have any session rows
    conn = db.get_conn()
    rows = conn.execute("SELECT DISTINCT user_id FROM sessions").fetchall()
    conn.close()
    return [r[0] if isinstance(r, tuple) else r["user_id"] for r in rows]

def db_sessions_strings(user_id: int) -> List[dict]:
    # prefer typed helper
    if hasattr(db, "sessions_strings"):
        return list(db.sessions_strings(user_id))
    if hasattr(db, "sessions_list"):
        return list(db.sessions_list(user_id))
    # fallback query
    conn = db.get_conn()
    rows = conn.execute("SELECT slot, api_id, api_hash, session_string FROM sessions WHERE user_id=? ORDER BY slot ASC",
                        (user_id,)).fetchall()
    conn.close()
    out = []
    for r in rows:
        if isinstance(r, dict):
            out.append(r)
        else:
            slot, api_id, api_hash, session_string = r
            out.append({"slot": slot, "api_id": api_id, "api_hash": api_hash, "session_string": session_string})
    return out

def db_list_groups(user_id: int) -> List[str]:
    if hasattr(db, "list_groups"):
        return list(db.list_groups(user_id))
    # fallback
    conn = db.get_conn()
    rows = conn.execute("SELECT target FROM groups WHERE user_id=? ORDER BY id ASC", (user_id,)).fetchall()
    conn.close()
    return [r[0] if isinstance(r, tuple) else r["target"] for r in rows]

def db_add_group(user_id: int, token: str) -> int:
    if hasattr(db, "add_group"):
        return int(db.add_group(user_id, token))
    conn = db.get_conn()
    conn.execute("INSERT INTO groups(user_id, target) VALUES (?,?)", (user_id, token))
    conn.commit(); conn.close()
    return 1

def db_clear_groups(user_id: int):
    if hasattr(db, "clear_groups"):
        db.clear_groups(user_id); return
    conn = db.get_conn()
    conn.execute("DELETE FROM groups WHERE user_id=?", (user_id,))
    conn.commit(); conn.close()

def db_get_interval(user_id: int) -> int:
    if hasattr(db, "get_interval"):
        v = db.get_interval(user_id)
        return int(v or DEFAULT_INTERVAL_MIN)
    v = db_get_setting(f"user:{user_id}:interval", DEFAULT_INTERVAL_MIN)
    try: return int(v)
    except Exception: return DEFAULT_INTERVAL_MIN

def db_set_interval(user_id: int, mins: int):
    if hasattr(db, "set_interval"):
        db.set_interval(user_id, mins); return
    db_set_setting(f"user:{user_id}:interval", int(mins))

def db_get_last_sent_ts(user_id: int) -> Optional[int]:
    if hasattr(db, "get_last_sent_at"):
        return db.get_last_sent_at(user_id)
    v = db_get_setting(f"user:{user_id}:last_sent_ts", None)
    try: return int(v) if v is not None else None
    except Exception: return None

def db_mark_sent_now(user_id: int):
    if hasattr(db, "mark_sent_now"):
        db.mark_sent_now(user_id); return
    db_set_setting(f"user:{user_id}:last_sent_ts", int(datetime.utcnow().timestamp()))

def db_inc_sent_ok(user_id: int, n: int):
    if hasattr(db, "inc_sent_ok"):
        db.inc_sent_ok(user_id, n); return
    k = f"user:{user_id}:sent_ok"
    cur = int(db_get_setting(k, 0) or 0)
    db_set_setting(k, cur + int(n))

def db_night_enabled() -> bool:
    if hasattr(db, "night_enabled"):
        return bool(db.night_enabled())
    v = db_get_setting("night:enabled", 0)
    return str(v) in ("1", "true", "True")

def db_groups_cap_for(user_id: int) -> int:
    # main_bot should set this when user unlocks GC; default 5
    v = db_get_setting(f"gc_cap:{user_id}", None)
    if v is None:
        # legacy global function?
        if hasattr(db, "groups_cap"):
            try:
                return int(db.groups_cap())
            except Exception:
                pass
        return 5
    try:
        return int(v)
    except Exception:
        return 5

def db_save_cursor(user_id: int, slot: int, msg_id: int):
    db_set_setting(f"cursor:{user_id}:{slot}", int(msg_id))

def db_load_cursor(user_id: int, slot: int) -> int:
    return int(db_get_setting(f"cursor:{user_id}:{slot}", 0) or 0)

def db_get_rr_slot(user_id: int, total: int) -> int:
    key = f"rrslot:{user_id}"
    cur = int(db_get_setting(key, -1) or -1)
    nxt = (cur + 1) % max(1, total)
    db_set_setting(key, nxt)
    return nxt

# ── Targets normalization ─────────────────────────────────────────────────────
SPLIT_RE = re.compile(r"[,\s]+")
USERNAME_RE = re.compile(r"^@?([A-Za-z0-9_]{5,})$")

def expand_tokens(raw: List[str]) -> List[str]:
    out, seen = [], set()
    for entry in raw or []:
        if not entry: continue
        for tok in str(entry).splitlines():
            for part in SPLIT_RE.split(tok.strip()):
                t = part.strip().rstrip("/.,")
                if t and t not in seen:
                    seen.add(t); out.append(t)
    return out

def extract_username_from_link(s: str) -> Optional[str]:
    if not s.startswith("http"): return None
    try:
        u = urlparse(s)
        if u.netloc.lower() != "t.me": return None
        path = u.path.strip("/")
        if not path or path.startswith("+") or path.startswith("joinchat"):
            return None
        uname = path.split("/")[0]
        return uname if USERNAME_RE.match(uname) else None
    except Exception:
        return None

def normalize_tokens(tokens: List[str]) -> List[str]:
    norm = []
    for t in tokens:
        t = t.strip()
        if not t: continue
        if t.lstrip("-").isdigit():  # numeric ID
            norm.append(t); continue
        m = USERNAME_RE.match(t.lstrip("@"))
        if m:
            norm.append(m.group(1)); continue
        u = extract_username_from_link(t)  # public link → username
        if u:
            norm.append(u); continue
        # invite/private link or unknown: keep as-is (we'll skip at send)
        norm.append(t)
    seen, out = set(), []
    for x in norm:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

# ── Notifier ──────────────────────────────────────────────────────────────────
async def notify_rehydrate(user_id: int, slot: int, reason: str):
    if BOT_NOTIFIER is None:
        return
    key = f"authping:{user_id}:{slot}"
    last = int(db_get_setting(key, 0) or 0)
    now = int(datetime.utcnow().timestamp())
    if now - last < AUTH_PING_COOLDOWN_SEC:
        return
    db_set_setting(key, now)
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

async def notify_once_for_invite(user_id: int, token: str):
    if BOT_NOTIFIER is None: return
    key = f"warn:invite:{user_id}:{token}"
    if db_get_setting(key, None):
        return
    db_set_setting(key, 1)
    tip = (
        "✇ You added a private invite link as target.\n"
        "✇ I cannot use invite links unless your account <b>joins them manually</b>.\n"
        "✇ After joining, replace the link with the chat's numeric ID (recommended)."
    )
    try:
        await BOT_NOTIFIER.send_message(user_id, tip)
    except Exception:
        pass

# ── Session Manager ───────────────────────────────────────────────────────────
Apps: Dict[Tuple[int, int], Client] = {}  # (user_id, slot) -> Client
HandlersReady: Dict[Tuple[int, int], bool] = {}

def app_key(u: int, s: int) -> Tuple[int, int]:
    return (u, s)

async def start_app_if_needed(sess: dict, user_id: int):
    key = app_key(user_id, sess["slot"])
    if key in Apps:
        return Apps[key]
    app = Client(
        name=f"user-{user_id}-s{sess['slot']}",
        api_id=int(sess["api_id"]),
        api_hash=str(sess["api_hash"]),
        session_string=str(sess["session_string"])
    )
    try:
        await app.start()
    except (Unauthorized, AuthKeyUnregistered, SessionRevoked, SessionExpired, UserDeactivated, UserDeactivatedBan) as e:
        await notify_rehydrate(user_id, sess["slot"], e.__class__.__name__)
        log.error(f"[u{user_id}s{sess['slot']}] start auth error: {e}")
        return None
    except Exception as e:
        log.error(f"[u{user_id}s{sess['slot']}] start failed: {e}")
        return None

    # Register command handlers once
    if not HandlersReady.get(key):
        register_session_handlers(app, user_id)
        HandlersReady[key] = True

    Apps[key] = app
    return app

# ── Saved-All traversal (oldest → newest) ─────────────────────────────────────
async def get_next_saved_message(app: Client, user_id: int, slot: int):
    """
    Strict oldest → newest traversal with wraparound.
    Uses per-(user,slot) cursor stored in settings (last consumed message_id).
    """
    last_id = db_load_cursor(user_id, slot)  # 0 if never sent
    try:
        # find first message with id > last_id, iterating oldest → newest
        async for m in app.get_chat_history("me", reverse=True):
            if m.id > last_id:
                db_save_cursor(user_id, slot, m.id)
                return m
        # wrap to absolute oldest
        first = None
        async for m in app.get_chat_history("me", reverse=True, limit=1):
            first = m
        if first:
            db_save_cursor(user_id, slot, first.id)
            return first
    except Exception as e:
        log.warning(f"[u{user_id}s{slot}] saved fetch error: {e}")
    return None

# ── Send helpers ──────────────────────────────────────────────────────────────
async def resolve_chat_id(app: Client, target: str, user_id: int) -> Optional[int]:
    # numeric id
    if target.lstrip("-").isdigit():
        try:
            return int(target)
        except Exception:
            return None
    # username
    m = USERNAME_RE.match(target.lstrip("@"))
    if m:
        uname = m.group(1)
        try:
            chat = await app.get_chat(uname)
            return int(chat.id)
        except (UsernameInvalid, UsernameNotOccupied):
            log.info(f"[u{user_id}] @{uname} invalid/not occupied")
            return None
        except Exception as e:
            log.info(f"[u{user_id}] resolve @{uname} failed: {e}")
            return None
    # link
    if target.startswith("http"):
        # public link with username
        u = extract_username_from_link(target)
        if u:
            try:
                chat = await app.get_chat(u)
                return int(chat.id)
            except Exception as e:
                log.info(f"[u{user_id}] resolve link→@{u} failed: {e}")
                return None
        # invite/private link → cannot resolve; warn once
        await notify_once_for_invite(user_id, target)
        return None
    return None

async def copy_to_targets(app: Client, msg: Message, user_id: int, targets: List[str]) -> int:
    sent = 0
    for t in targets:
        chat_id = await resolve_chat_id(app, t, user_id)
        if chat_id is None:
            log.info(f"[u{user_id}] skip unresolved target: {t}")
            continue
        try:
            await app.copy_message(chat_id=chat_id, from_chat_id="me", message_id=msg.id)
            sent += 1
            await asyncio.sleep(PER_GROUP_DELAY_SEC)
        except FloodWait as fw:
            log.warning(f"[u{user_id}] FloodWait {fw.value}s on {chat_id}")
            await asyncio.sleep(fw.value + 1)
        except UserNotParticipant:
            log.info(f"[u{user_id}] not a participant in {chat_id} (invite or restricted)")
        except (Unauthorized, AuthKeyUnregistered, SessionRevoked, SessionExpired, UserDeactivated, UserDeactivatedBan) as e:
            await notify_rehydrate(user_id, 0, e.__class__.__name__)
            log.error(f"[u{user_id}] auth error on send: {e}")
            break
        except RPCError as e:
            log.warning(f"[u{user_id}] RPCError on {chat_id}: {e}")
        except Exception as e:
            log.warning(f"[u{user_id}] send failed on {chat_id}: {e}")
    return sent

# ── Per-session command handlers ──────────────────────────────────────────────
HELP_TEXT = (
    "✇ Commands (send from your account)\n"
    "• .help — this help\n"
    "• .listgc — list saved groups\n"
    "• .cleargc — clear all groups\n"
    "• .addgc <links/usernames/ids on new lines> — add up to your cap\n"
    "• .time 30m|45m|60m — set interval\n"
    "• .fstats — show interval, groups, sessions, next send ETA\n\n"
    "Notes:\n"
    "• Add public usernames (@group) or numeric IDs. Private invite links are saved but can't be used until you join.\n"
    "• Mode: Saved-All (oldest → newest). Each cycle picks the next Saved Message, sends to all groups with 30s gap, then advances."
)

def _next_eta(user_id: int, interval_min: int) -> str:
    last = db_get_last_sent_ts(user_id)
    if last is None:
        return "ready now"
    now = int(datetime.utcnow().timestamp())
    remain = interval_min*60 - (now - last)
    if remain <= 0:
        return "due"
    h = remain // 3600
    m = (remain % 3600) // 60
    s = remain % 60
    if h:
        return f"in ~{h}h {m}m {s}s"
    if m:
        return f"in ~{m}m {s}s"
    return f"in ~{s}s"

def register_session_handlers(app: Client, user_id: int):
    @app.on_message(filters.me & filters.text)
    async def _me_cmds(_, m: Message):
        text = (m.text or "").strip()
        if not text.startswith("."):
            return

        low = text.lower()
        # .help
        if low == ".help":
            await m.reply_text(HELP_TEXT)
            return

        # .listgc
        if low == ".listgc":
            gs = db_list_groups(user_id)
            cap = db_groups_cap_for(user_id)
            if gs:
                body = "\n".join(f"• {g}" for g in gs)
                await m.reply_text(f"👥 Groups ({len(gs)}/{cap})\n{body}")
            else:
                await m.reply_text(f"👥 Groups (0/{cap})\n(no groups yet)")
            return

        # .cleargc
        if low == ".cleargc":
            before = len(db_list_groups(user_id))
            db_clear_groups(user_id)
            after = len(db_list_groups(user_id))
            await m.reply_text(f"🧹 Cleared groups: {before} → {after}")
            return

        # .addgc ...
        if low.startswith(".addgc"):
            lines = text.split("\n")[1:] if "\n" in text else []
            # also allow inline form: .addgc token1 token2 ...
            if not lines:
                parts = text.split(maxsplit=1)
                if len(parts) == 2:
                    lines = [x for x in parts[1].split() if x.strip()]
            if not lines:
                await m.reply_text("Usage:\n.addgc <one target per line>\nExamples:\n.addgc\n@public_group\n-1001234567890\nhttps://t.me/PublicGroup")
                return

            existing = db_list_groups(user_id)
            cap = db_groups_cap_for(user_id)
            room = max(0, cap - len(existing))
            if room <= 0:
                await m.reply_text(f"❌ Cap reached ({len(existing)}/{cap}).")
                return

            tokens = normalize_tokens(expand_tokens(lines))
            added = 0
            dup = 0
            for t in tokens:
                if t in existing:
                    dup += 1
                    continue
                if added >= room:
                    break
                try:
                    db_add_group(user_id, t)
                    existing.append(t)
                    added += 1
                except Exception as e:
                    log.warning(f"[u{user_id}] add_group fail for {t}: {e}")

            await m.reply_text(f"✅ Added {added}. Duplicates {dup}. Now {len(existing)}/{cap}.")
            return

        # .time 30m|45m|60m
        if low.startswith(".time"):
            parts = text.split(maxsplit=1)
            if len(parts) < 2:
                await m.reply_text("Usage: .time 30m|45m|60m"); return
            val = parts[1].strip().lower().rstrip("m")
            try:
                mins = int(val)
            except Exception:
                await m.reply_text("❌ Invalid. Allowed: 30, 45, 60."); return
            if mins not in (30,45,60):
                await m.reply_text("❌ Allowed: 30, 45, 60."); return
            db_set_interval(user_id, mins)
            await m.reply_text(f"⏱ Interval set to {mins} minutes ✅")
            return

        # .fstats
        if low == ".fstats":
            interval = db_get_interval(user_id)
            gs = db_list_groups(user_id)
            sessions = db_sessions_strings(user_id)
            eta = _next_eta(user_id, interval)
            txt = (
                "📟 Forward Stats\n"
                "✇ ▶️ Worker: RUNNING\n"
                f"✇ Interval: {interval} min\n"
                f"✇ Sessions: {len(sessions)}  |  Groups: {len(gs)}\n"
                f"✇ Next send: {eta}\n"
                f"{'🌙 Night Mode ON' if db_night_enabled() else '🌙 Night Mode OFF'}\n"
                "✇ Mode: Saved-All (oldest → newest)"
            )
            await m.reply_text(txt)
            return

# ── Per-user cycle ────────────────────────────────────────────────────────────
async def process_user(user_id: int):
    # Night pause
    if db_night_enabled() and is_night_now_ist():
        return

    groups_raw = db_list_groups(user_id)
    if not groups_raw:
        log.info(f"[u{user_id}] no groups configured")
        return
    targets = normalize_tokens(expand_tokens(groups_raw))
    if not targets:
        log.info(f"[u{user_id}] no valid targets after normalization")
        return

    sessions = db_sessions_strings(user_id)
    if not sessions:
        log.info(f"[u{user_id}] no sessions")
        return

    interval = db_get_interval(user_id) or DEFAULT_INTERVAL_MIN
    last_ts = db_get_last_sent_ts(user_id)
    now = int(datetime.utcnow().timestamp())
    if last_ts is not None and (now - last_ts) < interval*60:
        # not due yet
        return

    # round-robin session choice
    idx = db_get_rr_slot(user_id, len(sessions))
    sess = sessions[idx]

    # ensure app running
    app = await start_app_if_needed(sess, user_id)
    if app is None:
        return

    # pick next saved message for this session (oldest → newest)
    msg = await get_next_saved_message(app, user_id, sess["slot"])
    if not msg:
        log.info(f"[u{user_id}s{sess['slot']}] no saved messages available")
        return

    # send to all targets with per-group delay
    sent = await copy_to_targets(app, msg, user_id, targets)
    if sent > 0:
        db_mark_sent_now(user_id)
        db_inc_sent_ok(user_id, sent)
        log.info(f"[u{user_id}] sent_ok+={sent}")
    else:
        log.info(f"[u{user_id}] nothing sent this tick")

# ── Supervisor loop ───────────────────────────────────────────────────────────
async def main_loop():
    if hasattr(db, "init_db"):
        db.init_db()
    while True:
        try:
            user_ids = db_users_with_sessions()
            # run due users concurrently; don't await per-group delays serially across users
            tasks = [asyncio.create_task(process_user(uid)) for uid in user_ids]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            log.error(f"loop error: {e}")
        await asyncio.sleep(TICK_SEC)

async def main():
    await main_loop()

if __name__ == "__main__":
    asyncio.run(main())
