# worker_forward.py
# ─────────────────────────────────────────────────────────────────────────────
# Spinify Forward Worker (multi-user, multi-session)
# • Saved-All mode (oldest → newest), per-(user,slot) cursor
# • Sends to all saved targets with 30s gap (env-tunable)
# • In-session commands (from the logged-in account):
#     .help, .listgc, .cleargc, .addgc, .time 30m|45m|60m, .fstats
# • Night-mode aware; optional DM ping via MAIN_BOT_TOKEN when session revoked
# • Ticks ~15s; default interval 30m
# ─────────────────────────────────────────────────────────────────────────────

import os, re, asyncio, logging
from typing import Dict, Tuple, Optional, List
from datetime import datetime, time as dtime, timezone
from urllib.parse import urlparse

from aiogram import Bot  # notifier only (no polling)
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import (
    FloodWait, RPCError, UsernameInvalid, UsernameNotOccupied,
    Unauthorized, AuthKeyUnregistered, UserDeactivated, UserDeactivatedBan
)

# Optional Pyrogram errors (older/newer variants)
try:
    from pyrogram.errors import SessionRevoked, SessionExpired, UserNotParticipant
except Exception:  # pragma: no cover
    class _Dummy(Exception): ...
    SessionRevoked = SessionExpired = UserNotParticipant = _Dummy

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("worker")

# ── DB facade ─────────────────────────────────────────────────────────────────
try:
    import core.db as db
except Exception as e:
    raise RuntimeError(f"core.db import failed: {e}")

# ── Env ───────────────────────────────────────────────────────────────────────
TICK_SEC              = int(os.getenv("TICK_SEC", "15"))
PER_GROUP_DELAY_SEC   = int(os.getenv("GROUP_DELAY_SEC", "30"))
DEFAULT_INTERVAL_MIN  = int(os.getenv("DEFAULT_INTERVAL_MIN", "30"))
MAIN_BOT_TOKEN        = (os.getenv("MAIN_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
BOT_NOTIFIER          = Bot(MAIN_BOT_TOKEN) if MAIN_BOT_TOKEN and ":" in MAIN_BOT_TOKEN else None
AUTH_PING_COOLDOWN_SEC = 6 * 3600

# ── IST night window (00:00–07:00) ────────────────────────────────────────────
IST_OFFSET = 5 * 3600 + 1800
NIGHT_START = dtime(0, 0)
NIGHT_END   = dtime(7, 0)

def is_night_now_ist() -> bool:
    now_utc = datetime.utcnow()
    secs = (now_utc.timestamp() + IST_OFFSET) % 86400
    hh = int(secs // 3600); mm = int((secs % 3600) // 60)
    tt = dtime(hh, mm)
    return NIGHT_START <= tt < NIGHT_END

# ── Small helpers ─────────────────────────────────────────────────────────────
def now_epoch() -> int:
    return int(datetime.now(timezone.utc).timestamp())

def fmt_eta(seconds: int) -> str:
    if seconds <= 0: return "now"
    m, s = divmod(seconds, 60); h, m = divmod(m, 60)
    parts = []
    if h: parts.append(f"{h}h")
    if m: parts.append(f"{m}m")
    if s or not parts: parts.append(f"{s}s")
    return "in ~" + " ".join(parts)

# ── Settings wrappers / compatibility ─────────────────────────────────────────
def db_get_setting(key: str, default=None):
    try: return db.get_setting(key, default)
    except TypeError:  # older signature
        v = db.get_setting(key)
        return default if v is None else v

def db_set_setting(key: str, val):
    db.set_setting(key, val)

def db_users_with_sessions() -> List[int]:
    return list(db.users_with_sessions())

def db_sessions_strings(user_id: int) -> List[dict]:
    if hasattr(db, "sessions_strings"): return list(db.sessions_strings(user_id))
    return list(db.sessions_list(user_id))

def db_list_groups(user_id: int) -> List[str]:
    return list(db.list_groups(user_id))

def db_add_group(user_id: int, token: str) -> int:
    return int(db.add_group(user_id, token))

def db_clear_groups(user_id: int):
    db.clear_groups(user_id)

def db_get_interval(user_id: int) -> int:
    v = db.get_interval(user_id)
    try: return int(v or DEFAULT_INTERVAL_MIN)
    except Exception: return DEFAULT_INTERVAL_MIN

def db_set_interval(user_id: int, mins: int):
    db.set_interval(user_id, int(mins))

def db_get_last_sent_ts(user_id: int) -> Optional[int]:
    return db.get_last_sent_at(user_id)

def db_mark_sent_now(user_id: int):
    db.mark_sent_now(user_id)

def db_inc_sent_ok(user_id: int, n: int):
    db.inc_sent_ok(user_id, int(n))

def db_night_enabled() -> bool:
    return bool(db.night_enabled())

def db_groups_cap_for(user_id: int) -> int:
    """
    Priority:
      1) explicit setting groups_cap:{uid}
      2) db.groups_cap(uid) (handles unlock/premium)
      3) fallback 5
    """
    v = db_get_setting(f"groups_cap:{int(user_id)}", None)
    if v is not None:
        try: return int(v)
        except Exception: pass
    if hasattr(db, "groups_cap"):
        try: return int(db.groups_cap(int(user_id)))
        except Exception: pass
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

# ── Target normalization ──────────────────────────────────────────────────────
SPLIT_RE    = re.compile(r"[,\s]+")
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
        if t.lstrip("-").isdigit():
            norm.append(t); continue
        m = USERNAME_RE.match(t.lstrip("@"))
        if m:
            norm.append(m.group(1)); continue
        u = extract_username_from_link(t)
        if u:
            norm.append(u); continue
        norm.append(t)  # keep as-is (invite → will be skipped)
    seen, out = set(), []
    for x in norm:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

# ── Notifier ──────────────────────────────────────────────────────────────────
async def notify_rehydrate(user_id: int, slot: int, reason: str):
    if BOT_NOTIFIER is None: return
    key = f"authping:{user_id}:{slot}"
    last = int(db_get_setting(key, 0) or 0)
    now  = now_epoch()
    if now - last < AUTH_PING_COOLDOWN_SEC:
        return
    db_set_setting(key, now)
    msg = (
        "✇ Session issue detected\n"
        f"✇ Your account (slot {slot}) looks <b>expired/unauthorized</b>.\n"
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
        "✇ I cannot post via invite links until your account joins them.\n"
        "✇ After joining, prefer the chat’s numeric ID."
    )
    try:
        await BOT_NOTIFIER.send_message(user_id, tip)
    except Exception:
        pass

# ── Session Manager ───────────────────────────────────────────────────────────
Apps: Dict[Tuple[int,int], Client] = {}
HandlersReady: Dict[Tuple[int,int], bool] = {}

def app_key(u: int, s: int) -> Tuple[int,int]:
    return (u, s)

async def start_app_if_needed(sess: dict, user_id: int) -> Optional[Client]:
    key = app_key(user_id, int(sess["slot"]))
    if key in Apps:
        return Apps[key]
    app = Client(
        name=f"user-{user_id}-s{sess['slot']}",
        api_id=int(sess["api_id"]),
        api_hash=str(sess["api_hash"]),
        session_string=str(sess["session_string"]),
        in_memory=True,
        device_model="Spinify Worker",
        app_version="2.0",
        system_version="Linux",
        lang_code="en",
    )
    try:
        await app.start()
    except (Unauthorized, AuthKeyUnregistered, SessionRevoked, SessionExpired, UserDeactivated, UserDeactivatedBan) as e:
        await notify_rehydrate(user_id, int(sess["slot"]), e.__class__.__name__)
        log.error(f"[u{user_id}s{sess['slot']}] start auth error: {e}")
        return None
    except Exception as e:
        log.error(f"[u{user_id}s{sess['slot']}] start failed: {e}")
        return None

    if not HandlersReady.get(key):
        register_session_handlers(app, user_id)
        HandlersReady[key] = True

    Apps[key] = app
    return app

# ── Saved-All traversal (oldest → newest) ─────────────────────────────────────
async def get_next_saved_message(app: Client, user_id: int, slot: int) -> Optional[Message]:
    last_id = db_load_cursor(user_id, slot)
    try:
        async for m in app.get_chat_history("me", reverse=True):
            if m.id > last_id:
                db_save_cursor(user_id, slot, m.id)
                return m
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
    if target.lstrip("-").isdigit():
        try: return int(target)
        except Exception: return None
    m = USERNAME_RE.match(target.lstrip("@"))
    if m:
        uname = m.group(1)
        try:
            chat = await app.get_chat(uname)
            return int(chat.id)
        except (UsernameInvalid, UsernameNotOccupied):
            log.info(f"[u{user_id}] @{uname} invalid/not occupied"); return None
        except Exception as e:
            log.info(f"[u{user_id}] resolve @{uname} failed: {e}"); return None
    if target.startswith("http"):
        u = extract_username_from_link(target)
        if u:
            try:
                chat = await app.get_chat(u)
                return int(chat.id)
            except Exception as e:
                log.info(f"[u{user_id}] resolve link→@{u} failed: {e}"); return None
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
            log.info(f"[u{user_id}] not a participant in {chat_id}")
        except (Unauthorized, AuthKeyUnregistered, SessionRevoked, SessionExpired, UserDeactivated, UserDeactivatedBan) as e:
            await notify_rehydrate(user_id, 0, e.__class__.__name__)
            log.error(f"[u{user_id}] auth error on send: {e}")
            break
        except RPCError as e:
            log.warning(f"[u{user_id}] RPCError on {chat_id}: {e}")
        except Exception as e:
            log.warning(f"[u{user_id}] send failed on {chat_id}: {e}")
    return sent

# ── Command handlers ──────────────────────────────────────────────────────────
HELP_TEXT = (
    "✇ Commands (send from your account)\n"
    "• .help — this help\n"
    "• .listgc — list saved groups\n"
    "• .cleargc — clear all groups\n"
    "• .addgc <links/usernames/ids on new lines> — add up to your cap\n"
    "• .time 30m|45m|60m — set interval\n"
    "• .fstats — show interval, groups, sessions, next-send ETA\n\n"
    "Mode: Saved-All (oldest → newest) with 30s gap per group."
)

def _next_eta(user_id: int, interval_min: int) -> str:
    last = db_get_last_sent_ts(user_id)
    if last is None: return "now"
    remain = interval_min * 60 - (now_epoch() - int(last))
    return fmt_eta(remain)

def register_session_handlers(app: Client, user_id: int):
    @app.on_message(filters.me & filters.text & ~filters.edited)
    async def _me_cmds(_, m: Message):
        text = (m.text or "").strip()
        if not text.startswith("."): return
        low = text.lower()

        if low == ".help":
            await m.reply_text(HELP_TEXT); return

        if low == ".listgc":
            gs  = db_list_groups(user_id)
            cap = db_groups_cap_for(user_id)
            body = "\n".join(f"• {g}" for g in gs) if gs else "(none)"
            await m.reply_text(f"👥 Groups ({len(gs)}/{cap})\n{body}"); return

        if low == ".cleargc":
            before = len(db_list_groups(user_id))
            db_clear_groups(user_id)
            await m.reply_text(f"🧹 Cleared groups: {before} → 0"); return

        if low.startswith(".addgc"):
            lines = text.split("\n")[1:] if "\n" in text else []
            if not lines:
                parts = text.split(maxsplit=1)
                if len(parts) == 2:
                    lines = [x for x in parts[1].split() if x.strip()]
            if not lines:
                await m.reply_text("Usage:\n.addgc <one per line>\n@public\n-100123...\nhttps://t.me/Public"); return
            existing = db_list_groups(user_id)
            cap = db_groups_cap_for(user_id)
            room = max(0, cap - len(existing))
            if room <= 0:
                await m.reply_text(f"❌ Cap reached ({len(existing)}/{cap})."); return
            tokens = normalize_tokens(expand_tokens(lines))
            added = dup = 0
            for t in tokens:
                if t in existing:
                    dup += 1; continue
                if added >= room:
                    break
                try:
                    if db_add_group(user_id, t):
                        existing.append(t); added += 1
                except Exception as e:
                    log.warning(f"[u{user_id}] add_group({t}) failed: {e}")
            await m.reply_text(f"✅ Added {added}. Duplicates {dup}. Now {len(existing)}/{cap}."); return

        if low.startswith(".time"):
            parts = text.split(maxsplit=1)
            if len(parts) < 2:
                await m.reply_text("Usage: .time 30m|45m|60m"); return
            val = parts[1].strip().lower().rstrip("m")
            try: mins = int(val)
            except Exception:
                await m.reply_text("❌ Allowed: 30, 45, 60."); return
            if mins not in (30,45,60):
                await m.reply_text("❌ Allowed: 30, 45, 60."); return
            db_set_interval(user_id, mins)
            await m.reply_text(f"⏱ Interval set to {mins} minutes ✅"); return

        if low == ".fstats":
            interval = db_get_interval(user_id)
            gs       = db_list_groups(user_id)
            sessions = db_sessions_strings(user_id)
            eta      = _next_eta(user_id, interval)
            txt = (
                "📟 Forward Stats\n"
                "✇ ▶️ Worker: RUNNING\n"
                f"✇ Interval: {interval} min\n"
                f"✇ Sessions: {len(sessions)}  |  Groups: {len(gs)}\n"
                f"✇ Next send: {eta}\n"
                f"{'🌙 Night Mode ON' if db_night_enabled() else '🌙 Night Mode OFF'}\n"
                "✇ Mode: Saved-All (oldest → newest)"
            )
            await m.reply_text(txt); return

# ── Per-user tick ─────────────────────────────────────────────────────────────
async def process_user(user_id: int):
    # Respect night mode if globally enabled AND within IST window
    if db_night_enabled() and is_night_now_ist():
        return

    groups_raw = db_list_groups(user_id)
    if not groups_raw:
        return
    targets = normalize_tokens(expand_tokens(groups_raw))
    if not targets:
        return

    sessions = db_sessions_strings(user_id)
    if not sessions:
        return

    interval = db_get_interval(user_id) or DEFAULT_INTERVAL_MIN
    last_ts  = db_get_last_sent_ts(user_id)
    now      = now_epoch()
    if last_ts is not None and (now - last_ts) < interval * 60:
        return  # not due yet

    # round-robin session choice
    idx  = db_get_rr_slot(user_id, len(sessions))
    sess = sessions[idx]

    app = await start_app_if_needed(sess, user_id)
    if app is None:
        return

    msg = await get_next_saved_message(app, user_id, int(sess["slot"]))
    if not msg:
        log.info(f"[u{user_id}s{sess['slot']}] no saved messages")
        return

    sent = await copy_to_targets(app, msg, user_id, targets)
    if sent > 0:
        db_mark_sent_now(user_id)
        db_inc_sent_ok(user_id, sent)
        log.info(f"[u{user_id}] sent_ok+={sent}")

# ── Main loop ─────────────────────────────────────────────────────────────────
async def main_loop():
    if hasattr(db, "init_db"):
        db.init_db()
    while True:
        try:
            uids = db_users_with_sessions()
            if uids:
                await asyncio.gather(*(process_user(uid) for uid in uids), return_exceptions=True)
        except Exception as e:
            log.error(f"loop error: {e}")
        await asyncio.sleep(TICK_SEC)

async def main():
    await main_loop()

if __name__ == "__main__":
    asyncio.run(main())
