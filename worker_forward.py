# worker_forward.py
import os, re, asyncio, logging
from typing import Dict, Tuple, Optional, List
from datetime import datetime, time as dtime

from urllib.parse import urlparse

from aiogram import Bot
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import (
    FloodWait, RPCError, UsernameInvalid, UsernameNotOccupied,
    Unauthorized, AuthKeyUnregistered, UserDeactivated, UserDeactivatedBan
)

try:
    from pyrogram.errors import SessionRevoked, SessionExpired, UserNotParticipant
except Exception:
    class _Dummy(Exception): ...
    SessionRevoked = SessionExpired = UserNotParticipant = _Dummy

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("worker")

# --- DB facade ---------------------------------------------------------------
try:
    import core.db as db
except Exception as e:
    raise RuntimeError(f"core.db import failed: {e}")

# --- Tunables / Env ----------------------------------------------------------
TICK_SEC = 15
PER_GROUP_DELAY_SEC = 30
DEFAULT_INTERVAL_MIN = 30

# Health
HEALTH_PERIOD_SEC = 120          # how often to actively re-check a session
HEALTH_RECONNECT_GRACE = 1       # attempts per cycle (we reconnect next loop)

# IST night
IST_OFFSET = 5 * 3600 + 1800
NIGHT_START = dtime(0, 0)
NIGHT_END = dtime(7, 0)

MAIN_BOT_TOKEN = (os.getenv("MAIN_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
BOT_NOTIFIER = Bot(MAIN_BOT_TOKEN) if MAIN_BOT_TOKEN and ":" in MAIN_BOT_TOKEN else None
AUTH_PING_COOLDOWN_SEC = 6 * 3600

# --- Time helpers ------------------------------------------------------------
def is_night_now_ist() -> bool:
    now = int(datetime.utcnow().timestamp()) + IST_OFFSET
    hh = (now % 86400) // 3600
    mm = ((now % 86400) % 3600) // 60
    t = dtime(hh, mm)
    return NIGHT_START <= t < NIGHT_END

# --- DB wrappers (work with mongo/sqlite) ------------------------------------
def kv_get(k, d=None):
    try: return db.get_setting(k, d)
    except TypeError: 
        v = db.get_setting(k)
        return d if v is None else v

def kv_set(k, v):
    db.set_setting(k, v)

def users_with_sessions() -> List[int]:
    if hasattr(db, "users_with_sessions"):
        return list(db.users_with_sessions())
    conn = db.get_conn()
    rows = conn.execute("SELECT DISTINCT user_id FROM user_sessions").fetchall()
    conn.close()
    return [r[0] if isinstance(r, tuple) else r["user_id"] for r in rows]

def sessions_of(user_id: int) -> List[dict]:
    if hasattr(db, "sessions_strings"): return list(db.sessions_strings(user_id))
    if hasattr(db, "sessions_list"):    return list(db.sessions_list(user_id))
    conn = db.get_conn()
    rows = conn.execute("SELECT slot, api_id, api_hash, session_string FROM user_sessions WHERE user_id=? ORDER BY slot", (user_id,)).fetchall()
    conn.close()
    out = []
    for r in rows:
        if isinstance(r, dict): out.append(r)
        else:
            slot, api_id, api_hash, s = r
            out.append({"slot": slot, "api_id": api_id, "api_hash": api_hash, "session_string": s})
    return out

def list_groups(user_id: int) -> List[str]:
    if hasattr(db, "list_groups"): return list(db.list_groups(user_id))
    conn = db.get_conn()
    rows = conn.execute("SELECT target FROM groups WHERE user_id=? ORDER BY rowid", (user_id,)).fetchall()
    conn.close()
    return [r[0] if isinstance(r, tuple) else r["target"] for r in rows]

def add_group(user_id: int, t: str) -> int:
    if hasattr(db, "add_group"): return int(db.add_group(user_id, t))
    conn = db.get_conn()
    conn.execute("INSERT OR IGNORE INTO groups(user_id, target) VALUES(?,?)", (user_id, t))
    conn.commit(); conn.close()
    return 1

def clear_groups(user_id: int):
    if hasattr(db, "clear_groups"): return db.clear_groups(user_id)
    conn = db.get_conn()
    conn.execute("DELETE FROM groups WHERE user_id=?", (user_id,))
    conn.commit(); conn.close()

def groups_cap_for(user_id: int) -> int:
    if hasattr(db, "groups_cap"):
        try: return int(db.groups_cap(user_id))
        except TypeError:
            try: return int(db.groups_cap())
            except Exception: pass
    v = kv_get(f"groups_cap:{user_id}", None)
    if v is not None:
        try: return int(v)
        except Exception: pass
    # old unlock flag
    if str(kv_get(f"gc_unlock:{user_id}", 0)) in ("1","true","True"):
        return 10
    return 5

def get_interval(user_id: int) -> int:
    if hasattr(db, "get_interval"):
        v = db.get_interval(user_id); 
        try: return int(v or DEFAULT_INTERVAL_MIN)
        except Exception: return DEFAULT_INTERVAL_MIN
    return int(kv_get(f"interval:{user_id}", DEFAULT_INTERVAL_MIN))

def set_interval(user_id: int, mins: int):
    if hasattr(db, "set_interval"): return db.set_interval(user_id, mins)
    kv_set(f"interval:{user_id}", int(mins))

def get_last_sent(user_id: int) -> Optional[int]:
    if hasattr(db, "get_last_sent_at"): return db.get_last_sent_at(user_id)
    v = kv_get(f"last_sent_at:{user_id}", None)
    try: return int(v) if v is not None else None
    except Exception: return None

def mark_sent_now(user_id: int):
    if hasattr(db, "mark_sent_now"): return db.mark_sent_now(user_id)
    kv_set(f"last_sent_at:{user_id}", int(datetime.utcnow().timestamp()))

def inc_sent_ok(user_id: int, n: int):
    if hasattr(db, "inc_sent_ok"): return db.inc_sent_ok(user_id, n)
    k = f"user:{user_id}:sent_ok"
    cur = int(kv_get(k, 0) or 0); kv_set(k, cur + int(n))

def night_enabled() -> bool:
    if hasattr(db, "night_enabled"): return bool(db.night_enabled())
    return str(kv_get("night:enabled", 0)) in ("1","true","True")

def save_cursor(user_id: int, slot: int, mid: int):
    kv_set(f"cursor:{user_id}:{slot}", int(mid))

def load_cursor(user_id: int, slot: int) -> int:
    return int(kv_get(f"cursor:{user_id}:{slot}", 0) or 0)

def reset_cursor(user_id: int, slot: Optional[int] = None):
    if slot is None:
        for s in [x.get("slot") for x in sessions_of(user_id)]:
            kv_set(f"cursor:{user_id}:{int(s)}", 0)
    else:
        kv_set(f"cursor:{user_id}:{int(slot)}", 0)

def rr_slot(user_id: int, total: int) -> int:
    k = f"rrslot:{user_id}"
    cur = int(kv_get(k, -1) or -1)
    nxt = (cur + 1) % max(1, total)
    kv_set(k, nxt)
    return nxt

# --- Target normalization -----------------------------------------------------
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
        if t.lstrip("-").isdigit(): norm.append(t); continue
        m = USERNAME_RE.match(t.lstrip("@"))
        if m: norm.append(m.group(1)); continue
        u = extract_username_from_link(t)
        if u: norm.append(u); continue
        norm.append(t)  # keep (invite) for warning
    out, seen = [], set()
    for x in norm:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

# --- Notifier ----------------------------------------------------------------
async def notify_rehydrate(user_id: int, slot: int, reason: str):
    if BOT_NOTIFIER is None: return
    key = f"authping:{user_id}:{slot}"
    last = int(kv_get(key, 0) or 0)
    now = int(datetime.utcnow().timestamp())
    if now - last < AUTH_PING_COOLDOWN_SEC: return
    kv_set(key, now)
    msg = (
        "✇ Session issue detected\n"
        f"✇ Your account (slot {slot}) looks <b>expired or unauthorized</b>.\n"
        "✇ Please log in again via <b>@SpinifyLoginBot</b>.\n"
        f"✇ Reason: <code>{reason}</code>"
    )
    try: await BOT_NOTIFIER.send_message(user_id, msg)
    except Exception: pass

async def notify_once_invite(user_id: int, token: str):
    if BOT_NOTIFIER is None: return
    key = f"warn:invite:{user_id}:{token}"
    if kv_get(key, None): return
    kv_set(key, 1)
    tip = ("✇ You added a private invite link as target.\n"
           "✇ I cannot use invite links unless your account <b>joins them manually</b>.\n"
           "✇ After joining, replace the link with the chat's numeric ID.")
    try: await BOT_NOTIFIER.send_message(user_id, tip)
    except Exception: pass

# --- Session manager + health -------------------------------------------------
Apps: Dict[Tuple[int,int], Client] = {}
HandlersReady: Dict[Tuple[int,int], bool] = {}
LastHealthTs: Dict[Tuple[int,int], int] = {}
UserLocks: Dict[int, asyncio.Lock] = {}

def app_key(u: int, s: int) -> Tuple[int,int]: return (u, s)

async def _client_health(app: Client) -> bool:
    try:
        await app.get_me()
        return True
    except Exception:
        return False

async def ensure_app_healthy(user_id: int, sess: dict) -> Optional[Client]:
    key = app_key(user_id, int(sess["slot"]))
    app = Apps.get(key)

    # register handlers early (before start)
    if not HandlersReady.get(key):
        # temp client for handler binding (same instance used below)
        if app is None:
            app = Client(
                name=f"user-{user_id}-s{sess['slot']}",
                api_id=int(sess["api_id"]), api_hash=str(sess["api_hash"]),
                session_string=str(sess["session_string"])
            )
        register_session_handlers(app, user_id)
        HandlersReady[key] = True

    if app is None:
        app = Client(
            name=f"user-{user_id}-s{sess['slot']}",
            api_id=int(sess["api_id"]), api_hash=str(sess["api_hash"]),
            session_string=str(sess["session_string"])
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
        Apps[key] = app
        LastHealthTs[key] = 0
        return app

    # periodic health
    now = int(datetime.utcnow().timestamp())
    last = int(LastHealthTs.get(key, 0) or 0)
    if now - last >= HEALTH_PERIOD_SEC:
        if not await _client_health(app):
            # drop and reconnect on next call
            try: await app.stop()
            except Exception: pass
            Apps.pop(key, None)
            LastHealthTs[key] = now
            log.warning(f"[u{user_id}s{sess['slot']}] health failed → will reconnect")
            return await ensure_app_healthy(user_id, sess)
        LastHealthTs[key] = now

    return app

async def ensure_all_sessions_online():
    for uid in users_with_sessions():
        for sess in sessions_of(uid):
            await ensure_app_healthy(uid, sess)

# --- Saved-All traversal (oldest → newest) -----------------------------------
async def next_saved(app: Client, user_id: int, slot: int):
    last_id = load_cursor(user_id, slot)
    try:
        async for m in app.get_chat_history("me", reverse=True):
            if m.id > last_id:
                save_cursor(user_id, slot, m.id)
                return m
        # wrap to absolute oldest
        first = None
        async for m in app.get_chat_history("me", reverse=True, limit=1):
            first = m
        if first:
            save_cursor(user_id, slot, first.id)
            return first
    except Exception as e:
        log.warning(f"[u{user_id}s{slot}] saved fetch error: {e}")
    return None

# --- Send helpers -------------------------------------------------------------
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
            log.info(f"[u{user_id}] @{uname} invalid/not occupied")
            return None
        except Exception as e:
            log.info(f"[u{user_id}] resolve @{uname} failed: {e}")
            return None
    if target.startswith("http"):
        u = extract_username_from_link(target)
        if u:
            try:
                chat = await app.get_chat(u)
                return int(chat.id)
            except Exception as e:
                log.info(f"[u{user_id}] resolve link→@{u} failed: {e}")
                return None
        await notify_once_invite(user_id, target)
        return None
    return None

async def copy_to_targets(app: Client, msg: Message, user_id: int, targets: List[str]) -> int:
    sent = 0
    for t in targets:
        chat_id = await resolve_chat_id(app, t, user_id)
        if chat_id is None:
            log.info(f"[u{user_id}] skip unresolved: {t}")
            continue
        try:
            await app.copy_message(chat_id=chat_id, from_chat_id="me", message_id=msg.id)
            sent += 1
            await asyncio.sleep(PER_GROUP_DELAY_SEC)
        except FloodWait as fw:
            log.warning(f"[u{user_id}] FloodWait {fw.value}s on {chat_id}")
            await asyncio.sleep(fw.value + 1)
        except UserNotParticipant:
            log.info(f"[u{user_id}] not participant {chat_id}")
        except (Unauthorized, AuthKeyUnregistered, SessionRevoked, SessionExpired, UserDeactivated, UserDeactivatedBan) as e:
            await notify_rehydrate(user_id, 0, e.__class__.__name__)
            log.error(f"[u{user_id}] auth error on send: {e}")
            break
        except RPCError as e:
            log.warning(f"[u{user_id}] RPCError {chat_id}: {e}")
        except Exception as e:
            log.warning(f"[u{user_id}] send failed {chat_id}: {e}")
    return sent

# --- Commands ----------------------------------------------------------------
HELP_TEXT = (
    "✇ Commands (send from your account)\n"
    "• .help — this help\n"
    "• .listgc — list saved groups\n"
    "• .cleargc — clear all groups\n"
    "• .addgc <targets> — one per line or space-separated\n"
    "• .time 30m|45m|60m — set interval\n"
    "• .fstats — interval, groups, sessions, next ETA\n"
    "• .adreset — restart cycle from oldest Saved Message\n"
    "• .health — check session status\n"
    "Mode: Saved-All (oldest → newest) with ~30s gap per target."
)

def _eta_str(user_id: int, interval_min: int) -> str:
    last = get_last_sent(user_id)
    if last is None: return "ready now"
    now = int(datetime.utcnow().timestamp())
    rem = interval_min*60 - (now - last)
    if rem <= 0: return "due"
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    if h: return f"in ~{h}h {m}m {s}s"
    if m: return f"in ~{m}m {s}s"
    return f"in ~{s}s"

def register_session_handlers(app: Client, user_id: int):
    @app.on_message(filters.me & (filters.text | filters.caption) & ~filters.edited)
    async def _me_cmds(_, m: Message):
        try:
            text = (m.text or m.caption or "").strip()
            if not text.startswith("."): return
            low = text.lower()

            if low == ".help":
                await m.reply_text(HELP_TEXT); return

            if low == ".listgc":
                gs = list_groups(user_id); cap = groups_cap_for(user_id)
                body = "\n".join(f"• {g}" for g in gs) if gs else "(no groups yet)"
                await m.reply_text(f"👥 Groups ({len(gs)}/{cap})\n{body}"); return

            if low == ".cleargc":
                before = len(list_groups(user_id)); clear_groups(user_id)
                after = len(list_groups(user_id))
                await m.reply_text(f"🧹 Cleared groups: {before} → {after}"); return

            if low.startswith(".addgc"):
                lines = text.split("\n")[1:] if "\n" in text else []
                if not lines:
                    parts = text.split(maxsplit=1)
                    if len(parts) == 2: lines = [x for x in parts[1].split() if x.strip()]
                if not lines:
                    await m.reply_text("Usage:\n.addgc <one target per line>\nExample:\n.addgc\n@public\n-100123\nhttps://t.me/Public"); return
                existing = list_groups(user_id)
                cap = groups_cap_for(user_id); room = max(0, cap - len(existing))
                if room <= 0: await m.reply_text(f"❌ Cap reached ({len(existing)}/{cap})."); return
                tokens = normalize_tokens(expand_tokens(lines))
                added = dup = 0
                for t in tokens:
                    if t in existing: dup += 1; continue
                    if added >= room: break
                    try:
                        add_group(user_id, t); existing.append(t); added += 1
                    except Exception as e:
                        log.warning(f"[u{user_id}] add_group fail {t}: {e}")
                await m.reply_text(f"✅ Added {added}. Duplicates {dup}. Now {len(existing)}/{cap}."); return

            if low.startswith(".time"):
                parts = text.split(maxsplit=1)
                if len(parts) < 2: await m.reply_text("Usage: .time 30m|45m|60m"); return
                v = parts[1].strip().lower().rstrip("m")
                try: mins = int(v)
                except Exception: await m.reply_text("❌ Allowed: 30, 45, 60."); return
                if mins not in (30,45,60): await m.reply_text("❌ Allowed: 30, 45, 60."); return
                set_interval(user_id, mins); await m.reply_text(f"⏱ Interval set to {mins} minutes ✅"); return

            if low == ".fstats":
                interval = get_interval(user_id); gs = list_groups(user_id); sess = sessions_of(user_id)
                eta = _eta_str(user_id, interval)
                await m.reply_text(
                    "📟 Forward Stats\n"
                    "✇ ▶️ Worker: RUNNING\n"
                    f"✇ Interval: {interval} min\n"
                    f"✇ Sessions: {len(sess)}  |  Groups: {len(gs)}\n"
                    f"✇ Next send: {eta}\n"
                    f"{'🌙 Night Mode ON' if night_enabled() else '🌙 Night Mode OFF'}\n"
                    "✇ Mode: Saved-All (oldest → newest)"
                ); return

            if low == ".adreset":
                reset_cursor(user_id, None)
                kv_set(f"last_sent_at:{user_id}", None)
                await m.reply_text("🔄 Cycle reset to oldest Saved Message."); return

            if low == ".health":
                sess = sessions_of(user_id)
                if not sess: await m.reply_text("No sessions saved."); return
                ok = 0
                for s in sess:
                    app2 = await ensure_app_healthy(user_id, s)
                    if app2 is not None and await _client_health(app2): ok += 1
                await m.reply_text(f"Health: {ok}/{len(sess)} sessions OK."); return

        except Exception as e:
            log.exception(f"[u{user_id}] cmd handler error: {e}")

# --- Per-user cycle -----------------------------------------------------------
async def process_user(user_id: int):
    # prevent overlaps per user
    lock = UserLocks.setdefault(user_id, asyncio.Lock())
    if lock.locked(): return
    async with lock:
        if night_enabled() and is_night_now_ist():
            return

        groups_raw = list_groups(user_id)
        if not groups_raw: return
        targets = normalize_tokens(expand_tokens(groups_raw))
        if not targets: return

        sess = sessions_of(user_id)
        if not sess: return

        interval = get_interval(user_id) or DEFAULT_INTERVAL_MIN
        last = get_last_sent(user_id)
        now = int(datetime.utcnow().timestamp())
        if last is not None and (now - last) < interval*60:
            return

        i = rr_slot(user_id, len(sess))
        s = sess[i]
        app = await ensure_app_healthy(user_id, s)
        if app is None: return

        msg = await next_saved(app, user_id, int(s["slot"]))
        if not msg:
            log.info(f"[u{user_id}s{s['slot']}] no saved messages")
            return

        sent = await copy_to_targets(app, msg, user_id, targets)
        if sent > 0:
            mark_sent_now(user_id); inc_sent_ok(user_id, sent)
            log.info(f"[u{user_id}] sent_ok+={sent}")
        else:
            log.info(f"[u{user_id}] nothing sent this tick")

# --- Supervisor loop ----------------------------------------------------------
async def main_loop():
    if hasattr(db, "init_db"):
        db.init_db()
    log.info("worker booted")
    while True:
        try:
            await ensure_all_sessions_online()
            uids = users_with_sessions()
            tasks = [asyncio.create_task(process_user(uid)) for uid in uids]
            if tasks: await asyncio.gather(*tasks, return_exceptions=True)
        except Exception:
            log.exception("loop error")
        await asyncio.sleep(TICK_SEC)

async def main():
    await main_loop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        log.exception("worker fatal")
        raise
