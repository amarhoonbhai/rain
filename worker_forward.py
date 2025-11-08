# worker_forward.py — NO-JOIN sender, interval fanout, auto-rehydrate DM
import os, asyncio, logging, re, time as _time
from urllib.parse import urlparse
from datetime import datetime, time
from zoneinfo import ZoneInfo

from aiogram import Bot  # only for DM notify to users on auth problems (optional)

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
    # users/sessions
    users_with_sessions, sessions_strings,
    # groups & ad
    list_groups, get_ad,
    # interval & timing
    get_interval, get_last_sent_at, mark_sent_now,
    # settings & stats
    night_enabled, set_setting, get_setting, inc_sent_ok
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("worker")

IST = ZoneInfo("Asia/Kolkata")
NIGHT_START = time(0, 0)   # 00:00
NIGHT_END   = time(7, 0)   # 07:00

# --- NO JOIN MODE (as requested) ---
NO_AUTO_JOIN = True

# --- Bot notifier for auto-rehydrate (optional) ---
BOT_TOKEN = (os.getenv("MAIN_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
BOT_NOTIFIER = Bot(BOT_TOKEN) if BOT_TOKEN and ":" in BOT_TOKEN else None
AUTH_PING_COOLDOWN_SEC = 6 * 3600  # 6 hours

# ---------------- token normalization ----------------
SPLIT_RE = re.compile(r"[,\s]+")

USERNAME_RE = re.compile(r"^@?([A-Za-z0-9_]{5,})$")

def expand_targets(raw_targets):
    """Split on commas/whitespace; dedup while preserving order."""
    out, seen = [], set()
    for entry in raw_targets or []:
        if not entry: continue
        for tok in SPLIT_RE.split(str(entry).strip()):
            t = tok.strip().rstrip("/.,")
            if not t: continue
            if t not in seen:
                seen.add(t); out.append(t)
    return out

def extract_username_from_link(s: str):
    """Convert t.me/username to username; keep invite links None (unless join allowed)."""
    if not s or not s.startswith("http"):
        return None
    u = urlparse(s)
    if u.netloc.lower() != "t.me":
        return None
    path = (u.path or "").strip("/")

    if not path or path.startswith("+") or path.startswith("joinchat"):
        # invite links need join — we skip in NO-JOIN mode
        return None
    uname = path.split("/")[0]
    return uname if USERNAME_RE.match(uname) else None

def normalize_tokens(tokens):
    """Normalize to: numeric IDs, bare usernames, or original (invite links get skipped later)."""
    norm = []
    for t in tokens:
        s = str(t).strip()
        if not s: continue
        if s.lstrip("-").isdigit():           # numeric chat id
            norm.append(s); continue
        m = USERNAME_RE.match(s.lstrip("@"))  # @username / username
        if m:
            norm.append(m.group(1)); continue
        u = extract_username_from_link(s)     # t.me/username
        if u:
            norm.append(u); continue
        # invite link or unknown — keep as-is, but will be skipped in NO-JOIN mode
        norm.append(s)
    # de-dup keep order
    seen, out = set(), []
    for x in norm:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

def _parse_mode(s: str | None):
    if not s: return None
    s = str(s).strip().lower()
    if s in ("markdown", "md"): return "markdown"
    if s in ("html", "htm"): return "html"
    return None  # plain

def is_night_now_ist() -> bool:
    now = datetime.now(IST).time()
    return NIGHT_START <= now < NIGHT_END

def _next_slot_index(user_id: int, total_slots: int) -> int:
    """
    Round-robin session slot for a user; persisted in settings:
    key = worker:last_session:{user_id}
    """
    key = f"worker:last_session:{user_id}"
    try:
        cur = int(get_setting(key, -1) or -1)
    except Exception:
        cur = -1
    nxt = (cur + 1) % max(1, total_slots)
    set_setting(key, nxt)
    return nxt

async def _notify_rehydrate(user_id: int, slot: int, reason: str):
    """DM user to re-login a broken session; respects cooldown."""
    if BOT_NOTIFIER is None:
        return
    key = f"authping:{user_id}:{slot}"
    last = int(get_setting(key, 0) or 0)
    now = int(_time.time())
    if now - last < AUTH_PING_COOLDOWN_SEC:
        return
    set_setting(key, now)
    msg = (
        "✇ Session issue detected\n"
        f"✇ Your account (slot {slot}) looks <b>expired/unauthorized</b>.\n"
        "✇ Please login again via <b>@SpinifyLoginBot</b>.\n"
        f"✇ Reason: <code>{reason}</code>"
    )
    try:
        await BOT_NOTIFIER.send_message(user_id, msg)
    except Exception:
        pass

# ---------------- chat/target resolution ----------------
async def resolve_target_chat(app: Client, target: str):
    """
    Resolve target to a chat. NO-JOIN mode — we never join invites.
    Supported:
      - numeric IDs (e.g., -1001234567890)
      - usernames (foo_bar) or '@foo_bar'
      - t.me/username
    Invite links t.me/+... are skipped with a log.
    """
    # numeric id
    if target.lstrip("-").isdigit():
        try:
            return await app.get_chat(int(target))
        except Exception as e:
            log.info(f"[resolve] id {target} failed: {e}")
            return None
    # username
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
    # link
    if target.startswith("http"):
        # invite links: will require join => skip in NO-JOIN mode
        u = urlparse(target)
        if (u.netloc or "").lower() == "t.me" and ((u.path or "").startswith("/+")
                                                   or (u.path or "").startswith("/joinchat")):
            log.info(f"[resolve] invite link skipped (NO-JOIN): {target}")
            return None
        uname = extract_username_from_link(target)
        if uname:
            try:
                return await app.get_chat(uname)
            except Exception as e:
                log.info(f"[resolve] link→@{uname} failed: {e}")
                return None
    # unknown pattern
    log.info(f"[resolve] unsupported target format: {target}")
    return None

# ---------------- core send ----------------
async def _send_via_session(sess: dict, targets: list[str], text: str, parse_mode: str | None) -> int:
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
            log.info(f"[u{sess['user_id']}] unresolved/unsupported target: {tgt}")
            continue
        try:
            await app.send_message(chat_id=chat.id, text=text, parse_mode=parse_mode)
            ok += 1
            log.info(f"[u{sess['user_id']} s{sess['slot']}] sent to {getattr(chat,'username',None) or chat.id}")
            await asyncio.sleep(0.5)  # gentle
        except FloodWait as fw:
            log.warning(f"[u{sess['user_id']} s{sess['slot']}] FloodWait {fw.value}s on {chat.id}")
            await asyncio.sleep(fw.value + 1)
        except UserNotParticipant:
            log.info(f"[u{sess['user_id']}] not a participant in {getattr(chat,'username',None) or chat.id} (NO-JOIN)")
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

# ---------------- per-user cycle ----------------
def _user_paused(uid: int) -> bool:
    v = str(get_setting(f"user:{uid}:paused", "0")).lower()
    return v in ("1", "true", "yes", "on")

def _due_in_seconds(uid: int) -> int:
    """Return seconds remaining until next send (0 if due)."""
    interval = int(get_interval(uid) or 30) * 60
    last = get_last_sent_at(uid)
    if last is None:
        return 0
    now = int(datetime.utcnow().timestamp())
    remain = last + interval - now
    return max(0, remain)

async def process_user(uid: int):
    # Global night mode
    if night_enabled() and is_night_now_ist():
        return
    # Per-user pause
    if _user_paused(uid):
        return

    text, mode = get_ad(uid)
    if not text:
        log.info(f"[u{uid}] no ad text set")
        return

    raw_groups = list_groups(uid)
    if not raw_groups:
        log.info(f"[u{uid}] no groups configured")
        return
    targets = normalize_tokens(expand_targets(raw_groups))
    if not targets:
        log.info(f"[u{uid}] no valid targets after normalization")
        return

    sessions = sessions_strings(uid)
    if not sessions:
        log.info(f"[u{uid}] no sessions")
        return

    remain = _due_in_seconds(uid)
    if remain > 0:
        log.info(f"[u{uid}] not due yet ({remain}s left)")
        return

    # round-robin pick a session
    idx = _next_slot_index(uid, len(sessions))
    sess = sessions[idx]

    sent = await _send_via_session(sess, targets, text, _parse_mode(mode))
    if sent > 0:
        mark_sent_now(uid)
        inc_sent_ok(uid, sent)
        log.info(f"[u{uid}] sent_ok+={sent}")
    else:
        log.info(f"[u{uid}] nothing sent this tick")

# ---------------- main loop ----------------
async def main_loop():
    init_db()
    SLEEP_STEP = int(os.getenv("WORKER_TICK_SEC", "15"))  # tick every 15s
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
        await asyncio.sleep(SLEEP_STEP)

async def main():
    await main_loop()

if __name__ == "__main__":
    asyncio.run(main())
