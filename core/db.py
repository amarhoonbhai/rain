# worker_forward.py — NO-JOIN sender + invite-link aware + auto DB normalize + auto-rehydrate
import os, asyncio, logging, re
from urllib.parse import urlparse
from datetime import datetime, time
from zoneinfo import ZoneInfo

from aiogram import Bot  # only for DM notify on expired sessions

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
    list_groups, add_group, clear_groups,
    get_ad, get_interval, get_last_sent_at, mark_sent_now,
    night_enabled, set_setting, get_setting, inc_sent_ok
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("worker")

# ---------- time/locale ----------
IST = ZoneInfo("Asia/Kolkata")
NIGHT_START = time(0, 0)   # 00:00
NIGHT_END   = time(7, 0)   # 07:00

# ---------- behavior flags (env-tunable) ----------
# accept private invite links like t.me/+AbCd… / t.me/joinchat/AAAA…
ALLOW_INVITE_LINKS = os.getenv("ALLOW_INVITE_LINKS", "1") == "1"   # default ON
# keep no-join by default; flip to "0" only if you want worker to join
NO_AUTO_JOIN       = os.getenv("NO_AUTO_JOIN", "1") == "1"         # default ON
# allow auto-joining from invite links (effective only if NO_AUTO_JOIN == False)
JOIN_FROM_INVITE   = os.getenv("JOIN_FROM_INVITE", "0") == "1"     # default OFF

# accept only numeric targets (drop usernames/normal links). still keeps invite links if allowed
TARGET_NUMERIC_ONLY = os.getenv("TARGET_NUMERIC_ONLY", "1") == "1"

# auto-rehydrate notifier
BOT_TOKEN = (os.getenv("MAIN_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
BOT_NOTIFIER = Bot(BOT_TOKEN) if BOT_TOKEN and ":" in BOT_TOKEN else None
AUTH_PING_COOLDOWN_SEC = 6 * 3600  # 6h

def _as_int(v, default=0):
    try: return int(str(v))
    except Exception: return default

def _now_ts() -> int:
    return int(datetime.utcnow().timestamp())

def is_night_now_ist() -> bool:
    now = datetime.now(IST).time()
    return NIGHT_START <= now < NIGHT_END

def _parse_mode_string(s: str | None):
    if not s: return None
    s = s.strip().lower()
    if s in ("markdown","md"): return "markdown"
    if s in ("html","htm"):    return "html"
    return None

# ---------- splitting/normalizing targets ----------
SPLIT_RE = re.compile(r"[,\s]+")
USERNAME_RE = re.compile(r"^@?([A-Za-z0-9_]{5,})$")
INVITE_RE = re.compile(r"^(?:https?://)?t\.me/(?:\+|joinchat/)([A-Za-z0-9_\-]+)$", re.IGNORECASE)

def is_invite_link(s: str) -> bool:
    return bool(INVITE_RE.match((s or "").strip()))

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
    """
    - Keeps numeric ids always
    - Keeps private invite links if ALLOW_INVITE_LINKS
    - If TARGET_NUMERIC_ONLY==1, drops other non-numeric items (usernames/normal links)
    """
    norm = []
    for t in tokens:
        t = (t or "").strip()
        if not t:
            continue

        # numeric chat ids (e.g., -1001234567890)
        if t.lstrip("-").isdigit():
            norm.append(t)
            continue

        # invite links
        if ALLOW_INVITE_LINKS and is_invite_link(t):
            norm.append(t)
            continue

        # numeric-only mode → drop everything else
        if TARGET_NUMERIC_ONLY:
            continue

        # fallback: usernames / t.me/username
        m = USERNAME_RE.match(t.lstrip("@"))
        if m:
            norm.append(m.group(1)); continue

        u = extract_username_from_link(t)
        if u:
            norm.append(u); continue

        # keep as-is (unknown)
        norm.append(t)

    # de-dup while preserving order
    seen, out = set(), []
    for x in norm:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

def _next_slot_index(user_id: int, total_slots: int) -> int:
    key = f"worker:last_session:{user_id}"
    cur = _as_int(get_setting(key, -1), -1)
    nxt = (cur + 1) % max(1, total_slots)
    set_setting(key, nxt)
    return nxt

async def _notify_rehydrate(user_id: int, slot: int, reason: str):
    if BOT_NOTIFIER is None: return
    key = f"authping:{user_id}:{slot}"
    last = _as_int(get_setting(key, 0), 0)
    now = _now_ts()
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
    - numeric id -> get_chat(int)
    - username   -> get_chat('username')  (only if TARGET_NUMERIC_ONLY=0 and input survived normalize)
    - t.me/username -> by username
    - invite links (t.me/+… / joinchat/…):
        * if already member, get_chat(invite) usually works
        * if not member and NO_AUTO_JOIN, skip
        * if not member and NO_AUTO_JOIN==False and JOIN_FROM_INVITE==True, try join_chat(invite)
    """
    # numeric id
    if target.lstrip("-").isdigit():
        try:
            return await app.get_chat(int(target))
        except Exception as e:
            log.info(f"[resolve] id {target} failed: {e}")
            return None

    # invite links
    if ALLOW_INVITE_LINKS and is_invite_link(target):
        try:
            # if already a member, this often resolves
            return await app.get_chat(target)
        except UserNotParticipant:
            if not NO_AUTO_JOIN and JOIN_FROM_INVITE:
                try:
                    chat = await app.join_chat(target)
                    return chat
                except Exception as e:
                    log.info(f"[resolve] join from invite failed: {e}")
                    return None
            else:
                log.info(f"[resolve] invite requires join (NO_AUTO_JOIN): {target}")
                return None
        except Exception as e:
            log.info(f"[resolve] invite get_chat failed: {e}")
            return None

    # @username / t.me/username (only if not strict numeric-only)
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

    if target.startswith("http"):
        u = extract_username_from_link(target)
        if u:
            try:
                return await app.get_chat(u)
            except Exception as e:
                log.info(f"[resolve] link→@{u} failed: {e}")
                return None
        log.info(f"[resolve] unsupported link: {target}")
        return None

    return None

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
            await asyncio.sleep(0.5)
        except FloodWait as fw:
            wait = _as_int(getattr(fw, "value", 30), 30)
            log.warning(f"[u{sess['user_id']} s{sess['slot']}] FloodWait {wait}s on {getattr(chat,'id',None) or tgt}")
            await asyncio.sleep(wait + 1)
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

def _normalize_groups_in_db_once(user_id: int):
    key = f"norm:groups:{user_id}"
    if _as_int(get_setting(key, 0), 0):
        return
    original = list_groups(user_id)
    tokens = normalize_tokens(expand_targets(original))
    if tokens and (len(tokens) != len(original) or set(tokens) != set(original)):
        clear_groups(user_id)
        added = 0
        for t in tokens:
            try: added += add_group(user_id, t)
            except Exception: pass
        log.info(f"[u{user_id}] normalized groups {len(original)}→{len(tokens)}, added={added}")
    set_setting(key, 1)

def _consume_sendnow_flag(user_id: int) -> bool:
    key = f"user:{user_id}:sendnow"
    v = _as_int(get_setting(key, 0), 0)
    if v:
        set_setting(key, 0)
        return True
    return False

async def process_user(user_id: int):
    # respect per-user pause
    paused = str(get_setting(f"user:{user_id}:paused", 0) or "0").lower() in ("1", "true")
    if paused:
        return

    if night_enabled() and is_night_now_ist():
        return

    _normalize_groups_in_db_once(user_id)

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

    interval_min = _as_int(get_interval(user_id) or 30, 30)
    last_ts = get_last_sent_at(user_id)
    now = _now_ts()

    force_now = _consume_sendnow_flag(user_id)

    if not force_now and last_ts is not None and now - _as_int(last_ts, 0) < interval_min * 60:
        remain = interval_min*60 - (now - _as_int(last_ts, 0))
        log.info(f"[u{user_id}] not due yet ({remain}s left)")
        return

    idx = _next_slot_index(user_id, len(sessions))
    sess = sessions[idx]

    targets = normalize_tokens(expand_targets(groups))
    if not targets:
        log.info(f"[u{user_id}] no valid targets after expansion")
        return

    sent = await _send_via_session(sess, targets, text, _parse_mode_string(mode))
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
    import asyncio
    asyncio.run(main())
