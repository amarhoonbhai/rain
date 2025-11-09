# worker_forward.py — precise interval scheduler + 10s per-group delay + no-join mode
import os, asyncio, logging, re, time as _time
from urllib.parse import urlparse
from datetime import datetime, time
from zoneinfo import ZoneInfo

from pyrogram import Client
from pyrogram.errors import (
    FloodWait, RPCError,
    Unauthorized, AuthKeyUnregistered,
)
try:
    from pyrogram.errors import SessionRevoked, SessionExpired, UserDeactivated, UserDeactivatedBan, UsernameInvalid, UsernameNotOccupied
except Exception:  # best-effort fallbacks
    class _E(Exception): ...
    SessionRevoked = SessionExpired = UserDeactivated = UserDeactivatedBan = UsernameInvalid = UsernameNotOccupied = _E

from core.db import (
    init_db,
    users_with_sessions, sessions_strings,
    list_groups,
    get_ad, get_interval, get_last_sent_at, mark_sent_now,
    night_enabled, inc_sent_ok,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("worker")

# --------- time gates ----------
IST = ZoneInfo("Asia/Kolkata")
NIGHT_START = time(0, 0)
NIGHT_END   = time(7, 0)

def _is_night_now_ist() -> bool:
    now = datetime.now(IST).time()
    return NIGHT_START <= now < NIGHT_END

# --------- tokens / targets ----------
SPLIT_RE = re.compile(r"[,\s]+")
USERNAME_RE = re.compile(r"^@?([A-Za-z0-9_]{5,})$")

def expand_targets(rows: list[str]) -> list[str]:
    out, seen = [], set()
    for raw in rows or []:
        if not raw: continue
        for t in SPLIT_RE.split(str(raw).strip()):
            t = t.strip().rstrip("/.,")
            if not t: continue
            if t not in seen:
                seen.add(t); out.append(t)
    return out

def extract_username_from_link(s: str) -> str | None:
    # Converts https://t.me/username → username
    if not s.startswith("http"): return None
    u = urlparse(s)
    if u.netloc.lower() != "t.me": return None
    path = u.path.strip("/")
    if not path or path.startswith("+") or path.startswith("joinchat"):
        return None  # invite link → NO-JOIN (kept in DB; skip at send-time)
    uname = path.split("/")[0]
    return uname if USERNAME_RE.match(uname) else None

def normalize_tokens(tokens: list[str]) -> list[str]:
    norm = []
    for t in tokens:
        if t.lstrip("-").isdigit():         # numeric id (public/private) → keep
            norm.append(t); continue
        m = USERNAME_RE.match(t.lstrip("@"))
        if m:                               # @username → store bare username
            norm.append(m.group(1)); continue
        u = extract_username_from_link(t)   # t.me/username
        if u:
            norm.append(u); continue
        norm.append(t)  # keep invite links / unknowns; we’ll skip at runtime
    # de-dup, preserve order
    seen, out = set(), []
    for x in norm:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

# --------- chat resolution (NO-JOIN) ----------
async def resolve_chat(app: Client, target: str):
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
        try:
            return await app.get_chat(m.group(1))
        except (UsernameInvalid, UsernameNotOccupied):
            log.info(f"[resolve] @{m.group(1)} invalid/not occupied"); return None
        except Exception as e:
            log.info(f"[resolve] @{m.group(1)} failed: {e}"); return None
    # links
    if target.startswith("http"):
        u = extract_username_from_link(target)
        if u:
            try:
                return await app.get_chat(u)
            except Exception as e:
                log.info(f"[resolve] link→@{u} failed: {e}")
                return None
        # invite link (private) — require manual join
        log.info(f"[resolve] invite link kept (NO-JOIN): {target}")
        return None
    return None

# --------- send with 10s per-group delay ----------
async def _send_round(sess: dict, targets: list[str], text: str, parse_mode: str | None) -> int:
    ok = 0
    app = Client(
        name=f"user-{sess['user_id']}-s{sess['slot']}",
        api_id=int(sess["api_id"]),
        api_hash=str(sess["api_hash"]),
        session_string=str(sess["session_string"]),
    )
    try:
        await app.start()
    except (Unauthorized, AuthKeyUnregistered, SessionRevoked, SessionExpired, UserDeactivated, UserDeactivatedBan) as e:
        log.error(f"[u{sess['user_id']} s{sess['slot']}] auth/start error: {e}")
        return 0
    except Exception as e:
        log.error(f"[u{sess['user_id']} s{sess['slot']}] start failed: {e}")
        return 0

    for i, tgt in enumerate(targets, 1):
        chat = await resolve_chat(app, tgt)
        if chat is None:
            log.info(f"[u{sess['user_id']}] skip (not resolved / invite-only): {tgt}")
        else:
            try:
                await app.send_message(chat_id=chat.id, text=text, parse_mode=parse_mode)
                ok += 1
                log.info(f"[u{sess['user_id']} s{sess['slot']}] sent to {getattr(chat,'username',None) or chat.id} ({i}/{len(targets)})")
            except FloodWait as fw:
                log.warning(f"[u{sess['user_id']} s{sess['slot']}] FloodWait {fw.value}s → waiting")
                await asyncio.sleep(fw.value + 1)
            except RPCError as e:
                log.warning(f"[u{sess['user_id']} s{sess['slot']}] RPCError on {tgt}: {e}")
            except Exception as e:
                log.warning(f"[u{sess['user_id']} s{sess['slot']}] send failed on {tgt}: {e}")

        # fixed 10s delay between each group (even if previous failed)
        if i < len(targets):
            await asyncio.sleep(10)

    try:
        await app.stop()
    except Exception:
        pass
    return ok

# --------- scheduler (exactly mirrors /fstats logic) ----------
def _parse_mode_string(s: str | None):
    if not s: return None
    s = s.strip().lower()
    if s in ("markdown", "md"): return "markdown"
    if s in ("html", "htm"): return "html"
    return None

def _next_slot_index(user_id: int, total: int) -> int:
    # simple round-robin across user’s sessions
    key = f"worker:last_session:{user_id}"
    try:
        import core.db as db
        cur = int(db.get_setting(key, -1) or -1)
        nxt = (cur + 1) % max(1, total)
        db.set_setting(key, nxt)
        return nxt
    except Exception:
        return 0

async def process_user(user_id: int):
    # night mode hard-gate
    if night_enabled() and _is_night_now_ist():
        return

    # must have text & groups & sessions
    text, mode = get_ad(user_id)
    if not text:
        log.info(f"[u{user_id}] no ad text set"); return
    groups = list_groups(user_id)
    if not groups:
        log.info(f"[u{user_id}] no groups configured"); return
    sessions = sessions_strings(user_id)
    if not sessions:
        log.info(f"[u{user_id}] no sessions"); return

    # interval check
    interval_min = int(get_interval(user_id) or 30)
    interval_sec = interval_min * 60
    last_ts = get_last_sent_at(user_id)
    now = int(_time.time())
    if last_ts is not None:
        try: last_ts = int(last_ts)
        except Exception: last_ts = int(float(str(last_ts)))
        left = interval_sec - (now - last_ts)
        if left > 0:
            log.info(f"[u{user_id}] not due yet ({left}s left)")
            return

    # pick a session and send to ALL targets with 10s gaps
    idx = _next_slot_index(user_id, len(sessions))
    sess = sessions[idx]

    targets = normalize_tokens(expand_targets(groups))
    if not targets:
        log.info(f"[u{user_id}] no valid targets after normalization")
        return

    sent_ok = await _send_round(sess, targets, text, _parse_mode_string(mode))
    if sent_ok > 0:
        mark_sent_now(user_id)
        inc_sent_ok(user_id, sent_ok)
        log.info(f"[u{user_id}] round complete: sent_ok+={sent_ok}")
    else:
        log.info(f"[u{user_id}] round complete: nothing sent")

async def main_loop():
    init_db()
    while True:
        try:
            for uid in users_with_sessions():
                try:
                    await process_user(uid)
                except Exception as e:
                    log.error(f"[u{uid}] process error: {e}")
                await asyncio.sleep(0.2)  # small breath per user
        except Exception as e:
            log.error(f"loop error: {e}")
        await asyncio.sleep(10)  # tick every 10s

async def main():
    await main_loop()

if __name__ == "__main__":
    asyncio.run(main())
