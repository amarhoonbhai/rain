# worker_forward.py — Saved-Pinned sender; accepts ANY stored group tokens; DM reminder for invite links
# - Uses pinned message from Saved Messages per session (text/media, premium emojis supported).
# - Accepts any saved token: @username, numeric id, https://t.me/username, private invites.
# - NO-JOIN: we *never* auto-join. Private invites are skipped unless the account is already a member.
# - When encountering invite links, we log and (once/day) DM the user a reminder to join manually.

import os, re, asyncio, logging, time as _time
from datetime import datetime, time
from urllib.parse import urlparse
from zoneinfo import ZoneInfo
from typing import Optional, List, Dict, Any

from aiogram import Bot  # notifier for DMs (optional)

from pyrogram import Client
from pyrogram.errors import (
    FloodWait, RPCError, UsernameInvalid, UsernameNotOccupied,
    Unauthorized, AuthKeyUnregistered, UserDeactivated, UserDeactivatedBan
)
try:
    from pyrogram.errors import SessionRevoked, SessionExpired, UserNotParticipant
except Exception:
    class _X(Exception): pass
    SessionRevoked = SessionExpired = UserNotParticipant = _X

from core.db import (
    init_db,
    users_with_sessions, sessions_strings,
    list_groups,
    get_ad, get_interval, get_last_sent_at, mark_sent_now,
    night_enabled, set_setting, get_setting, inc_sent_ok
)

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"))
log = logging.getLogger("worker")

IST = ZoneInfo("Asia/Kolkata")
NIGHT_START = time(0,0)
NIGHT_END   = time(7,0)

NO_AUTO_JOIN = True  # hard off

BOT_TOKEN = (os.getenv("MAIN_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
BOT_NOTIFIER = Bot(BOT_TOKEN) if BOT_TOKEN and ":" in BOT_TOKEN else None
AUTH_PING_COOLDOWN_SEC = 6 * 3600  # 6h
INVITE_DM_COOLDOWN_SEC = 24 * 3600  # per target reminder, once/day

# ---------- helpers ----------
def is_night_now_ist() -> bool:
    now = datetime.now(IST).time()
    return NIGHT_START <= now < NIGHT_END

def _parse_mode_string(s: Optional[str]) -> Optional[str]:
    if not s: return None
    s = s.strip().lower()
    if s in ("markdown","md"): return "markdown"
    if s in ("html","htm"):   return "html"
    return None

def _user_paused(uid: int) -> bool:
    v = str(get_setting(f"user:{uid}:paused", "0")).lower()
    return v in ("1","true","yes","on")

SPLIT_RE = re.compile(r"[,\s]+")
USERNAME_RE = re.compile(r"^@?([A-Za-z0-9_]{5,})$")

def _expand_tokens(raw: List[str]) -> List[str]:
    out, seen = [], set()
    for entry in raw or []:
        if not entry: continue
        for tok in SPLIT_RE.split(entry.strip()):
            t = tok.strip().rstrip("/.,")
            if not t: continue
            if t not in seen:
                seen.add(t); out.append(t)
    return out

def _extract_username_from_link(s: str) -> Optional[str]:
    if not s.startswith("http"): return None
    u = urlparse(s)
    if u.netloc.lower() != "t.me": return None
    path = u.path.strip("/")
    if not path or path.startswith("+") or path.startswith("joinchat"):
        return None
    uname = path.split("/")[0]
    return uname if USERNAME_RE.match(uname) else None

def _normalize_tokens(tokens: List[str]) -> List[str]:
    norm = []
    for t in tokens:
        if t.lstrip("-").isdigit():
            norm.append(t); continue
        m = USERNAME_RE.match(t.lstrip("@"))
        if m:
            norm.append(m.group(1)); continue
        u = _extract_username_from_link(t)
        if u:
            norm.append(u); continue
        norm.append(t)  # keep invites/unknown as-is
    # de-dup preserve order
    seen, out = set(), []
    for x in norm:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

async def _notify_dm(uid: int, text: str, throttle_key: str, throttle_sec: int):
    if not BOT_NOTIFIER: return
    now = int(_time.time())
    last = int(get_setting(throttle_key, "0") or 0)
    if now - last < throttle_sec:
        return
    set_setting(throttle_key, now)
    try:
        await BOT_NOTIFIER.send_message(uid, text)
    except Exception:
        pass

async def _notify_invite_reminder(uid: int, target: str):
    hkey = f"invite_rem:{uid}:{hash(target) & 0xffffffff}"
    await _notify_dm(uid,
        "✇ Private invite saved.\n"
        "✇ Make sure your SENDER account already joined this group, or the worker will skip it.",
        hkey, INVITE_DM_COOLDOWN_SEC
    )

async def _notify_rehydrate(uid: int, slot: int, reason: str):
    await _notify_dm(uid,
        f"✇ Session issue on slot {slot}: <b>{reason}</b>\n"
        "✇ Please re-login via @SpinifyLoginBot.",
        f"authping:{uid}:{slot}", AUTH_PING_COOLDOWN_SEC
    )

# ---------- Saved Messages (pinned) fetch ----------
async def _fetch_saved_pinned(app: Client):
    try:
        async for m in app.get_chat_history("me", limit=100):
            if getattr(m, "pinned", False):
                if m.text:
                    return ("text", {"text": m.text, "entities": m.entities or []})
                for attr in ("photo","video","document","animation"):
                    media = getattr(m, attr, None)
                    if media and getattr(media, "file_id", None):
                        return ("media", {
                            "kind": attr,
                            "file_id": media.file_id,
                            "caption": m.caption or "",
                            "caption_entities": m.caption_entities or []
                        })
                return None
        return None
    except Exception as e:
        log.info(f"[saved] fetch pinned failed: {e}")
        return None

async def _send_any(app: Client, chat_id: int, content, fallback_text: Optional[str], parse_mode: Optional[str]):
    if content:
        k, p = content
        if k == "text":
            if p.get("entities"): await app.send_message(chat_id, p["text"], entities=p["entities"])
            else:                 await app.send_message(chat_id, p["text"], parse_mode=parse_mode)
            return
        if k == "media":
            fid = p["file_id"]; cap = p.get("caption") or ""; cents = p.get("caption_entities") or []
            kind = p["kind"]
            if kind == "photo":
                if cents: await app.send_photo(chat_id, fid, caption=cap, caption_entities=cents)
                else:     await app.send_photo(chat_id, fid, caption=cap, parse_mode=parse_mode)
            elif kind == "video":
                if cents: await app.send_video(chat_id, fid, caption=cap, caption_entities=cents)
                else:     await app.send_video(chat_id, fid, caption=cap, parse_mode=parse_mode)
            elif kind == "document":
                if cents: await app.send_document(chat_id, fid, caption=cap, caption_entities=cents)
                else:     await app.send_document(chat_id, fid, caption=cap, parse_mode=parse_mode)
            elif kind == "animation":
                if cents: await app.send_animation(chat_id, fid, caption=cap, caption_entities=cents)
                else:     await app.send_animation(chat_id, fid, caption=cap, parse_mode=parse_mode)
            else:
                await app.send_message(chat_id, cap or "(unsupported media)")
            return
    if fallback_text:
        await app.send_message(chat_id, fallback_text, parse_mode=parse_mode)

# ---------- resolving ----------
async def _resolve_target_chat(app: Client, target: str):
    # numeric id
    if target.lstrip("-").isdigit():
        try:
            return await app.get_chat(int(target))
        except Exception as e:
            log.info(f"[resolve] id {target} failed: {e}"); return None
    # @username
    m = USERNAME_RE.match(target.lstrip("@"))
    if m:
        uname = m.group(1)
        try:
            return await app.get_chat(uname)
        except (UsernameInvalid, UsernameNotOccupied):
            log.info(f"[resolve] @{uname} invalid/not occupied"); return None
        except Exception as e:
            log.info(f"[resolve] @{uname} failed: {e}"); return None
    # links
    if target.startswith("http"):
        u = urlparse(target)
        if u.netloc.lower() == "t.me":
            path = u.path.strip("/")
            if path and not path.startswith("+") and not path.startswith("joinchat"):
                # public t.me/username
                try:
                    return await app.get_chat(path.split("/")[0])
                except Exception as e:
                    log.info(f"[resolve] link→{path} failed: {e}")
                    return None
            else:
                # private invite — NO-JOIN: just remind/skip
                return "INVITE_LINK"
    return None

# ---------- slot cooldown ----------
def _slot_cool_key(uid:int, slot:int) -> str:
    return f"slot:{uid}:{slot}:cooldown_until"

def _block_session(uid:int, slot:int, seconds:int):
    from core.db import set_setting
    set_setting(_slot_cool_key(uid, slot), int(_time.time()) + int(seconds))

def _is_blocked(uid:int, slot:int) -> bool:
    from core.db import get_setting
    until = int(get_setting(_slot_cool_key(uid, slot), 0) or 0)
    return until > int(_time.time())

def _next_slot_index(uid: int, total: int) -> int:
    from core.db import get_setting, set_setting
    key = f"worker:last_session:{uid}"
    cur = int(get_setting(key, -1) or -1)
    nxt = (cur + 1) % max(1, total)
    set_setting(key, nxt)
    return nxt

def _pick_session(sessions: List[Dict[str,Any]], uid: int) -> Optional[Dict[str,Any]]:
    total = len(sessions)
    start = _next_slot_index(uid, total)
    for k in range(total):
        idx = (start + k) % total
        s = sessions[idx]
        if not _is_blocked(uid, s["slot"]):
            from core.db import set_setting
            set_setting(f"worker:last_session:{uid}", idx)
            return s
    return None

# ---------- per-user processing ----------
async def _send_via_session(sess: Dict[str,Any], targets: List[str],
                            db_text: Optional[str], parse_mode: Optional[str]) -> int:
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
        _block_session(sess["user_id"], sess["slot"], 24*3600)
        log.error(f"[u{sess['user_id']} s{sess['slot']}] start auth error: {e}")
        return 0
    except Exception as e:
        log.error(f"[u{sess['user_id']} s{sess['slot']}] start failed: {e}")
        return 0

    saved_content = await _fetch_saved_pinned(app)

    async def _send_one(t: str) -> int:
        chat = await _resolve_target_chat(app, t)
        if chat is None:
            log.info(f"[u{sess['user_id']}] unresolved: {t}")
            return 0
        if chat == "INVITE_LINK":
            log.info(f"[u{sess['user_id']}] invite link saved; NO-JOIN; remind user.")
            await _notify_invite_reminder(sess["user_id"], t)
            return 0
        try:
            await _send_any(app, chat.id, saved_content, db_text, parse_mode)
            await asyncio.sleep(0.35)
            return 1
        except FloodWait as fw:
            log.warning(f"[u{sess['user_id']} s{sess['slot']}] FloodWait {fw.value}s on {chat.id}")
            await asyncio.sleep(fw.value + 1)
        except UserNotParticipant:
            log.info(f"[u{sess['user_id']}] not a participant in {getattr(chat,'username',None) or chat.id} (NO-JOIN)")
        except (Unauthorized, AuthKeyUnregistered, SessionRevoked, SessionExpired, UserDeactivated, UserDeactivatedBan) as e:
            await _notify_rehydrate(sess["user_id"], sess["slot"], e.__class__.__name__)
            _block_session(sess["user_id"], sess["slot"], 24*3600)
            log.error(f"[u{sess['user_id']} s{sess['slot']}] auth error on send: {e}")
        except RPCError as e:
            if "PEER_FLOOD" in str(e):
                _block_session(sess["user_id"], sess["slot"], 24*3600)
            log.warning(f"[u{sess['user_id']} s{sess['slot']}] RPCError on {chat.id}: {e}")
        except Exception as e:
            log.warning(f"[u{sess['user_id']} s{sess['slot']}] send failed on {chat.id}: {e}")
        return 0

    sem = asyncio.Semaphore(3)
    async def _wrap(x): 
        async with sem: 
            return await _send_one(x)

    try:
        results = await asyncio.gather(*[_wrap(t) for t in targets])
        ok = sum(results)
    finally:
        try: await app.stop()
        except Exception: pass
    return ok

async def process_user(uid: int):
    if night_enabled() and is_night_now_ist():
        return
    if _user_paused(uid):
        return

    groups = list_groups(uid)
    if not groups:
        log.info(f"[u{uid}] no groups"); return
    tokens = _normalize_tokens(_expand_tokens(groups))
    if not tokens:
        log.info(f"[u{uid}] no usable targets"); return

    sessions = sessions_strings(uid)
    if not sessions:
        log.info(f"[u{uid}] no sessions"); return

    interval = get_interval(uid) or 30
    last_ts = get_last_sent_at(uid)
    now = int(datetime.utcnow().timestamp())
    if last_ts is not None and now - last_ts < interval*60:
        remain = interval*60 - (now - last_ts)
        log.info(f"[u{uid}] not due yet ({remain}s left)"); return

    sess = _pick_session(sessions, uid)
    if not sess:
        log.info(f"[u{uid}] sessions cooling down"); return

    db_text, db_mode = get_ad(uid)
    sent = await _send_via_session(sess, tokens, db_text, _parse_mode_string(db_mode))
    if sent > 0:
        mark_sent_now(uid)
        inc_sent_ok(uid, sent)
        log.info(f"[u{uid}] sent_ok+={sent}")
    else:
        log.info(f"[u{uid}] nothing sent this tick")

async def main_loop():
    init_db()
    while True:
        try:
            for uid in users_with_sessions():
                try:
                    await process_user(uid)
                except Exception as e:
                    log.error(f"[u{uid}] process error: {e}")
                await asyncio.sleep(0.25)
        except Exception as e:
            log.error(f"loop error: {e}")
        await asyncio.sleep(15)

async def main():
    await main_loop()

if __name__ == "__main__":
    asyncio.run(main())
