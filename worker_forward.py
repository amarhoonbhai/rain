# worker_forward.py — Saved-All forwarder (multi-user, Mongo DB)
# - Sends ALL messages from Saved Messages, oldest -> newest, cycling.
# - Per send tick: copies the NEXT ad to EACH saved target with ~30s gap.
# - Interval per user (default 30m) from DB; honors Night Mode via core.db.
# - Self-commands (from the logged-in account):
#     .help              -> show all commands
#     .addgc <lines>     -> add up to cap targets (handles @user, id, t.me links)
#     .gc                -> list targets
#     .cleargc           -> clear targets
#     .time 30m|45m|60m  -> set interval (minutes)
#     .adreset           -> reset ad pointer to start (oldest)
# - No filters.edited (uses custom filter).
# - Auto-rehydrate DM (optional): if session fails, notifies the owner via MAIN_BOT_TOKEN (no polling).
#
# Requires:
#   core.db (Mongo-backed) : init_db, users_with_sessions, sessions_list,
#   list_groups, add_group, clear_groups, groups_cap, get_interval, set_interval,
#   get_last_sent_at, mark_sent_now, inc_sent_ok, night_enabled, set_setting, get_setting
#
# ENV:
#   LOG_LEVEL=INFO
#   MAIN_BOT_TOKEN=xxxx   (optional; used only to send DM pings on session revoke)
#   OWNER_ID=123456789    (optional; recipient for crash pings)
#   PARALLEL_USERS=3      (optional; max concurrent users processed per tick)
#   PER_GROUP_DELAY=30    (seconds between groups on one tick; default 30)
#   SEND_TIMEOUT=60       (Telegram send timeout seconds; default 60)
#   RETRY_MIN=2 RETRY_MAX=10 (backoff bounds in seconds)
#
# Start via run_all.py. This file does NOT poll as a bot; no getUpdates conflict.

import os
import asyncio
import logging
from contextlib import suppress
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

from pyrogram import Client, filters, types, errors as perror

from core.db import (
    init_db,
    users_with_sessions, sessions_list,
    list_groups, add_group, clear_groups, groups_cap,
    get_interval, set_interval, get_last_sent_at, mark_sent_now,
    inc_sent_ok, night_enabled, set_setting, get_setting
)

# -------------- Logging --------------
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("worker")

# -------------- ENV --------------
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0") or 0)
PARALLEL_USERS = int(os.getenv("PARALLEL_USERS", "3") or 3)
PER_GROUP_DELAY = int(os.getenv("PER_GROUP_DELAY", "30") or 30)
SEND_TIMEOUT = int(os.getenv("SEND_TIMEOUT", "60") or 60)
RETRY_MIN = int(os.getenv("RETRY_MIN", "2") or 2)
RETRY_MAX = int(os.getenv("RETRY_MAX", "10") or 10)

# -------------- In-memory state --------------
# app_map[user_id][slot] -> Client
APP_MAP: Dict[int, Dict[int, Client]] = {}
# registered mark to avoid handler duplication
APP_HANDLERS: Dict[Tuple[int, int], bool] = {}
# cached ads list per user (list of message_ids, oldest->newest)
ADS_CACHE: Dict[int, List[int]] = {}
# next pointer per user (persisted in DB settings as savedall:idx:<uid>)
POINTER_KEY = "savedall:idx:{}"

# -------------- Utils --------------
def _now() -> int:
    return int(datetime.now(timezone.utc).timestamp())

def _fmt_exc(e: Exception) -> str:
    return f"{e.__class__.__name__}: {e}"

def _get_pointer(uid: int) -> int:
    return int(get_setting(POINTER_KEY.format(uid), 0) or 0)

def _set_pointer(uid: int, val: int) -> None:
    set_setting(POINTER_KEY.format(uid), int(val))

def _not_edited(_, __, m: types.Message) -> bool:
    # Pyrogram-safe "not edited" check
    return not bool(getattr(m, "edit_date", None))

not_edited = filters.create(_not_edited)

async def _bot_ping(text: str):
    """Send a one-way diagnostic ping to OWNER_ID via MAIN_BOT_TOKEN (no polling)."""
    if not (MAIN_BOT_TOKEN and OWNER_ID):
        return
    try:
        from aiogram import Bot
        bot = Bot(MAIN_BOT_TOKEN)
        with suppress(Exception):
            await bot.send_message(OWNER_ID, text)
        await bot.session.close()
    except Exception:
        pass

# -------------- Pyrogram session lifecycle --------------
async def start_session(user_id: int, slot: int, api_id: int, api_hash: str, session_string: str) -> Optional[Client]:
    key = (user_id, slot)
    if user_id not in APP_MAP:
        APP_MAP[user_id] = {}
    # Stop & remove old
    old = APP_MAP[user_id].get(slot)
    if old:
        with suppress(Exception):
            await old.stop()
    app = Client(name=f"u{user_id}s{slot}", api_id=api_id, api_hash=api_hash, session_string=session_string, workdir=f"/tmp/rain_u{user_id}_s{slot}")
    try:
        await app.start()
        APP_MAP[user_id][slot] = app
        log.info("[u%ss%s] started", user_id, slot)
        register_session_handlers(app, user_id, slot)
        return app
    except perror.AuthKeyUnregistered:
        log.warning("[u%ss%s] session revoked", user_id, slot)
        await _bot_ping(f"⚠️ Session revoked for user {user_id}, slot {slot}. Ask them to /login again.")
        return None
    except Exception as e:
        log.error("[u%ss%s] start failed: %s", user_id, slot, _fmt_exc(e))
        return None

async def ensure_all_sessions_online():
    for uid in users_with_sessions():
        for row in sessions_list(uid):
            slot = int(row.get("slot"))
            if uid in APP_MAP and slot in APP_MAP[uid]:
                app = APP_MAP[uid][slot]
                if app.is_connected:
                    continue
                # try reconnect cleanly
                with suppress(Exception):
                    await app.connect()
                if app.is_connected:
                    continue
            # start new
            await start_session(uid, slot, int(row["api_id"]), str(row["api_hash"]), str(row["session_string"]))

def register_session_handlers(app: Client, user_id: int, slot: int):
    key = (user_id, slot)
    if APP_HANDLERS.get(key):
        return  # already attached

    # --- Commands from self (me) ---
    @app.on_message(filters.me & filters.text & not_edited)
    async def _on_me_text(_, m: types.Message):
        text = (m.text or "").strip()
        if not text.startswith("."):
            return
        cmd, *rest = text.split(maxsplit=1)
        arg = rest[0] if rest else ""

        if cmd == ".help":
            await m.reply_text(
                "📜 Commands (send from THIS account)\n"
                "• .help — show this\n"
                "• .addgc <targets> — add targets (1 per line). Supports @username, numeric ID, t.me links (public/private)\n"
                "• .gc — list saved targets (cap depends on unlock/premium)\n"
                "• .cleargc — remove all targets\n"
                "• .time 30m|45m|60m — set interval in minutes\n"
                "• .adreset — restart Saved-All cycle (from oldest)"
            )
        elif cmd == ".gc":
            targets = list_groups(user_id)
            cap = groups_cap(user_id)
            if not targets:
                await m.reply_text(f"✇ No targets saved. Cap {cap}. Use:\n.addgc <list>")
                return
            body = "\n".join(f"• {t}" for t in targets[:200])
            extra = "" if len(targets) <= 200 else f"\n… and {len(targets)-200} more"
            await m.reply_text(f"✇ Targets {len(targets)}/{cap}\n{body}{extra}")
        elif cmd == ".cleargc":
            clear_groups(user_id)
            await m.reply_text("✅ Cleared all targets.")
        elif cmd == ".addgc":
            # Accept multiline; up to cap
            lines = [ln.strip() for ln in arg.splitlines() if ln.strip()]
            if not lines:
                await m.reply_text("❌ Provide targets line-by-line after .addgc"); return
            added = 0
            cap = groups_cap(user_id)
            current = len(list_groups(user_id))
            for ln in lines:
                if current + added >= cap:
                    break
                if add_group(user_id, ln):
                    added += 1
            await m.reply_text(f"✅ Added {added} target(s). Total {len(list_groups(user_id))}/{cap}.")
        elif cmd == ".time":
            s = arg.lower().strip()
            if s.endswith("m"): s = s[:-1]
            try:
                minutes = int(s)
                if minutes not in (30, 45, 60):
                    raise ValueError
            except Exception:
                await m.reply_text("❌ Usage: .time 30m|45m|60m"); return
            set_interval(user_id, minutes)
            await m.reply_text(f"✅ Interval set to {minutes} minutes.")
        elif cmd == ".adreset":
            _set_pointer(user_id, 0)
            ADS_CACHE.pop(user_id, None)
            await m.reply_text("✅ Ad pointer reset; will start from oldest message next tick.")
        else:
            # ignore other dot commands
            pass

    # Copy captions/media edits? We ignore edits; not_edited already filters them.

    APP_HANDLERS[key] = True

# -------------- Saved-All logic --------------
async def fetch_saved_ids(app: Client, uid: int) -> List[int]:
    """Return list of message_ids in Saved Messages, oldest->newest."""
    # cache short-living; rebuild each tick to reflect user changes (cost is small)
    ids: List[int] = []
    async for m in app.get_chat_history("me", limit=1000):  # newest->oldest
        # accept: text, media with or without captions
        if bool(m.text) or bool(m.caption) or m.media:
            ids.append(int(m.id))
    ids.reverse()
    ADS_CACHE[uid] = ids
    return ids

async def copy_one_ad_to_target(app: Client, src_msg_id: int, target: str) -> bool:
    """Copy the message (preserve media & caption)."""
    try:
        # Pyrogram resolve: can pass username, link, chat id, or invite link (if member already)
        await app.copy_message(chat_id=target, from_chat_id="me", message_id=src_msg_id)
        return True
    except perror.UsernameNotOccupied:
        log.warning("target username not occupied: %s", target)
    except perror.UserChannelsTooMuch:
        log.warning("user channels too much: %s", target)
    except perror.UserNotParticipant:
        log.warning("not a participant for: %s (join manually for private invites)", target)
    except perror.InviteHashInvalid:
        log.warning("invalid invite: %s", target)
    except perror.InviteHashExpired:
        log.warning("expired invite: %s", target)
    except perror.ChannelPrivate:
        log.warning("channel private (not joined): %s", target)
    except perror.ChatWriteForbidden:
        log.warning("write forbidden (no post permission): %s", target)
    except Exception as e:
        log.warning("copy failed to %s: %s", target, _fmt_exc(e))
    return False

async def send_cycle_for_user(uid: int):
    # choose first alive session for this user
    sessions = sessions_list(uid)
    if not sessions:
        return
    app: Optional[Client] = None
    # prefer the lowest slot available
    for s in sorted(sessions, key=lambda r: int(r["slot"])):
        a = APP_MAP.get(uid, {}).get(int(s["slot"]))
        if a and a.is_connected:
            app = a; break
    if not app:
        return

    # Build Saved list
    ids = ADS_CACHE.get(uid)
    if not ids:
        ids = await fetch_saved_ids(app, uid)
    if not ids:
        log.info("[u%s] no saved messages to send", uid); return

    ptr = _get_pointer(uid)
    if ptr >= len(ids):
        ptr = 0
    msg_id = ids[ptr]

    targets = list_groups(uid)
    if not targets:
        log.info("[u%s] no targets", uid); return

    ok = 0
    for i, tgt in enumerate(targets, 1):
        if await copy_one_ad_to_target(app, msg_id, tgt):
            inc_sent_ok(uid, 1)
            ok += 1
        # stagger between targets
        if i < len(targets):
            await asyncio.sleep(PER_GROUP_DELAY)

    # Advance pointer and mark schedule
    ptr = (ptr + 1) % len(ids)
    _set_pointer(uid, ptr)
    mark_sent_now(uid)
    log.info("[u%s] sent msg#%s to %s/%s targets; next ptr=%s", uid, msg_id, ok, len(targets), ptr)

# -------------- Scheduler loop --------------
async def main_loop():
    init_db()
    backoff = RETRY_MIN
    hb = 0
    while True:
        try:
            # keep sessions alive
            await ensure_all_sessions_online()

            if night_enabled():
                await asyncio.sleep(10)
                continue

            # process a limited number of users per tick to avoid burst
            uids = users_with_sessions()
            now = _now()
            processed = 0
            for uid in uids:
                if processed >= PARALLEL_USERS:
                    break
                last = get_last_sent_at(uid)
                interval_min = get_interval(uid) or 30
                due = (last is None) or ((now - int(last)) >= (interval_min * 60))
                if not due:
                    continue
                await send_cycle_for_user(uid)
                processed += 1

            # Heartbeat
            hb += 1
            if hb % 30 == 0:
                log.info("[hb] alive @ %s", datetime.utcnow().isoformat())

            await asyncio.sleep(2)
            backoff = RETRY_MIN  # reset after successful loop

        except Exception as e:
            log.error("loop error: %s", _fmt_exc(e))
            # mild backoff
            await asyncio.sleep(backoff)
            backoff = min(RETRY_MAX, max(RETRY_MIN, backoff + 1))

# -------------- Entry --------------
if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        pass
            
