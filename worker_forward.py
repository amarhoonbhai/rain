# worker_forward.py — Saved-All forwarder (multi-user, Mongo DB)
# - Cycles Saved Messages (oldest -> newest) per user
# - Sends to all saved groups with 30s gap between groups
# - Per-user interval (default 30m); persists last_sent_at and next index
# - Dot-commands from user accounts: .help .addgc .gc .cleargc .time .adreset .fstats
# - Auto-join invite links (t.me/+...), caches target chat IDs
# - Night Mode respected (global)
# - Auto-rehydrate DM via main bot on session failure
# - Exports async main() for run_all.py

import os
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Tuple, List, Optional

from pyrogram import Client, filters
from pyrogram.errors import (
    FloodWait, RPCError, Unauthorized, AuthKeyUnregistered, UserDeactivated, UserDeactivatedBan
)
from pyrogram.types import Message as PyroMessage

from aiogram import Bot as AioBot
from aiogram.client.default import DefaultBotProperties

from core.db import (
    init_db, users_with_sessions, sessions_list,
    list_groups, add_group, clear_groups, groups_cap,
    get_interval, get_last_sent_at, mark_sent_now, inc_sent_ok,
    set_setting, get_setting, night_enabled,
)

# ---------------- Config ----------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
MAIN_BOT_TOKEN = (os.getenv("MAIN_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()

# Gaps
PER_GROUP_GAP_SEC = 30       # wait between groups
AFTER_CYCLE_COOLDOWN = 2     # small pause after a full send cycle
HEALTH_CHECK_PERIOD = 90     # how often to re-ensure sessions are online
LOOP_TICK = 5                # how often to check timers

# Saved-All keys
IDX_KEY = "saved:index:{uid}"
TARGET_CACHE_KEY = "tgid_cache:{uid}"

# Night window (global toggle only; if ON, skip all)
# If you later want fixed hours, add a check here.
REHYDRATE_NOTE = (
    "⚠️ Your session looks offline/revoked. Please re-login via @SpinifyLoginBot\n"
    "Tip: If you changed password/2FA or got logged out, you must export a fresh session."
)

# ---------------- Logging ----------------
logging.basicConfig(level=LOG_LEVEL)
log = logging.getLogger("worker")

# ---------------- Globals ----------------
# Keep one Client per (user_id, slot)
APPS: Dict[Tuple[int, int], Client] = {}
LAST_HEALTH = 0.0

# Keep track of registered handlers (avoid double-registration)
HANDLER_SETS: set[Tuple[int, int]] = set()

# Main bot for rehydrate pings (optional)
aiobot: Optional[AioBot] = None
if MAIN_BOT_TOKEN and ":" in MAIN_BOT_TOKEN:
    aiobot = AioBot(token=MAIN_BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))


# ---------------- Utils ----------------
def _now_epoch() -> int:
    return int(datetime.now(timezone.utc).timestamp())

def _fmt_td(seconds: int) -> str:
    if seconds <= 0:
        return "now"
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    parts = []
    if h: parts.append(f"{h}h")
    if m: parts.append(f"{m}m")
    if s or not parts: parts.append(f"{s}s")
    return "in ~" + " ".join(parts)

async def _rehydrate_ping(user_id: int, text: str = REHYDRATE_NOTE):
    if not aiobot:
        return
    try:
        await aiobot.send_message(user_id, text)
    except Exception as e:
        log.info("rehydrate ping failed u%s: %s", user_id, e)

def _get_target_cache(uid: int) -> Dict[str, int]:
    return get_setting(TARGET_CACHE_KEY.format(uid=uid), {}) or {}

def _set_target_cache(uid: int, data: Dict[str, int]):
    set_setting(TARGET_CACHE_KEY.format(uid=uid), dict(data))

# ---------------- Pyrogram helpers ----------------
async def resolve_target_chat_id(app: Client, uid: int, target: str) -> Optional[int]:
    """
    Accepts @usernames, bare usernames, numeric -100 IDs, and t.me URLs (public or private invites).
    Returns numeric chat_id or None if cannot resolve.
    Caches results per-user to speed up.
    """
    target = (target or "").strip()
    if not target:
        return None

    cache = _get_target_cache(uid)
    if target in cache:
        return cache[target]

    # Parse numeric id
    if target.lstrip("-").isdigit():
        try:
            chat_id = int(target)
            # test access
            await app.get_chat(chat_id)
            cache[target] = chat_id
            _set_target_cache(uid, cache)
            return chat_id
        except Exception:
            pass

    # Strip URL forms
    normalized = target
    if target.startswith("http://") or target.startswith("https://"):
        # t.me/<username_or_plus_invite>
        try:
            part = target.split("t.me/", 1)[1]
            normalized = part.strip().strip("/")
        except Exception:
            normalized = target
    if normalized.startswith("@"):
        normalized = normalized[1:]

    # Invite links begin with '+'
    if normalized.startswith("+"):
        link = "https://t.me/" + normalized
        try:
            # Try joining (won't work if approval-only)
            await app.join_chat(link)
            chat = await app.get_chat(link)
            cid = int(chat.id)
            cache[target] = cid
            _set_target_cache(uid, cache)
            return cid
        except Exception as e:
            log.info("[u%s] join via invite failed for %s: %s", uid, target, e)
            # If already a member, get_chat might still work
            try:
                chat = await app.get_chat(link)
                cid = int(chat.id)
                cache[target] = cid
                _set_target_cache(uid, cache)
                return cid
            except Exception:
                return None
    else:
        # Public username
        try:
            chat = await app.get_chat(normalized)
            cid = int(chat.id)
            cache[target] = cid
            _set_target_cache(uid, cache)
            return cid
        except Exception as e:
            log.info("[u%s] resolve failed for %s: %s", uid, target, e)
            return None

async def copy_to_targets(app: Client, uid: int, msg: PyroMessage, targets: List[str]) -> int:
    """
    Copy a Saved message to all targets, with 30s gap.
    Returns count of successful sends.
    """
    ok = 0
    for i, t in enumerate(targets, 1):
        cid = await resolve_target_chat_id(app, uid, t)
        if cid is None:
            log.info("[u%s] skip target %s (unresolved)", uid, t)
            continue
        try:
            await app.copy_message(chat_id=cid, from_chat_id="me", message_id=msg.id)
            ok += 1
        except FloodWait as fw:
            log.info("[u%s] FloodWait %ss while copying to %s", uid, fw.value, t)
            await asyncio.sleep(fw.value + 1)
            try:
                await app.copy_message(chat_id=cid, from_chat_id="me", message_id=msg.id)
                ok += 1
            except Exception as e:
                log.info("[u%s] failed after FW to %s: %s", uid, t, e)
        except Exception as e:
            log.info("[u%s] copy to %s failed: %s", uid, t, e)

        if i != len(targets):
            await asyncio.sleep(PER_GROUP_GAP_SEC)
    return ok

async def fetch_saved_ads(app: Client) -> List[PyroMessage]:
    """
    Grab a reasonable slice of Saved Messages; keep text/media.
    Oldest->Newest for “Saved-All” mode: reverse the result list.
    """
    msgs: List[PyroMessage] = []
    try:
        # Pull a page of history; adjust limit if you want more
        async for m in app.get_chat_history("me", limit=200):
            if m.empty:
                continue
            # Allow text-only or media+caption (stickers are okay too)
            if m.text or m.caption or m.media:
                msgs.append(m)
    except Exception as e:
        log.info("fetch_saved_ads fail: %s", e)

    # Pyrogram history yields newest->oldest; reverse to oldest->newest
    msgs.reverse()
    return msgs

def _get_idx(uid: int) -> int:
    return int(get_setting(IDX_KEY.format(uid=uid), 0) or 0)

def _set_idx(uid: int, idx: int):
    set_setting(IDX_KEY.format(uid=uid), int(idx))

# ---------------- Dot-command handlers ----------------
HELP_TEXT = (
    "✇ <b>Spinify Commands</b>\n"
    "• <code>.help</code> — show this help\n"
    "• <code>.addgc</code> lines of targets — add up to cap (5, or 10 after Unlock, or 50 premium)\n"
    "   targets can be @user, username, numeric id (-100..), or t.me link (incl. private invites)\n"
    "• <code>.gc</code> — list saved targets\n"
    "• <code>.cleargc</code> — remove all saved targets\n"
    "• <code>.time</code> 30m|45m|60m — set interval\n"
    "• <code>.adreset</code> — start Saved-All from the oldest again\n"
    "• <code>.fstats</code> — show status/interval/next send\n"
)

def register_session_handlers(app: Client, user_id: int):
    key = (user_id, hash(app))
    if key in HANDLER_SETS:
        return

    @app.on_message(filters.me & filters.text)
    async def _cmd_handler(m: PyroMessage):
        txt = (m.text or "").strip()
        if not txt.startswith("."):
            return
        parts = txt.splitlines()
        head = parts[0].strip().lower()

        async def reply(s: str):  # reply to the message
            try:
                await m.reply_text(s, quote=True)
            except Exception:
                try:
                    await app.send_message("me", s)
                except Exception:
                    pass

        if head.startswith(".help"):
            await reply(HELP_TEXT); return

        if head.startswith(".addgc"):
            # Accept up to 5 lines following; or everything after first token as lines
            lines = parts[1:] if len(parts) > 1 else []
            if not lines:
                # allow space-separated after command
                tail = txt[len(".addgc"):].strip()
                if tail:
                    lines = [s.strip() for s in tail.split() if s.strip()]
            cap = groups_cap(user_id)
            current = len(list_groups(user_id))
            added = 0
            skipped = 0
            for t in lines:
                if current + added >= cap:
                    break
                ok = add_group(user_id, t)
                if ok:
                    added += 1
                else:
                    skipped += 1
            await reply(f"✅ Added {added} target(s); skipped {skipped}. Cap {cap}."); return

        if head == ".gc":
            targets = list_groups(user_id)
            if not targets:
                await reply("ℹ️ No targets saved. Use <code>.addgc</code> to add."); return
            body = "\n".join(f"• {t}" for t in targets[:50])
            more = "" if len(targets) <= 50 else f"\n… and {len(targets)-50} more"
            await reply(f"📄 Targets ({len(targets)}/{groups_cap(user_id)}):\n{body}{more}"); return

        if head == ".cleargc":
            clear_groups(user_id)
            await reply("🧹 Cleared all targets."); return

        if head.startswith(".time"):
            try:
                arg = head.split(maxsplit=1)[1]
            except Exception:
                arg = ""
            arg = arg.replace(" ", "")
            if arg.endswith("m"):
                arg = arg[:-1]
            ok = False
            try:
                mins = int(arg)
                if mins in (30, 45, 60):
                    from core.db import set_interval
                    set_interval(user_id, mins)
                    ok = True
            except Exception:
                pass
            await reply("✅ Interval set." if ok else "❌ Use 30m, 45m or 60m."); return

        if head.startswith(".adreset"):
            _set_idx(user_id, 0)
            await reply("🔁 Saved-All pointer reset to the oldest."); return

        if head.startswith(".fstats"):
            targets = len(list_groups(user_id))
            interval = get_interval(user_id) or 30
            last = get_last_sent_at(user_id)
            now = _now_epoch()
            eta = "now" if (last is None) else _fmt_td(interval*60 - (now - last))
            await reply(
                "📟 Forward Stats\n"
                f"✇ Sessions: 1\n"
                f"✇ Targets: {targets}/{groups_cap(user_id)}\n"
                f"✇ Interval: {interval} min\n"
                f"✇ Next send: {eta}\n"
                f"🌙 Night Mode: {'ON' if night_enabled() else 'OFF'}"
            )
            return

    HANDLER_SETS.add(key)

# ---------------- Session lifecycle ----------------
async def ensure_app_healthy(uid: int, sess_row: dict) -> Optional[Client]:
    api_id = int(sess_row["api_id"])
    api_hash = str(sess_row["api_hash"])
    session_string = str(sess_row["session_string"])
    slot = int(sess_row["slot"])
    k = (uid, slot)

    app = APPS.get(k)
    if app is None:
        app = Client(
            name=f"u{uid}s{slot}",
            api_id=api_id,
            api_hash=api_hash,
            session_string=session_string,
            no_updates=True,  # we register only our own on_message
            workdir=None,     # in-memory only
        )
        APPS[k] = app

    if not getattr(app, "is_connected", False):
        try:
            await app.start()
            register_session_handlers(app, uid)
            log.info("[u%s s%s] started", uid, slot)
        except (Unauthorized, AuthKeyUnregistered, UserDeactivated, UserDeactivatedBan) as e:
            log.info("[u%s s%s] auth problem: %s", uid, slot, e)
            await _rehydrate_ping(uid)
            return None
        except Exception as e:
            log.info("[u%s s%s] start failed: %s", uid, slot, e)
            return None
    return app

async def ensure_all_sessions_online():
    global LAST_HEALTH
    now = asyncio.get_event_loop().time()
    if now - LAST_HEALTH < HEALTH_CHECK_PERIOD:
        return
    LAST_HEALTH = now
    uids = users_with_sessions()
    for uid in uids:
        for s in sessions_list(uid):
            await ensure_app_healthy(uid, s)
            await asyncio.sleep(0.1)

# ---------------- Send cycle ----------------
async def send_cycle_for_user(uid: int):
    if night_enabled():
        return  # globally paused

    sess_rows = sessions_list(uid)
    if not sess_rows:
        return

    # Use the first healthy session to send (you can randomize/round-robin if you prefer)
    app: Optional[Client] = None
    for s in sess_rows:
        a = await ensure_app_healthy(uid, s)
        if a is not None:
            app = a
            break
    if app is None:
        return

    targets = list_groups(uid)
    if not targets:
        return

    # Check interval
    interval = get_interval(uid) or 30
    last = get_last_sent_at(uid)
    now = _now_epoch()
    if last is not None and (now - last) < interval * 60:
        return

    # Saved-All message selection
    msgs = await fetch_saved_ads(app)
    if not msgs:
        log.info("[u%s] no Saved Messages to send", uid)
        return

    idx = _get_idx(uid)
    if idx >= len(msgs):
        idx = 0
    msg = msgs[idx]

    # Send to every target with 30s gap
    sent_ok = await copy_to_targets(app, uid, msg, targets)
    if sent_ok > 0:
        inc_sent_ok(uid, sent_ok)
        mark_sent_now(uid)
        _set_idx(uid, idx + 1)
        await asyncio.sleep(AFTER_CYCLE_COOLDOWN)

# ---------------- Main loop ----------------
async def main_loop():
    log.info("worker starting…")
    init_db()
    while True:
        try:
            # Keep sessions healthy occasionally
            await ensure_all_sessions_online()

            # Iterate over users that have sessions
            for uid in users_with_sessions():
                try:
                    await send_cycle_for_user(uid)
                except FloodWait as fw:
                    log.info("[u%s] FloodWait loop %s", uid, fw.value)
                    await asyncio.sleep(fw.value + 1)
                except RPCError as e:
                    log.info("[u%s] RPC error: %s", uid, e)
                except Exception as e:
                    log.exception("[u%s] cycle error: %s", uid, e)

            await asyncio.sleep(LOOP_TICK)
        except Exception as e:
            log.exception("loop error: %s", e)
            await asyncio.sleep(3)

# --- runner export for run_all.py ---
__all__ = ["main"]

async def main():
    await main_loop()

if __name__ == "__main__":
    asyncio.run(main())
