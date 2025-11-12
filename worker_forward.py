# worker_forward.py — Saved-All mode (round-robin over your Saved Messages)
# At each interval:
#   1) Pick the next message from your "Saved Messages" (oldest→newest, round-robin)
#   2) Copy it to every saved target (10s delay per target)
# Commands (send from the logged-in account):
#   .help            — show usage
#   .addgc …         — add targets (@usernames, numeric ids, ANY t.me link incl. private)
#   .gc              — list saved targets
#   .time 30m|45m|60m — set interval
#   .adreset         — reset Saved-message cursor to the first item
#
# Requires core.db:
#   init_db(), users_with_sessions(), sessions_list(),
#   list_groups(), add_group(), groups_cap(),
#   get_interval(), set_interval(),
#   get_last_sent_at(), mark_sent_now(), inc_sent_ok(),
#   night_enabled(), set_setting(), get_setting()

import os, asyncio, logging, re, time as _time
from typing import Optional, List, Dict
from datetime import datetime, timezone
from urllib.parse import urlparse

from aiogram import Bot  # for optional DM notifier

from pyrogram import Client, filters
from pyrogram.errors import FloodWait, RPCError, Unauthorized, AuthKeyUnregistered
try:
    from pyrogram.errors import SessionRevoked, SessionExpired, UserDeactivated, UserDeactivatedBan, UserNotParticipant
except Exception:
    class _E(Exception): pass
    SessionRevoked = SessionExpired = UserDeactivated = UserDeactivatedBan = UserNotParticipant = _E

from core.db import (
    init_db,
    users_with_sessions, sessions_list,
    list_groups, add_group, groups_cap,
    get_interval, set_interval,
    get_last_sent_at, mark_sent_now, inc_sent_ok,
    night_enabled, set_setting, get_setting,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("worker")

# Optional DM notifier for session problems
BOT_TOKEN = (os.getenv("MAIN_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
BOT_NOTIFIER = Bot(BOT_TOKEN) if BOT_TOKEN and ":" in BOT_TOKEN else None
AUTH_PING_COOLDOWN_SEC = 6 * 3600

async def _notify_rehydrate(user_id: int, slot: int, reason: str):
    if BOT_NOTIFIER is None:
        return
    key = f"authping:{user_id}:{slot}"
    last = int(get_setting(key, 0) or 0)
    now = int(_time.time())
    if now - last < AUTH_PING_COOLDOWN_SEC:
        return
    set_setting(key, now)
    try:
        await BOT_NOTIFIER.send_message(
            user_id,
            "✇ Session issue detected\n"
            f"✇ Slot <b>{slot}</b> unauthorized/expired.\n"
            "✇ Re-login via <b>@SpinifyLoginBot</b>.\n"
            f"✇ Reason: <code>{reason}</code>"
        )
    except Exception:
        pass

# ---- target parsing ----
SPLIT_RE = re.compile(r"[,\n\r\t ]+")
USERNAME_RE = re.compile(r"^@?([A-Za-z0-9_]{5,})$")

def _extract_username_from_link(s: str) -> Optional[str]:
    # Convert t.me/username → username. Keep invite links as-is (we can’t send to them).
    if not s.startswith("http"): return None
    u = urlparse(s)
    if u.netloc.lower() != "t.me": return None
    path = u.path.strip("/")
    if not path or path.startswith("+") or path.startswith("joinchat"):
        return None
    uname = path.split("/")[0]
    return uname if USERNAME_RE.match(uname) else None

def _normalize_targets(chunks: List[str]) -> List[str]:
    out, seen = [], set()
    for raw in chunks:
        t = (raw or "").strip().rstrip("/.,")
        if not t:
            continue
        if t.lstrip("-").isdigit():
            key = t
        else:
            m = USERNAME_RE.match(t.lstrip("@"))
            if m:
                key = m.group(1)                 # clean @
            else:
                u = _extract_username_from_link(t)
                key = u if u else t              # keep private invite link string
        if key and key not in seen:
            seen.add(key); out.append(key)
    return out

def _is_invite_link(t: str) -> bool:
    if not t.startswith("http"): return False
    u = urlparse(t)
    if u.netloc.lower() != "t.me": return False
    p = u.path.strip("/")
    return (p.startswith("+") or p.startswith("joinchat"))

# ---- saved messages window / cursor ----
def _ad_window_key(user_id: int) -> str:
    return f"ad_window:{user_id}"   # latest N to consider (default 200)

def _ad_cursor_key(user_id: int) -> str:
    return f"ad_cursor:{user_id}"   # 0-based index

async def _fetch_saved_window(app: Client, user_id: int) -> List[int]:
    try:
        win = int(get_setting(_ad_window_key(user_id), 200) or 200)
    except Exception:
        win = 200
    win = max(1, min(500, win))

    ids = []
    try:
        async for msg in app.get_chat_history("me", limit=win):
            # skip dot-commands and empty/service
            if msg.text and msg.text.strip().startswith("."):
                continue
            if not (msg.text or msg.caption or msg.media):
                continue
            ids.append(msg.id)
        ids.reverse()  # oldest → newest
    except Exception as e:
        log.error(f"[saved] fetch error: {e}")
        return []
    return ids

def _advance_cursor(user_id: int, size: int):
    try:
        cur = int(get_setting(_ad_cursor_key(user_id), 0) or 0)
    except Exception:
        cur = 0
    cur = 0 if size <= 0 else (cur + 1) % size
    set_setting(_ad_cursor_key(user_id), cur)
    return cur

def _reset_cursor(user_id: int):
    set_setting(_ad_cursor_key(user_id), 0)

# ---- session wrapper ----
class SessionNode:
    def __init__(self, user_id: int, slot: int, api_id: int, api_hash: str, session_string: str):
        self.user_id = int(user_id)
        self.slot = int(slot)
        self.api_id = int(api_id)
        self.api_hash = str(api_hash)
        self.session_string = str(session_string)
        self.app = Client(name=f"sess-u{user_id}-s{slot}", api_id=self.api_id, api_hash=self.api_hash, session_string=self.session_string)
        self._started = False

    async def start(self):
        if self._started: return
        try:
            await self.app.start()
            self._bind_handlers()
            self._started = True
            log.info(f"[u{self.user_id}s{self.slot}] started")
        except (Unauthorized, AuthKeyUnregistered, SessionRevoked, SessionExpired, UserDeactivated, UserDeactivatedBan) as e:
            await _notify_rehydrate(self.user_id, self.slot, e.__class__.__name__)
            log.error(f"[u{self.user_id}s{self.slot}] start auth error: {e}")
        except Exception as e:
            log.error(f"[u{self.user_id}s{self.slot}] start failed: {e}")

    async def stop(self):
        if not self._started: return
        try: await self.app.stop()
        except Exception: pass
        self._started = False
        log.info(f"[u{self.user_id}s{self.slot}] stopped")

    # ---- self-commands (from that account) ----
    def _bind_handlers(self):
        @self.app.on_message(filters.me & filters.text & filters.regex(r"(?i)^\.(help|start)$"))
        async def _help(_, m):
            await m.reply_text(
                "✇ Saved-All mode\n"
                "• <b>.addgc</b> targets — add @usernames, numeric ids, ANY t.me link (private invites are stored; join manually)\n"
                "• <b>.gc</b> — list targets\n"
                "• <b>.time</b> 30m|45m|60m — set interval\n"
                "• <b>.adreset</b> — restart the Saved-message cycle\n"
                "✇ Put your ads in <b>Saved Messages</b> — I will cycle through them every interval."
            )

        @self.app.on_message(filters.me & filters.text & filters.regex(r"(?i)^\.gc$"))
        async def _gc(_, m):
            gs = list_groups(self.user_id)
            cap = groups_cap(self.user_id)
            if not gs:
                await m.reply_text(f"👥 No targets yet. Cap: {cap}")
            else:
                listing = "\n".join(f"• {g}" for g in gs)
                await m.reply_text(f"👥 Targets ({len(gs)}/{cap})\n{listing}\n✇ Reminder: join private links manually.")

        @self.app.on_message(filters.me & filters.text & filters.regex(r"(?i)^\.adreset$"))
        async def _adreset(_, m):
            _reset_cursor(self.user_id)
            await m.reply_text("✅ Saved-message cursor reset.")

        @self.app.on_message(filters.me & filters.text & filters.regex(r"(?i)^\.time\s+(.+)$"))
        async def _time_cmd(_, m):
            arg = m.matches[0].group(1).strip().lower()
            if arg.endswith("m"): arg = arg[:-1]
            try:
                mins = int(arg)
            except Exception:
                await m.reply_text("❌ Use: .time 30m | 45m | 60m"); return
            if mins not in (30, 45, 60):
                await m.reply_text("❌ Allowed: 30m, 45m, 60m"); return
            set_interval(self.user_id, mins)
            await m.reply_text(f"✅ Interval set to {mins} minutes")

        @self.app.on_message(filters.me & filters.text & filters.regex(r"(?i)^\.addgc\s+(.+)$"))
        async def _addgc(_, m):
            payload = m.matches[0].group(1)
            tokens = _normalize_targets(SPLIT_RE.split(payload))
            if not tokens:
                await m.reply_text("❌ No valid targets found."); return
            cap = groups_cap(self.user_id)
            have = len(list_groups(self.user_id))
            remain = max(0, cap - have)
            added = 0
            for t in tokens:
                if added >= remain:
                    break
                try:
                    added += add_group(self.user_id, t)
                except Exception:
                    pass
            if added == 0:
                await m.reply_text(f"ℹ️ Nothing added (cap {cap} or duplicates).")
            else:
                await m.reply_text(
                    f"✅ Added {added} target(s). Now {len(list_groups(self.user_id))}/{cap}.\n"
                    "✇ Note: private invite links are stored but sending will be skipped until you join."
                )

    # ---- sender ----
    async def send_next_saved_to_targets(self, targets: List[str]) -> int:
        if not self._started:
            await self.start()
            if not self._started:
                return 0

        msg_ids = await _fetch_saved_window(self.app, self.user_id)
        if not msg_ids:
            log.info(f"[u{self.user_id}s{self.slot}] no saved items to send")
            return 0

        try:
            cur = int(get_setting(_ad_cursor_key(self.user_id), 0) or 0)
        except Exception:
            cur = 0
        if cur < 0 or cur >= len(msg_ids):
            cur = 0
        mid = msg_ids[cur]

        try:
            ad = await self.app.get_messages("me", mid)
        except Exception as e:
            log.warning(f"[u{self.user_id}s{self.slot}] get_messages mid={mid} failed: {e}")
            _advance_cursor(self.user_id, len(msg_ids))
            return 0

        ok = 0
        for tgt in targets:
            # Skip invite links (user must join manually)
            if _is_invite_link(tgt):
                log.info(f"[u{self.user_id}s{self.slot}] invite link skipped (join manually): {tgt}")
                continue
            try:
                chat_id = int(tgt) if tgt.lstrip("-").isdigit() else (_extract_username_from_link(tgt) or tgt)
                await ad.copy(chat_id=chat_id)
                ok += 1
                await asyncio.sleep(10)  # 10s per target
            except FloodWait as fw:
                log.warning(f"[u{self.user_id}s{self.slot}] FloodWait {fw.value}s — sleeping")
                await asyncio.sleep(fw.value + 1)
            except UserNotParticipant:
                log.info(f"[u{self.user_id}s{self.slot}] not a participant: {tgt} (skipped)")
            except (Unauthorized, AuthKeyUnregistered, SessionRevoked, SessionExpired, UserDeactivated, UserDeactivatedBan) as e:
                await _notify_rehydrate(self.user_id, self.slot, e.__class__.__name__)
                log.error(f"[u{self.user_id}s{self.slot}] auth error during send: {e}")
                break
            except RPCError as e:
                log.warning(f"[u{self.user_id}s{self.slot}] RPC error on {tgt}: {e}")
            except Exception as e:
                log.warning(f"[u{self.user_id}s{self.slot}] send failed on {tgt}: {e}")

        _advance_cursor(self.user_id, len(msg_ids))
        return ok

# ---- pool / scheduler ----
class SessionPool:
    def __init__(self):
        self.nodes: Dict[tuple, SessionNode] = {}

    async def refresh(self):
        present = set()
        for uid in users_with_sessions():
            for r in sessions_list(uid):
                key = (r["user_id"], r["slot"])
                present.add(key)
                if key not in self.nodes:
                    node = SessionNode(r["user_id"], r["slot"], r["api_id"], r["api_hash"], r["session_string"])
                    self.nodes[key] = node
                    await node.start()
        # purge removed
        for key in list(self.nodes.keys()):
            if key not in present:
                try: await self.nodes[key].stop()
                finally: self.nodes.pop(key, None)

    def pick_node(self, user_id: int) -> Optional[SessionNode]:
        slots = sorted([slot for (uid, slot) in self.nodes.keys() if uid == user_id])
        if not slots:
            return None
        cfg_key = f"worker:last_session:{user_id}"
        try:
            cur = int(get_setting(cfg_key, -1) or -1)
        except Exception:
            cur = -1
        nxt = slots[(slots.index(cur) + 1) % len(slots)] if cur in slots else slots[0]
        set_setting(cfg_key, nxt)
        return self.nodes.get((user_id, nxt))

POOL = SessionPool()

async def _tick_user(user_id: int):
    if night_enabled():
        return

    gs = list_groups(user_id)
    if not gs:
        log.info(f"[u{user_id}] no groups")
        return

    interval = get_interval(user_id) or 30
    last_ts = get_last_sent_at(user_id)
    now = int(datetime.now(timezone.utc).timestamp())
    if last_ts is not None and now - last_ts < interval * 60:
        remain = interval * 60 - (now - last_ts)
        log.info(f"[u{user_id}] not due yet ({remain}s left)")
        return

    node = POOL.pick_node(user_id)
    if not node:
        log.info(f"[u{user_id}] no live sessions")
        return

    sent = await node.send_next_saved_to_targets(gs)
    if sent > 0:
        mark_sent_now(user_id)
        inc_sent_ok(user_id, sent)
        log.info(f"[u{user_id}] sent_ok+={sent}")
    else:
        log.info(f"[u{user_id}] nothing sent this tick")

async def scheduler_loop():
    while True:
        try:
            await POOL.refresh()
            for uid in users_with_sessions():
                try:
                    await _tick_user(uid)
                except Exception as e:
                    log.error(f"[u{uid}] tick error: {e}")
                await asyncio.sleep(0.2)
        except Exception as e:
            log.error(f"[loop] error: {e}")
        await asyncio.sleep(10)

async def main():
    init_db()
    await scheduler_loop()

if __name__ == "__main__":
    asyncio.run(main())
