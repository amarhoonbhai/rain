# worker_forward.py — persistent session hub (scheduler + account commands)
# Features:
#  • Persistent Pyrogram client per session (handles .addgc, .time, .gc, .help)
#  • Scheduler per user_id (30/45/60) → send to all groups with 10s gaps
#  • Ad source: pinned message in "Saved Messages" (text or media; media is copied)
#  • No auto-join; stores whatever targets user gives (username / numeric id / links)
#  • Night mode respected; stats updated; auto rehydrate notify via MAIN_BOT_TOKEN (optional)

import os, asyncio, logging, re, time as _time
from typing import Dict, Optional, List
from datetime import datetime, timezone
from urllib.parse import urlparse

from aiogram import Bot  # only used for DM notifications on auth issues

from pyrogram import Client, filters
from pyrogram.errors import (
    FloodWait, RPCError, Unauthorized, AuthKeyUnregistered
)
try:
    from pyrogram.errors import SessionRevoked, SessionExpired, UserDeactivated, UserDeactivatedBan, UserNotParticipant
except Exception:  # keep compatibility across minor versions
    class _E(Exception): pass
    SessionRevoked = SessionExpired = UserDeactivated = UserDeactivatedBan = UserNotParticipant = _E

from core.db import (
    init_db,
    users_with_sessions, sessions_list,
    list_groups, add_group, clear_groups, groups_cap,
    get_interval, set_interval,
    get_last_sent_at, mark_sent_now, inc_sent_ok,
    night_enabled, set_setting, get_setting
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("worker")

# ---------- Optional notifier (owner bot DM on session auth errors) ----------
BOT_TOKEN = (os.getenv("MAIN_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
BOT_NOTIFIER = Bot(BOT_TOKEN) if BOT_TOKEN and ":" in BOT_TOKEN else None
AUTH_PING_COOLDOWN_SEC = 6 * 3600  # 6 hours

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
            f"✇ Slot <b>{slot}</b> looks <b>expired/unauthorized</b>.\n"
            "✇ Log in again via <b>@SpinifyLoginBot</b>.\n"
            f"✇ Reason: <code>{reason}</code>"
        )
    except Exception:
        pass

# ---------- Target normalization ----------
SPLIT_RE = re.compile(r"[,\n\r\t ]+")
USERNAME_RE = re.compile(r"^@?([A-Za-z0-9_]{5,})$")

def _extract_username_from_link(s: str) -> Optional[str]:
    if not s.startswith("http"): return None
    u = urlparse(s)
    if u.netloc.lower() != "t.me": return None
    path = u.path.strip("/")
    if not path or path.startswith("+") or path.startswith("joinchat"):  # private invite → keep as-is
        return None
    uname = path.split("/")[0]
    return uname if USERNAME_RE.match(uname) else None

def _normalize_targets(chunks: List[str]) -> List[str]:
    out, seen = [], set()
    for raw in chunks:
        t = (raw or "").strip().rstrip("/.,")
        if not t: continue
        if t.lstrip("-").isdigit():
            key = t
        else:
            m = USERNAME_RE.match(t.lstrip("@"))
            if m:
                key = m.group(1)  # store username w/o @
            else:
                u = _extract_username_from_link(t)
                key = u if u else t  # store raw link for private invites
        if key and key not in seen:
            seen.add(key); out.append(key)
    return out

# ---------- Ad (pinned) fetch ----------
async def _find_pinned_in_saved(app: Client):
    async for msg in app.get_chat_history("me", limit=200):
        if getattr(msg, "pinned", False):
            return msg
    return None

# ---------- Session Node ----------
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
        try:
            await self.app.stop()
        except Exception:
            pass
        self._started = False
        log.info(f"[u{self.user_id}s{self.slot}] stopped")

    # -------- Account-side commands --------
    def _bind_handlers(self):
        @self.app.on_message(filters.me & filters.text & filters.regex(r"(?i)^\.(help|start)$"))
        async def _help(_, m):
            txt = (
                "✇ Commands\n"
                "• <b>.addgc</b> <i>one-per-line or space separated</i>\n"
                "    Save public/private links or @usernames or numeric IDs (max depends on your plan).\n"
                "• <b>.gc</b> — show saved targets\n"
                "• <b>.time</b> 30m|45m|60m — set interval\n"
                "• <b>.help</b> — show this help\n\n"
                "✇ Ads are taken from your <b>pinned Saved Message</b> (text or media)."
            )
            await m.reply_text(txt)

        @self.app.on_message(filters.me & filters.text & filters.regex(r"(?i)^\.gc$"))
        async def _gc(_, m):
            gs = list_groups(self.user_id)
            cap = groups_cap(self.user_id)
            if not gs:
                await m.reply_text(f"👥 No groups saved yet. Cap: {cap}")
            else:
                listing = "\n".join(f"• {g}" for g in gs)
                await m.reply_text(f"👥 Groups ({len(gs)}/{cap})\n{listing}")

        @self.app.on_message(filters.me & filters.text & filters.regex(r"(?i)^\.time\s+(.+)$"))
        async def _time_cmd(_, m):
            arg = m.matches[0].group(1).strip().lower()
            if arg.endswith("m"): arg = arg[:-1]
            try:
                mins = int(arg)
            except Exception:
                await m.reply_text("❌ Use: .time 30m | 45m | 60m"); return
            if mins not in (30, 45, 60):
                await m.reply_text("❌ Allowed intervals: 30m, 45m, 60m"); return
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
                await m.reply_text(f"✅ Added {added} target(s). Now {len(list_groups(self.user_id))}/{cap}.")

    # -------- Send API used by scheduler --------
    async def send_pinned_to_targets(self, targets: List[str]) -> int:
        """
        Send pinned Saved Message to each target with 10s gaps.
        Supports text or media by copying the pinned message.
        Returns number of successful sends.
        """
        if not self._started:
            await self.start()
            if not self._started:
                return 0

        pinned = await _find_pinned_in_saved(self.app)
        if not pinned:
            log.info(f"[u{self.user_id}s{self.slot}] no pinned message in Saved Messages")
            return 0

        ok = 0
        for tgt in targets:
            try:
                # Resolve chat id: username (without @), numeric id, or private invite link
                chat_id = None
                if tgt.lstrip("-").isdigit():
                    chat_id = int(tgt)
                else:
                    if tgt.startswith("http"):
                        u = _extract_username_from_link(tgt)
                        chat_id = u if u else tgt   # allow private invite link (will error if not a member)
                    else:
                        chat_id = tgt  # bare username without @

                await pinned.copy(chat_id=chat_id)
                ok += 1
                await asyncio.sleep(10)  # 10s gap per target
            except FloodWait as fw:
                log.warning(f"[u{self.user_id}s{self.slot}] FloodWait {fw.value}s; sleeping…")
                await asyncio.sleep(fw.value + 1)
            except UserNotParticipant:
                log.info(f"[u{self.user_id}s{self.slot}] not a participant for {tgt} (skipped)")
            except (Unauthorized, AuthKeyUnregistered, SessionRevoked, SessionExpired, UserDeactivated, UserDeactivatedBan) as e:
                await _notify_rehydrate(self.user_id, self.slot, e.__class__.__name__)
                log.error(f"[u{self.user_id}s{self.slot}] auth error during send: {e}")
                break
            except RPCError as e:
                log.warning(f"[u{self.user_id}s{self.slot}] RPC error on {tgt}: {e}")
            except Exception as e:
                log.warning(f"[u{self.user_id}s{self.slot}] send failed on {tgt}: {e}")

        return ok

# ---------- Pool of sessions ----------
class SessionPool:
    def __init__(self):
        self.nodes: Dict[tuple, SessionNode] = {}  # (user_id, slot) -> node

    async def refresh(self):
        """Start new nodes for new sessions; stop nodes no longer present."""
        present = set()
        for uid in users_with_sessions():
            for r in sessions_list(uid):
                key = (r["user_id"], r["slot"])
                present.add(key)
                if key not in self.nodes:
                    node = SessionNode(r["user_id"], r["slot"], r["api_id"], r["api_hash"], r["session_string"])
                    self.nodes[key] = node
                    await node.start()

        # stop removed
        for key in list(self.nodes.keys()):
            if key not in present:
                try:
                    await self.nodes[key].stop()
                finally:
                    self.nodes.pop(key, None)

    def pick_node(self, user_id: int) -> Optional[SessionNode]:
        # round-robin across available slots for this user
        slots = sorted([slot for (uid, slot) in self.nodes.keys() if uid == user_id])
        if not slots:
            return None
        set_key = f"worker:last_session:{user_id}"
        cur = int(get_setting(set_key, -1) or -1)
        nxt = slots[(slots.index(cur) + 1) % len(slots)] if cur in slots else slots[0]
        set_setting(set_key, nxt)
        return self.nodes.get((user_id, nxt))

POOL = SessionPool()

# ---------- Scheduler ----------
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

    sent = await node.send_pinned_to_targets(gs)
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
                await asyncio.sleep(0.2)  # light pacing across users
        except Exception as e:
            log.error(f"[loop] error: {e}")
        await asyncio.sleep(10)  # main loop cadence

# ---------- Entrypoint ----------
async def main():
    init_db()
    await scheduler_loop()

if __name__ == "__main__":
    asyncio.run(main())
