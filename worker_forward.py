# worker_forward.py — Pyrogram-based forward worker using Mongo DB
#
# Features:
#   • Uses sessions stored by SpinifyLoginBot (core.db.sessions_list)
#   • Commands from logged-in account (filters.me):
#       .help        — show commands
#       .gc / .groups — list targets
#       .addgc       — add targets (@, t.me links, IDs)
#       .cleargc     — clear targets
#       .delgc       — remove one target
#       .time        — set interval (free: 30/45/60, premium: any minutes)
#       .adreset     — restart Saved Messages cycle
#       .status      — show plan, groups, interval, next send
#
#   • Every interval, picks next Saved Message from "me" and
#     copy_message() to all configured groups with delay between groups.

import os
import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from pyrogram import Client, filters
from pyrogram.errors import FloodWait, Unauthorized, RPCError
from pyrogram.types import Message

from core.db import (
    init_db,
    users_with_sessions,
    sessions_list,
    list_groups,
    add_group,
    clear_groups,
    groups_cap,
    get_interval,
    set_interval,
    get_last_sent_at,
    mark_sent_now,
    inc_sent_ok,
    is_premium,
)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL)
log = logging.getLogger("worker")

PARALLEL_USERS = int(os.getenv("PARALLEL_USERS", "3"))
PER_GROUP_DELAY = float(os.getenv("PER_GROUP_DELAY", "30"))  # seconds
SEND_TIMEOUT = int(os.getenv("SEND_TIMEOUT", "60"))           # seconds
TICK_INTERVAL = int(os.getenv("TICK_INTERVAL", "15"))         # seconds
DEFAULT_INTERVAL_MIN = int(os.getenv("DEFAULT_INTERVAL_MIN", "30"))

# per-user runtime state: {uid: {"apps":[Client...], "idx":int, "saved_ids":[int], "delay":float}}
STATE: Dict[int, Dict[str, Any]] = {}


# ---------- helpers ----------

def _now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def _panel_is_premium(uid: int) -> bool:
    """Use DB premium flag; owner can be treated premium if you want."""
    try:
        return bool(is_premium(uid))
    except Exception:
        return False


def _get_state(uid: int) -> Dict[str, Any]:
    st = STATE.setdefault(uid, {})
    if "idx" not in st:
        st["idx"] = 0
    if "saved_ids" not in st:
        st["saved_ids"] = []
    if "delay" not in st:
        st["delay"] = PER_GROUP_DELAY
    return st


def _cur_idx(uid: int) -> int:
    return int(_get_state(uid).get("idx", 0))


def _next_idx(uid: int, n: int) -> int:
    st = _get_state(uid)
    if n <= 0:
        st["idx"] = 0
        return 0
    i = (int(st.get("idx", 0)) + 1) % n
    st["idx"] = i
    return i


def _format_eta(uid: int) -> str:
    last = get_last_sent_at(uid)
    interval = get_interval(uid) or DEFAULT_INTERVAL_MIN
    if last is None:
        return "now"
    now = _now_ts()
    left = interval * 60 - (now - int(last))
    if left <= 0:
        return "now"
    h, rem = divmod(left, 3600)
    m, s = divmod(rem, 60)
    parts = []
    if h:
        parts.append(f"{h}h")
    if m:
        parts.append(f"{m}m")
    if s and not parts:
        parts.append(f"{s}s")
    return "in ~" + " ".join(parts)


async def fetch_saved_ids(app: Client) -> List[int]:
    """Collect Saved Messages IDs from 'me', oldest → newest."""
    ids: List[int] = []
    async for m in app.get_chat_history("me", limit=1000):
        if m.text or m.caption or m.media:
            ids.append(m.id)
    ids.reverse()
    return ids


async def send_copy(app: Client, from_chat, msg_id: int, to_target: str):
    try:
        await app.copy_message(chat_id=to_target, from_chat_id=from_chat, message_id=msg_id)
        return True
    except FloodWait as fw:
        log.warning("FloodWait to %s: sleep %ss", to_target, fw.value)
        await asyncio.sleep(fw.value)
        return False
    except Exception as e:
        log.warning("copy fail → %s: %s", to_target, e)
        return False


# ---------- command handlers (per session) ----------

def register_session_handlers(app: Client, uid: int) -> None:
    @app.on_message(filters.me & filters.text)
    async def my_text(_, msg: Message):
        t = (msg.text or "").strip()
        if not t.startswith("."):
            return

        st = _get_state(uid)
        premium = _panel_is_premium(uid)
        log.info("[u%s] cmd: %s (premium=%s)", uid, t, premium)

        # .help
        if t.startswith(".help"):
            await msg.reply_text(
                "📜 Commands (send from this account)\n"
                "• <code>.help</code> — this help\n"
                "• <code>.gc</code> / <code>.groups</code> — list targets\n"
                "• <code>.addgc</code> — add targets (see below)\n"
                "• <code>.cleargc</code> — clear all targets\n"
                "• <code>.delgc &lt;target&gt;</code> — remove one\n"
                "• <code>.time</code> — set interval\n"
                "• <code>.adreset</code> — restart Saved cycle\n"
                "• <code>.status</code> — show plan + ETA\n\n"
                "🧷 <b>.addgc</b> usage:\n"
                "  1) <code>.addgc @group1 @group2</code>\n"
                "  2) <code>.addgc</code> then paste one per line\n"
                "  3) Reply to a message that has @/t.me links and send <code>.addgc</code>."
            )
            return

        # .gc / .groups
        if t.startswith(".gc") or t.startswith(".groups"):
            targets = list_groups(uid)
            cap = groups_cap(uid)
            if not targets:
                await msg.reply_text(f"GC list empty (cap {cap}).")
                return
            head = f"GC ({len(targets)}/{cap})"
            body = "\n".join(f"• {x}" for x in targets[:100])
            more = "" if len(targets) <= 100 else f"\n… +{len(targets)-100} more"
            await msg.reply_text(f"{head}\n{body}{more}")
            return

        # .cleargc
        if t.startswith(".cleargc"):
            clear_groups(uid)
            await msg.reply_text("✅ Cleared all groups.")
            return

        # .addgc
        if t.startswith(".addgc"):
            lines: List[str] = []

            # 1) same-line args: ".addgc @g1 @g2"
            parts = t.split(maxsplit=1)
            if len(parts) > 1:
                for token in parts[1].split():
                    token = token.strip()
                    if token:
                        lines.append(token)

            # 2) extra lines in same message
            extra = t.splitlines()[1:]
            for ln in extra:
                ln = ln.strip()
                if ln:
                    lines.append(ln)

            # 3) reply body
            if not lines and msg.reply_to_message:
                body = msg.reply_to_message.text or msg.reply_to_message.caption or ""
                for ln in body.splitlines():
                    ln = ln.strip()
                    if ln:
                        lines.append(ln)

            if not lines:
                await msg.reply_text(
                    "⚠️ No targets found.\n"
                    "Examples:\n"
                    "  <code>.addgc @group1</code>\n"
                    "  <code>.addgc</code> then paste one per line\n"
                    "  Or reply to a message that contains @usernames / t.me links and send <code>.addgc</code>."
                )
                return

            added = 0
            cap = groups_cap(uid)
            for ln in lines:
                added += add_group(uid, ln.strip())
                if len(list_groups(uid)) >= cap:
                    break

            await msg.reply_text(f"✅ Added {added}. Now {len(list_groups(uid))}/{cap}.")
            return

        # .delgc
        if t.startswith(".delgc"):
            parts = t.split(maxsplit=1)
            if len(parts) < 2:
                await msg.reply_text("❗ Usage: <code>.delgc &lt;@user or t.me/link&gt;</code>")
                return
            target = parts[1].strip()
            targets = list_groups(uid)
            if target in targets:
                targets.remove(target)
                clear_groups(uid)
                for x in targets:
                    add_group(uid, x)
                await msg.reply_text("✅ Group removed.")
            else:
                await msg.reply_text("❗ That target is not in your group list.")
            return

        # .time  (free vs premium)
        if t.startswith(".time"):
            tokens = t.split()
            if len(tokens) == 1:
                await msg.reply_text(
                    "⏱ Usage:\n"
                    "  Free: <code>.time 30</code>, <code>.time 45</code>, <code>.time 60</code>\n"
                    "  Premium: any minutes, e.g. <code>.time 10</code>, <code>.time 90</code>"
                )
                return

            try:
                val = int(tokens[1])
            except Exception:
                await msg.reply_text("❗ Interval must be integer minutes.")
                return

            if val <= 0:
                await msg.reply_text("❗ Interval must be > 0.")
                return

            if not premium and val not in (30, 45, 60):
                await msg.reply_text(
                    "💎 Custom interval is premium-only.\n"
                    "Free can only use 30, 45 or 60 minutes.\n"
                    "Try: <code>.time 30</code>, <code>.time 45</code>, <code>.time 60</code>"
                )
                return

            set_interval(uid, val)
            await msg.reply_text(f"✅ Interval set to <b>{val} minutes</b> ({'Premium' if premium else 'Free'}).")
            return

        # .adreset
        if t.startswith(".adreset"):
            st["idx"] = 0
            st["saved_ids"] = []
            await msg.reply_text("✅ Saved Messages cycle reset to first message.")
            return

        # .status
        if t.startswith(".status"):
            gs = len(list_groups(uid))
            cap = groups_cap(uid)
            interval = get_interval(uid) or DEFAULT_INTERVAL_MIN
            eta = "—" if gs == 0 else _format_eta(uid)
            plan = "Premium 💎" if premium else "Free ⚪"
            await msg.reply_text(
                "📊 Status\n"
                f"• Plan: {plan}\n"
                f"• Groups: {gs}/{cap}\n"
                f"• Interval: {interval} min\n"
                f"• Next send: {eta}"
            )
            return


# ---------- per-user lifecycle ----------

async def build_clients_for_user(uid: int) -> List[Client]:
    apps: List[Client] = []
    for s in sessions_list(uid):
        try:
            c = Client(
                name=f"u{uid}s{s['slot']}",
                api_id=int(s["api_id"]),
                api_hash=str(s["api_hash"]),
                session_string=str(s["session_string"]),
            )
            await c.start()
            register_session_handlers(c, uid)
            apps.append(c)
            log.info("[u%s] session slot %s started", uid, s["slot"])
        except Unauthorized:
            log.warning("[u%s] session slot %s unauthorized", uid, s.get("slot"))
        except Exception as e:
            log.error("[u%s] start session failed slot %s: %s", uid, s.get("slot"), e)
    return apps


async def ensure_state(uid: int):
    st = STATE.get(uid)
    if st and st.get("apps"):
        return
    apps = await build_clients_for_user(uid)
    STATE[uid] = {"apps": apps, "idx": 0, "saved_ids": [], "delay": PER_GROUP_DELAY}


async def refresh_saved(uid: int):
    st = _get_state(uid)
    if not st.get("apps"):
        return
    try:
        saved = await fetch_saved_ids(st["apps"][0])
        st["saved_ids"] = saved
        log.info("[u%s] loaded %s saved messages", uid, len(saved))
    except Exception as e:
        log.error("[u%s] fetch_saved_ids error: %s", uid, e)


async def run_cycle(uid: int):
    st = _get_state(uid)
    apps = st.get("apps") or []
    if not apps:
        return

    targets = list_groups(uid)
    if not targets:
        return

    if not st["saved_ids"]:
        await refresh_saved(uid)
    if not st["saved_ids"]:
        log.info("[u%s] no saved messages", uid)
        return

    idx = _cur_idx(uid)
    msg_id = st["saved_ids"][idx]
    app = apps[0]  # use first healthy session
    ok_any = False

    for tg in targets:
        try:
            done = await asyncio.wait_for(
                send_copy(app, "me", msg_id, tg),
                timeout=SEND_TIMEOUT,
            )
            if done:
                ok_any = True
                inc_sent_ok(uid, 1)
        except asyncio.TimeoutError:
            log.warning("[u%s] send timeout to %s", uid, tg)
        await asyncio.sleep(st["delay"])

    if ok_any:
        mark_sent_now(uid)
        _next_idx(uid, len(st["saved_ids"]))


async def user_loop(uid: int):
    await ensure_state(uid)
    interval = get_interval(uid) or DEFAULT_INTERVAL_MIN
    last = get_last_sent_at(uid)
    now = _now_ts()
    if last is None or (now - last) >= interval * 60:
        await run_cycle(uid)


# ---------- main loop ----------

async def main_loop():
    init_db()
    log.info("worker started (Pyrogram)")

    while True:
        uids = users_with_sessions()
        sem = asyncio.Semaphore(PARALLEL_USERS)

        async def run_one(u: int):
            async with sem:
                try:
                    await user_loop(u)
                except Unauthorized:
                    log.warning("[u%s] Unauthorized in loop", u)
                except RPCError as e:
                    log.error("[u%s] RPCError: %s", u, e)
                except Exception as e:
                    log.error("loop error u%s: %s", u, e)

        tasks = [asyncio.create_task(run_one(u)) for u in uids]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(TICK_INTERVAL)


async def main():
    try:
        await main_loop()
    except KeyboardInterrupt:
        log.info("worker stopped by KeyboardInterrupt")


if __name__ == "__main__":
    asyncio.run(main())
