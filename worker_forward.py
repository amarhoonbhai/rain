import os
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import RPCError

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
PER_GROUP_DELAY = float(os.getenv("PER_GROUP_DELAY", "30"))   # seconds
SEND_TIMEOUT = int(os.getenv("SEND_TIMEOUT", "60"))            # seconds (used as soft limit)
TICK_INTERVAL = int(os.getenv("TICK_INTERVAL", "15"))          # seconds
DEFAULT_INTERVAL_MIN = int(os.getenv("DEFAULT_INTERVAL_MIN", "30"))

# Per-user runtime state
# STATE[uid] = {
#   "client": TelegramClient,
#   "idx": current saved message index,
#   "saved_ids": [int],
#   "delay": float,
# }
STATE: Dict[int, Dict[str, Any]] = {}


def _now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


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


def _panel_is_premium(uid: int) -> bool:
    try:
        return bool(is_premium(uid))
    except Exception:
        return False


async def fetch_saved_ids(client: TelegramClient) -> List[int]:
    ids: List[int] = []
    async for m in client.iter_messages("me", limit=1000):
        if m.message or m.media:
            ids.append(m.id)
    ids.reverse()
    return ids


# ---------- command handlers per user ----------

def register_session_handlers(client: TelegramClient, uid: int) -> None:
    @client.on(events.NewMessage(outgoing=True))
    async def self_cmd_handler(event: events.NewMessage.Event):
        text = (event.raw_text or "").strip()
        if not text.startswith("."):
            return

        st = _get_state(uid)
        premium = _panel_is_premium(uid)
        log.info("[u%s] cmd: %s (premium=%s)", uid, text, premium)

        # .help
        if text.startswith(".help"):
            await event.reply(
                "📜 Commands (send from this account)\n"
                "• <code>.help</code> — this help\n"
                "• <code>.gc</code> / <code>.groups</code> — list targets\n"
                "• <code>.addgc</code> — add targets (@/t.me/ID)\n"
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
        if text.startswith(".gc") or text.startswith(".groups"):
            targets = list_groups(uid)
            cap = groups_cap(uid)
            if not targets:
                await event.reply(f"GC list empty (cap {cap}).")
                return
            head = f"GC ({len(targets)}/{cap})"
            body = "\n".join(f"• {x}" for x in targets[:100])
            more = "" if len(targets) <= 100 else f"\n… +{len(targets)-100} more"
            await event.reply(f"{head}\n{body}{more}")
            return

        # .cleargc
        if text.startswith(".cleargc"):
            clear_groups(uid)
            await event.reply("✅ Cleared all groups.")
            return

        # .addgc
        if text.startswith(".addgc"):
            lines: List[str] = []

            # 1) same-line args
            parts = text.split(maxlength := 1)
            if len(parts) > 1:
                for token in parts[1].split():
                    token = token.strip()
                    if token:
                        lines.append(token)

            # 2) extra lines
            extra = text.splitlines()[1:]
            for ln in extra:
                ln = ln.strip()
                if ln:
                    lines.append(ln)

            # 3) reply body
            if not lines and event.is_reply:
                reply_msg = await event.get_reply_message()
                if reply_msg and (reply_msg.raw_text or ""):
                    for ln in (reply_msg.raw_text or "").splitlines():
                        ln = ln.strip()
                        if ln:
                            lines.append(ln)

            if not lines:
                await event.reply(
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

            await event.reply(f"✅ Added {added}. Now {len(list_groups(uid))}/{cap}.")
            return

        # .delgc
        if text.startswith(".delgc"):
            parts = text.split(maxsplit=1)
            if len(parts) < 2:
                await event.reply("❗ Usage: <code>.delgc &lt;@user or t.me/link&gt;</code>")
                return
            target = parts[1].strip()
            targets = list_groups(uid)
            if target in targets:
                targets.remove(target)
                clear_groups(uid)
                for x in targets:
                    add_group(uid, x)
                await event.reply("✅ Group removed.")
            else:
                await event.reply("❗ That target is not in your group list.")
            return

        # .time
        if text.startswith(".time"):
            tokens = text.split()
            if len(tokens) == 1:
                await event.reply(
                    "⏱ Usage:\n"
                    "  Free: <code>.time 30</code>, <code>.time 45</code>, <code>.time 60</code>\n"
                    "  Premium: any minutes, e.g. <code>.time 10</code>, <code>.time 90</code>"
                )
                return
            try:
                val = int(tokens[1])
            except Exception:
                await event.reply("❗ Interval must be integer minutes.")
                return

            if val <= 0:
                await event.reply("❗ Interval must be > 0.")
                return

            if not premium and val not in (30, 45, 60):
                await event.reply(
                    "💎 Custom interval is premium-only.\n"
                    "Free can only use 30, 45 or 60 minutes.\n"
                    "Try: <code>.time 30</code>, <code>.time 45</code>, <code>.time 60</code>"
                )
                return

            set_interval(uid, val)
            await event.reply(
                f"✅ Interval set to <b>{val} minutes</b> "
                f"({'Premium' if premium else 'Free'})."
            )
            return

        # .adreset
        if text.startswith(".adreset"):
            st["idx"] = 0
            st["saved_ids"] = []
            await event.reply("✅ Saved Messages cycle reset to first message.")
            return

        # .status
        if text.startswith(".status"):
            gs = len(list_groups(uid))
            cap = groups_cap(uid)
            interval = get_interval(uid) or DEFAULT_INTERVAL_MIN
            eta = "—" if gs == 0 else _format_eta(uid)
            plan = "Premium 💎" if premium else "Free ⚪"
            await event.reply(
                "📊 Status\n"
                f"• Plan: {plan}\n"
                f"• Groups: {gs}/{cap}\n"
                f"• Interval: {interval} min\n"
                f"• Next send: {eta}"
            )
            return


# ---------- per-user lifecycle ----------

async def build_client_for_user(uid: int) -> TelegramClient | None:
    """
    Use first available session for this uid as sender account.
    """
    sess_list = sessions_list(uid)
    if not sess_list:
        return None

    s = sess_list[0]
    api_id = int(s["api_id"])
    api_hash = str(s["api_hash"])
    session_str = str(s["session_string"])

    client = TelegramClient(StringSession(session_str), api_id, api_hash)
    await client.connect()
    if not await client.is_user_authorized():
        log.warning("[u%s] session not authorized", uid)
        await client.disconnect()
        return None

    register_session_handlers(client, uid)
    log.info("[u%s] Telethon client started (slot %s)", uid, s.get("slot"))
    return client


async def ensure_state(uid: int):
    st = _get_state(uid)
    if st.get("client") and await st["client"].is_connected():
        return
    # build new client
    try:
        client = await build_client_for_user(uid)
    except Exception as e:
        log.error("[u%s] build_client error: %s", uid, e)
        return
    if client:
        st["client"] = client
        st.setdefault("idx", 0)
        st.setdefault("saved_ids", [])


async def refresh_saved(uid: int):
    st = _get_state(uid)
    client: TelegramClient = st.get("client")
    if not client:
        return
    try:
        saved = await fetch_saved_ids(client)
        st["saved_ids"] = saved
        log.info("[u%s] loaded %s saved messages", uid, len(saved))
    except Exception as e:
        log.error("[u%s] fetch_saved_ids error: %s", uid, e)


async def run_cycle(uid: int):
    st = _get_state(uid)
    client: TelegramClient = st.get("client")
    if not client:
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
    ok_any = False

    try:
        msg = await client.get_messages("me", ids=msg_id)
    except Exception as e:
        log.error("[u%s] get_messages error for id %s: %s", uid, msg_id, e)
        # skip this message and move index forward
        _next_idx(uid, len(st["saved_ids"]))
        return

    if not msg:
        _next_idx(uid, len(st["saved_ids"]))
        return

    for tg in targets:
        try:
            await asyncio.wait_for(msg.forward_to(tg), timeout=SEND_TIMEOUT)
            ok_any = True
            inc_sent_ok(uid, 1)
            log.info("[u%s] forwarded msg %s to %s", uid, msg_id, tg)
        except asyncio.TimeoutError:
            log.warning("[u%s] send timeout to %s", uid, tg)
        except RPCError as e:
            log.warning("[u%s] RPCError while sending to %s: %s", uid, tg, e)
        except Exception as e:
            log.warning("[u%s] error forwarding to %s: %s", uid, tg, e)
        await asyncio.sleep(st["delay"])

    if ok_any:
        mark_sent_now(uid)
        _next_idx(uid, len(st["saved_ids"]))


async def user_loop(uid: int):
    await ensure_state(uid)
    st = _get_state(uid)
    client: TelegramClient = st.get("client")
    if not client:
        return

    interval = get_interval(uid) or DEFAULT_INTERVAL_MIN
    last = get_last_sent_at(uid)
    now = _now_ts()
    if last is None or (now - last) >= interval * 60:
        await run_cycle(uid)


# ---------- main loop ----------

async def main_loop():
    init_db()
    log.info("worker started (Telethon)")

    while True:
        uids = users_with_sessions()
        sem = asyncio.Semaphore(PARALLEL_USERS)

        async def run_one(u: int):
            async with sem:
                try:
                    await user_loop(u)
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
        # graceful shutdown of clients
        for uid, st in STATE.items():
            client = st.get("client")
            if client:
                try:
                    asyncio.create_task(client.disconnect())
                except Exception:
                    pass


if __name__ == "__main__":
    asyncio.run(main())
