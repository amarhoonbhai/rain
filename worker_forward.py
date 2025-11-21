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
SEND_TIMEOUT = int(os.getenv("SEND_TIMEOUT", "60"))            # seconds
TICK_INTERVAL = int(os.getenv("TICK_INTERVAL", "15"))          # seconds
DEFAULT_INTERVAL_MIN = int(os.getenv("DEFAULT_INTERVAL_MIN", "30"))

# Shown to user when they need premium
PREMIUM_CONTACT = os.getenv("PREMIUM_CONTACT", "@spinify")

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
    """
    Human ETA for next send.

    Behaviour:
      - If never sent: show full interval (e.g., 'in ~30m (first cycle)')
      - If overdue / very close: show 'very soon'
      - Otherwise: 'in ~Xm Ys'
    """
    interval = get_interval(uid) or DEFAULT_INTERVAL_MIN
    last = get_last_sent_at(uid)

    # Never sent yet
    if last is None:
        return f"in ~{interval}m (first cycle)"

    now = _now_ts()
    left = interval * 60 - (now - int(last))

    # Already due or within a few seconds → show "very soon"
    if left <= 5:
        return "very soon"

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


def _is_control_message_text(text: str | None) -> bool:
    """
    Any message that starts with '.' (after stripping spaces) is treated as a control/command.
    We never forward such messages to groups.
    """
    if not text:
        return False
    return text.lstrip().startswith(".")


def _is_control_message(msg) -> bool:
    return _is_control_message_text(getattr(msg, "message", None))


async def fetch_saved_ids(client: TelegramClient) -> List[int]:
    """
    Get IDs of Saved Messages (oldest → newest), only those with text or media.
    Also SKIP any message whose text starts with '.' (commands).
    """
    ids: List[int] = []
    async for m in client.iter_messages("me", limit=1000):
        if not (m.message or m.media):
            continue
        # skip command-style messages from Saved
        if _is_control_message(m):
            continue
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
                "📜 ✹ Self Commands ✹\n"
                "• .help  — show this help\n"
                "• .gc / .groups  — list targets\n"
                "• .addgc         — add targets (@ / t.me / ID)\n"
                "• .cleargc       — clear all targets\n"
                "• .delgc <target> — remove one target\n"
                "• .time          — set interval\n"
                "• .adreset       — restart Saved cycle\n"
                "• .status        — show plan + ETA\n\n"
                "🧷 .addgc usage:\n"
                "  1) .addgc @group1 @group2\n"
                "  2) .addgc  (then paste one per line)\n"
                "  3) Reply to a message that has @ / t.me links and send .addgc\n\n"
                f"💎 Premium upgrade: contact {PREMIUM_CONTACT}"
            )
            return

        # .gc / .groups
        if text.startswith(".gc") or text.startswith(".groups"):
            targets = list_groups(uid)
            cap = groups_cap(uid)
            if not targets:
                await event.reply(
                    f"🎯 Target list is empty (0/{cap}).\n"
                    "Use .addgc to add groups."
                )
                return

            lines = [f"{i+1:02d}. {g}" for i, g in enumerate(targets)]
            head = f"🎯 Targets ({len(targets)}/{cap})"
            await event.reply(f"{head}\n" + "\n".join(lines))
            return

        # .cleargc
        if text.startswith(".cleargc"):
            clear_groups(uid)
            await event.reply(
                f"✅ Cleared all group targets (0/{groups_cap(uid)})."
            )
            return

        # .addgc
        if text.startswith(".addgc"):
            lines: List[str] = []

            # 1) same-line args
            parts = text.split(maxsplit=1)
            if len(parts) > 1:
                for token in parts[1].split():
                    token = token.strip()
                    if token:
                        lines.append(token)

            # 2) extra lines (multi-line paste)
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
                    "  .addgc @group1\n"
                    "  .addgc   (then paste one per line)\n"
                    "  Or reply to a message that contains @usernames / t.me links and send .addgc"
                )
                return

            added = 0
            cap = groups_cap(uid)
            for ln in lines:
                added += add_group(uid, ln.strip())
                if len(list_groups(uid)) >= cap:
                    break

            now_count = len(list_groups(uid))
            await event.reply(
                f"✅ Added {added} target(s).\n"
                f"🎯 Now linked: {now_count}/{cap} groups."
            )
            return

        # .delgc
        if text.startswith(".delgc"):
            parts = text.split(maxsplit=1)
            if len(parts) < 2:
                await event.reply(
                    "❗ Usage: .delgc <@user or t.me/link>"
                )
                return
            target = parts[1].strip()
            targets = list_groups(uid)
            if target in targets:
                targets.remove(target)
                clear_groups(uid)
                for x in targets:
                    add_group(uid, x)
                await event.reply(
                    f"✅ Group {target} removed."
                )
            else:
                await event.reply(
                    "❗ That target is not in your group list."
                )
            return

        # .time
        if text.startswith(".time"):
            tokens = text.split()
            if len(tokens) == 1:
                await event.reply(
                    "⏱ Usage:\n"
                    "  Free: .time 30, .time 45, .time 60\n"
                    "  Premium: any minutes, e.g. .time 10, .time 90\n\n"
                    f"💎 Upgrade contact: {PREMIUM_CONTACT}"
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
                    "Try: .time 30 / .time 45 / .time 60\n\n"
                    f"For upgrade, contact {PREMIUM_CONTACT}."
                )
                return

            set_interval(uid, val)
            await event.reply(
                f"✅ Interval set to {val} minutes "
                f"({'Premium' if premium else 'Free'} plan)."
            )
            return

        # .adreset
        if text.startswith(".adreset"):
            st["idx"] = 0
            st["saved_ids"] = []
            await event.reply(
                "✅ Saved Messages cycle reset to first message."
            )
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
                f"• Next send: {eta}\n\n"
                f"💎 Upgrade: contact {PREMIUM_CONTACT}"
            )
            return

        # (future) .redeem CODE can be added here once DB-side redeem is ready
        # if text.startswith(".redeem"):
        #     ...


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
    client = st.get("client")
    if client is not None:
        try:
            if await client.is_connected():
                return
        except Exception:
            pass

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
    """
    Simple logic:
      - Take next Saved Message (by index)
      - Forward to all groups
      - Move index forward
      - Never forward messages that start with '.'
    """
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

    # hard guard: never forward command-style messages, even if they slipped into saved_ids
    if _is_control_message(msg):
        log.info("[u%s] skipping control Saved msg %s", uid, msg_id)
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
                    await client.disconnect()
                except Exception:
                    pass


if __name__ == "__main__":
    asyncio.run(main())
