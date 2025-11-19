# worker_forward.py — Pyrogram-based forwarder using Mongo sessions
#
# Uses:
#   - core.db sessions (saved by SpinifyLoginBot)
#   - groups list from core.db (per panel user)
#   - interval from core.db (per panel user)
#
# Commands (send from your logged-in account = same account whose session you added via login bot):
#   .help
#   .gc / .groups
#   .cleargc
#   .addgc   (also .addgroup)
#   .delgc   (also .delgroup)
#   .time 30|45|60          (free)
#   .time <any>[m|h]        (premium)
#   .delay <sec>            (premium)
#   .status
#   .adreset
#   .night ...              (premium)
#   .upgrade
#
# Free:
#   - Only .time 30 / 45 / 60
# Premium (determined by groups_cap(uid) > 10, i.e. 50-cap users):
#   - Custom interval (.time 10 / 90 / 2h / 120m ...)
#   - Custom delay (.delay)
#   - Auto-Night (.night)


import os
import asyncio
import logging
import re
from typing import Dict, Any, List
from datetime import datetime, date, time, timedelta, timezone

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None  # fallback

from pyrogram import Client, filters
from pyrogram.errors import FloodWait, Unauthorized
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
)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL)
log = logging.getLogger("worker")

PARALLEL_USERS = int(os.getenv("PARALLEL_USERS", "3"))
DEFAULT_DELAY_SEC = float(os.getenv("PER_GROUP_DELAY", "30"))
SEND_TIMEOUT = int(os.getenv("SEND_TIMEOUT", "60"))
TICK_INTERVAL = int(os.getenv("TICK_INTERVAL", "15"))

UPGRADE_CONTACT = os.getenv("UPGRADE_CONTACT", "@SpinifyAdsBot")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))

# --------------------------------------------------------------------
# Auto-Night (global window, premium can change via .night)
# --------------------------------------------------------------------

AUTONIGHT_PATH = os.path.join(os.path.dirname(__file__), "autonight.json")
DEFAULT_AUTONIGHT = {
    "enabled": True,
    "start": "23:00",      # 24h format HH:MM
    "end": "07:00",        # 24h format HH:MM
    "tz": "Asia/Kolkata",
}


def _load_autonight() -> dict:
    cfg = DEFAULT_AUTONIGHT.copy()
    try:
        if os.path.exists(AUTONIGHT_PATH):
            with open(AUTONIGHT_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                for k in cfg:
                    if k in data:
                        cfg[k] = data[k]
    except Exception:
        pass
    return cfg


def _save_autonight(cfg: dict) -> None:
    try:
        with open(AUTONIGHT_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _parse_hhmm(s: str) -> time:
    s = s.strip()
    if re.fullmatch(r"\d{1,2}", s):
        h = int(s)
        if not (0 <= h <= 23):
            raise ValueError("Hour must be 0..23")
        return time(h, 0)
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", s)
    if not m:
        raise ValueError("Time must be HH or HH:MM (24h)")
    h, mm = int(m.group(1)), int(m.group(2))
    if not (0 <= h <= 23 and 0 <= mm <= 59):
        raise ValueError("Invalid time")
    return time(h, mm)


def _get_now_tz(tz_name: str) -> datetime:
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo(tz_name))
        except Exception:
            pass
    return datetime.now()


def _in_window(now_t: time, start_t: time, end_t: time) -> bool:
    if start_t <= end_t:
        return start_t <= now_t < end_t
    # wrap midnight
    return (now_t >= start_t) or (now_t < end_t)


def _seconds_until_quiet_end(cfg: dict) -> int:
    tz = cfg.get("tz") or DEFAULT_AUTONIGHT["tz"]
    now = _get_now_tz(tz)
    start_t = _parse_hhmm(cfg.get("start", DEFAULT_AUTONIGHT["start"]))
    end_t = _parse_hhmm(cfg.get("end", DEFAULT_AUTONIGHT["end"]))
    today: date = now.date()

    if start_t <= end_t:
        end_dt = datetime.combine(today, end_t, tzinfo=now.tzinfo)
        if now.time() >= end_t:
            end_dt = end_dt + timedelta(days=1)
    else:
        if now.time() < end_t:
            end_dt = datetime.combine(today, end_t, tzinfo=now.tzinfo)
        else:
            end_dt = datetime.combine(today + timedelta(days=1), end_t, tzinfo=now.tzinfo)

    seconds = int((end_dt - now).total_seconds())
    return max(1, seconds)


def autonight_is_quiet(cfg: dict) -> bool:
    if not cfg.get("enabled", True):
        return False
    try:
        now = _get_now_tz(cfg.get("tz", DEFAULT_AUTONIGHT["tz"]))
        start_t = _parse_hhmm(cfg.get("start", DEFAULT_AUTONIGHT["start"]))
        end_t = _parse_hhmm(cfg.get("end", DEFAULT_AUTONIGHT["end"]))
        return _in_window(now.time(), start_t, end_t)
    except Exception:
        return False


def autonight_status_text(cfg: dict) -> str:
    state = "ON ✅" if cfg.get("enabled", True) else "OFF ❌"
    return (
        f"🌙 Auto-Night: <b>{state}</b>\n"
        f"Window: <b>{cfg.get('start','23:00')} → {cfg.get('end','07:00')}</b>\n"
        f"TZ: <b>{cfg.get('tz','Asia/Kolkata')}</b>"
    )


def autonight_parse_command(arg: str, cfg: dict) -> tuple[str, dict]:
    arg = (arg or "").strip()
    if not arg:
        return (autonight_status_text(cfg), cfg)

    low = arg.lower()
    if low in {"on", "enable", "enabled"}:
        cfg = cfg.copy()
        cfg["enabled"] = True
        _save_autonight(cfg)
        return ("✅ Auto-Night <b>enabled</b>.\n" + autonight_status_text(cfg), cfg)

    if low in {"off", "disable", "disabled"}:
        cfg = cfg.copy()
        cfg["enabled"] = False
        _save_autonight(cfg)
        return ("🚫 Auto-Night <b>disabled</b>.\n" + autonight_status_text(cfg), cfg)

    m = re.fullmatch(
        r"\s*(\d{1,2}(?::\d{2})?)\s*(?:to|–|—|-)\s*(\d{1,2}(?::\d{2})?)\s*",
        arg,
    )
    if not m:
        return (
            "❗ Format: <code>.night 23:00 to 07:00</code>\n"
            "Also works: <code>.night 23:00-07:00</code> (24h).",
            cfg,
        )

    start_raw, end_raw = m.group(1), m.group(2)
    try:
        start_t = _parse_hhmm(start_raw)
        end_t = _parse_hhmm(end_raw)
    except ValueError as e:
        return (f"❗ {e}", cfg)

    cfg = cfg.copy()
    cfg["start"] = f"{start_t.hour:02d}:{start_t.minute:02d}"
    cfg["end"] = f"{end_t.hour:02d}:{end_t.minute:02d}"
    _save_autonight(cfg)
    return (
        f"🕒 Auto-Night window updated:\n"
        f"<b>{cfg['start']} → {cfg['end']}</b> ({cfg.get('tz','Asia/Kolkata')})\n"
        + autonight_status_text(cfg),
        cfg,
    )


AUTONIGHT_CFG = _load_autonight()

# --------------------------------------------------------------------
# Forwarder core (Pyrogram + Mongo sessions)
# --------------------------------------------------------------------

HELP_TEXT = (
    "🛠 <b>Spinify Worker Commands</b>\n"
    "\n"
    "<b>Groups</b>\n"
    "• <code>.gc</code> or <code>.groups</code> — list groups\n"
    "• <code>.cleargc</code> — clear all groups\n"
    "• <code>.addgc</code> (or <code>.addgroup</code>) + paste @links / t.me links\n"
    "  → Works in the same message or by replying to a list.\n"
    "\n"
    "<b>Timing</b>\n"
    "• Free: <code>.time 30</code> / <code>.time 45</code> / <code>.time 60</code>\n"
    "• Premium: <code>.time 10</code>, <code>.time 90</code>, <code>.time 2h</code>, <code>.time 120m</code>\n"
    "• Premium: <code>.delay 5</code> (seconds between groups)\n"
    "• <code>.status</code> — show plan, interval, delay, Auto-Night\n"
    "• <code>.adreset</code> — restart Saved Messages cycle (back to first)\n"
    "\n"
    "<b>Night Mode (Premium)</b>\n"
    "• <code>.night</code> — show current window\n"
    "• <code>.night on</code> / <code>.night off</code>\n"
    "• <code>.night 23:00-07:00</code> — change quiet window\n"
    "\n"
    "<b>Upgrade</b>\n"
    "• <code>.upgrade</code> — get your user ID + contact for premium."
)

# Per-panel-user state: clients, saved message ids, current index, delay
STATE: Dict[int, Dict[str, Any]] = {}


def _panel_is_premium(uid: int) -> bool:
    """
    Treat users with groups_cap > 10 as premium
    (normal = 5, unlocked = 10, premium menu sets 50).
    Owner is always premium.
    """
    if OWNER_ID and uid == OWNER_ID:
        return True
    try:
        return groups_cap(uid) > 10
    except Exception:
        return False


async def fetch_saved_ids(app: Client) -> List[int]:
    """Fetch Saved Messages ids (oldest→newest) for this account."""
    ids: List[int] = []
    async for m in app.get_chat_history("me", limit=1000):
        if m.text or m.caption or m.media:
            ids.append(m.id)
    ids.reverse()
    return ids


def _next_idx(uid: int, n: int) -> int:
    st = STATE.setdefault(uid, {})
    if n <= 0:
        st["idx"] = 0
        return 0
    i = int(st.get("idx", 0))
    i = (i + 1) % n
    st["idx"] = i
    return i


def _cur_idx(uid: int) -> int:
    return int(STATE.get(uid, {}).get("idx", 0))


def _get_delay(uid: int) -> float:
    st = STATE.setdefault(uid, {})
    if "delay" not in st:
        st["delay"] = DEFAULT_DELAY_SEC
    return float(st["delay"])


def _set_delay(uid: int, value: float) -> None:
    st = STATE.setdefault(uid, {})
    st["delay"] = float(value)


def register_session_handlers(app: Client, uid: int) -> None:
    @app.on_message(filters.me & filters.text)
    async def my_text(_, msg: Message):
        t = (msg.text or "").strip()
        if not t.startswith("."):
            return

        premium = _panel_is_premium(uid)
        delay_sec = _get_delay(uid)

        try:
            # .help
            if t.startswith(".help"):
                await msg.reply_text(HELP_TEXT)
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

            # .addgc / .addgroup
            if t.startswith(".addgc") or t.startswith(".addgroup"):
                parts = t.splitlines()
                first = parts[0]
                if first.startswith(".addgroup"):
                    tail = first[len(".addgroup"):].strip()
                else:
                    tail = first[len(".addgc"):].strip()
                lines: List[str] = []
                if tail:
                    lines.append(tail)
                if len(parts) > 1:
                    for ln in parts[1:]:
                        ln = ln.strip()
                        if ln:
                            lines.append(ln)
                if not lines and msg.reply_to_message and (msg.reply_to_message.text or msg.reply_to_message.caption):
                    body = msg.reply_to_message.text or msg.reply_to_message.caption
                    for ln in body.splitlines():
                        ln = ln.strip()
                        if ln:
                            lines.append(ln)

                if not lines:
                    await msg.reply_text(
                        "⚠️ No targets found.\n"
                        "Usage examples:\n"
                        "  <code>.addgc @group1</code>\n"
                        "  <code>.addgc</code> then paste one per line\n"
                        "  Or reply to a message containing @links / t.me links."
                    )
                    return

                added = 0
                cap = groups_cap(uid)
                for ln in lines:
                    if not ln:
                        continue
                    added += add_group(uid, ln)
                    if len(list_groups(uid)) >= cap:
                        break
                await msg.reply_text(f"✅ Added {added}. Now {len(list_groups(uid))}/{cap}.")
                return

            # .delgc / .delgroup
            if t.startswith(".delgc") or t.startswith(".delgroup"):
                parts = t.split(maxsplit=1)
                if len(parts) < 2:
                    await msg.reply_text("❗ Usage: <code>.delgc &lt;@user or t.me/link&gt;</code>")
                    return
                target = parts[1].strip()
                targets = list_groups(uid)
                if target in targets:
                    targets.remove(target)
                    # re-save list by clearing then re-adding
                    clear_groups(uid)
                    for x in targets:
                        add_group(uid, x)
                    await msg.reply_text("✅ Group removed.")
                else:
                    await msg.reply_text("❗ That target is not in your group list.")
                return

            # .time
            if t.startswith(".time"):
                parts = t.split(maxsplit=1)
                if len(parts) < 2:
                    await msg.reply_text(
                        "❗ Usage (free): <code>.time 30</code> / <code>.time 45</code> / <code>.time 60</code>\n"
                        "Premium: <code>.time 10</code>, <code>.time 90</code>, <code>.time 2h</code>, <code>.time 120m</code>."
                    )
                    return
                arg = parts[1].strip()
                num_str = "".join(ch for ch in arg if ch.isdigit())
                if not num_str:
                    await msg.reply_text("❗ Please provide a numeric value, e.g. <code>.time 30</code>.")
                    return
                value = int(num_str)
                if value <= 0:
                    await msg.reply_text("❗ Interval must be > 0.")
                    return

                if not premium:
                    if value not in (30, 45, 60):
                        await msg.reply_text(
                            "💎 Custom interval is a premium feature.\n"
                            "Free users can only use <b>30, 45, or 60 minutes</b>:\n"
                            "  • <code>.time 30</code>\n"
                            "  • <code>.time 45</code>\n"
                            "  • <code>.time 60</code>\n"
                            f"For other values, use <code>.upgrade</code> and DM {UPGRADE_CONTACT}."
                        )
                        return
                    set_interval(uid, value)
                    await msg.reply_text(f"✅ Interval set to <b>{value} minutes</b> (free plan).")
                    return

                # premium
                if "h" in arg.lower():
                    mins = value * 60
                else:
                    mins = value
                set_interval(uid, mins)
                await msg.reply_text(f"✅ Interval set to <b>{mins} minutes</b> (premium).")
                return

            # .delay  (premium only)
            if t.startswith(".delay"):
                if not premium:
                    await msg.reply_text(
                        "💎 Custom delay is a premium feature.\n"
                        f"Use <code>.upgrade</code> and DM {UPGRADE_CONTACT}."
                    )
                    return
                parts = t.split(maxsplit=1)
                if len(parts) < 2:
                    await msg.reply_text("❗ Usage: <code>.delay 5</code> (seconds).")
                    return
                num_str = "".join(ch for ch in parts[1] if ch.isdigit())
                if not num_str:
                    await msg.reply_text("❗ Please provide a numeric delay in seconds.")
                    return
                value = int(num_str)
                if value <= 0:
                    await msg.reply_text("❗ Delay must be > 0 seconds.")
                    return
                _set_delay(uid, value)
                await msg.reply_text(f"✅ Message delay set to <b>{value} seconds</b>.")
                return

            # .status
            if t.startswith(".status"):
                interval = get_interval(uid)
                delay_sec = _get_delay(uid)
                plan = "Premium ✅" if premium else "Free ⚪"
                targets = list_groups(uid)
                await msg.reply_text(
                    "📊 <b>Status</b>\n"
                    f"• Plan: {plan}\n"
                    f"• Groups: <b>{len(targets)}/{groups_cap(uid)}</b>\n"
                    f"• Interval: <b>{interval} minutes</b>\n"
                    f"• Delay between groups: <b>{int(delay_sec)} sec</b>\n\n"
                    + autonight_status_text(AUTONIGHT_CFG)
                )
                return

            # .adreset
            if t.startswith(".adreset"):
                st = STATE.setdefault(uid, {})
                st["idx"] = 0
                await msg.reply_text("✅ Saved Messages cycle reset to first message.")
                return

            # .night (premium only)
            if t.startswith(".night"):
                if not premium:
                    await msg.reply_text(
                        "💎 Auto-Night scheduling is a premium feature.\n"
                        f"Use <code>.upgrade</code> and DM {UPGRADE_CONTACT}."
                    )
                    return
                arg = t[6:].strip() if len(t) > 6 else ""
                msg_txt, new_cfg = autonight_parse_command(arg, AUTONIGHT_CFG)
                for k in AUTONIGHT_CFG.keys():
                    if k in new_cfg:
                        AUTONIGHT_CFG[k] = new_cfg[k]
                await msg.reply_text(msg_txt)
                return

            # .upgrade
            if t.startswith(".upgrade"):
                me = msg.from_user
                uid_str = me.id if me else "unknown"
                await msg.reply_text(
                    "💎 <b>Upgrade Info</b>\n"
                    f"• Your Telegram user ID: <code>{uid_str}</code>\n"
                    f"• Please send this ID to {UPGRADE_CONTACT} to upgrade.\n"
                    "\nAfter upgrade you unlock:\n"
                    "  – Custom interval (.time any value)\n"
                    "  – Custom delay (.delay)\n"
                    "  – Auto-Night (.night)\n"
                )
                return

        except Exception as e:
            log.error("cmd error uid=%s: %s", uid, e)


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
            log.info("[u%s] started session slot %s", uid, s["slot"])
        except Unauthorized:
            log.warning("[u%s] session slot %s unauthorized", uid, s.get("slot"))
        except Exception as e:
            log.error("[u%s] start session failed: %s", uid, e)
    return apps


async def ensure_state(uid: int) -> None:
    st = STATE.get(uid)
    if st and st.get("apps"):
        return
    apps = await build_clients_for_user(uid)
    STATE[uid] = {
        "apps": apps,
        "idx": 0,
        "saved_ids": [],
        "delay": DEFAULT_DELAY_SEC,
    }


async def refresh_saved(uid: int) -> None:
    st = STATE.get(uid)
    if not st or not st.get("apps"):
        return
    app = st["apps"][0]
    try:
        saved = await fetch_saved_ids(app)
        st["saved_ids"] = saved
    except Exception as e:
        log.error("[u%s] fetch_saved_ids error: %s", uid, e)


async def send_copy(app: Client, from_chat, msg_id: int, to_target: str) -> bool:
    try:
        await app.copy_message(chat_id=to_target, from_chat_id=from_chat, message_id=msg_id)
        return True
    except FloodWait as fw:
        await asyncio.sleep(fw.value)
        return False
    except Exception as e:
        log.warning("copy fail → %s", e)
        return False


async def run_cycle(uid: int) -> None:
    st = STATE.get(uid)
    if not st or not st.get("apps"):
        return
    apps = st["apps"]
    targets = list_groups(uid)
    if not targets:
        return
    if not st["saved_ids"]:
        await refresh_saved(uid)
    if not st["saved_ids"]:
        log.info("[u%s] no saved messages to send", uid)
        return

    idx = _cur_idx(uid)
    msg_id = st["saved_ids"][idx]
    app = apps[0]
    delay_sec = _get_delay(uid)

    ok_any = False
    for tg in targets:
        try:
            good = await asyncio.wait_for(send_copy(app, "me", msg_id, tg), timeout=SEND_TIMEOUT)
            if good:
                ok_any = True
                inc_sent_ok(uid, 1)
        except asyncio.TimeoutError:
            log.warning("[u%s] send timeout to %s", uid, tg)
        await asyncio.sleep(delay_sec)

    if ok_any:
        mark_sent_now(uid)
        _next_idx(uid, len(st["saved_ids"]))


async def user_loop(uid: int) -> None:
    await ensure_state(uid)
    interval = get_interval(uid)
    last = get_last_sent_at(uid)
    now = int(datetime.now(timezone.utc).timestamp())
    if last is None or (now - last) >= interval * 60:
        # global Auto-Night check before sending
        if autonight_is_quiet(AUTONIGHT_CFG):
            log.info("[u%s] Auto-Night quiet window active, skipping cycle.", uid)
            return
        await run_cycle(uid)


async def main_loop() -> None:
    init_db()
    log.info("worker started")
    while True:
        uids = users_with_sessions()
        sem = asyncio.Semaphore(PARALLEL_USERS)

        async def run_for(uid: int) -> None:
            async with sem:
                try:
                    await user_loop(uid)
                except Exception as e:
                    log.error("loop error u%s: %s", uid, e)

        tasks = [asyncio.create_task(run_for(uid)) for uid in uids]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(TICK_INTERVAL)


async def main() -> None:
    # ensure Auto-Night config file exists
    if not os.path.exists(AUTONIGHT_PATH):
        _save_autonight(AUTONIGHT_CFG)
    await main_loop()


if __name__ == "__main__":
    import json  # needed for Auto-Night load/save

    asyncio.run(main())
