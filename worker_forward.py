import os
import json
import asyncio
import logging
import sqlite3
import re
from datetime import datetime, date, time, timedelta
from typing import Tuple

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None  # will fall back to local time without TZ

from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError, RPCError

from core.db import (
    init_db,
    is_premium, set_premium,
    get_voucher, mark_voucher_used,
)

# =========================
# Auto-Night configuration
# =========================
AUTONIGHT_PATH = os.path.join(os.path.dirname(__file__), "autonight.json")
DEFAULT_AUTONIGHT = {
    "enabled": True,
    "start": "23:00",        # 24h format HH:MM
    "end": "07:00",          # 24h format HH:MM
    "tz": "Asia/Kolkata"
}


def _load_autonight() -> dict:
    cfg = DEFAULT_AUTONIGHT.copy()
    try:
        if os.path.exists(AUTONIGHT_PATH):
            with open(AUTONIGHT_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                cfg.update({k: data.get(k, cfg[k]) for k in cfg})
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
    # Accept "7", "07", "7:00", "07:00"
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
    # Fallback: naive local time
    return datetime.now()


def _in_window(now_t: time, start_t: time, end_t: time) -> bool:
    """True if now is within [start, end) with midnight wrap support."""
    if start_t <= end_t:
        return start_t <= now_t < end_t
    # crosses midnight, e.g., 23:00 -> 07:00
    return (now_t >= start_t) or (now_t < end_t)


def _seconds_until_quiet_end(cfg: dict) -> int:
    """Return seconds until the end of quiet window (>= 1), assuming we are currently in quiet."""
    tz = cfg.get("tz") or DEFAULT_AUTONIGHT["tz"]
    now = _get_now_tz(tz)
    start_t = _parse_hhmm(cfg.get("start", DEFAULT_AUTONIGHT["start"]))
    end_t   = _parse_hhmm(cfg.get("end", DEFAULT_AUTONIGHT["end"]))
    today = now.date()

    # Compute next end datetime
    if start_t <= end_t:
        # non-wrapping window (e.g., 02:00 -> 05:00)
        end_dt = datetime.combine(today, end_t, tzinfo=now.tzinfo)
        if now.time() >= end_t:
            end_dt = end_dt + timedelta(days=1)
    else:
        # wrapping window (e.g., 23:00 -> 07:00)
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
        end_t   = _parse_hhmm(cfg.get("end", DEFAULT_AUTONIGHT["end"]))
        return _in_window(now.time(), start_t, end_t)
    except Exception:
        # Fail open if config broken
        return False


def autonight_status_text(cfg: dict) -> str:
    state = "ON ✅" if cfg.get("enabled", True) else "OFF ❌"
    return (
        f"🌙 Auto-Night: {state}\n"
        f"Window: {cfg.get('start','23:00')} → {cfg.get('end','07:00')}\n"
        f"TZ: {cfg.get('tz','Asia/Kolkata')}"
    )


def autonight_parse_command(arg: str, cfg: dict) -> Tuple[str, dict]:
    """
    Returns (message_text, updated_cfg or same).
    Supported:
      .night
      .night on | off
      .night 23:00 to 07:00   (also supports -, – , —)
      .night 23-7
    """
    arg = (arg or "").strip()
    if not arg:
        return (autonight_status_text(cfg), cfg)

    low = arg.lower()
    if low in {"on", "enable", "enabled"}:
        cfg = cfg.copy()
        cfg["enabled"] = True
        _save_autonight(cfg)
        return ("✅ Auto-Night enabled.\n" + autonight_status_text(cfg), cfg)

    if low in {"off", "disable", "disabled"}:
        cfg = cfg.copy()
        cfg["enabled"] = False
        _save_autonight(cfg)
        return ("🚫 Auto-Night disabled.\n" + autonight_status_text(cfg), cfg)

    # Time range
    m = re.fullmatch(
        r"\s*(\d{1,2}(?::\d{2})?)\s*(?:to|–|—|-)\s*(\d{1,2}(?::\d{2})?)\s*",
        arg
    )
    if not m:
        return (
            "❗ Format: ✹ .night 23:00 to 07:00 ✹\n"
            "Also works: ✹ .night 23:00-07:00 ✹ (24-hour times).",
            cfg
        )

    start_raw, end_raw = m.group(1), m.group(2)
    try:
        start_t = _parse_hhmm(start_raw)
        end_t   = _parse_hhmm(end_raw)
    except ValueError as e:
        return (f"❗ {e}", cfg)

    cfg = cfg.copy()
    cfg["start"] = f"{start_t.hour:02d}:{start_t.minute:02d}"
    cfg["end"]   = f"{end_t.hour:02d}:{end_t.minute:02d}"
    _save_autonight(cfg)
    return (
        f"🕒 Auto-Night window updated:\n"
        f"{cfg['start']} → {cfg['end']} ({cfg.get('tz','Asia/Kolkata')})\n"
        + autonight_status_text(cfg),
        cfg,
    )


# =========================
# Forwarder logic + premium
# =========================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

USERS_DIR = "users"
SESSIONS_DIR = "sessions"
clients = {}
started_phones = set()

AUTONIGHT_CFG = _load_autonight()

UPGRADE_CONTACT = os.getenv("UPGRADE_CONTACT", "@SpinifyAdsBot")
DEVELOPER_ID = 7876302875  # always premium


def _is_control_message(msg) -> bool:
    """
    True if this Saved Message looks like a command (.help/.time/...), so we do NOT forward it.
    """
    txt = (getattr(msg, "raw_text", None) or "").strip()
    if not txt:
        return False
    return txt.startswith(".")


async def run_user_bot(config):
    phone = config["phone"]
    if phone in started_phones:
        return

    session_path = os.path.join(SESSIONS_DIR, f"{phone}.session")
    api_id = int(config["api_id"])
    api_hash = config["api_hash"]
    groups = config.get("groups", [])
    delay = config.get("msg_delay_sec", 5)
    cycle = config.get("cycle_delay_min", 15)

    user_state = {
        "delay": delay,   # seconds between forwards
        "cycle": cycle,   # minutes between cycles
    }

    client = TelegramClient(session_path, api_id, api_hash)

    try:
        await client.start()
    except sqlite3.OperationalError as e:
        logger.error(f"[{phone}] SQLite lock error: {e}")
        return
    except SessionPasswordNeededError:
        logger.error(f"[{phone}] 2FA password required. Skipping.")
        return
    except RPCError as e:
        logger.error(f"[{phone}] RPC Error: {e}")
        return
    except Exception as e:
        logger.exception(f"[{phone}] Failed to start client: {e}")
        return

    started_phones.add(phone)
    logger.info(f"[✔] Started bot for {config.get('name','N/A')} ({phone})")

    # ---------- COMMAND HANDLER ----------
    @client.on(events.NewMessage(outgoing=True))
    async def command_handler(event):
        """Commands: only outgoing messages from this account."""
        text = (event.raw_text or "").strip()
        if not text.startswith("."):
            return

        me = await client.get_me()
        uid = int(me.id)

        # premium from DB
        premium = is_premium(uid)
        if uid == DEVELOPER_ID:
            premium = True

        logger.info(f"[{phone}] cmd: {text!r} premium={premium}")

        # ----- TIME -----
        if text.startswith(".time"):
            value_str = ''.join(filter(str.isdigit, text)) or "0"
            value = int(value_str)
            if value <= 0:
                await event.reply(
                    "❗ Usage (free): ✹ .time 30 ✹ / ✹ .time 45 ✹ / ✹ .time 60 ✹\n"
                    "Premium can also use ✹ .time 10 ✹ / ✹ .time 90 ✹ / ✹ .time 2h ✹ etc."
                )
                return

            if not premium:
                if value not in (30, 45, 60):
                    await event.reply(
                        "💎 Custom interval is a premium feature.\n"
                        "Free users can only use:\n"
                        "  • ✹ .time 30 ✹\n"
                        "  • ✹ .time 45 ✹\n"
                        "  • ✹ .time 60 ✹\n"
                        f"For more, ask for a code from {UPGRADE_CONTACT} and use ✹ .redeem CODE ✹."
                    )
                    return
                user_state["cycle"] = value
                await event.reply(f"✅ Interval set to {value} minutes (free plan).")
                return

            # PREMIUM → full custom support with optional 'h'
            if 'h' in text.lower():
                user_state["cycle"] = value * 60
            else:
                user_state["cycle"] = value
            await event.reply(f"✅ Cycle delay set to {user_state['cycle']} minutes (premium).")

        # ----- PREMIUM-ONLY: .delay -----
        elif text.startswith(".delay"):
            if not premium:
                await event.reply(
                    "💎 Custom delay is a premium feature.\n"
                    f"Ask {UPGRADE_CONTACT} for a code and use ✹ .redeem CODE ✹."
                )
                return
            value_str = ''.join(filter(str.isdigit, text)) or "0"
            value = int(value_str)
            if value <= 0:
                await event.reply("❗ Usage: ✹ .delay 5 ✹ (seconds)")
                return
            user_state["delay"] = value
            await event.reply(f"✅ Message delay set to {value} seconds.")

        # ----- STATUS -----
        elif text.startswith(".status"):
            plan = "Premium ✅" if premium else "Free ⚪"
            await event.reply(
                "📊 Status\n"
                f"• Plan: {plan}\n"
                f"• Cycle Delay: {user_state['cycle']} minutes\n"
                f"• Message Delay: {user_state['delay']} seconds\n\n"
                + autonight_status_text(AUTONIGHT_CFG)
            )

        # ----- INFO -----
        elif text.startswith(".info"):
            expiry = config.get("plan_expiry", "N/A")
            plan = "Premium" if premium else "Free"
            reply = (
                "❀ User Info\n"
                f"❀ Name: {config.get('name')}\n"
                f"❀ Phone: {phone}\n"
                f"❀ Telegram ID: {uid}\n"
                f"❀ Plan: {plan}\n"
                f"❀ Cycle Delay: {user_state['cycle']} min\n"
                f"❀ Message Delay: {user_state['delay']} sec\n"
                f"❀ Groups: {len(groups)}\n"
                f"❀ Legacy Plan Expiry (if any): {expiry}\n\n"
                + autonight_status_text(AUTONIGHT_CFG)
            )
            await event.reply(reply)

        # ----- REDEEM PREMIUM CODE -----
        elif text.startswith(".redeem"):
            parts = text.split(maxsplit=1)
            if len(parts) < 2:
                await event.reply("Usage: ✹ .redeem SPN-XXXXXX ✹")
                return

            code = parts[1].strip().upper()

            v = get_voucher(code)
            if not v:
                await event.reply("❌ Invalid or unknown code.")
                return

            if v.get("used"):
                await event.reply("⚠️ This code has already been used.")
                return

            allowed_uid = v.get("user_id")
            if allowed_uid is not None and int(allowed_uid) != uid:
                await event.reply(
                    "⚠️ This code is locked to a different user.\n"
                    f"If you think this is a mistake, contact {UPGRADE_CONTACT}."
                )
                return

            # activate premium on this Telegram user
            set_premium(uid, True)
            mark_voucher_used(code, uid)

            await event.reply(
                "💎 Premium activated!\n"
                "You now have:\n"
                "  • Custom interval: ✹ .time <value>[m|h] ✹\n"
                "  • Custom delay: ✹ .delay <sec> ✹\n"
                "  • Auto-Night: ✹ .night ✹ commands"
            )

        # ----- GROUPS ADD -----
        elif text.startswith(".addgroup"):
            pattern = r"(https?://t\.me/\S+|t\.me/\S+|@\w+)"
            links = re.findall(pattern, text)

            if event.is_reply and not links:
                reply_msg = await event.get_reply_message()
                if reply_msg and reply_msg.raw_text:
                    links = re.findall(pattern, reply_msg.raw_text)

            if not links:
                await event.reply(
                    "⚠️ No valid group links or usernames found.\n"
                    "Example: ✹ .addgroup https://t.me/yourgc ✹ or ✹ .addgroup @yourgc ✹"
                )
                return

            added, skipped = [], []
            for link in links:
                if link not in groups:
                    groups.append(link)
                    added.append(link)
                else:
                    skipped.append(link)

            config["groups"] = groups
            try:
                os.makedirs(USERS_DIR, exist_ok=True)
                with open(os.path.join(USERS_DIR, f"{phone}.json"), "w", encoding="utf-8") as f:
                    json.dump(config, f, ensure_ascii=False, indent=2)
            except Exception as e:
                logger.error(f"[{phone}] Error saving user config: {e}")

            msg_parts = []
            if added:
                msg_parts.append(f"✅ Added {len(added)} new group(s).")
            if skipped:
                msg_parts.append(f"⚠️ Skipped {len(skipped)} duplicate(s).")
            await event.reply("\n".join(msg_parts) or "No changes.")

        # ----- GROUPS DEL -----
        elif text.startswith(".delgroup"):
            parts = text.split()
            if len(parts) == 2 and parts[1] in groups:
                groups.remove(parts[1])
                config["groups"] = groups
                try:
                    with open(os.path.join(USERS_DIR, f"{phone}.json"), "w", encoding="utf-8") as f:
                        json.dump(config, f, ensure_ascii=False, indent=2)
                except Exception as e:
                    logger.error(f"[{phone}] Error saving user config on delgroup: {e}")
                await event.reply("❀ Group removed.")
            else:
                await event.reply(
                    "❗ Usage: ✹ .delgroup https://t.me/yourgc ✹ or ✹ .delgroup @yourgc ✹"
                )

        # ----- GROUPS LIST -----
        elif text.startswith(".groups"):
            if groups:
                lines = [f"{i+1}. {g}" for i,g in enumerate(groups)]
                txt = "❀ Groups:\n" + "\n".join(lines)
                await event.reply(txt)
            else:
                await event.reply("📋 No groups configured.")

        # ----- PREMIUM-ONLY: NIGHT -----
        elif text.startswith(".night"):
            if not premium:
                await event.reply(
                    "💎 Auto-Night is a premium feature.\n"
                    f"Ask {UPGRADE_CONTACT} for a code and use ✹ .redeem CODE ✹."
                )
                return
            arg = text[6:].strip() if len(text) > 6 else ""
            msg_text, new_cfg = autonight_parse_command(arg, AUTONIGHT_CFG)
            for k in list(AUTONIGHT_CFG.keys()):
                AUTONIGHT_CFG[k] = new_cfg.get(k, AUTONIGHT_CFG[k])
            await event.reply(msg_text)

        # ----- HELP -----
        elif text.startswith(".help"):
            await event.reply(
                "🛠 Available Commands\n"
                "Free:\n"
                "• ✹ .status ✹ – status + plan + Auto-Night\n"
                "• ✹ .info ✹ – account info\n"
                "• ✹ .groups ✹ – list groups\n"
                "• ✹ .addgroup <url/@user ...> ✹ – add group(s)\n"
                "• ✹ .delgroup <url/@user> ✹ – remove group\n"
                "• ✹ .time 30|45|60 ✹ – basic interval (minutes)\n"
                "• ✹ .redeem CODE ✹ – activate premium code\n\n"
                "Premium only:\n"
                "• ✹ .time <value>[m|h] ✹ – custom interval (e.g. 10, 90, 2h)\n"
                "• ✹ .delay <sec> ✹ – custom delay between messages\n"
                "• ✹ .night / .night on|off / .night 23:00-07:00 ✹ – Auto-Night"
            )

    # ---------- Forward loop ----------
    async def forward_loop():
        while True:
            try:
                if autonight_is_quiet(AUTONIGHT_CFG):
                    secs = _seconds_until_quiet_end(AUTONIGHT_CFG)
                    mins = max(1, secs // 60)
                    logger.info(f"[{phone}] 🌙 Auto-Night active. Sleeping ~{mins} min (until window ends).")
                    await asyncio.sleep(secs)
                    continue

                messages = await client.get_messages("me", limit=100)
                messages = list(reversed(messages))

                interrupted_by_night = False

                for msg in messages:
                    # skip empty
                    if msg.raw_text is None and not msg.media:
                        continue

                    # 🚫 don't forward commands (.help, .time, .redeem, etc.)
                    if _is_control_message(msg):
                        continue

                    if autonight_is_quiet(AUTONIGHT_CFG):
                        interrupted_by_night = True
                        logger.info(f"[{phone}] Entered Auto-Night mid-cycle. Pausing forwards.")
                        break

                    for group in groups:
                        try:
                            await client.forward_messages(group, msg)
                            logger.info(f"[{phone}] Forwarded to {group}")
                        except Exception as e:
                            logger.warning(f"[{phone}] Error forwarding to {group}: {e}")

                    await asyncio.sleep(user_state["delay"])

                if interrupted_by_night:
                    secs = _seconds_until_quiet_end(AUTONIGHT_CFG)
                    mins = max(1, secs // 60)
                    logger.info(f"[{phone}] 🌙 Auto-Night active. Sleeping ~{mins} min.")
                    await asyncio.sleep(secs)
                    continue

                logger.info(f"[{phone}] Cycle complete. Sleeping for {user_state['cycle']} minutes...")
                await asyncio.sleep(user_state["cycle"] * 60)

            except Exception as e:
                logger.exception(f"[{phone}] Error in forward loop: {e}")
                await asyncio.sleep(60)

    asyncio.create_task(forward_loop())
    await client.run_until_disconnected()


async def user_loader():
    init_db()  # make sure Mongo ready for vouchers/premium
    while True:
        os.makedirs(USERS_DIR, exist_ok=True)
        for file in os.listdir(USERS_DIR):
            if file.endswith(".json"):
                path = os.path.join(USERS_DIR, file)
                try:
                    with open(path, 'r', encoding="utf-8") as f:
                        config = json.load(f)
                        expiry = config.get("plan_expiry")
                        if expiry:
                            try:
                                if datetime.now() > datetime.fromisoformat(expiry):
                                    logger.info(f"[⏳] Legacy plan expired for {config['phone']}. Skipping.")
                                    # we still allow DB premium via vouchers regardless of legacy expiry
                            except Exception:
                                pass
                        asyncio.create_task(run_user_bot(config))
                except Exception as e:
                    logger.error(f"Error loading user config {file}: {e}")
        await asyncio.sleep(60)


async def main():
    os.makedirs(SESSIONS_DIR, exist_ok=True)
    os.makedirs(USERS_DIR, exist_ok=True)
    if not os.path.exists(AUTONIGHT_PATH):
        _save_autonight(AUTONIGHT_CFG)
    await user_loader()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown requested. Exiting.")
