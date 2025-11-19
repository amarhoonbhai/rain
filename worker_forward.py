import os
import json
import asyncio
import logging
import sqlite3
import re
from datetime import datetime, date, time, timedelta
from typing import Tuple, List

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None  # will fall back to local time without TZ

from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError, RPCError, FloodWait

# =========================
# Paths & logging
# =========================

USERS_DIR = "users"
SESSIONS_DIR = "sessions"

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("worker")

started_phones = set()

# Where to send ID for upgrade (env or default text)
UPGRADE_CONTACT = os.getenv("UPGRADE_CONTACT", "the bot owner")

# Your own dev ID always premium
DEVELOPER_ID = 7876302875

# Defaults if not present in user config
DEFAULT_MSG_DELAY_SEC = 5      # seconds between groups
DEFAULT_CYCLE_DELAY_MIN = 15   # minutes between cycles

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
    end_t = _parse_hhmm(cfg.get("end", DEFAULT_AUTONIGHT["end"]))
    today: date = now.date()

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
        end_t = _parse_hhmm(cfg.get("end", DEFAULT_AUTONIGHT["end"]))
        return _in_window(now.time(), start_t, end_t)
    except Exception:
        # Fail open if config broken
        return False


def autonight_status_text(cfg: dict) -> str:
    state = "ON ✅" if cfg.get("enabled", True) else "OFF ❌"
    return (
        f"🌙 Auto-Night: **{state}**\n"
        f"Window: **{cfg.get('start','23:00')} → {cfg.get('end','07:00')}**\n"
        f"TZ: **{cfg.get('tz','Asia/Kolkata')}**"
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
        return ("✅ Auto-Night **enabled**.\n" + autonight_status_text(cfg), cfg)

    if low in {"off", "disable", "disabled"}:
        cfg = cfg.copy()
        cfg["enabled"] = False
        _save_autonight(cfg)
        return ("🚫 Auto-Night **disabled**.\n" + autonight_status_text(cfg), cfg)

    # Time range
    m = re.fullmatch(
        r"\s*(\d{1,2}(?::\d{2})?)\s*(?:to|–|—|-)\s*(\d{1,2}(?::\d{2})?)\s*",
        arg
    )
    if not m:
        return (
            "❗ Format: `.night 23:00 to 07:00`\n"
            "Also works with a dash: `.night 23:00-07:00` (24-hour times).",
            cfg
        )

    start_raw, end_raw = m.group(1), m.group(2)
    try:
        start_t = _parse_hhmm(start_raw)
        end_t = _parse_hhmm(end_raw)
    except ValueError as e:
        return (f"❗ {e}", cfg)

    cfg = cfg.copy()
    cfg["start"] = f"{start_t.hour:02d}:{start_t.minute:02d}"
    cfg["end"]   = f"{end_t.hour:02d}:{end_t.minute:02d}"
    _save_autonight(cfg)
    return (
        f"🕒 Auto-Night window updated:\n"
        f"**{cfg['start']} → {cfg['end']}** ({cfg.get('tz','Asia/Kolkata')})\n"
        + autonight_status_text(cfg),
        cfg,
    )


# =========================
# Premium helpers
# =========================

def _is_premium(config: dict, me_id: int | None) -> bool:
    """
    Premium if:
      - plan_expiry exists AND is in future, OR
      - account id == DEVELOPER_ID.
    """
    if me_id == DEVELOPER_ID:
        return True
    try:
        expiry = config.get("plan_expiry")
        if not expiry:
            return False
        dt = datetime.fromisoformat(expiry)
        return datetime.now() <= dt
    except Exception:
        return False


# =========================
# Telethon forwarder logic
# =========================

AUTONIGHT_CFG = _load_autonight()


def _normalize_target(raw: str) -> str:
    """
    Accept:
      - @username
      - t.me/username
      - https://t.me/username
      - numeric id (as text)
    Return as user typed (no special transform), just stripped.
    """
    raw = (raw or "").strip()
    return raw


def _extract_targets(text: str) -> List[str]:
    """
    Extract @usernames and t.me links from text.
    """
    if not text:
        return []
    pattern = r"(https?://t\.me/\S+|t\.me/\S+|@\w+)"
    found = re.findall(pattern, text)
    return [_normalize_target(x) for x in found]


async def forward_one_message(client: TelegramClient, msg, group: str, phone: str) -> bool:
    """
    Forward a single message object from Saved Messages ('me') to group.
    """
    try:
        await client.forward_messages(entity=group, messages=msg.id, from_peer="me")
        logger.info("[%s] Forwarded msg_id=%s to %s", phone, msg.id, group)
        return True
    except FloodWait as fw:
        logger.warning("[%s] FloodWait %ss forwarding to %s", phone, fw.seconds, group)
        await asyncio.sleep(fw.seconds)
        return False
    except Exception as e:
        logger.warning("[%s] Error forwarding to %s: %s", phone, group, e)
        return False


async def run_user_bot(config: dict):
    phone = config["phone"]
    if phone in started_phones:
        return

    os.makedirs(SESSIONS_DIR, exist_ok=True)
    os.makedirs(USERS_DIR, exist_ok=True)

    session_path = os.path.join(SESSIONS_DIR, f"{phone}.session")
    api_id = int(config["api_id"])
    api_hash = config["api_hash"]
    groups = config.get("groups", [])
    delay = int(config.get("msg_delay_sec", DEFAULT_MSG_DELAY_SEC))
    cycle = int(config.get("cycle_delay_min", DEFAULT_CYCLE_DELAY_MIN))

    user_state = {
        "delay": delay,   # seconds between forwards
        "cycle": cycle,   # minutes between cycles
        "idx": 0,         # index for Saved Messages
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

    # fetch self for premium checks
    try:
        me = await client.get_me()
        me_id = me.id
    except Exception:
        me = None
        me_id = None

    # ---------- COMMAND HANDLER ----------
    @client.on(events.NewMessage(outgoing=True))
    async def command_handler(event):
        nonlocal groups
        text = (event.raw_text or "").strip()
        if not text.startswith("."):
            return

        premium = _is_premium(config, me_id)
        logger.info("[%s] cmd: %r premium=%s", phone, text, premium)

        # ----- HELP -----
        if text.startswith(".help"):
            await event.respond(
                "🛠 Available Commands:\n"
                "• `.status` — Show status + plan + Auto-Night\n"
                "• `.info` — Show full user info\n"
                "• `.groups` / `.gc` — List groups\n"
                "• `.addgroup` / `.addgc` — Add group(s)\n"
                "• `.delgroup <target>` / `.delgc <target>` — Remove\n"
                "• `.time 30|45|60` — Set basic interval (minutes, free)\n"
                "• `.upgrade` — Show your user ID & upgrade instructions\n"
                "\nPremium-only (after upgrade):\n"
                "• `.time <value>[m|h]` — Custom interval (e.g., 10, 90, 2h)\n"
                "• `.delay <sec>` — Custom delay between messages\n"
                "• `.night` / `.night on|off` / `.night 23:00-07:00` — Auto-Night\n"
                "• `.adreset` — Reset Saved Messages cycle"
            )
            return

        # ----- GROUPS LIST -----
        if text.startswith(".gc") or text.startswith(".groups"):
            if groups:
                lines = "\n".join(f"• {g}" for g in groups)
                await event.respond(f"❀ Groups ({len(groups)}):\n{lines}")
            else:
                await event.respond("📋 No groups configured yet.\nUse `.addgroup` or `.addgc` to add.")
            return

        # ----- CLEAR GROUPS -----
        if text.startswith(".cleargc"):
            groups = []
            config["groups"] = groups
            try:
                with open(os.path.join(USERS_DIR, f"{phone}.json"), "w", encoding="utf-8") as f:
                    json.dump(config, f, ensure_ascii=False, indent=2)
            except Exception as e:
                logger.error("[%s] Error saving user config (cleargc): %s", phone, e)
            await event.respond("✅ Cleared all groups.")
            return

        # ----- ADD GROUP(S)  (.addgroup / .addgc) -----
        if text.startswith(".addgroup") or text.startswith(".addgc"):
            # collect everything after command + following lines + reply text
            body_lines: List[str] = []

            # first line tail
            parts = text.splitlines()
            cmd_line = parts[0]
            tail = cmd_line.split(maxsplit=1)[1] if len(cmd_line.split()) > 1 else ""
            if tail:
                body_lines.append(tail)

            # extra lines in same message
            if len(parts) > 1:
                for ln in parts[1:]:
                    ln = ln.strip()
                    if ln:
                        body_lines.append(ln)

            # if nothing typed after cmd, but replied to a message with list
            if not body_lines and event.is_reply:
                reply_msg = await event.get_reply_message()
                if reply_msg and (reply_msg.raw_text or reply_msg.message):
                    rt = reply_msg.raw_text or reply_msg.message
                    for ln in rt.splitlines():
                        ln = ln.strip()
                        if ln:
                            body_lines.append(ln)

            # final targets
            targets: List[str] = []
            for ln in body_lines:
                targets.extend(_extract_targets(ln))
                # also accept plain @something or t.me/xxx with no regex match (fallback)
                if not targets and (ln.startswith("@") or "t.me/" in ln):
                    targets.append(_normalize_target(ln))

            if not targets:
                await event.respond(
                    "⚠️ No valid group handles found.\n"
                    "Examples:\n"
                    "  `.addgroup @yourgroup`\n"
                    "  `.addgroup https://t.me/yourgroup`\n"
                    "Or reply to a message with one handle per line."
                )
                return

            added, skipped = [], []
            for t in targets:
                if t in groups:
                    skipped.append(t)
                else:
                    groups.append(t)
                    added.append(t)

            config["groups"] = groups
            try:
                with open(os.path.join(USERS_DIR, f"{phone}.json"), "w", encoding="utf-8") as f:
                    json.dump(config, f, ensure_ascii=False, indent=2)
            except Exception as e:
                logger.error("[%s] Error saving user config (addgroup): %s", phone, e)

            parts_msg = []
            if added:
                parts_msg.append(f"✅ Added **{len(added)}** new group(s).")
            if skipped:
                parts_msg.append(f"⚠️ Skipped **{len(skipped)}** duplicate(s).")
            parts_msg.append(f"Now total: **{len(groups)}** group(s).")
            await event.respond("\n".join(parts_msg))
            return

        # ----- DEL GROUP (.delgroup / .delgc) -----
        if text.startswith(".delgroup") or text.startswith(".delgc"):
            parts = text.split(maxsplit=1)
            if len(parts) != 2:
                await event.respond("❗ Usage: `.delgroup @handle` or `.delgc https://t.me/...`")
                return
            target = _normalize_target(parts[1])
            if target in groups:
                groups.remove(target)
                config["groups"] = groups
                try:
                    with open(os.path.join(USERS_DIR, f"{phone}.json"), "w", encoding="utf-8") as f:
                        json.dump(config, f, ensure_ascii=False, indent=2)
                except Exception as e:
                    logger.error("[%s] Error saving user config (delgroup): %s", phone, e)
                await event.respond(f"❀ Group removed: {target}")
            else:
                await event.respond("❗ That target is not in your group list.")
            return

        # ----- TIME (interval) -----
        if text.startswith(".time"):
            value_str = ''.join(filter(str.isdigit, text)) or "0"
            value = int(value_str)
            if value <= 0:
                await event.respond(
                    "❗ Usage (free): `.time 30` or `.time 45` or `.time 60`\n"
                    "Premium can also use `.time 10m` / `.time 90` / `.time 1h` etc."
                )
                return

            if not premium:
                # FREE PLAN → allow only 30, 45, 60
                if value not in (30, 45, 60):
                    await event.respond(
                        "💎 Custom interval is a premium feature.\n"
                        "Free users can only use **30, 45, or 60 minutes**:\n"
                        "  • `.time 30`\n"
                        "  • `.time 45`\n"
                        "  • `.time 60`\n"
                        f"For other values, use `.upgrade` and send your ID to {UPGRADE_CONTACT}."
                    )
                    return
                user_state["cycle"] = value
                await event.respond(f"✅ Interval set to **{value} minutes** (free plan).")
                return

            # PREMIUM → full custom support with optional 'h'
            if 'h' in text.lower():
                user_state["cycle"] = value * 60
            else:
                user_state["cycle"] = value
            await event.respond(f"✅ Cycle delay set to **{user_state['cycle']} minutes** (premium).")
            return

        # ----- DELAY (premium only) -----
        if text.startswith(".delay"):
            if not premium:
                await event.respond(
                    f"💎 Custom delay is a premium feature.\n"
                    f"Use `.upgrade` and send your ID to {UPGRADE_CONTACT}."
                )
                return
            value_str = ''.join(filter(str.isdigit, text)) or "0"
            value = int(value_str)
            if value <= 0:
                await event.respond("❗ Usage: `.delay 5` (seconds)")
                return
            user_state["delay"] = value
            await event.respond(f"✅ Message delay set to **{value} seconds**")
            return

        # ----- STATUS -----
        if text.startswith(".status"):
            plan = "Premium ✅" if premium else "Free ⚪"
            await event.respond(
                "📊 Status:\n"
                f"• Plan: {plan}\n"
                f"• Groups: **{len(groups)}**\n"
                f"• Cycle Delay: **{user_state['cycle']} minutes**\n"
                f"• Message Delay: **{user_state['delay']} seconds**\n\n"
                + autonight_status_text(AUTONIGHT_CFG)
            )
            return

        # ----- INFO -----
        if text.startswith(".info"):
            expiry = "Developer" if (me_id == DEVELOPER_ID) else config.get("plan_expiry", "N/A")
            plan = "Premium" if premium else "Free"
            reply = (
                f"❀ User Info:\n"
                f"❀ Name: {config.get('name')}\n"
                f"❀ Phone: {phone}\n"
                f"❀ Plan: {plan}\n"
                f"❀ Cycle Delay: {user_state['cycle']} min\n"
                f"❀ Message Delay: {user_state['delay']} sec\n"
                f"❀ Groups: {len(groups)}\n"
                f"❀ Plan Expiry: {expiry}\n\n"
                + autonight_status_text(AUTONIGHT_CFG)
            )
            await event.respond(reply)
            return

        # ----- UPGRADE -----
        if text.startswith(".upgrade"):
            uid = me_id or "unknown"
            await event.respond(
                f"💎 Upgrade Info:\n"
                f"• Your Telegram user ID: `<code>{uid}</code>`\n"
                f"• Please send this ID to {UPGRADE_CONTACT} to upgrade to premium.\n"
                "After upgrade, you will unlock:\n"
                "  – Custom interval (.time any value)\n"
                "  – Custom per-message delay (.delay)\n"
                "  – Auto-Night scheduling (.night)"
            )
            return

        # ----- NIGHT (premium only) -----
        if text.startswith(".night"):
            if not premium:
                await event.respond(
                    f"💎 Auto-Night is a premium feature.\n"
                    f"Use `.upgrade` and send your ID to {UPGRADE_CONTACT}."
                )
                return
            arg = text[6:].strip() if len(text) > 6 else ""
            msg_txt, new_cfg = autonight_parse_command(arg, AUTONIGHT_CFG)
            # Update global config in memory
            for k in list(AUTONIGHT_CFG.keys()):
                AUTONIGHT_CFG[k] = new_cfg.get(k, AUTONIGHT_CFG[k])
            await event.respond(msg_txt)
            return

        # ----- ADRESET -----
        if text.startswith(".adreset"):
            user_state["idx"] = 0
            await event.respond("✅ Saved-All cycle reset to first message.")
            return

    # ---------- Forward loop ----------
    async def forward_loop():
        while True:
            try:
                # 🌙 If within quiet hours, sleep until end of window
                if autonight_is_quiet(AUTONIGHT_CFG):
                    secs = _seconds_until_quiet_end(AUTONIGHT_CFG)
                    mins = max(1, secs // 60)
                    logger.info(f"[{phone}] 🌙 Auto-Night active. Sleeping ~{mins} min (until window ends).")
                    await asyncio.sleep(secs)
                    continue

                if not groups:
                    logger.info("[%s] No groups configured, skipping cycle.", phone)
                    await asyncio.sleep(user_state["cycle"] * 60)
                    continue

                # Fetch Saved Messages (oldest→newest)
                messages = await client.get_messages("me", limit=100)
                messages = [m for m in reversed(messages) if (m.message or m.media)]

                if not messages:
                    logger.info("[%s] No Saved Messages to forward.", phone)
                    await asyncio.sleep(user_state["cycle"] * 60)
                    continue

                # Move index safely
                if user_state["idx"] >= len(messages):
                    user_state["idx"] = 0
                msg = messages[user_state["idx"]]

                logger.info("[%s] Running cycle: msg_id=%s to %s group(s)", phone, msg.id, len(groups))

                interrupted_by_night = False
                delivered_any = False

                for group in groups:
                    # If night starts mid-cycle, break early
                    if autonight_is_quiet(AUTONIGHT_CFG):
                        interrupted_by_night = True
                        logger.info(f"[{phone}] Entered Auto-Night mid-cycle. Pausing forwards.")
                        break

                    ok = await forward_one_message(client, msg, group, phone)
                    if ok:
                        delivered_any = True
                    await asyncio.sleep(user_state["delay"])

                # Move to next Saved Message if at least one delivered
                if delivered_any:
                    user_state["idx"] = (user_state["idx"] + 1) % len(messages)

                if interrupted_by_night:
                    secs = _seconds_until_quiet_end(AUTONIGHT_CFG)
                    mins = max(1, secs // 60)
                    logger.info(f"[{phone}] 🌙 Auto-Night active. Sleeping ~{mins} min.")
                    await asyncio.sleep(secs)
                    continue

                logger.info(f"[{phone}] Cycle complete. Sleeping for {user_state['cycle']} minutes…")
                await asyncio.sleep(user_state["cycle"] * 60)

            except Exception as e:
                logger.exception(f"[{phone}] Error in forward loop: {e}")
                await asyncio.sleep(60)

    # Start loop in background
    asyncio.create_task(forward_loop())
    await client.run_until_disconnected()


async def user_loader():
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
                                    logger.info(f"[⏳] Plan expired for {config['phone']}. Skipping.")
                                    continue
                            except Exception:
                                pass
                        asyncio.create_task(run_user_bot(config))
                except Exception as e:
                    logger.error(f"Error loading user config {file}: {e}")
        await asyncio.sleep(60)


async def main():
    # Ensure paths + Auto-Night file
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
