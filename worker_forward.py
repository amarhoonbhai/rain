# worker_forward.py
# Spinify Ads – Telethon Forwarder
# - Reads latest ad from Saved Messages ("me")
# - Supports self-commands from your login account:
#     .help, .status, .time 30/45/60, .gc, .addgc, .cleargc, .adreset
# - Forwards only NON-command texts to all target groups
# - 100 seconds delay between each group

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List

from telethon import TelegramClient, events, errors

from core.db import (
    init_db,
    get_conn,
    sessions_list,
    list_groups,
    add_group,
    clear_groups,
    groups_cap,
    get_interval,
    set_interval,
    get_last_sent_at,
    set_last_sent_at,
    set_setting,
    get_setting,
)

log = logging.getLogger("forwarder")


# =========================
# Helpers
# =========================

def now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def is_cmd(text: str | None) -> bool:
    if not text:
        return False
    return text.strip().startswith(".")


def get_saved_ad(uid: int) -> str | None:
    return get_setting(f"ad_text:{uid}", None)


def set_saved_ad(uid: int, text: str) -> None:
    set_setting(f"ad_text:{uid}", text)


# =========================
# Command handler helpers
# =========================

async def cmd_help(event, uid: int):
    msg = (
        "✹ Spinify Ads – Self Commands\n\n"
        "Use these from your <b>logged-in account</b> (not bot):\n\n"
        "Basic:\n"
        "• .help – show this help\n"
        "• .status – show timer, next send, groups\n"
        "• .time 30 / .time 45 / .time 60 – set interval (minutes)\n"
        "• .gc – list groups\n"
        "• .addgc <links/@user> – add groups (one or multiple)\n"
        "• .cleargc – clear all target groups\n"
        "• .adreset – re-use latest Saved Message as ad\n\n"
        "✹ Put your ad text in Saved Messages.\n"
        "✹ Only latest non-command message is used for forwarding.\n"
    )
    await event.reply(msg, parse_mode="html")


async def cmd_status(event, uid: int):
    interval = get_interval(uid)
    if interval not in (30, 45, 60):
        interval = 30
    last = get_last_sent_at(uid)
    now = now_ts()
    if last is None:
        eta = f"in ~{interval}m (first cycle)"
    else:
        left = interval * 60 - (now - last)
        if left <= 5:
            eta = "very soon"
        else:
            m, s = divmod(max(0, left), 60)
            eta = f"in ~{m}m {s}s"

    groups = list_groups(uid)
    ad = get_saved_ad(uid)
    ad_status = "set ✅" if ad else "not set ❌"

    msg = (
        "📟 Spinify Status\n\n"
        f"• Interval: {interval} min\n"
        f"• Groups: {len(groups)}/{groups_cap(uid)}\n"
        f"• Next send: {eta}\n"
        f"• Saved Ad: {ad_status}\n"
    )
    await event.reply(msg)


async def cmd_time(event, uid: int, text: str):
    parts = text.split()
    if len(parts) < 2:
        await event.reply("Usage: `.time 30` or `.time 45` or `.time 60`")
        return
    try:
        val = int(parts[1])
    except Exception:
        await event.reply("Time must be 30, 45 or 60 (minutes).")
        return

    if val not in (30, 45, 60):
        await event.reply("Only 30 / 45 / 60 minutes are allowed.")
        return

    set_interval(uid, val)
    await event.reply(f"✅ Interval set to {val} minutes.")


async def cmd_gc(event, uid: int):
    groups = list_groups(uid)
    if not groups:
        await event.reply("No groups added yet. Use `.addgc` with links/usernames.")
        return
    lines = [f"{i+1}. {g}" for i, g in enumerate(groups)]
    await event.reply("🎯 Target Groups:\n" + "\n".join(lines))


async def cmd_addgc(event, uid: int, text: str):
    # Accept from message text AND reply message text
    import re
    pattern = r"(https?://t\.me/\S+|t\.me/\S+|@\w+|-100\d+)"
    all_text = text or ""
    if event.is_reply:
        rep = await event.get_reply_message()
        if rep and rep.raw_text:
            all_text += "\n" + rep.raw_text

    links = re.findall(pattern, all_text)
    if not links:
        await event.reply(
            "No valid targets found.\n"
            "Example: `.addgc https://t.me/yourgroup` or `.addgc @yourgroup`"
        )
        return

    added = 0
    skipped = 0
    for l in links:
        ok = add_group(uid, l)
        if ok:
            added += 1
        else:
            skipped += 1

    await event.reply(f"✅ Added: {added} | Skipped (dupe/cap): {skipped}")


async def cmd_cleargc(event, uid: int):
    clear_groups(uid)
    await event.reply("🧹 All groups cleared.")


async def cmd_adreset(event, uid: int, client):
    # Re-read latest suitable Saved Message
    msgs = await client.get_messages("me", limit=50)
    for m in msgs:
        txt = m.message or ""
        if not txt:
            continue
        if is_cmd(txt):
            continue
        set_saved_ad(uid, txt)
        await event.reply("✅ Ad text reset from latest Saved Message.")
        return
    await event.reply("No valid ad text found in Saved Messages.")


async def handle_command(event, uid: int, client):
    text = (event.raw_text or "").strip()
    low = text.lower()

    if low.startswith(".help"):
        await cmd_help(event, uid)
    elif low.startswith(".status"):
        await cmd_status(event, uid)
    elif low.startswith(".time"):
        await cmd_time(event, uid, text)
    elif low.startswith(".gc"):
        await cmd_gc(event, uid)
    elif low.startswith(".addgc"):
        await cmd_addgc(event, uid, text)
    elif low.startswith(".cleargc"):
        await cmd_cleargc(event, uid)
    elif low.startswith(".adreset"):
        await cmd_adreset(event, uid, client)
    else:
        await event.reply("Unknown command. Type `.help` for list.")


# =========================
# Forward loop
# =========================

async def forward_loop(client: TelegramClient, uid: int):
    while True:
        interval = get_interval(uid)
        if interval not in (30, 45, 60):
            interval = 30

        last = get_last_sent_at(uid)
        now = now_ts()

        if last is not None and (now - last) < interval * 60:
            await asyncio.sleep(5)
            continue

        ad = get_saved_ad(uid)
        if not ad:
            await asyncio.sleep(5)
            continue

        if is_cmd(ad):
            # safety: never forward commands
            await asyncio.sleep(5)
            continue

        targets = list_groups(uid)
        if not targets:
            await asyncio.sleep(5)
            continue

        for t in targets:
            try:
                await client.send_message(t, ad)
                log.info(f"[user {uid}] sent ad to {t}")
            except errors.FloodWaitError as e:
                log.warning(f"[user {uid}] FloodWait {e.seconds}s")
                await asyncio.sleep(e.seconds)
            except Exception as e:
                log.error(f"[user {uid}] send error to {t}: {e}")
            await asyncio.sleep(100)  # REQUIRED 100s DELAY

        set_last_sent_at(uid, now_ts())


# =========================
# Per-user Telethon client
# =========================

async def client_worker(uid: int, sess: Dict[str, Any]):
    api_id = int(sess["api_id"])
    api_hash = sess["api_hash"]
    session_string = sess["session_string"]

    client = TelegramClient(
        session=f"user-{uid}-slot-{sess['slot']}",
        api_id=api_id,
        api_hash=api_hash,
        system_version="Android",
    )

    # use string session
    client = client.start(string_session=session_string)

    log.info(f"Telethon client started for user {uid}, slot {sess['slot']}")

    # 1) listen Saved Messages for ad content
    @client.on(events.NewMessage(chats="me"))
    async def saved_handler(ev):
        txt = ev.raw_text or ""
        if not txt or is_cmd(txt):
            return
        set_saved_ad(uid, txt)
        log.info(f"[user {uid}] updated ad from Saved Messages")

    # 2) listen outgoing self commands (any chat)
    @client.on(events.NewMessage(outgoing=True))
    async def cmd_handler(ev):
        txt = ev.raw_text or ""
        if not is_cmd(txt):
            return
        await handle_command(ev, uid, client)

    try:
        await forward_loop(client, uid)
    finally:
        await client.disconnect()
        log.info(f"Telethon client stopped for user {uid}, slot {sess['slot']}")


# =========================
# Entry for run_all.py
# =========================

async def start():
    init_db()
    rows = get_conn().execute("SELECT DISTINCT user_id FROM sessions").fetchall()
    if not rows:
        log.info("No sessions found in DB.")
        await asyncio.sleep(10)
        return

    tasks: List[asyncio.Task] = []
    for r in rows:
        uid = r["user_id"]
        for sess in sessions_list(uid):
            tasks.append(asyncio.create_task(client_worker(uid, sess)))

    if tasks:
        await asyncio.gather(*tasks)
    else:
        log.info("No active sessions for forwarder.")
