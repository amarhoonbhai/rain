# worker_forward.py
# Spinify Ads – Telethon Forwarder (Stable A-Version)
# - Uses Saved Messages as ad source
# - Supports self-commands:
#     .help, .status, .time 30/45/60, .gc, .addgc, .cleargc, .adreset
# - Never forwards commands
# - Fixes duplicate replies, session loops, and Telethon start issues
# - 100-second delay between each group

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List

from telethon import TelegramClient, events, errors
from telethon.sessions import StringSession

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


# ==========================================
# Helpers
# ==========================================

def now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def is_cmd(text: str | None) -> bool:
    """Return True if message is a command (.something)"""
    if not text:
        return False
    return text.strip().startswith(".")


def get_saved_ad(uid: int) -> str | None:
    return get_setting(f"ad_text:{uid}", None)


def set_saved_ad(uid: int, text: str):
    set_setting(f"ad_text:{uid}", text)


# ==========================================
# Self-Command Handlers
# ==========================================

async def cmd_help(event, uid: int):
    msg = (
        "✹ <b>Spinify Ads – Commands</b>\n\n"
        "Use from your <b>logged-in account:</b>\n\n"
        "• .help — show this help\n"
        "• .status — show timer, groups, next send\n"
        "• .time 30|45|60 — set interval\n"
        "• .gc — list groups\n"
        "• .addgc LINK — add group(s)\n"
        "• .cleargc — clear all groups\n"
        "• .adreset — set last Saved Message as ad\n"
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
        if left <= 0:
            eta = "very soon"
        else:
            m, s = divmod(left, 60)
            eta = f"in ~{m}m {s}s"

    g = list_groups(uid)
    ad = get_saved_ad(uid)

    msg = (
        "📟 <b>Spinify Status</b>\n\n"
        f"• Interval: {interval}m\n"
        f"• Groups: {len(g)}/{groups_cap(uid)}\n"
        f"• Next send: {eta}\n"
        f"• Ad saved: {'yes ✅' if ad else 'no ❌'}\n"
    )
    await event.reply(msg, parse_mode="html")


async def cmd_time(event, uid: int, text: str):
    try:
        val = int(text.split()[1])
    except:
        return await event.reply("Usage: `.time 30|45|60`")

    if val not in (30, 45, 60):
        return await event.reply("Only 30 / 45 / 60 allowed.")

    set_interval(uid, val)
    await event.reply(f"✅ Interval set to {val} minutes.")


async def cmd_gc(event, uid: int):
    groups = list_groups(uid)
    if not groups:
        return await event.reply("No groups added yet.")

    msg = "🎯 <b>Your Groups</b>\n" + "\n".join(
        f"{i+1}. {g}" for i, g in enumerate(groups)
    )
    await event.reply(msg, parse_mode="html")


async def cmd_addgc(event, uid: int, text: str):
    import re
    pattern = r"(https?://t\.me/\S+|t\.me/\S+|@\w+|-100\d+)"

    all_text = text
    if event.is_reply:
        rep = await event.get_reply_message()
        if rep and rep.raw_text:
            all_text += "\n" + rep.raw_text

    found = re.findall(pattern, all_text)
    if not found:
        return await event.reply("❌ No valid group links found.")

    added = 0
    skipped = 0
    for g in found:
        ok = add_group(uid, g)
        added += 1 if ok else 0
        skipped += 0 if ok else 1

    await event.reply(f"Added: {added} | Skipped: {skipped}")


async def cmd_cleargc(event, uid: int):
    clear_groups(uid)
    await event.reply("🧹 Cleared all groups.")


async def cmd_adreset(event, uid: int, client):
    msgs = await client.get_messages("me", limit=30)
    for m in msgs:
        txt = m.raw_text or ""
        if txt and not is_cmd(txt):
            set_saved_ad(uid, txt)
            return await event.reply("✅ Ad updated from Saved Messages.")

    await event.reply("❌ No valid ad found in Saved Messages.")


async def handle_command(event, uid: int, client):
    text = (event.raw_text or "").strip().lower()

    if text.startswith(".help"):
        return await cmd_help(event, uid)
    if text.startswith(".status"):
        return await cmd_status(event, uid)
    if text.startswith(".time"):
        return await cmd_time(event, uid, event.raw_text)
    if text.startswith(".gc"):
        return await cmd_gc(event, uid)
    if text.startswith(".addgc"):
        return await cmd_addgc(event, uid, event.raw_text)
    if text.startswith(".cleargc"):
        return await cmd_cleargc(event, uid)
    if text.startswith(".adreset"):
        return await cmd_adreset(event, uid, client)

    await event.reply("Unknown command. Use `.help`.")


# ==========================================
# Forwarding Loop
# ==========================================

async def forward_loop(client: TelegramClient, uid: int):
    """Responsible for sending ads at interval."""

    while True:
        interval = get_interval(uid)
        if interval not in (30, 45, 60):
            interval = 30

        last = get_last_sent_at(uid)
        now = now_ts()

        if last and (now - last) < interval * 60:
            await asyncio.sleep(5)
            continue

        ad = get_saved_ad(uid)
        if not ad or is_cmd(ad):
            await asyncio.sleep(5)
            continue

        targets = list_groups(uid)
        if not targets:
            await asyncio.sleep(5)
            continue

        for t in targets:
            try:
                await client.send_message(t, ad)
                log.info(f"[UID {uid}] → Sent to {t}")
            except errors.FloodWaitError as e:
                await asyncio.sleep(e.seconds)
            except Exception as e:
                log.error(f"[UID {uid}] Send error → {t}: {e}")

            await asyncio.sleep(100)  

        set_last_sent_at(uid, now_ts())


# ==========================================
# Client Worker
# ==========================================

async def client_worker(uid: int, sess: Dict[str, Any]):
    api_id = int(sess["api_id"])
    api_hash = sess["api_hash"]
    string_sess = sess["session_string"]

    client = TelegramClient(
        session=StringSession(string_sess),
        api_id=api_id,
        api_hash=api_hash,
        system_version="Android",
    )

    await client.start()
    log.info(f"Telethon started for user {uid}, slot {sess['slot']}")

    @client.on(events.NewMessage(chats="me"))
    async def saved(ev):
        txt = ev.raw_text or ""
        if txt and not is_cmd(txt):
            set_saved_ad(uid, txt)
            log.info(f"[{uid}] Updated ad from Saved Messages")

    @client.on(events.NewMessage(outgoing=True))
    async def cmds(ev):
        txt = ev.raw_text or ""
        if is_cmd(txt):
            await handle_command(ev, uid, client)

    try:
        await forward_loop(client, uid)
    finally:
        await client.disconnect()
        log.info(f"Telethon stopped for user {uid}, slot {sess['slot']}")


# ==========================================
# Entry for run_all.py
# ==========================================

async def start():
    init_db()
    rows = get_conn().execute("SELECT DISTINCT user_id FROM sessions").fetchall()
    if not rows:
        log.info("Forwarder: no sessions in DB.")
        await asyncio.sleep(10)
        return

    tasks = []
    for r in rows:
        uid = r["user_id"]
        for sess in sessions_list(uid):
            tasks.append(asyncio.create_task(client_worker(uid, sess)))

    if tasks:
        await asyncio.gather(*tasks)
