# worker_forward.py — Spinify Forwarder (A1 Stable Final)
# Clean, compact, fully compatible with db.py + run_all.py + enforcer.

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any

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
    inc_sent_ok,
)

log = logging.getLogger("forwarder")


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def now_ts():
    return int(datetime.now(timezone.utc).timestamp())


def is_cmd(t: str | None):
    return bool(t and t.strip().startswith("."))


def get_saved_ad(uid: int):
    return get_setting(f"ad_text:{uid}", None)


def set_saved_ad(uid: int, text: str):
    set_setting(f"ad_text:{uid}", text)


# ---------------------------------------------------------
# Self Commands
# ---------------------------------------------------------
async def cmd_help(ev, uid):
    await ev.reply(
        "✹ <b>Spinify Commands</b>\n\n"
        "• .help\n"
        "• .status\n"
        "• .time 30|45|60\n"
        "• .gc\n"
        "• .addgc LINK\n"
        "• .cleargc\n"
        "• .adreset\n",
        parse_mode="html",
    )


async def cmd_status(ev, uid):
    interval = get_interval(uid)
    last = get_last_sent_at(uid)
    now = now_ts()

    if last:
        left = interval * 60 - (now - last)
        eta = "soon" if left <= 0 else f"in {left//60}m {left%60}s"
    else:
        eta = f"in ~{interval}m (first run)"

    g = list_groups(uid)
    ad = get_saved_ad(uid)

    await ev.reply(
        "📟 <b>Status</b>\n\n"
        f"• Interval: {interval}m\n"
        f"• Groups: {len(g)}/{groups_cap(uid)}\n"
        f"• Next: {eta}\n"
        f"• Ad: {'Yes' if ad else 'No'}",
        parse_mode="html",
    )


async def cmd_time(ev, uid, txt):
    try:
        v = int(txt.split()[1])
    except:
        return await ev.reply("Usage: .time 30|45|60")

    if v not in (30, 45, 60):
        return await ev.reply("Allowed: 30 / 45 / 60")

    set_interval(uid, v)
    await ev.reply(f"Interval set to {v} minutes")


async def cmd_gc(ev, uid):
    groups = list_groups(uid)
    if not groups:
        return await ev.reply("No groups added")

    msg = "🎯 <b>Your Groups</b>\n" + "\n".join(
        f"{i+1}. {g}" for i, g in enumerate(groups)
    )
    await ev.reply(msg, parse_mode="html")


async def cmd_addgc(ev, uid, txt):
    import re

    pattern = r"(https?://t\.me/\S+|t\.me/\S+|@\w+|-100\d+)"
    all_text = txt

    if ev.is_reply:
        r = await ev.get_reply_message()
        if r and r.raw_text:
            all_text += "\n" + r.raw_text

    found = re.findall(pattern, all_text)
    if not found:
        return await ev.reply("No valid group links found")

    added = 0
    skipped = 0
    for g in found:
        ok = add_group(uid, g)
        added += ok
        skipped += 0 if ok else 1

    await ev.reply(f"Added: {added} | Skipped: {skipped}")


async def cmd_cleargc(ev, uid):
    clear_groups(uid)
    await ev.reply("Cleared all groups")


async def cmd_adreset(ev, uid, client):
    msgs = await client.get_messages("me", limit=20)
    for m in msgs:
        t = m.raw_text or ""
        if t and not is_cmd(t):
            set_saved_ad(uid, t)
            return await ev.reply("Ad refreshed from Saved Messages")

    await ev.reply("No valid ad in Saved Messages")


async def handle_command(ev, uid, client):
    t = (ev.raw_text or "").strip().lower()

    if t.startswith(".help"): return await cmd_help(ev, uid)
    if t.startswith(".status"): return await cmd_status(ev, uid)
    if t.startswith(".time"): return await cmd_time(ev, uid, ev.raw_text)
    if t.startswith(".gc"): return await cmd_gc(ev, uid)
    if t.startswith(".addgc"): return await cmd_addgc(ev, uid, ev.raw_text)
    if t.startswith(".cleargc"): return await cmd_cleargc(ev, uid)
    if t.startswith(".adreset"): return await cmd_adreset(ev, uid, client)

    await ev.reply("Unknown command (.help)")


# ---------------------------------------------------------
# Forward Loop
# ---------------------------------------------------------
async def forward_loop(client: TelegramClient, uid: int):
    while True:
        interval = get_interval(uid)
        last = get_last_sent_at(uid)
        now = now_ts()

        if last and (now - last) < interval * 60:
            await asyncio.sleep(4)
            continue

        ad = get_saved_ad(uid)
        if not ad or is_cmd(ad):
            await asyncio.sleep(4)
            continue

        groups = list_groups(uid)
        if not groups:
            await asyncio.sleep(5)
            continue

        for g in groups:
            try:
                await client.send_message(g, ad)
                inc_sent_ok(uid)
                log.info(f"[{uid}] Sent to {g}")

            except errors.FloodWaitError as e:
                await asyncio.sleep(e.seconds)

            except Exception as e:
                log.error(f"Send error to {g}: {e}")
                await asyncio.sleep(1)

            await asyncio.sleep(100)

        set_last_sent_at(uid, now_ts())


# ---------------------------------------------------------
# Per-user Telethon Client
# ---------------------------------------------------------
async def client_worker(uid: int, sess: Dict[str, Any]):
    api_id = int(sess["api_id"])
    api_hash = sess["api_hash"]
    ss = sess["session_string"]

    client = TelegramClient(
        session=StringSession(ss),
        api_id=api_id,
        api_hash=api_hash,
        system_version="Android",
    )

    await client.start()
    log.info(f"Telethon started user {uid}, slot={sess['slot']}")

    @client.on(events.NewMessage(chats="me"))
    async def saved(ev):
        t = ev.raw_text or ""
        if t and not is_cmd(t):
            set_saved_ad(uid, t)

    @client.on(events.NewMessage(outgoing=True))
    async def cmds(ev):
        if is_cmd(ev.raw_text or ""):
            await handle_command(ev, uid, client)

    try:
        await forward_loop(client, uid)
    finally:
        await client.disconnect()
        log.info(f"Telethon stopped user {uid}")


# ---------------------------------------------------------
# Entry for run_all.py
# ---------------------------------------------------------
async def start():
    init_db()

    rows = get_conn().execute("SELECT DISTINCT user_id FROM sessions").fetchall()
    if not rows:
        log.info("Forwarder: no sessions found")
        await asyncio.sleep(10)
        return

    tasks = []
    for r in rows:
        uid = r["user_id"]
        for sess in sessions_list(uid):
            tasks.append(asyncio.create_task(client_worker(uid, sess)))

    if tasks:
        await asyncio.gather(*tasks)
