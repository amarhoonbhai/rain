# worker_forward.py
# Spinify Ads — Telethon Forwarder (Stable B2 Version)
# Fully compatible with: db.py, login_bot, main_bot, run_all.py

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
    get_setting,
    set_setting,
    inc_sent_ok,
)

log = logging.getLogger("worker")


# -------------------------
# Helpers
# -------------------------
def now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def is_cmd(txt: str | None) -> bool:
    return bool(txt and txt.strip().startswith("."))


def get_saved_ad(uid: int):
    return get_setting(f"ad_text:{uid}", None)


def set_saved_ad(uid: int, text: str):
    set_setting(f"ad_text:{uid}", text)


# -------------------------
# Self Commands
# -------------------------
async def cmd_help(ev, uid):
    await ev.reply(
        "✹ <b>Spinify Commands</b>\n\n"
        "• .help\n"
        "• .status\n"
        "• .time 30|45|60\n"
        "• .gc — list groups\n"
        "• .addgc link\n"
        "• .cleargc — clear all\n"
        "• .adreset — use Saved Messages\n",
        parse_mode="html",
    )


async def cmd_status(ev, uid):
    interval = get_interval(uid)
    last = get_last_sent_at(uid)
    now = now_ts()

    if last:
        left = interval * 60 - (now - last)
        eta = "very soon" if left <= 0 else f"in {left//60}m {left%60}s"
    else:
        eta = f"in ~{interval}m"

    groups = list_groups(uid)
    ad = get_saved_ad(uid)

    await ev.reply(
        "📟 <b>Status</b>\n\n"
        f"• Interval: {interval}m\n"
        f"• Groups: {len(groups)}/{groups_cap(uid)}\n"
        f"• Next Send: {eta}\n"
        f"• Ad Set: {'Yes' if ad else 'No'}\n",
        parse_mode="html",
    )


async def cmd_time(ev, uid, raw):
    try:
        v = int(raw.split()[1])
    except:
        return await ev.reply("Usage: .time 30|45|60")

    if v not in (30, 45, 60):
        return await ev.reply("Allowed: 30 / 45 / 60")

    set_interval(uid, v)
    await ev.reply(f"Interval set to {v} minutes.")


async def cmd_gc(ev, uid):
    g = list_groups(uid)
    if not g:
        return await ev.reply("No groups added.")

    txt = "🎯 <b>Your Groups</b>\n" + "\n".join(
        f"{i+1}. {x}" for i, x in enumerate(g)
    )
    await ev.reply(txt, parse_mode="html")


async def cmd_addgc(ev, uid, raw):
    import re
    found = re.findall(r"(t.me/\S+|@\w+|-100\d+)", raw)
    if not found:
        return await ev.reply("No valid group links found.")

    added = sum(add_group(uid, g) for g in found)
    skipped = len(found) - added

    await ev.reply(f"Added: {added} | Skipped: {skipped}")


async def cmd_cleargc(ev, uid):
    clear_groups(uid)
    await ev.reply("All groups cleared.")


async def cmd_adreset(ev, uid, client):
    msgs = await client.get_messages("me", limit=20)
    for m in msgs:
        if m.raw_text and not is_cmd(m.raw_text):
            set_saved_ad(uid, m.raw_text)
            return await ev.reply("Ad updated from Saved Messages.")

    await ev.reply("No valid ad found.")


async def handle_command(ev, uid, client):
    t = (ev.raw_text or "").lower()

    if t.startswith(".help"): return await cmd_help(ev, uid)
    if t.startswith(".status"): return await cmd_status(ev, uid)
    if t.startswith(".time"): return await cmd_time(ev, uid, ev.raw_text)
    if t.startswith(".gc"): return await cmd_gc(ev, uid)
    if t.startswith(".addgc"): return await cmd_addgc(ev, uid, ev.raw_text)
    if t.startswith(".cleargc"): return await cmd_cleargc(ev, uid)
    if t.startswith(".adreset"): return await cmd_adreset(ev, uid, client)

    await ev.reply("Unknown command. Use .help")


# -------------------------
# Forward Loop
# -------------------------
async def forward_loop(client: TelegramClient, uid: int):
    while True:
        interval = get_interval(uid)
        last = get_last_sent_at(uid)
        now = now_ts()

        if last and (now - last) < interval * 60:
            await asyncio.sleep(5)
            continue

        ad = get_saved_ad(uid)
        if not ad:
            await asyncio.sleep(5)
            continue

        groups = list_groups(uid)
        if not groups:
            await asyncio.sleep(5)
            continue

        for g in groups:
            try:
                await client.send_message(g, ad)
                inc_sent_ok(uid)
                log.info(f"[{uid}] → Sent to {g}")
            except errors.FloodWaitError as e:
                await asyncio.sleep(e.seconds)
            except Exception as e:
                log.error(f"Error sending to {g}: {e}")

            await asyncio.sleep(100)

        set_last_sent_at(uid, now_ts())


# -------------------------
# Per-session Telethon Client
# -------------------------
async def client_worker(uid: int, sess: Dict[str, Any]):
    client = TelegramClient(
        StringSession(sess["session_string"]),
        api_id=int(sess["api_id"]),
        api_hash=sess["api_hash"],
        system_version="Android",
    )

    await client.start()
    log.info(f"Started session: uid={uid}, slot={sess['slot']}")

    @client.on(events.NewMessage(chats="me"))
    async def saved(ev):
        if ev.raw_text and not is_cmd(ev.raw_text):
            set_saved_ad(uid, ev.raw_text)

    @client.on(events.NewMessage(outgoing=True))
    async def cmds(ev):
        if is_cmd(ev.raw_text):
            await handle_command(ev, uid, client)

    try:
        await forward_loop(client, uid)
    finally:
        await client.disconnect()
        log.info(f"Stopped session: uid={uid}, slot={sess['slot']}")


# -------------------------
# Entry for run_all.py
# -------------------------
async def start():
    init_db()

    rows = get_conn().execute("SELECT user_id FROM users").fetchall()
    if not rows:
        log.info("Forwarder: no users found.")
        await asyncio.sleep(5)
        return

    tasks = []

    for r in rows:
        uid = r["user_id"]
        for sess in sessions_list(uid):
            tasks.append(asyncio.create_task(client_worker(uid, sess)))

    if tasks:
        await asyncio.gather(*tasks)
