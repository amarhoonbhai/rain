# worker_forward.py
# Spinify Ads – Telethon Forwarder (Stable A+ Version)
# Fully fixed & improved: flood-safe, stable, command-safe.

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
    inc_sent_ok
)

log = logging.getLogger("forwarder")


# ----------------------------------------------------
# Helpers
# ----------------------------------------------------
def now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def is_cmd(msg: str | None) -> bool:
    if not msg:
        return False
    return msg.strip().startswith(".")


def get_saved_ad(uid: int) -> str | None:
    return get_setting(f"ad_text:{uid}", None)


def set_saved_ad(uid: int, text: str):
    set_setting(f"ad_text:{uid}", text)


# ----------------------------------------------------
# Self Commands
# ----------------------------------------------------
async def cmd_help(ev, uid: int):
    await ev.reply(
        "✹ <b>Spinify Commands</b>\n\n"
        "• .help\n"
        "• .status\n"
        "• .time 30|45|60\n"
        "• .gc — list groups\n"
        "• .addgc @link\n"
        "• .cleargc — clear\n"
        "• .adreset — use last Saved Message\n",
        parse_mode="html"
    )


async def cmd_status(ev, uid: int):
    interval = get_interval(uid)
    last = get_last_sent_at(uid)
    now = now_ts()

    if last:
        left = interval * 60 - (now - last)
        if left <= 0:
            eta = "very soon"
        else:
            m, s = divmod(left, 60)
            eta = f"in {m}m {s}s"
    else:
        eta = f"in ~{interval}m (first cycle)"

    groups = list_groups(uid)
    ad = get_saved_ad(uid)

    await ev.reply(
        "📟 <b>Spinify Status</b>\n\n"
        f"• Interval: {interval}m\n"
        f"• Groups: {len(groups)}/{groups_cap(uid)}\n"
        f"• Next Send: {eta}\n"
        f"• Ad Set: {'Yes' if ad else 'No'}\n",
        parse_mode="html"
    )


async def cmd_time(ev, uid: int, text: str):
    try:
        v = int(text.split()[1])
    except:
        return await ev.reply("Usage: .time 30|45|60")

    if v not in (30, 45, 60):
        return await ev.reply("Allowed: 30 / 45 / 60")

    set_interval(uid, v)
    await ev.reply(f"Interval set to {v} minutes.")


async def cmd_gc(ev, uid: int):
    groups = list_groups(uid)
    if not groups:
        return await ev.reply("No groups added.")

    txt = "🎯 <b>Your Groups</b>\n" + "\n".join(
        f"{i+1}. {g}" for i, g in enumerate(groups)
    )
    await ev.reply(txt, parse_mode="html")


async def cmd_addgc(ev, uid: int, text: str):
    import re
    pattern = r"(https?://t\.me/\S+|t\.me/\S+|@\w+|-100\d+)"

    all_text = text
    if ev.is_reply:
        rep = await ev.get_reply_message()
        if rep and rep.raw_text:
            all_text += "\n" + rep.raw_text

    found = re.findall(pattern, all_text)
    if not found:
        return await ev.reply("No valid @group links found.")

    added = 0
    skipped = 0

    for g in found:
        ok = add_group(uid, g)
        added += ok
        skipped += 0 if ok else 1

    await ev.reply(f"Added: {added} | Skipped: {skipped}")


async def cmd_cleargc(ev, uid: int):
    clear_groups(uid)
    await ev.reply("All groups cleared.")


async def cmd_adreset(ev, uid: int, client):
    msgs = await client.get_messages("me", limit=30)
    for m in msgs:
        txt = m.raw_text or ""
        if txt and not is_cmd(txt):
            set_saved_ad(uid, txt)
            return await ev.reply("Ad updated from last Saved Message.")

    await ev.reply("No valid ad text found in Saved Messages.")


async def handle_command(ev, uid, client):
    t = (ev.raw_text or "").strip().lower()

    if t.startswith(".help"):
        return await cmd_help(ev, uid)
    if t.startswith(".status"):
        return await cmd_status(ev, uid)
    if t.startswith(".time"):
        return await cmd_time(ev, uid, ev.raw_text)
    if t.startswith(".gc"):
        return await cmd_gc(ev, uid)
    if t.startswith(".addgc"):
        return await cmd_addgc(ev, uid, ev.raw_text)
    if t.startswith(".cleargc"):
        return await cmd_cleargc(ev, uid)
    if t.startswith(".adreset"):
        return await cmd_adreset(ev, uid, client)

    await ev.reply("Unknown command. Use .help")


# ----------------------------------------------------
# Forward Loop
# ----------------------------------------------------
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
                log.info(f"[{uid}] Sent to {g}")
                inc_sent_ok(uid)

            except errors.FloodWaitError as e:
                await asyncio.sleep(e.seconds)

            except Exception as e:
                log.error(f"Error sending to {g}: {e}")
                await asyncio.sleep(1)

            await asyncio.sleep(100)

        set_last_sent_at(uid, now_ts())


# ----------------------------------------------------
# Per-user Telethon Client
# ----------------------------------------------------
async def client_worker(uid: int, sess: Dict[str, Any]):
    api_id = int(sess["api_id"])
    api_hash = sess["api_hash"]
    string_sess = sess["session_string"]

    client = TelegramClient(
        session=StringSession(string_sess),
        api_id=api_id,
        api_hash=api_hash,
        system_version="Android"
    )

    await client.start()
    log.info(f"Started user {uid} slot={sess['slot']}")

    @client.on(events.NewMessage(chats="me"))
    async def saved(ev):
        txt = ev.raw_text or ""
        if txt and not is_cmd(txt):
            set_saved_ad(uid, txt)

    @client.on(events.NewMessage(outgoing=True))
    async def cmds(ev):
        txt = ev.raw_text or ""
        if is_cmd(txt):
            await handle_command(ev, uid, client)

    try:
        await forward_loop(client, uid)
    except Exception as e:
        log.error(f"Worker crashed for {uid}: {e}")
    finally:
        await client.disconnect()
        log.info(f"Stopped user {uid}")


# ----------------------------------------------------
# Entry for run_all.py
# ----------------------------------------------------
async def start():
    init_db()

    rows = get_conn().execute("SELECT DISTINCT user_id FROM users").fetchall()
    if not rows:
        log.info("Forwarder: no users found.")
        await asyncio.sleep(10)
        return

    tasks = []

    for r in rows:
        uid = r["user_id"]
        for sess in sessions_list(uid):
            tasks.append(asyncio.create_task(client_worker(uid, sess)))

    if tasks:
        await asyncio.gather(*tasks)
