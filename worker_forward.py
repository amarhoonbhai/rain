# worker_forward.py
# Spinify Ads – Telethon Forwarder (B-2 Stable Version)
# ✔ Clean & compact
# ✔ Flood-safe
# ✔ Works with your DB & Login bot
# ✔ Supports all commands
# ✔ 100s delay between groups
# ✔ Saved Messages → Auto Ad Detection

import asyncio
import logging
from datetime import datetime, timezone

from telethon import TelegramClient, events, errors
from telethon.sessions import StringSession

from core.db import (
    init_db, get_conn,
    sessions_list, list_groups,
    add_group, clear_groups,
    groups_cap, get_interval,
    get_last_sent_at, set_last_sent_at,
    set_setting, get_setting,
    inc_sent_ok
)

log = logging.getLogger("forwarder")


# ----------------------------------------------------
# Helpers
# ----------------------------------------------------

def now_ts():
    return int(datetime.now(timezone.utc).timestamp())

def is_cmd(text):
    return text.strip().startswith(".") if text else False

def get_saved_ad(uid):
    return get_setting(f"ad_text:{uid}")

def set_saved_ad(uid, text):
    set_setting(f"ad_text:{uid}", text)


# ----------------------------------------------------
# Commands
# ----------------------------------------------------

async def cmd_help(ev, uid):
    await ev.reply(
        "✹ Spinify Commands:\n"
        "• .help\n"
        "• .status\n"
        "• .time 30|45|60\n"
        "• .gc\n"
        "• .addgc @group\n"
        "• .cleargc\n"
        "• .adreset\n"
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

    groups = list_groups(uid)
    ad = get_saved_ad(uid)

    await ev.reply(
        f"📟 Spinify Status\n\n"
        f"• Interval: {interval}m\n"
        f"• Groups: {len(groups)}/{groups_cap(uid)}\n"
        f"• Next: {eta}\n"
        f"• Ad set: {'Yes' if ad else 'No'}"
    )

async def cmd_time(ev, uid, text):
    try:
        v = int(text.split()[1])
    except:
        return await ev.reply("Usage: .time 30|45|60")

    if v not in (30, 45, 60):
        return await ev.reply("Allowed only: 30, 45, 60")

    set_setting(f"interval:{uid}", v)
    await ev.reply(f"Interval updated → {v}m")

async def cmd_gc(ev, uid):
    groups = list_groups(uid)
    if not groups:
        return await ev.reply("No groups added yet.")

    await ev.reply("🎯 Groups:\n" + "\n".join(groups))

async def cmd_addgc(ev, uid, text):
    import re
    pattern = r"(@\w+|https?://t\.me/\S+|t\.me/\S+|-100\d+)"

    all_text = text
    if ev.is_reply:
        rep = await ev.get_reply_message()
        if rep and rep.raw_text:
            all_text += "\n" + rep.raw_text

    found = re.findall(pattern, all_text)
    if not found:
        return await ev.reply("No valid group links found.")

    added = 0
    skipped = 0

    for g in found:
        ok = add_group(uid, g)
        added += ok
        skipped += 1 - ok

    await ev.reply(f"Added: {added} | Skipped: {skipped}")

async def cmd_cleargc(ev, uid):
    clear_groups(uid)
    await ev.reply("Cleared all groups.")

async def cmd_adreset(ev, uid, client):
    msgs = await client.get_messages("me", limit=30)

    for m in msgs:
        txt = m.raw_text or ""
        if txt and not is_cmd(txt):
            set_saved_ad(uid, txt)
            return await ev.reply("Ad updated from Saved Messages.")

    await ev.reply("No usable ad found in Saved Messages.")

async def handle_command(ev, uid, client):
    t = (ev.raw_text or "").strip().lower()

    if t.startswith(".help"): return await cmd_help(ev, uid)
    if t.startswith(".status"): return await cmd_status(ev, uid)
    if t.startswith(".time"): return await cmd_time(ev, uid, ev.raw_text)
    if t.startswith(".gc"): return await cmd_gc(ev, uid)
    if t.startswith(".addgc"): return await cmd_addgc(ev, uid, ev.raw_text)
    if t.startswith(".cleargc"): return await cmd_cleargc(ev, uid)
    if t.startswith(".adreset"): return await cmd_adreset(ev, uid, client)

    await ev.reply("Unknown command. Use .help")


# ----------------------------------------------------
# Forwarding Loop
# ----------------------------------------------------

async def forward_loop(client, uid):
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
                log.info(f"[{uid}] → Sent to {g}")
            except errors.FloodWaitError as e:
                await asyncio.sleep(e.seconds)
            except Exception as e:
                log.error(f"Send error → {g}: {e}")

            await asyncio.sleep(100)

        set_last_sent_at(uid, now_ts())


# ----------------------------------------------------
# Per-user Client Worker
# ----------------------------------------------------

async def client_worker(uid, sess):
    api_id = sess["api_id"]
    api_hash = sess["api_hash"]
    s = sess["session_string"]

    client = TelegramClient(StringSession(s), api_id, api_hash)

    await client.start()
    log.info(f"Worker started → UID {uid}, slot {sess['slot']}")

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
        log.error(f"Worker crash for {uid}: {e}")
    finally:
        await client.disconnect()
        log.info(f"Worker stopped → UID {uid}")


# ----------------------------------------------------
# Entry for run_all.py
# ----------------------------------------------------

async def start():
    init_db()

    rows = get_conn().execute("SELECT user_id FROM users").fetchall()
    if not rows:
        log.info("No users yet.")
        await asyncio.sleep(10)
        return

    tasks = []

    for r in rows:
        uid = r["user_id"]
        for sess in sessions_list(uid):
            tasks.append(asyncio.create_task(client_worker(uid, sess)))

    if tasks:
        await asyncio.gather(*tasks)
