# worker_forward.py
# Spinify Forwarder — A1 FINAL VERSION (stable + compact)

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
    get_last_sent_at,
    set_last_sent_at,
    get_setting,
    set_setting,
    inc_sent_ok
)

log = logging.getLogger("forwarder")


# ----------------------------------------------------
# Helpers
# ----------------------------------------------------
def now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def is_cmd(text: str | None) -> bool:
    return bool(text and text.strip().startswith("."))


def get_saved_ad(uid: int) -> str | None:
    return get_setting(f"ad_text:{uid}", None)


def set_saved_ad(uid: int, txt: str):
    set_setting(f"ad_text:{uid}", txt)


# ----------------------------------------------------
# Self-commands
# ----------------------------------------------------
async def cmd_help(ev, uid):
    await ev.reply(
        "✹ <b>Spinify Commands</b>\n"
        "• .help\n"
        "• .status\n"
        "• .time 30|45|60\n"
        "• .gc – list groups\n"
        "• .addgc @group\n"
        "• .cleargc – remove all\n"
        "• .adreset – fetch last Saved Message\n",
        parse_mode="html",
    )


async def cmd_status(ev, uid):
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

    g = list_groups(uid)
    ad = get_saved_ad(uid)

    await ev.reply(
        "📟 <b>Spinify Status</b>\n\n"
        f"Interval: {interval} min\n"
        f"Groups: {len(g)}/{groups_cap(uid)}\n"
        f"Next Send: {eta}\n"
        f"Ad Set: {'Yes' if ad else 'No'}",
        parse_mode="html",
    )


async def cmd_time(ev, uid, text):
    try:
        v = int(text.split()[1])
    except:
        return await ev.reply("Usage: .time 30|45|60")

    if v not in (30, 45, 60):
        return await ev.reply("Allowed: 30 / 45 / 60")

    set_setting(f"interval:{uid}", v)
    await ev.reply(f"Interval set → {v} minutes")


async def cmd_gc(ev, uid):
    groups = list_groups(uid)
    if not groups:
        return await ev.reply("No groups added yet.")

    msg = "🎯 <b>Your Groups</b>\n" + "\n".join(
        f"{i+1}. {g}" for i, g in enumerate(groups)
    )
    await ev.reply(msg, parse_mode="html")


async def cmd_addgc(ev, uid, text):
    import re
    pat = r"(https?://t\.me/\S+|t\.me/\S+|@\w+|-100\d+)"
    all_text = text

    if ev.is_reply:
        rep = await ev.get_reply_message()
        if rep and rep.raw_text:
            all_text += "\n" + rep.raw_text

    found = re.findall(pat, all_text)
    if not found:
        return await ev.reply("No valid group links found.")

    added = 0
    skipped = 0
    for g in found:
        ok = add_group(uid, g)
        added += ok
        skipped += 0 if ok else 1

    await ev.reply(f"Added {added}, skipped {skipped}")


async def cmd_cleargc(ev, uid):
    clear_groups(uid)
    await ev.reply("All groups cleared.")


async def cmd_adreset(ev, uid, client):
    msgs = await client.get_messages("me", limit=30)
    for m in msgs:
        txt = m.raw_text or ""
        if txt and not is_cmd(txt):
            set_saved_ad(uid, txt)
            return await ev.reply("Ad updated successfully.")

    await ev.reply("No valid ad found in Saved Messages.")


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
# Forwarder Loop
# ----------------------------------------------------
async def forward_loop(client: TelegramClient, uid: int):
    """ Main forward cycle """
    while True:
        interval = get_interval(uid)
        last = get_last_sent_at(uid)
        now = now_ts()

        if last and (now - last) < interval * 60:
            await asyncio.sleep(5)
            continue

        ad = get_saved_ad(uid)
        if not ad or is_cmd(ad):
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
                await asyncio.sleep(1)

            await asyncio.sleep(100)

        set_last_sent_at(uid, now_ts())


# ----------------------------------------------------
# Worker per user
# ----------------------------------------------------
async def client_worker(uid: int, sess: Dict[str, Any]):
    api_id = int(sess["api_id"])
    api_hash = sess["api_hash"]
    sstr = sess["session_string"]

    client = TelegramClient(
        session=StringSession(sstr),
        api_id=api_id,
        api_hash=api_hash,
        system_version="Android"
    )

    await client.start()
    log.info(f"Started Telethon for UID {uid}, slot={sess['slot']}")

    @client.on(events.NewMessage(chats="me"))
    async def saved(ev):
        txt = ev.raw_text or ""
        if txt and not is_cmd(txt):
            set_saved_ad(uid, txt)

    @client.on(events.NewMessage(outgoing=True))
    async def cmds(ev):
        if is_cmd(ev.raw_text or ""):
            await handle_command(ev, uid, client)

    try:
        await forward_loop(client, uid)
    except Exception as e:
        log.error(f"Worker crashed for {uid}: {e}")
    finally:
        await client.disconnect()


# ----------------------------------------------------
# Entry used by run_all.py
# ----------------------------------------------------
async def start():
    init_db()

    rows = get_conn().execute("SELECT DISTINCT user_id FROM users").fetchall()
    if not rows:
        log.info("Forwarder: no users.")
        await asyncio.sleep(10)
        return

    tasks = []
    for r in rows:
        uid = r["user_id"]
        for sess in sessions_list(uid):
            tasks.append(asyncio.create_task(client_worker(uid, sess)))

    if tasks:
        await asyncio.gather(*tasks)
