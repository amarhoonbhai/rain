# worker_forward.py
# Spinify Ads – Clean Forwarder (Telethon)
# Final Version – Commands not forwarded, fixed intervals, 100s delay

import asyncio
import logging
from datetime import datetime, timezone

from telethon import TelegramClient, events, errors

from core.db import (
    get_conn,
    sessions_list,
    list_groups,
    get_interval,
    get_last_sent_at,
    set_last_sent_at,
    get_saved_ad,
)

log = logging.getLogger("forwarder")

API_ID_FIELD = "api_id"
API_HASH_FIELD = "api_hash"
SESSION_FIELD = "session_string"

# ===================================
# Helpers
# ===================================

def now_ts():
    return int(datetime.now(timezone.utc).timestamp())


def is_control_message(msg_text: str) -> bool:
    """
    Any message starting with '.' is a self-command (.status, .time, .help)
    These MUST NOT be forwarded.
    """
    if not msg_text:
        return False
    return msg_text.strip().startswith(".")


async def safe_send(client, chat_id, message_obj):
    """
    Safely forward/send message to group.
    """
    try:
        await client.send_message(chat_id, message_obj)
        return True
    except errors.FloodWaitError as e:
        log.warning(f"FloodWait: sleeping {e.seconds}s")
        await asyncio.sleep(e.seconds)
        return False
    except Exception as e:
        log.error(f"Send error → {chat_id} → {e}")
        return False


# ===================================
# Main Loop
# ===================================

async def forward_loop(client, uid: int):
    """
    Main per-user forwarding loop.
    """
    while True:
        interval = get_interval(uid) or 30  # default
        last = get_last_sent_at(uid)

        # Time check
        now = now_ts()
        if last is not None and now - last < interval * 60:
            await asyncio.sleep(5)
            continue

        # Fetch saved ad
        ad_msg = get_saved_ad(uid)
        if not ad_msg:
            await asyncio.sleep(10)
            continue

        # Skip if it's a command
        if is_control_message(ad_msg):
            log.info(f"User {uid} tried to forward a command. Skipped.")
            await asyncio.sleep(10)
            continue

        groups = list_groups(uid)
        if not groups:
            await asyncio.sleep(10)
            continue

        # Forward to all groups – 100s delay between each
        for target in groups:
            ok = await safe_send(client, target, ad_msg)
            if ok:
                log.info(f"User {uid} → sent to {target}")
            await asyncio.sleep(100)  # REQUIRED DELAY

        set_last_sent_at(uid, now_ts())


async def client_worker(uid: int, row: dict):
    """
    One logged-in user account = one Telethon client.
    """
    api_id = int(row[API_ID_FIELD])
    api_hash = row[API_HASH_FIELD]
    session = row[SESSION_FIELD]

    client = TelegramClient(
        session=f"user-{uid}-slot-{row['slot']}",
        api_id=api_id,
        api_hash=api_hash,
        system_version="Android",
    )

    await client.start()

    log.info(f"Forwarder session started for user {uid}, slot {row['slot']}")

    try:
        await forward_loop(client, uid)
    finally:
        await client.disconnect()


async def main():
    """
    Start ALL Telethon client workers.
    """
    rows = get_conn().execute("SELECT DISTINCT user_id FROM sessions").fetchall()
    if not rows:
        log.info("No sessions found.")
        await asyncio.sleep(10)
        return

    tasks = []

    for r in rows:
        uid = r["user_id"]
        for row in sessions_list(uid):
            tasks.append(asyncio.create_task(client_worker(uid, row)))

    if tasks:
        await asyncio.gather(*tasks)
    else:
        log.info("No active Telethon sessions.")


# Run
async def start():
    await main()
