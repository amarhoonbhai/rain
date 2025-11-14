# worker_forward.py — Saved-All forwarder + in-session self-commands
# Requires: core.db (Mongo), Pyrogram 2.x
import os, asyncio, logging, re
from datetime import datetime, timezone

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait, Unauthorized, AuthKeyUnregistered, UserDeactivated, UserDeactivatedBan, RPCError

from aiogram import Bot as AioBot  # for optional DM rehydrate ping

from core.db import (
    init_db,
    users_with_sessions, sessions_list, sessions_strings,
    list_groups, add_group, clear_groups, groups_cap,
    get_interval, set_interval,
    get_setting, set_setting,
    last_sent_at_for, mark_sent_now,
    inc_sent_ok,
    night_enabled,
)

log = logging.getLogger("worker")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

# ---------- ENV ----------
GROUP_DELAY_SEC = int(os.getenv("GROUP_DELAY_SEC", "30"))     # delay between groups
TICK_SEC        = int(os.getenv("TICK_SEC", "5"))             # scheduler tick
MAIN_BOT_TOKEN  = os.getenv("MAIN_BOT_TOKEN", "").strip()     # for DM ping on revoke

# ---------- Helpers ----------
def now_utc() -> int:
    return int(datetime.now(timezone.utc).timestamp())

def fmt_eta(seconds: int) -> str:
    if seconds <= 0: return "now"
    m, s = divmod(seconds, 60); h, m = divmod(m, 60)
    out = []
    if h: out.append(f"{h}h")
    if m: out.append(f"{m}m")
    if s or not out: out.append(f"{s}s")
    return "in ~" + " ".join(out)

def _cycle_idx_key(uid: int) -> str:
    return f"cycle:index:{uid}"

def get_cycle_index(uid: int) -> int:
    return int(get_setting(_cycle_idx_key(uid), 0) or 0)

def set_cycle_index(uid: int, idx: int):
    set_setting(_cycle_idx_key(uid), int(max(0, idx)))

def reset_cycle(uid: int):
    set_cycle_index(uid, 0)

async def dm_rehydrate(user_id: int, text: str):
    if not MAIN_BOT_TOKEN: return
    try:
        bot = AioBot(MAIN_BOT_TOKEN)
        await bot.send_message(user_id, text)
    except Exception:
        pass
    finally:
        try: await bot.session.close()
        except Exception: pass

# ---------- Saved-All fetch ----------
async def fetch_saved_all(app: Client, limit: int = 200):
    """
    Returns a list of messages from 'Saved Messages' (newest→oldest).
    We will reverse to oldest→newest for stable cycling.
    """
    msgs = []
    try:
        async for m in app.get_chat_history("me", limit=limit):
            # Exclude service actions/empty
            if not (m.text or m.caption or m.media):
                continue
            msgs.append(m)
    except Exception as e:
        log.info("[%s] fetch_saved_all failed: %s", app.name, e)
    # oldest first:
    msgs.reverse()
    return msgs

# ---------- Copy helper ----------
async def copy_one(app: Client, src: Message, target: str):
    """
    Copy (not forward) to preserve premium emoji and captions cleanly.
    Accepts target as @username, invite link, or numeric id.
    """
    try:
        # Resolve target
        chat = target
        if target.startswith("https://t.me/") or target.startswith("t.me/") or target.startswith("http://t.me/"):
            chat = target
        elif re.fullmatch(r"-?\d{5,}", target):
            chat = int(target)
        elif target.startswith("@"):
            chat = target
        else:
            # try as-is; pyrogram will resolve or fail
            chat = target

        if src.media:
            return await app.copy_message(chat_id=chat, from_chat_id="me", message_id=src.id)
        if src.text:
            return await app.send_message(chat_id=chat, text=src.text, entities=src.entities)
        if src.caption:
            # media without media? Fallback handled in media branch; keep guard
            return await app.send_message(chat_id=chat, text=src.caption, entities=src.caption_entities)
    except FloodWait as fw:
        await asyncio.sleep(fw.value + 1)
        # one retry
        try:
            if src.media:
                return await app.copy_message(chat_id=chat, from_chat_id="me", message_id=src.id)
            if src.text:
                return await app.send_message(chat_id=chat, text=src.text, entities=src.entities)
            if src.caption:
                return await app.send_message(chat_id=chat, text=src.caption, entities=src.caption_entities)
        except Exception as e2:
            log.info("[%s] copy retry failed → %s", app.name, e2)
            return None
    except (Unauthorized, AuthKeyUnregistered, UserDeactivated, UserDeactivatedBan) as e:
        log.warning("[%s] auth revoked: %s", app.name, e)
        await dm_rehydrate(app.storage.user_id or 0, "⚠️ Your session looks revoked/expired. Please /start @SpinifyLoginBot and relogin.")
        return None
    except RPCError as e:
        log.info("[%s] RPCError to %s: %s", app.name, target, e)
        return None
    except Exception as e:
        log.info("[%s] copy error to %s: %s", app.name, target, e)
        return None
    return None

# ---------- Per-user scheduler ----------
async def run_user_scheduler(uid: int, app: Client):
    log.info("[u%s] scheduler start", uid)
    while True:
        try:
            # Night mode skip
            if night_enabled():
                await asyncio.sleep(TICK_SEC); continue

            groups = list_groups(uid)
            if not groups:
                await asyncio.sleep(TICK_SEC); continue

            interval_min = max(1, int(get_interval(uid) or 30))
            last_sent = last_sent_at_for(uid)
            now = now_utc()
            if last_sent is not None:
                due_in = interval_min * 60 - (now - int(last_sent))
                if due_in > 0:
                    await asyncio.sleep(min(TICK_SEC, max(1, due_in)))
                    continue

            # Load saved messages (oldest→newest)
            msgs = await fetch_saved_all(app, limit=400)
            if not msgs:
                log.info("[u%s] no Saved Messages content", uid)
                mark_sent_now(uid)  # avoid tight loop
                await asyncio.sleep(TICK_SEC)
                continue

            idx = get_cycle_index(uid)
            if idx >= len(msgs):
                idx = 0
            src = msgs[idx]

            # Send to all groups with per-target delay
            sent_any = False
            for i, g in enumerate(groups, 1):
                r = await copy_one(app, src, g)
                if r: 
                    sent_any = True
                    inc_sent_ok(uid, 1)
                await asyncio.sleep(GROUP_DELAY_SEC)  # gap between groups

            # Advance cycle/mark time
            set_cycle_index(uid, idx + 1)
            mark_sent_now(uid)

            # Small idle so we don’t immediately loop
            await asyncio.sleep(TICK_SEC)
        except Exception as e:
            log.error("[u%s] scheduler error: %s", uid, e)
            await asyncio.sleep(2)

# ---------- Command handlers (run on EACH session client) ----------
HELP_TXT = (
    "📜 Commands\n"
    "• .help — this help\n"
    "• .addgc  (up to 5 per message, one per line) — add targets (@handle / id / any t.me link)\n"
    "• .gc — list saved targets\n"
    "• .cleargc — clear all targets\n"
    "• .time 30m|45m|60m — set interval\n"
    "• .adreset — restart Saved-All cycle to first message\n"
    "• .fstats — show worker status for your account\n"
    "Tip: Put all your ads in Saved Messages; the worker cycles them one by one every interval.\n"
)

async def handle_cmd_me(client: Client, msg: Message, uid: int):
    txt = msg.text or msg.caption or ""
    t = txt.strip()
    low = t.lower()

    # .help
    if low.startswith(".help"):
        await msg.reply_text(HELP_TXT); return

    # .gc
    if low.startswith(".gc"):
        items = list_groups(uid)
        cap = groups_cap(uid)
        if not items:
            await msg.reply_text(f"✇ No targets yet. Cap {cap}. Use <code>.addgc</code>.")
        else:
            body = "\n".join(f"• {x}" for x in items)
            await msg.reply_text(f"✇ Targets ({len(items)}/{cap}):\n{body}")
        return

    # .cleargc
    if low.startswith(".cleargc"):
        clear_groups(uid)
        await msg.reply_text("✅ Cleared all targets.")
        return

    # .adreset
    if low.startswith(".adreset"):
        reset_cycle(uid)
        await msg.reply_text("✅ Ad cycle reset. Next send will use the first Saved Message.")
        return

    # .time
    if low.startswith(".time"):
        m = re.search(r"\.time\s+(\d+)\s*(m|min|minutes?)?$", low)
        if not m:
            await msg.reply_text("❌ Use: <code>.time 30m</code> (30|45|60)"); return
        mins = int(m.group(1))
        if mins not in (30, 45, 60):
            await msg.reply_text("❌ Allowed: 30, 45, 60 minutes."); return
        set_interval(uid, mins)
        await msg.reply_text(f"✅ Interval set to {mins} min.")
        return

    # .addgc  (multi-line up to 5 in one go)
    if low.startswith(".addgc"):
        lines = [ln.strip() for ln in t.splitlines()[1:]]  # everything after first line
        # also support inline: ".addgc @a @b"
        if len(lines) == 0:
            inline = t.split(maxsplit=1)
            if len(inline) == 2:
                lines = [p.strip() for p in inline[1].split() if p.strip()]
        if not lines:
            await msg.reply_text("❌ Send targets after the command. Example:\n<code>.addgc\n@group1\nhttps://t.me/+abc123\n-1001234567890</code>")
            return
        added = 0
        cap = groups_cap(uid)
        for raw in lines[:5]:
            if len(list_groups(uid)) >= cap:
                break
            ok = add_group(uid, raw)
            if ok: added += 1
        cur = len(list_groups(uid))
        await msg.reply_text(f"✅ Added {added}. Now {cur}/{cap}.")
        return

    # .fstats
    if low.startswith(".fstats"):
        items = list_groups(uid)
        interval = get_interval(uid) or 30
        last = last_sent_at_for(uid)
        eta = "now" if last is None else fmt_eta(interval*60 - (now_utc() - int(last)))
        await msg.reply_text(
            "📟 Forward Stats\n"
            f"✇ Interval: {interval} min\n"
            f"✇ Groups: {len(items)}\n"
            f"✇ Next send: {eta}\n"
            f"🌙 Night: {'ON' if night_enabled() else 'OFF'}"
        )
        return

# ---------- Wire handlers ----------
def wire_handlers(app: Client, uid: int):
    @app.on_message(filters.me & filters.text & ~filters.edited)
    async def me_text(m: Message):
        try:
            # only dot-prefixed commands
            if (m.text or "").strip().startswith("."):
                await handle_cmd_me(app, m, uid)
        except Exception as e:
            log.error("[u%s] cmd error: %s", uid, e)

# ---------- Build Client ----------
def build_client(uid: int, slotrec: dict) -> Client:
    return Client(
        name=f"u{uid}s{slotrec['slot']}",
        api_id=int(slotrec["api_id"]),
        api_hash=str(slotrec["api_hash"]),
        session_string=str(slotrec["session_string"]),
        workdir=None,  # memory-only
        device_model="Spinify Worker",
        app_version="2.0",
        system_version="Linux",
        lang_code="en",
        in_memory=True,
    )

# ---------- Main ----------
async def run_user(uid: int):
    slots = sessions_list(uid)
    if not slots:
        return
    # Use the first slot as the sender; others also listen for commands (you can fan-out later if you want)
    main_slot = slots[0]
    app = build_client(uid, main_slot)
    await app.start()
    try:
        wire_handlers(app, uid)
        # scheduler as task
        sched = asyncio.create_task(run_user_scheduler(uid, app))
        await asyncio.Event().wait()  # keep alive forever; ^C handled by run_all
    finally:
        try: await app.stop()
        except Exception: pass

async def main():
    init_db()
    tasks = []
    for uid in users_with_sessions():
        tasks.append(asyncio.create_task(run_user(uid)))
    if not tasks:
        log.info("no users with sessions; idle…")
    await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
