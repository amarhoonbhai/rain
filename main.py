import os, asyncio, logging, re, time, pytz
from datetime import datetime
from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from dotenv import load_dotenv
from storage import Storage
from telethon import TelegramClient, functions, types as tt
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, UserBannedInChannelError, ChatWriteForbiddenError

logging.basicConfig(level=logging.INFO)
load_dotenv()

MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
DB_PATH = os.getenv("DB_PATH", "data/spinify.sqlite")
GATE_CHANNEL = os.getenv("GATE_CHANNEL", "@PhiloBots")
GATE_GROUP = os.getenv("GATE_GROUP", "@YourGroup")
TIMEZONE = os.getenv("TIMEZONE", "Asia/Kolkata")

def welcome_text(has_session: bool) -> str:
    lines = [
        "<b>Welcome to Spinify Ads 👋</b>",
        "Public autoposting with safe pacing.",
        "",
        "<b>How it works</b>",
        "1) <b>Join gate</b>: you must be in our channel and group.",
        "2) <b>Connect account</b>: create a secure session via @SpinifyLoginBot.",
        "3) <b>Add Message</b>: forward the original message you want to repeat.",
        "4) <b>Load Groups</b>: paste up to 5 t.me links per batch to add targets.",
        "5) <b>Start</b>: posts every 30m, with a 1‑minute gap between groups.",
        "",
        "Use <b>Profile</b> to see status, targets and recent activity."
    ]
    if not has_session:
        lines.append("\\n<b>Next step:</b> Tap “Connect account (@SpinifyLoginBot)” below.")
    return "\\n".join(lines)

if not MAIN_BOT_TOKEN:
    raise RuntimeError("MAIN_BOT_TOKEN missing in .env")
if not ENCRYPTION_KEY:
    raise RuntimeError("ENCRYPTION_KEY missing in .env")

bot = Bot(token=MAIN_BOT_TOKEN, parse_mode=types.ParseMode.HTML)
dp = Dispatcher(bot, storage=MemoryStorage())
store = Storage(DB_PATH, ENCRYPTION_KEY)
TZ = pytz.timezone(TIMEZONE)

LINK_RX = re.compile(r"(https?://)?t\.me/(joinchat/|c/|\\+)?([A-Za-z0-9_\\-+/]+)")
BATCH_LIMIT = 5

# --- Keyboards ---
def main_menu(has_session: bool):
    rows = []
    if has_session:
        rows.append([types.InlineKeyboardButton("➕ Add Message", callback_data="addmsg"),
                     types.InlineKeyboardButton("🧩 Load Groups", callback_data="loadgroups")])
        rows.append([types.InlineKeyboardButton("🔁 Refresh Groups", callback_data="refresh_groups"),
                     types.InlineKeyboardButton("👤 Profile", callback_data="profile")])
        rows.append([types.InlineKeyboardButton("▶️ Start (30m)", callback_data="start"),
                     types.InlineKeyboardButton("⏹ Stop", callback_data="stop")])
    else:
        rows.append([types.InlineKeyboardButton("🔐 Connect account (@SpinifyLoginBot)", url="https://t.me/SpinifyLoginBot")])
    rows.append([types.InlineKeyboardButton("👨‍💻 Developer", url="https://t.me/Spinify"),
                 types.InlineKeyboardButton("📘 Guide", url="https://t.me/PhiloBots")])
    rows.append([types.InlineKeyboardButton("🚪 Logout", callback_data="logout")])
    return types.InlineKeyboardMarkup(inline_keyboard=rows)

async def check_join_gate(user_id: int) -> bool:
    ok1 = ok2 = False
    try:
        m1 = await bot.get_chat_member(GATE_CHANNEL, user_id)
        ok1 = m1.status in ("member", "administrator", "creator")
    except Exception:
        ok1 = False
    try:
        m2 = await bot.get_chat_member(GATE_GROUP, user_id)
        ok2 = m2.status in ("member", "administrator", "creator")
    except Exception:
        ok2 = False
    return ok1 and ok2

@dp.message_handler(commands=["start"])
async def cmd_start(msg: types.Message):
    await store.ensure_user(msg.from_user.id)
    gate_ok = await check_join_gate(msg.from_user.id)
    if not gate_ok:
        kb = types.InlineKeyboardMarkup(inline_keyboard=[[
            types.InlineKeyboardButton("Join Channel", url=f"https://t.me/{GATE_CHANNEL.lstrip('@')}"),
            types.InlineKeyboardButton("Join Group", url=f"https://t.me/{GATE_GROUP.lstrip('@')}"),
        ], [
            types.InlineKeyboardButton("I Joined ✅ (Recheck)", callback_data="recheck_gate")
        ]])
        await msg.answer("Please join our <b>channel</b> and <b>group</b> to continue.", reply_markup=kb)
        return
    has_session = bool(await store.get_session(msg.from_user.id))
    await msg.answer(welcome_text(has_session), reply_markup=main_menu(has_session))

@dp.callback_query_handler(lambda c: c.data == "recheck_gate")
async def cb_recheck_gate(call: types.CallbackQuery):
    ok = await check_join_gate(call.from_user.id)
    if not ok:
        await call.answer("Still not joined in both. Please join first.", show_alert=True)
        return
    has_session = bool(await store.get_session(call.from_user.id))
    await call.message.edit_text(welcome_text(has_session), reply_markup=main_menu(has_session))
    await call.answer()

# --- Add Message (forward-only) ---
@dp.callback_query_handler(lambda c: c.data == "addmsg")
async def cb_addmsg(call: types.CallbackQuery):
    if not await store.get_session(call.from_user.id):
        await call.answer("Connect account first in @SpinifyLoginBot.", show_alert=True)
        return
    await call.message.answer("Forward the <b>original message</b> you want to repeat here.\n(Forward from its source channel/group; we will store the source to forward later.)")
    await call.answer()

@dp.message_handler(content_types=types.ContentTypes.ANY)
async def catch_forward(msg: types.Message):
    # Only handle forwards after pressing Add Message; heuristic: forward must have forward_from_chat
    if not msg.forward_from_chat:
        return  # ignore random dm; keep handlers simple
    sess = await store.get_session(msg.from_user.id)
    if not sess:
        await msg.answer("Connect your account first in @SpinifyLoginBot.")
        return
    src = msg.forward_from_chat
    src_chat_id = src.id
    src_msg_id = msg.forward_from_message_id or msg.message_id  # fallback
    await store.save_blueprint(msg.from_user.id, src_chat_id, src_msg_id)
    await store.audit(msg.from_user.id, "blueprint_saved", {"src_chat_id": src_chat_id, "src_msg_id": src_msg_id})
    await msg.answer("✅ Saved this message as your active blueprint. Use <b>Load Groups</b> to add targets.")

# --- Load Groups (links only, max 5) ---
@dp.callback_query_handler(lambda c: c.data == "loadgroups")
async def cb_loadgroups(call: types.CallbackQuery):
    if not await store.get_session(call.from_user.id):
        await call.answer("Connect account first in @SpinifyLoginBot.", show_alert=True)
        return
    await call.message.answer("Paste up to <b>5</b> Telegram links (each on a new line):\n"
                              "• https://t.me/PublicName\n• https://t.me/+InviteHash\n• https://t.me/joinchat/InviteHash")
    await call.answer()

@dp.message_handler(lambda m: LINK_RX.search(m.text or "") is not None)
async def handle_links(msg: types.Message):
    sess = await store.get_session(msg.from_user.id)
    if not sess:
        return
    api_id, api_hash, session_str = sess
    links = [s.strip() for s in (msg.text or "").splitlines() if s.strip()]
    picked = links[:BATCH_LIMIT]
    ignored = len(links) - len(picked)

    saved = []
    skipped = []

    async with TelegramClient(StringSession(session_str), api_id, api_hash) as client:
        for link in picked:
            try:
                chat = await resolve_and_join(client, link)
                if not chat:
                    skipped.append((link, "invalid or cannot join"))
                    continue
                title = getattr(chat, "title", getattr(chat, "username", str(chat.id)))
                type_ = "channel" if getattr(chat, "broadcast", False) else "group"
                # Check posting permission heuristically
                can_post = await has_post_rights(client, chat)
                if not can_post:
                    skipped.append((link, "no post permission (channel admin needed?)"))
                    continue
                await store.add_target(msg.from_user.id, chat.id, title, type_)
                await store.update_target_meta(msg.from_user.id, chat.id, title=title, enabled=True)
                saved.append(title)
            except FloodWaitError as e:
                skipped.append((link, f"flood wait {e.seconds}s; try later"))
            except Exception as e:
                skipped.append((link, f"{type(e).__name__}: {e}"))

    txt = f"Links received: {len(picked)}\n✅ Saved: {len(saved)}\n⚠️ Skipped: {len(skipped)}"
    if skipped:
        for link, reason in skipped:
            txt += f"\n - {link}: {reason}"
    if ignored > 0:
        txt += f"\n\n(ignored {ignored} extra links beyond the 5-limit)"
    kb = types.InlineKeyboardMarkup(inline_keyboard=[[
        types.InlineKeyboardButton("🔁 Refresh Groups", callback_data="refresh_groups"),
        types.InlineKeyboardButton("Done", callback_data="profile")
    ]])
    await msg.answer(txt, reply_markup=kb)

async def resolve_and_join(client: TelegramClient, link: str):
    # Normalize link
    link = link.strip()
    if not link.startswith("http"):
        link = "https://" + link
    m = LINK_RX.search(link)
    if not m:
        return None
    suffix = m.group(2) or ""
    payload = m.group(3)
    try:
        if suffix.startswith("+") or "joinchat" in suffix:
            # Private invite
            invite_hash = payload.replace("joinchat/", "").replace("+", "")
            ok = await client(functions.messages.ImportChatInviteRequest(hash=invite_hash))
            chat = ok.chats[0] if ok.chats else None
            return chat
        else:
            username_or_path = payload.split("/")[0]
            ent = await client.get_entity(username_or_path)
            # Try join (no-op if already a participant)
            try:
                await client(functions.channels.JoinChannelRequest(ent))
            except Exception:
                pass
            return ent
    except Exception:
        return None

async def has_post_rights(client: TelegramClient, chat):
    # Heuristic:
    # - For broadcast channels: require admin rights
    # - For megagroups/groups: if banned rights forbid sending -> False, else True
    try:
        full = await client(functions.channels.GetFullChannelRequest(chat))
        ch = full.chats[0]
        if getattr(ch, "broadcast", False) and not getattr(ch, "megagroup", False):
            # broadcast channel
            me = await client.get_permissions(ch, 'me')
            return bool(getattr(me, "is_admin", False) or getattr(me, "is_creator", False) or getattr(me, "send_messages", False))
        # group / megagroup
        me = await client.get_permissions(ch, 'me')
        if getattr(me, "banned_rights", None) and getattr(me.banned_rights, "send_messages", False):
            return False
        return True
    except Exception:
        # Fallback best-effort
        return True

# --- Refresh ---
@dp.callback_query_handler(lambda c: c.data == "refresh_groups")
async def cb_refresh(call: types.CallbackQuery):
    # For links-only design, we re-check titles and enablement via get_entity
    sess = await store.get_session(call.from_user.id)
    if not sess:
        await call.answer("Connect account first in @SpinifyLoginBot.", show_alert=True)
        return
    api_id, api_hash, session_str = sess
    targets = await store.list_targets(call.from_user.id)
    updated = 0
    async with TelegramClient(StringSession(session_str), api_id, api_hash) as client:
        for chat_id, title, type_, enabled in targets:
            try:
                ent = await client.get_entity(int(chat_id))
                t = getattr(ent, "title", getattr(ent, "username", str(ent.id)))
                await store.update_target_meta(call.from_user.id, chat_id, title=t)
                updated += 1
            except Exception:
                pass
    await call.answer("Refreshed.", show_alert=False)
    await call.message.edit_text(f"Refreshed metadata for {updated} targets.", reply_markup=main_menu(True))

# --- Start/Stop/Profile/Logout ---
@dp.callback_query_handler(lambda c: c.data == "start")
async def cb_start(call: types.CallbackQuery):
    if not await store.get_session(call.from_user.id):
        await call.answer("Connect account first in @SpinifyLoginBot.", show_alert=True)
        return
    await store.set_posting(call.from_user.id, True)
    await call.answer("Started ✅")
    await call.message.edit_text("Posting enabled. Interval: 30m. 1-minute gap between groups.", reply_markup=main_menu(True))

@dp.callback_query_handler(lambda c: c.data == "stop")
async def cb_stop(call: types.CallbackQuery):
    await store.set_posting(call.from_user.id, False)
    await call.answer("Stopped ⏹")
    await call.message.edit_text("Posting paused.", reply_markup=main_menu(True))

@dp.callback_query_handler(lambda c: c.data == "profile")
async def cb_profile(call: types.CallbackQuery):
    sess_ok = bool(await store.get_session(call.from_user.id))
    bp = await store.get_blueprint(call.from_user.id)
    targets = await store.list_targets(call.from_user.id)
    st = await store.get_settings(call.from_user.id)
    posting_on, last_global, interval_s = st if st else (0, None, 1800)
    last_global_dt = datetime.fromtimestamp(last_global, TZ).strftime("%Y-%m-%d %H:%M:%S") if last_global else "—"
    lines = [
        f"<b>Profile</b>",
        f"Session: {'✅' if sess_ok else '❌'}",
        f"Active Msg: {'✅' if bp else '❌'}",
        f"Targets: {len(targets)}",
        f"Posting: {'ON' if posting_on else 'OFF'} (interval: 30m)",
        f"Last send: {last_global_dt} {TIMEZONE}",
    ]
    logs = await store.last_audit(call.from_user.id, 5)
    if logs:
        lines.append("\n<b>Recent</b>:")
        for a, payload, ts in logs:
            tsd = datetime.fromtimestamp(ts, TZ).strftime("%H:%M")
            lines.append(f"• {tsd} {a}")
    await call.message.edit_text("\n".join(lines), reply_markup=main_menu(sess_ok))
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "logout")
async def cb_logout(call: types.CallbackQuery):
    await store.invalidate_session(call.from_user.id)
    await call.message.edit_text("Logged out. Connect again via @SpinifyLoginBot when ready.", reply_markup=main_menu(False))
    await call.answer()

# --- Owner /broadcast ---
@dp.message_handler(commands=["broadcast"])
async def owner_broadcast(msg: types.Message):
    if msg.from_user.id != OWNER_ID:
        return
    text = msg.text.split(" ", 1)[1] if " " in msg.text else (msg.reply_to_message.text if msg.reply_to_message else None)
    if not text:
        await msg.answer("Usage: reply or /broadcast <text>")
        return
    # Send to all users
    async with store_conn() as db:
        pass
    # Simpler: fetch from users table
    sent = 0
    async with store_ctx() as s:
        async with aiosqlite.connect(s.db_path) as db:
            cur = await db.execute("SELECT tg_id FROM users")
            ids = [r[0] for r in await cur.fetchall()]
    for uid in ids:
        try:
            await bot.send_message(uid, f"[Broadcast]\n\n{text}")
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass
    await msg.answer(f"Broadcast sent to {sent} users.")

# Helper context for owner broadcast
import aiosqlite
from contextlib import asynccontextmanager
@asynccontextmanager
async def store_ctx():
    yield store

# --- Dispatcher (every minute) ---
async def dispatcher_loop():
    while True:
        try:
            await tick_all_users()
        except Exception as e:
            logging.exception("dispatcher error: %s", e)
        await asyncio.sleep(60)  # 1-minute global gap

async def tick_all_users():
    now_ts = int(time.time())
    # Fetch all users with posting_on
    async with aiosqlite.connect(store.db_path) as db:
        cur = await db.execute("SELECT u.tg_id FROM users u JOIN settings s ON u.tg_id=s.tg_id WHERE s.posting_on=1")
        users = [r[0] for r in await cur.fetchall()]

    for uid in users:
        sess = await store.get_session(uid)
        bp = await store.get_blueprint(uid)
        if not sess or not bp:
            continue
        api_id, api_hash, session_str = sess
        src_chat_id, src_msg_id = bp
        due = await store.targets_due(uid, now_ts)
        if not due:
            continue
        target_chat_id = due[0]  # pick one
        try:
            async with TelegramClient(StringSession(session_str), api_id, api_hash) as client:
                # forward_messages accepts list or id
                await client.forward_messages(entity=target_chat_id, messages=src_msg_id, from_peer=src_chat_id)
            await store.mark_sent(uid, target_chat_id)
            await store.touch_global_send(uid)
            await store.audit(uid, "send_ok", {"chat_id": target_chat_id, "src": [src_chat_id, src_msg_id]})
        except FloodWaitError as e:
            await store.set_cooldown(uid, target_chat_id, e.seconds + 10)
            await store.audit(uid, "flood_wait", {"chat_id": target_chat_id, "sec": e.seconds})
        except (UserBannedInChannelError, ChatWriteForbiddenError) as e:
            fails = await store.inc_fail(uid, target_chat_id)
            await store.audit(uid, "send_forbidden", {"chat_id": target_chat_id, "err": str(e), "fails": fails})
            if fails >= 3:
                await store.update_target_meta(uid, target_chat_id, enabled=False)
        except Exception as e:
            fails = await store.inc_fail(uid, target_chat_id)
            await store.audit(uid, "send_fail", {"chat_id": target_chat_id, "err": str(e), "fails": fails})
            if fails >= 3:
                await store.update_target_meta(uid, target_chat_id, enabled=False)

async def on_startup(_):
    await store.init()
    # Fire dispatcher
    asyncio.get_event_loop().create_task(dispatcher_loop())
    logging.info("Main bot ready.")

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)


@dp.message_handler(commands=["welcome"])
async def cmd_welcome(msg: types.Message):
    has_session = bool(await store.get_session(msg.from_user.id))
    await msg.answer(welcome_text(has_session), reply_markup=main_menu(has_session))
