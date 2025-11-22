import os
import asyncio
import logging
from datetime import datetime, timezone

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)

from dotenv import load_dotenv

from core.db import (
    init_db, get_conn, ensure_user,
    sessions_list, sessions_delete, sessions_count_user, sessions_count,
    list_groups, groups_cap, get_interval, get_last_sent_at,
    users_count, get_total_sent_ok, top_users,
    get_gate_channels_effective, set_setting, get_setting,
    night_enabled, set_night_enabled,
)

# ===========================================================
# BOOTSTRAP
# ===========================================================
load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("main-bot")

TOKEN = os.getenv("MAIN_BOT_TOKEN", "").strip()
if ":" not in TOKEN:
    raise RuntimeError("MAIN_BOT_TOKEN missing")

OWNER_ID = int(os.getenv("OWNER_ID", "0"))
REQUIRED_CHANNELS = os.getenv("REQUIRED_CHANNELS", "@PhiloBots,@TheTrafficZone")
DEVELOPER_TAG = "@Spinify"
UNLOCK_GC_LINK = os.getenv("UNLOCK_GC_LINK", "")

bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

init_db()

# ===========================================================
# HELPERS
# ===========================================================

def is_owner(uid: int) -> bool:
    return OWNER_ID and uid == OWNER_ID

def _gate_channels():
    ch1, ch2 = get_gate_channels_effective()
    return [c for c in (ch1, ch2) if c]

async def _check_gate(uid: int):
    missing = []
    for ch in _gate_channels():
        try:
            m = await bot.get_chat_member(ch, uid)
            if str(getattr(m, "status", "left")).lower() in ["left", "kicked"]:
                missing.append(ch)
        except Exception:
            missing.append(ch)
    return len(missing) == 0, missing

def gate_text():
    lines = "\n".join(f"   ✹ {c}" for c in _gate_channels())
    return (
        "🔐 <b>Access Locked</b>\n"
        "Join all required channels to continue:\n\n"
        f"{lines}\n\n"
        "After joining, press <b>I've Joined</b>."
    )

def gate_kb():
    rows = []
    for c in _gate_channels():
        rows.append([InlineKeyboardButton(text=f"🔗 {c}", url=f"https://t.me/{c.lstrip('@')}")])
    rows.append([InlineKeyboardButton(text="✅ I've Joined", callback_data="gate:check")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def format_eta(uid: int):
    last = get_last_sent_at(uid)
    interval = get_interval(uid)

    if last is None:
        return f"in ~{interval}m"

    now = int(datetime.now(timezone.utc).timestamp())
    left = interval * 60 - (now - int(last))

    if left <= 2:
        return "very soon"

    m = left // 60
    s = left % 60
    return f"in ~{m}m {s}s"

# ===========================================================
# MAIN MENU — BLUE GRID UI
# ===========================================================

def main_kb(uid: int):
    kb = [
        [
            InlineKeyboardButton(text="👤 Accounts", callback_data="menu:acc"),
            InlineKeyboardButton(text="📜 Commands", callback_data="menu:cmds"),
        ],
        [
            InlineKeyboardButton(text="🔓 Unlock GC", callback_data="menu:unlock"),
            InlineKeyboardButton(text="⚠️ Disclaimer", callback_data="menu:disc"),
        ],
        [
            InlineKeyboardButton(text="📣 Developer", url=f"https://t.me/{DEVELOPER_TAG.lstrip('@')}"),
            InlineKeyboardButton(text="🔄 Refresh", callback_data="menu:home"),
        ],
    ]
    if is_owner(uid):
        kb.append(
            [
                InlineKeyboardButton(text="🌙 Auto Night", callback_data="owner:night"),
                InlineKeyboardButton(text="📊 Stats", callback_data="owner:stats"),
            ]
        )
        kb.append(
            [
                InlineKeyboardButton(text="🏆 Top", callback_data="owner:top"),
                InlineKeyboardButton(text="📣 Broadcast", callback_data="owner:bcast"),
            ]
        )
    return InlineKeyboardMarkup(inline_keyboard=kb)

async def home(event, uid: int):
    gs = len(list_groups(uid))
    ss = sessions_count_user(uid)
    interval = get_interval(uid)
    eta = "—" if ss == 0 or gs == 0 else format_eta(uid)

    text = (
        "💙 <b>Welcome to Spinify Ads Bot</b>\n"
        "Automated forwarding using your login account.\n"
        "Use <b>@SpinifyLoginBot</b> to add accounts.\n\n"
        f"👤 Sessions: {ss}\n"
        f"🧩 Groups: {gs}/{groups_cap(uid)}\n"
        f"⏱ Interval: {interval}m\n"
        f"📤 Next Send: {eta}\n"
        f"🌙 Night Mode: {'ON' if night_enabled() else 'OFF'}\n\n"
        f"💎 Want paid version with full features? Contact {DEVELOPER_TAG}"
    )

    if isinstance(event, Message):
        await event.answer(text, reply_markup=main_kb(uid))
    else:
        try:
            await event.message.edit_text(text, reply_markup=main_kb(uid))
        except TelegramBadRequest:
            pass

# ===========================================================
# HANDLERS
# ===========================================================

@dp.message(Command("start"))
async def start(msg: Message):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.username)

    ok, _ = await _check_gate(uid)
    if not ok:
        return await msg.answer(gate_text(), reply_markup=gate_kb())

    await home(msg, uid)

@dp.callback_query(F.data == "gate:check")
async def check_gate(cq: CallbackQuery):
    ok, _ = await _check_gate(cq.from_user.id)
    if ok:
        await home(cq, cq.from_user.id)
    else:
        await cq.message.edit_text(gate_text(), reply_markup=gate_kb())

@dp.callback_query(F.data == "menu:home")
async def cb_home(cq: CallbackQuery):
    await home(cq, cq.from_user.id)

# ===========================================================
# ACCOUNTS MENU
# ===========================================================

@dp.callback_query(F.data == "menu:acc")
async def cb_acc(cq: CallbackQuery):
    uid = cq.from_user.id
    rows = sessions_list(uid)

    if not rows:
        text = "👤 <b>No Accounts Added</b>\nUse @SpinifyLoginBot to add login sessions."
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="➕ Open Login Bot", url="https://t.me/SpinifyLoginBot")],
                [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")],
            ]
        )
        return await cq.message.edit_text(text, reply_markup=kb)

    line = "\n".join(f"✹ Slot {r['slot']} — API {r['api_id']}" for r in rows)
    kb = [
        [
            InlineKeyboardButton(text=f"🗑 Remove S{r['slot']}", callback_data=f"acc:del:{r['slot']}")
        ]
        for r in rows
    ]
    kb.append([InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")])

    try:
        await cq.message.edit_text(f"👤 <b>Your Accounts</b>\n\n{line}", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    except:
        pass

@dp.callback_query(F.data.startswith("acc:del:"))
async def remove_slot(cq: CallbackQuery):
    try:
        slot = int(cq.data.split(":")[-1])
        sessions_delete(cq.from_user.id, slot)
    except:
        pass
    await cb_acc(cq)

# ===========================================================
# COMMANDS PAGE
# ===========================================================

@dp.callback_query(F.data == "menu:cmds")
async def cmds(cq: CallbackQuery):
    txt = (
        "📜 <b>Self Commands</b>\n"
        "Send these from your <b>logged-in account</b>:\n\n"
        "✹ .help — show help\n"
        "✹ .status — view settings\n"
        "✹ .addgroup <link> — add target\n"
        "✹ .delgroup <link> — remove group\n"
        "✹ .groups — list all targets\n"
        "✹ .time 30 / 45 / 60 — set interval\n"
        "✹ .delay 5 — seconds per message\n"
        "✹ .night 23:00-07:00 — auto night (owner only)\n"
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]
    )

    await cq.message.edit_text(txt, reply_markup=kb)

# ===========================================================
# DISCLAIMER
# ===========================================================

@dp.callback_query(F.data == "menu:disc")
async def disclaimer(cq: CallbackQuery):
    txt = (
        "⚠️ <b>Disclaimer</b>\n"
        "Spinify Ads Bot automates message forwarding.\n"
        "We are not responsible for bans or misuse.\n"
        "Use at your own risk and follow Telegram TOS."
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]
    )
    await cq.message.edit_text(txt, reply_markup=kb)

# ===========================================================
# UNLOCK GC → 20 GROUPS
# ===========================================================

@dp.callback_query(F.data == "menu:unlock")
async def unlock_menu(cq: CallbackQuery):
    uid = cq.from_user.id
    cap = groups_cap(uid)

    kb = []
    if UNLOCK_GC_LINK:
        kb.append([InlineKeyboardButton(text="🔗 Join GC", url=UNLOCK_GC_LINK)])
    kb.append([InlineKeyboardButton(text="✅ I've Joined", callback_data="unlock:ok")])
    kb.append([InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")])

    await cq.message.edit_text(
        f"🔓 <b>Unlock GC</b>\nCurrent Target Limit: {cap}\nJoin GC to unlock up to 20 groups.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )

@dp.callback_query(F.data == "unlock:ok")
async def unlock_ok(cq: CallbackQuery):
    set_setting(f"groups_cap:{cq.from_user.id}", 20)
    await cq.message.edit_text(
        f"✅ Your group limit is now 20.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]
        ),
    )

# ===========================================================
# PUBLIC COMMANDS: /fstats
# ===========================================================

@dp.message(Command("fstats"))
async def fstats(msg: Message):
    uid = msg.from_user.id
    gs = len(list_groups(uid))
    ss = sessions_count_user(uid)
    eta = "—" if ss == 0 or gs == 0 else format_eta(uid)

    await msg.answer(
        f"📟 <b>Your Stats</b>\n"
        f"Sessions: {ss}\n"
        f"Groups: {gs}/{groups_cap(uid)}\n"
        f"Interval: {get_interval(uid)}m\n"
        f"Next Send: {eta}\n"
        f"Night: {'ON' if night_enabled() else 'OFF'}"
    )

# ===========================================================
# OWNER: NIGHT MODE
# ===========================================================

@dp.callback_query(F.data == "owner:night")
async def owner_night(cq: CallbackQuery):
    if not is_owner(cq.from_user.id):
        return
    set_night_enabled(not night_enabled())
    await home(cq, cq.from_user.id)

# ===========================================================
# OWNER: STATS / TOP / BROADCAST
# ===========================================================

@dp.callback_query(F.data == "owner:stats")
async def stats_owner(cq: CallbackQuery):
    if not is_owner(cq.from_user.id):
        return

    await cq.message.edit_text(
        f"📊 <b>Global Stats</b>\n"
        f"Users: {users_count()}\n"
        f"Active Sessions: {sessions_count()}\n"
        f"Total Sent: {get_total_sent_ok()}",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]
        )
    )

@dp.callback_query(F.data == "owner:top")
async def owner_top(cq: CallbackQuery):
    if not is_owner(cq.from_user.id):
        return

    rows = top_users(10)
    text = "🏆 <b>Top Users</b>\n" + "\n".join(
        f"{i+1}. {r['user_id']} — {r['sent_ok']}"
        for i, r in enumerate(rows)
    )
    await cq.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]
        )
    )

class Bcast(StatesGroup):
    text = State()

@dp.callback_query(F.data == "owner:bcast")
async def bcast_start(cq: CallbackQuery, state: FSMContext):
    if not is_owner(cq.from_user.id):
        return
    await state.set_state(Bcast.text)
    await cq.message.edit_text(
        "📣 Send broadcast message:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]
        )
    )

@dp.message(Bcast.text)
async def bcast_do(msg: Message, state: FSMContext):
    if not is_owner(msg.from_user.id):
        await state.clear()
        return

    uids = [r["user_id"] for r in get_conn().execute("SELECT user_id FROM users").fetchall()]
    sent = fail = 0

    for uid in uids:
        try:
            await bot.send_message(uid, msg.html_text or msg.text)
            sent += 1
        except:
            fail += 1
        await asyncio.sleep(0.05)

    await msg.answer(f"Done. Sent: {sent}, Failed: {fail}")
    await state.clear()

# ===========================================================
# ENTRYPOINT
# ===========================================================

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
