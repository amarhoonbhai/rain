# main_bot.py — Spinify Ads Panel (No Premium, Clean UI)

import os, asyncio, logging
from datetime import datetime, timezone
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv

from core.db import (
    init_db, ensure_user,
    sessions_list, sessions_delete, sessions_count_user,
    list_groups, groups_cap, get_interval, get_last_sent_at,
    users_count, get_total_sent_ok, top_users,
    get_gate_channels_effective, set_setting,
)

load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("main-bot")

TOKEN = (os.getenv("MAIN_BOT_TOKEN") or "").strip()
if not TOKEN or ":" not in TOKEN:
    raise RuntimeError("MAIN_BOT_TOKEN missing")

OWNER_ID = int(os.getenv("OWNER_ID", "0"))
UNLOCK_GC_LINK = os.getenv("UNLOCK_GC_LINK", "").strip()
DEVELOPER_TAG = "@spinify"

bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
init_db()


# -------------------------------
# Helpers
# -------------------------------
def is_owner(uid: int) -> bool:
    return OWNER_ID and uid == OWNER_ID


def _gate_channels():
    ch1, ch2 = get_gate_channels_effective()
    return [x for x in (ch1, ch2) if x]


async def _check_gate(uid: int):
    missing = []
    for ch in _gate_channels():
        try:
            st = await bot.get_chat_member(ch, uid)
            if str(getattr(st, "status", "left")).lower() in {"left", "kicked"}:
                missing.append(ch)
        except Exception:
            missing.append(ch)
    return (not missing), missing


def _gate_text():
    return (
        "📘 <b>Welcome to Spinify Ads</b>\n"
        "To access the dashboard, please join the required channels:\n\n"
        + "\n".join(f"• {c}" for c in _gate_channels()) +
        "\n\nTap <b>I've Joined</b> after subscribing."
    )


def _gate_kb():
    rows = [
        [InlineKeyboardButton(text=f"🔗 {c}", url=f"https://t.me/{c.lstrip('@')}")]
        for c in _gate_channels()
    ]
    rows.append([InlineKeyboardButton("✅ I've Joined", callback_data="gate:check")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _format_eta(uid: int):
    last = get_last_sent_at(uid)
    interval = get_interval(uid)
    if last is None:
        return f"in ~{interval}m"
    now = int(datetime.now(timezone.utc).timestamp())
    left = interval * 60 - (now - last)
    if left <= 0:
        return "very soon"
    m, s = divmod(left, 60)
    return f"in ~{m}m"


# -------------------------------
# UI Layout
# -------------------------------
def kb_main(uid: int):
    rows = [
        [
            InlineKeyboardButton("👤 Accounts", callback_data="menu:acc"),
            InlineKeyboardButton("🧭 Commands", callback_data="menu:cmds"),
        ],
        [
            InlineKeyboardButton("🎯 Groups", callback_data="menu:groups"),
            InlineKeyboardButton("🔓 Unlock", callback_data="menu:unlock"),
        ],
        [
            InlineKeyboardButton("📊 Stats", callback_data="menu:stats"),
            InlineKeyboardButton("🏆 Top", callback_data="menu:top"),
        ],
        [
            InlineKeyboardButton("📣 Broadcast", callback_data="menu:bcast"),
            InlineKeyboardButton("🛠 Developer", callback_data="menu:dev"),
        ],
        [
            InlineKeyboardButton("⚠ Disclaimer", callback_data="menu:disc"),
        ],
        [
            InlineKeyboardButton("🔄 Refresh", callback_data="menu:home"),
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def show_home(m, uid: int):
    gs = len(list_groups(uid))
    ss = sessions_count_user(uid)
    interval = get_interval(uid)
    eta = "—" if gs == 0 or ss == 0 else _format_eta(uid)

    text = (
        "📘 <b>Spinify Ads Dashboard</b>\n\n"
        "Use <b>@SpinifyLoginBot</b> to login your Telegram accounts.\n"
        "Then send your ad message from your logged-in account.\n\n"
        f"👤 Sessions: {ss}\n"
        f"🎯 Groups: {gs}/{groups_cap(uid)}\n"
        f"⏱ Interval: {interval}m\n"
        f"📤 Next Send: {eta}\n\n"
        f"Need a paid Ads bot with powerful features? Contact {DEVELOPER_TAG}"
    )

    if isinstance(m, Message):
        await m.answer(text, reply_markup=kb_main(uid))
    else:
        try:
            await m.message.edit_text(text, reply_markup=kb_main(uid))
        except TelegramBadRequest:
            pass


# -------------------------------
# Start Command
# -------------------------------
@dp.message(Command("start"))
async def start(msg: Message):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.username)

    ok, _ = await _check_gate(uid)
    if not ok:
        return await msg.answer(_gate_text(), reply_markup=_gate_kb())

    await show_home(msg, uid)


@dp.callback_query(F.data == "gate:check")
async def gate_check(cq: CallbackQuery):
    ok, _ = await _check_gate(cq.from_user.id)
    if not ok:
        return await cq.message.edit_text(_gate_text(), reply_markup=_gate_kb())
    await show_home(cq, cq.from_user.id)


# -------------------------------
# Accounts Manager
# -------------------------------
@dp.callback_query(F.data == "menu:acc")
async def cb_acc(cq: CallbackQuery):
    uid = cq.from_user.id
    rows = sessions_list(uid)
    if not rows:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton("➕ Open Login Bot", url="https://t.me/SpinifyLoginBot")],
                [InlineKeyboardButton("⬅ Back", callback_data="menu:home")],
            ]
        )
        return await cq.message.edit_text("👤 <b>No active sessions.</b>", reply_markup=kb)

    text = "👤 <b>Your Accounts</b>\n\n" + "\n".join(
        f"• Slot {r['slot']} — API_ID {r['api_id']}"
        for r in rows
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(f"🗑 Remove Slot {r['slot']}", callback_data=f"acc:del:{r['slot']}")]
            for r in rows
        ] + [[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]]
    )
    await cq.message.edit_text(text, reply_markup=kb)


@dp.callback_query(F.data.startswith("acc:del:"))
async def acc_del(cq: CallbackQuery):
    slot = int(cq.data.split(":")[-1])
    sessions_delete(cq.from_user.id, slot)
    await cb_acc(cq)


# -------------------------------
# Groups Manager
# -------------------------------
@dp.callback_query(F.data == "menu:groups")
async def menu_groups(cq: CallbackQuery):
    uid = cq.from_user.id
    gs = list_groups(uid)
    cap = groups_cap(uid)

    text = (
        "🎯 <b>Your Target Groups</b>\n"
        f"Count: {len(gs)}/{cap}\n\n"
        "Add groups using <code>.addgroup link</code>\n"
        "Remove groups using <code>.delgroup link</code>"
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]]
    )
    await cq.message.edit_text(text, reply_markup=kb)


# -------------------------------
# Unlock GC
# -------------------------------
@dp.callback_query(F.data == "menu:unlock")
async def cb_unlock(cq: CallbackQuery):
    uid = cq.from_user.id
    cap = groups_cap(uid)

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton("🔗 Join Unlock GC", url=UNLOCK_GC_LINK)],
            [InlineKeyboardButton("I've Joined ✔️", callback_data="unlock:ok")],
            [InlineKeyboardButton("⬅ Back", callback_data="menu:home")],
        ]
    )

    await cq.message.edit_text(
        f"🔓 <b>Unlock Extra Group Slots</b>\n"
        "Join the GC above to unlock up to 20 groups.\n"
        f"Current limit: {cap}",
        reply_markup=kb,
    )


@dp.callback_query(F.data == "unlock:ok")
async def unlock_ok(cq: CallbackQuery):
    uid = cq.from_user.id
    set_setting(f"groups_cap:{uid}", 20)
    await cq.message.edit_text(
        "✅ Unlocked successfully!\nYour group limit is now <b>20</b>.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]]
        ),
    )


# -------------------------------
# Commands panel
# -------------------------------
@dp.callback_query(F.data == "menu:cmds")
async def cb_cmds(cq: CallbackQuery):
    text = (
        "🧭 <b>Self Commands</b>\n\n"
        "• <code>.help</code>\n"
        "• <code>.addgroup LINK</code>\n"
        "• <code>.delgroup LINK</code>\n"
        "• <code>.groups</code>\n"
        "• <code>.time 30|45|60</code>\n"
        "• <code>.delay N</code>\n"
        "• <code>.night 23:00-07:00</code>\n"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]]
    )
    await cq.message.edit_text(text, reply_markup=kb)


# -------------------------------
# Stats
# -------------------------------
@dp.callback_query(F.data == "menu:stats")
async def cb_stats(cq: CallbackQuery):
    total = users_count()
    active = sessions_count_user(cq.from_user.id)
    sent = get_total_sent_ok()

    txt = (
        "📊 <b>Global Stats</b>\n\n"
        f"Users: {total}\n"
        f"Total Sent: {sent}\n"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]])
    await cq.message.edit_text(txt, reply_markup=kb)


# -------------------------------
# Top List
# -------------------------------
@dp.callback_query(F.data == "menu:top")
async def cb_top(cq: CallbackQuery):
    rows = top_users(10)
    if not rows:
        txt = "🏆 No data available."
    else:
        txt = "🏆 <b>Top Users</b>\n" + "\n".join(
            f"{i+1}. <code>{r['user_id']}</code> — {r['sent_ok']}"
            for i, r in enumerate(rows)
        )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]])
    await cq.message.edit_text(txt, reply_markup=kb)


# -------------------------------
# Broadcast (Owner)
# -------------------------------
@dp.callback_query(F.data == "menu:bcast")
async def menu_bcast(cq: CallbackQuery):
    if not is_owner(cq.from_user.id):
        return
    await cq.message.edit_text(
        "📣 Send broadcast text.\n(Owner only)",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]]
        ),
    )


# -------------------------------
# Developer Panel
# -------------------------------
@dp.callback_query(F.data == "menu:dev")
async def menu_dev(cq: CallbackQuery):
    text = f"🛠 <b>Developer</b>\n\nTelegram: {DEVELOPER_TAG}"
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]]
    )
    await cq.message.edit_text(text, reply_markup=kb)


# -------------------------------
# Disclaimer
# -------------------------------
@dp.callback_query(F.data == "menu:disc")
async def menu_disc(cq: CallbackQuery):
    text = (
        "⚠ <b>Disclaimer</b>\n\n"
        "This tool automates message forwarding using your own Telegram account.\n"
        "Use responsibly. We are not responsible for bans or misuse."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]])
    await cq.message.edit_text(text, reply_markup=kb)


# -------------------------------
# Entrypoint
# -------------------------------
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
