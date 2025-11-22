# main_bot.py — Spinify Ads Panel (Modern Blue UI – Option B)
# Works with: worker_forward.py + run_all.py + login_bot.py

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
    list_groups, groups_cap, add_group, remove_group,
    get_interval, get_last_sent_at,
    users_count, get_total_sent_ok, top_users,
    get_gate_channels_effective, set_setting, get_setting
)

load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("main-bot")

TOKEN = (os.getenv("MAIN_BOT_TOKEN") or "").strip()
if not TOKEN:
    raise RuntimeError("MAIN_BOT_TOKEN missing")

OWNER_ID = int(os.getenv("OWNER_ID", "0"))
UNLOCK_GC_LINK = os.getenv("UNLOCK_GC_LINK", "")
DEVELOPER_TAG = "@Spinify"

bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
init_db()


# ------------------------------------------------
# Helpers
# ------------------------------------------------

def is_owner(uid: int) -> bool:
    return uid == OWNER_ID


def _gate_channels():
    c1, c2 = get_gate_channels_effective()
    return [x for x in (c1, c2) if x]


async def _check_gate(uid: int):
    missing = []
    for ch in _gate_channels():
        try:
            st = await bot.get_chat_member(ch, uid)
            if str(st.status).lower() in {"left", "kicked"}:
                missing.append(ch)
        except Exception:
            missing.append(ch)
    return (not missing), missing


def _gate_text():
    return (
        "📘 <b>Welcome to Spinify Ads</b>\n"
        "Join all required channels to access the dashboard:\n\n"
        + "\n".join(f"• {c}" for c in _gate_channels()) +
        "\n\nTap <b>I've Joined</b> once completed."
    )


def _gate_kb():
    kb = [
        [InlineKeyboardButton(f"🔗 {c}", url=f"https://t.me/{c.lstrip('@')}")]
        for c in _gate_channels()
    ]
    kb.append([InlineKeyboardButton("✅ I've Joined", callback_data="gate:check")])
    return InlineKeyboardMarkup(inline_keyboard=kb)


def _format_eta(uid: int):
    last = get_last_sent_at(uid)
    interval = get_interval(uid)
    if not last:
        return f"in ~{interval}m"
    now = int(datetime.now(timezone.utc).timestamp())
    left = interval * 60 - (now - last)
    if left <= 0:
        return "very soon"
    m, s = divmod(left, 60)
    return f"in ~{m}m"


# ------------------------------------------------
# UI Layout (Modern Blue)
# ------------------------------------------------

def kb_main(uid: int):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton("👤 Accounts", callback_data="menu:acc"),
                InlineKeyboardButton("🧭 Commands", callback_data="menu:cmds"),
            ],
            [
                InlineKeyboardButton("🎯 Groups", callback_data="menu:groups"),
                InlineKeyboardButton("🔓 Unlock GC", callback_data="menu:unlock"),
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
    )


async def show_home(m, uid: int):
    gs = len(list_groups(uid))
    ss = sessions_count_user(uid)
    interval = get_interval(uid)
    eta = "—" if gs == 0 or ss == 0 else _format_eta(uid)

    text = (
        "📘 <b>Spinify Ads Dashboard</b>\n\n"
        "Use <b>@SpinifyLoginBot</b> to add your Telegram accounts.\n"
        "Send your ad text to <b>Saved Messages</b> to activate the worker.\n\n"
        f"👤 Sessions: <b>{ss}</b>\n"
        f"🎯 Groups: <b>{gs}/{groups_cap(uid)}</b>\n"
        f"⏱ Interval: <b>{interval}m</b>\n"
        f"📤 Next Send: <b>{eta}</b>\n\n"
        f"For Pro Ads automation, contact {DEVELOPER_TAG}"
    )

    if isinstance(m, Message):
        await m.answer(text, reply_markup=kb_main(uid))
    else:
        try:
            await m.message.edit_text(text, reply_markup=kb_main(uid))
        except TelegramBadRequest:
            pass


# ------------------------------------------------
# /start + gating
# ------------------------------------------------

@dp.message(Command("start"))
async def start(msg: Message):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.username)

    ok, missing = await _check_gate(uid)
    if not ok:
        return await msg.answer(_gate_text(), reply_markup=_gate_kb())

    # If previously unlocked but user left → reset to 5
    if groups_cap(uid) > 5:
        ok, _ = await _check_gate(uid)
        if not ok:
            set_setting(f"groups_cap:{uid}", 5)

    await show_home(msg, uid)


@dp.callback_query(F.data == "gate:check")
async def gate_check(cq: CallbackQuery):
    ok, _ = await _check_gate(cq.from_user.id)
    if not ok:
        return await cq.message.edit_text(_gate_text(), reply_markup=_gate_kb())
    await show_home(cq, cq.from_user.id)


# ------------------------------------------------
# Accounts Manager
# ------------------------------------------------

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
    slot = int(cq.data.split(":")[2])
    sessions_delete(cq.from_user.id, slot)
    await cb_acc(cq)


# ------------------------------------------------
# Groups Manager (Option B)
# ------------------------------------------------

@dp.callback_query(F.data == "menu:groups")
async def menu_groups(cq: CallbackQuery):
    uid = cq.from_user.id
    groups = list_groups(uid)

    if not groups:
        txt = (
            "🎯 <b>Your Groups</b>\n\n"
            "No groups added.\n"
            "Use <code>.addgc</code> to add groups."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]])
        return await cq.message.edit_text(txt, reply_markup=kb)

    txt = "🎯 <b>Your Target Groups</b>\nTap ❌ to remove.\n"
    kb_rows = []

    for g in groups:
        kb_rows.append([
            InlineKeyboardButton(f"{g}", callback_data="noop"),
            InlineKeyboardButton("❌", callback_data=f"gdel:{g}")
        ])

    kb_rows.append([InlineKeyboardButton("⬅ Back", callback_data="menu:home")])

    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    await cq.message.edit_text(txt, reply_markup=kb)


@dp.callback_query(F.data.startswith("gdel:"))
async def gdel(cq: CallbackQuery):
    uid = cq.from_user.id
    g = cq.data[5:]
    remove_group(uid, g)
    await menu_groups(cq)


# ------------------------------------------------
# Unlock GC → 20 slots (reset if user leaves)
# ------------------------------------------------

@dp.callback_query(F.data == "menu:unlock")
async def cb_unlock(cq: CallbackQuery):
    uid = cq.from_user.id
    cap = groups_cap(uid)

    txt = (
        "🔓 <b>Unlock Extra Slots</b>\n"
        "Join the GC below to unlock <b>20 group slots</b>.\n"
        f"Current limit: <b>{cap}</b>"
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton("🔗 Join Unlock GC", url=UNLOCK_GC_LINK)],
            [InlineKeyboardButton("I've Joined ✔️", callback_data="unlock:ok")],
            [InlineKeyboardButton("⬅ Back", callback_data="menu:home")],
        ]
    )

    await cq.message.edit_text(txt, reply_markup=kb)


@dp.callback_query(F.data == "unlock:ok")
async def unlock_ok(cq: CallbackQuery):
    uid = cq.from_user.id

    # Must pass gate to unlock
    ok, _ = await _check_gate(uid)
    if not ok:
        return await cq.message.answer("❌ Join required channels first.")

    set_setting(f"groups_cap:{uid}", 20)

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]])
    await cq.message.edit_text("✅ Unlocked! New limit: <b>20</b>", reply_markup=kb)


# ------------------------------------------------
# Commands Menu
# ------------------------------------------------

@dp.callback_query(F.data == "menu:cmds")
async def cb_cmds(cq: CallbackQuery):
    text = (
        "🧭 <b>Self Commands</b>\n\n"
        "• <code>.help</code>\n"
        "• <code>.status</code>\n"
        "• <code>.time 30|45|60</code>\n"
        "• <code>.gc</code>\n"
        "• <code>.addgc LINK</code>\n"
        "• <code>.cleargc</code>\n"
        "• <code>.adreset</code>\n"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]])
    await cq.message.edit_text(text, reply_markup=kb)


# ------------------------------------------------
# Stats
# ------------------------------------------------

@dp.callback_query(F.data == "menu:stats")
async def cb_stats(cq: CallbackQuery):
    total = users_count()
    sent = get_total_sent_ok()

    text = (
        "📊 <b>Global Stats</b>\n\n"
        f"• Registered Users: {total}\n"
        f"• Total Forwarded: {sent}\n"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]])
    await cq.message.edit_text(text, reply_markup=kb)


# ------------------------------------------------
# Top Users
# ------------------------------------------------

@dp.callback_query(F.data == "menu:top")
async def cb_top(cq: CallbackQuery):
    rows = top_users(10)

    if not rows:
        txt = "🏆 No ranking yet."
    else:
        txt = "🏆 <b>Top Users</b>\n" + "\n".join(
            f"{i+1}. <code>{r['user_id']}</code> — {r['sent_ok']}"
            for i, r in enumerate(rows)
        )

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]])
    await cq.message.edit_text(txt, reply_markup=kb)


# ------------------------------------------------
# Broadcast
# ------------------------------------------------

@dp.callback_query(F.data == "menu:bcast")
async def menu_bcast(cq: CallbackQuery):
    if not is_owner(cq.from_user.id):
        return
    await cq.message.edit_text(
        "📣 Send broadcast text now.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]]
        )
    )


# ------------------------------------------------
# Developer
# ------------------------------------------------

@dp.callback_query(F.data == "menu:dev")
async def menu_dev(cq: CallbackQuery):
    text = f"🛠 <b>Developer</b>\n\nContact: {DEVELOPER_TAG}"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]])
    await cq.message.edit_text(text, reply_markup=kb)


# ------------------------------------------------
# Disclaimer
# ------------------------------------------------

@dp.callback_query(F.data == "menu:disc")
async def menu_disc(cq: CallbackQuery):
    text = (
        "⚠ <b>Disclaimer</b>\n\n"
        "This tool automates forwarding via your own Telegram account.\n"
        "Use responsibly. We are not liable for any bans or risks."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]])
    await cq.message.edit_text(text, reply_markup=kb)


# ------------------------------------------------
# Entrypoint
# ------------------------------------------------

async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
