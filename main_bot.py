# main_bot.py — Spinify Ads Panel (B2 Stable Clean UI)

import os
import asyncio
import logging
from datetime import datetime, timezone

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.exceptions import TelegramBadRequest

from dotenv import load_dotenv

from core.db import (
    init_db,
    ensure_user,
    sessions_list,
    sessions_count_user,
    sessions_delete,
    list_groups,
    groups_cap,
    get_interval,
    get_last_sent_at,
    users_count,
    get_total_sent_ok,
    top_users,
    set_setting,
    get_setting,
    get_gate_channels_effective
)

# ---------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------
load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("main-bot")

TOKEN = (os.getenv("MAIN_BOT_TOKEN") or "").strip()
if not TOKEN:
    raise RuntimeError("MAIN_BOT_TOKEN missing")

OWNER_ID = int(os.getenv("OWNER_ID", 0))
UNLOCK_GC_LINK = os.getenv("UNLOCK_GC_LINK", "")
DEVELOPER = "@Spinify"

bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
init_db()


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
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
    return f"in {m}m"


# ---------------------------------------------------------
# UI
# ---------------------------------------------------------
def kb_main(uid: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton("👤 Accounts", callback_data="menu:acc"),
            InlineKeyboardButton("🎯 Groups", callback_data="menu:groups"),
        ],
        [
            InlineKeyboardButton("🧭 Commands", callback_data="menu:cmds"),
            InlineKeyboardButton("📊 Stats", callback_data="menu:stats"),
        ],
        [
            InlineKeyboardButton("🏆 Top Users", callback_data="menu:top"),
            InlineKeyboardButton("🔓 Unlock GC", callback_data="menu:unlock"),
        ],
        [
            InlineKeyboardButton("⚠ Disclaimer", callback_data="menu:disc"),
        ],
        [
            InlineKeyboardButton("🔄 Refresh", callback_data="menu:home"),
        ]
    ])


async def show_home(m, uid: int):
    gs = len(list_groups(uid))
    ss = sessions_count_user(uid)
    interval = get_interval(uid)
    eta = _format_eta(uid) if ss and gs else "—"

    text = (
        "📘 <b>Spinify Ads Dashboard</b>\n\n"
        "• Add accounts from <b>@SpinifyLoginBot</b>\n"
        "• Put ads in <b>Saved Messages</b>\n"
        "• Worker forwards automatically\n\n"
        f"👤 Sessions: <b>{ss}</b>\n"
        f"🎯 Groups: <b>{gs}/{groups_cap(uid)}</b>\n"
        f"⏱ Interval: <b>{interval}m</b>\n"
        f"📤 Next Send: <b>{eta}</b>\n\n"
        f"For premium tools → {DEVELOPER}"
    )

    if isinstance(m, Message):
        await m.answer(text, reply_markup=kb_main(uid))
    else:
        try:
            await m.message.edit_text(text, reply_markup=kb_main(uid))
        except TelegramBadRequest:
            pass


# ---------------------------------------------------------
# START
# ---------------------------------------------------------
@dp.message(Command("start"))
async def start_cmd(msg: Message):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.username)

    ok, missing = await _check_gate(uid)
    if not ok:
        kb = [
            [InlineKeyboardButton(f"🔗 {c}", url=f"https://t.me/{c.lstrip('@')}")]
            for c in missing
        ]
        kb.append([InlineKeyboardButton("I've Joined ✔️", callback_data="gate:check")])
        return await msg.answer(
            "📘 Join required channels first.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
        )

    await show_home(msg, uid)


@dp.callback_query(F.data == "gate:check")
async def gate_check(cq: CallbackQuery):
    ok, _ = await _check_gate(cq.from_user.id)
    if not ok:
        return await start_cmd(cq.message)
    await show_home(cq, cq.from_user.id)


# ---------------------------------------------------------
# ACCOUNTS
# ---------------------------------------------------------
@dp.callback_query(F.data == "menu:acc")
async def menu_acc(cq: CallbackQuery):
    uid = cq.from_user.id
    rows = sessions_list(uid)

    if not rows:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton("➕ Open Login Bot", url="https://t.me/SpinifyLoginBot")],
            [InlineKeyboardButton("⬅ Back", callback_data="menu:home")]
        ])
        return await cq.message.edit_text("👤 No accounts saved.", reply_markup=kb)

    text = "👤 <b>Your Accounts</b>\n\n" + "\n".join(
        f"• Slot {r['slot']} — API_ID {r['api_id']}"
        for r in rows
    )

    kb = []
    for r in rows:
        kb.append([InlineKeyboardButton(
            f"🗑 Remove Slot {r['slot']}",
            callback_data=f"acc:del:{r['slot']}"
        )])

    kb.append([InlineKeyboardButton("⬅ Back", callback_data="menu:home")])

    await cq.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@dp.callback_query(F.data.startswith("acc:del:"))
async def acc_del(cq: CallbackQuery):
    slot = int(cq.data.split(":")[2])
    sessions_delete(cq.from_user.id, slot)
    await menu_acc(cq)


# ---------------------------------------------------------
# GROUPS
# ---------------------------------------------------------
@dp.callback_query(F.data == "menu:groups")
async def menu_groups(cq: CallbackQuery):
    uid = cq.from_user.id
    groups = list_groups(uid)

    if not groups:
        txt = (
            "🎯 <b>Your Groups</b>\n\n"
            "No groups added.\n"
            "Add via <code>.addgc @group</code> from your logged-in account."
        )
        kb = [[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]]
        return await cq.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

    txt = "🎯 <b>Groups</b>\nTap ❌ to remove."
    kb = []

    for g in groups:
        kb.append([
            InlineKeyboardButton(g, callback_data="noop"),
            InlineKeyboardButton("❌", callback_data=f"gdel:{g}")
        ])

    kb.append([InlineKeyboardButton("⬅ Back", callback_data="menu:home")])

    await cq.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@dp.callback_query(F.data.startswith("gdel:"))
async def gdel(cq: CallbackQuery):
    uid = cq.from_user.id
    g = cq.data[5:]
    arr = list_groups(uid)
    if g in arr:
        arr.remove(g)
        set_setting(f"groups:{uid}", arr)
    await menu_groups(cq)


# ---------------------------------------------------------
# COMMANDS MENU
# ---------------------------------------------------------
@dp.callback_query(F.data == "menu:cmds")
async def menu_cmds(cq: CallbackQuery):
    text = (
        "🧭 <b>Self Commands</b>\n\n"
        "• .help\n"
        "• .status\n"
        "• .time 30|45|60\n"
        "• .gc\n"
        "• .addgc LINK\n"
        "• .cleargc\n"
        "• .adreset\n"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]])
    await cq.message.edit_text(text, reply_markup=kb)


# ---------------------------------------------------------
# STATS
# ---------------------------------------------------------
@dp.callback_query(F.data == "menu:stats")
async def menu_stats(cq: CallbackQuery):
    text = (
        "📊 <b>Global Stats</b>\n\n"
        f"Users: {users_count()}\n"
        f"Total Forwarded: {get_total_sent_ok()}\n"
    )
    await cq.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]]
        )
    )


# ---------------------------------------------------------
# TOP USERS
# ---------------------------------------------------------
@dp.callback_query(F.data == "menu:top")
async def menu_top(cq: CallbackQuery):
    rows = top_users(10)

    if not rows:
        txt = "🏆 No ranking yet."
    else:
        txt = "🏆 <b>Top Users</b>\n" + "\n".join(
            f"{i+1}. <code>{r['user_id']}</code> — {r['sent_ok']}"
            for i, r in enumerate(rows)
        )

    await cq.message.edit_text(
        txt,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]]
        )
    )


# ---------------------------------------------------------
# UNLOCK GROUP CAP
# ---------------------------------------------------------
@dp.callback_query(F.data == "menu:unlock")
async def menu_unlock(cq: CallbackQuery):
    uid = cq.from_user.id
    cap = groups_cap(uid)

    text = (
        f"🔓 <b>Unlock Extra Slots</b>\n\n"
        f"Current: <b>{cap}</b>\n"
        f"Join GC to unlock <b>20 slots</b>."
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton("🔗 Join", url=UNLOCK_GC_LINK)],
        [InlineKeyboardButton("I've Joined ✔️", callback_data="unlock:ok")],
        [InlineKeyboardButton("⬅ Back", callback_data="menu:home")]
    ])

    await cq.message.edit_text(text, reply_markup=kb)


@dp.callback_query(F.data == "unlock:ok")
async def unlock_ok(cq: CallbackQuery):
    uid = cq.from_user.id
    set_setting(f"groups_cap:{uid}", 20)

    await cq.message.edit_text(
        "✅ Unlocked 20 group slots!",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]]
        )
    )


# ---------------------------------------------------------
# DISCLAIMER
# ---------------------------------------------------------
@dp.callback_query(F.data == "menu:disc")
async def menu_disc(cq: CallbackQuery):
    text = (
        "⚠ <b>Disclaimer</b>\n\n"
        "This bot automates forwarding using your own Telegram account.\n"
        "Use responsibly. We are not liable for bans."
    )
    await cq.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]]
        )
    )


# ---------------------------------------------------------
# MAIN LOOP
# ---------------------------------------------------------
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
