# main_bot.py — Spinify Ads Dashboard (Stable B2 Version)

import os
import asyncio
import logging
from datetime import datetime, timezone
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramBadRequest
from dotenv import load_dotenv

from core.db import (
    init_db, ensure_user,
    sessions_list, sessions_delete, sessions_count_user,
    list_groups, groups_cap,
    get_interval, get_last_sent_at,
    users_count, get_total_sent_ok, top_users,
    get_gate_channels_effective, set_setting
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
        except:
            missing.append(ch)
    return (not missing), missing


def _gate_text():
    return (
        "📘 <b>Welcome to Spinify Ads</b>\n"
        "Join all required channels to continue:\n\n" +
        "\n".join(f"• {c}" for c in _gate_channels()) +
        "\n\nTap <b>I've Joined</b> once done."
    )


def _gate_kb():
    kb = [
        [InlineKeyboardButton(f"🔗 {c}", url=f"https://t.me/{c.lstrip('@')}")]
        for c in _gate_channels()
    ]
    kb.append([InlineKeyboardButton("✅ I've Joined", callback_data="gate_ok")])
    return InlineKeyboardMarkup(inline_keyboard=kb)


def _eta(uid: int):
    last = get_last_sent_at(uid)
    interval = get_interval(uid)
    if not last:
        return f"in ~{interval}m"

    now = int(datetime.now(timezone.utc).timestamp())
    left = interval * 60 - (now - last)

    if left <= 0:
        return "very soon"

    m, _ = divmod(left, 60)
    return f"in ~{m}m"


# ------------------------------------------------
# UI
# ------------------------------------------------
def kb_main(uid: int):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton("👤 Accounts", callback_data="menu_acc"),
             InlineKeyboardButton("🎯 Groups", callback_data="menu_groups")],
            [InlineKeyboardButton("🧭 Commands", callback_data="menu_cmds"),
             InlineKeyboardButton("📊 Stats", callback_data="menu_stats")],
            [InlineKeyboardButton("🏆 Top", callback_data="menu_top"),
             InlineKeyboardButton("🔓 Unlock GC", callback_data="menu_unlock")],
            [InlineKeyboardButton("📣 Broadcast", callback_data="menu_bcast")],
            [InlineKeyboardButton("🔄 Refresh", callback_data="menu_home")]
        ]
    )


async def show_home(m, uid: int):
    gs = len(list_groups(uid))
    ss = sessions_count_user(uid)
    interval = get_interval(uid)
    eta = "—" if gs == 0 or ss == 0 else _eta(uid)

    txt = (
        "📘 <b>Spinify Ads Dashboard</b>\n\n"
        "Use @SpinifyLoginBot to add your sessions.\n"
        "Forward ads from Saved Messages.\n\n"
        f"👤 Sessions: <b>{ss}</b>\n"
        f"🎯 Groups: <b>{gs}/{groups_cap(uid)}</b>\n"
        f"⏱ Interval: <b>{interval}m</b>\n"
        f"📤 Next Send: <b>{eta}</b>\n\n"
        f"Developer: {DEVELOPER_TAG}"
    )

    if isinstance(m, Message):
        await m.answer(txt, reply_markup=kb_main(uid))
    else:
        try:
            await m.message.edit_text(txt, reply_markup=kb_main(uid))
        except TelegramBadRequest:
            pass


# ------------------------------------------------
# /start
# ------------------------------------------------
@dp.message(Command("start"))
async def start_cmd(msg: Message):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.username)

    ok, _ = await _check_gate(uid)
    if not ok:
        return await msg.answer(_gate_text(), reply_markup=_gate_kb())

    await show_home(msg, uid)


@dp.callback_query(F.data == "gate_ok")
async def gate_ok(cq: CallbackQuery):
    ok, _ = await _check_gate(cq.from_user.id)
    if not ok:
        return await cq.message.edit_text(_gate_text(), reply_markup=_gate_kb())

    await show_home(cq, cq.from_user.id)


# ------------------------------------------------
# Accounts
# ------------------------------------------------
@dp.callback_query(F.data == "menu_acc")
async def menu_acc(cq: CallbackQuery):
    uid = cq.from_user.id
    rows = sessions_list(uid)

    if not rows:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton("➕ Login Bot", url="https://t.me/SpinifyLoginBot")],
                [InlineKeyboardButton("⬅ Back", callback_data="menu_home")]
            ]
        )
        return await cq.message.edit_text("👤 <b>No active sessions</b>", reply_markup=kb)

    txt = "👤 <b>Your Accounts</b>\n\n" + "\n".join(
        f"• Slot {r['slot']} — API_ID {r['api_id']}"
        for r in rows
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(f"🗑 Remove Slot {r['slot']}",
                                  callback_data=f"acc_del:{r['slot']}")] for r in rows
        ] + [[InlineKeyboardButton("⬅ Back", callback_data="menu_home")]]
    )

    await cq.message.edit_text(txt, reply_markup=kb)


@dp.callback_query(F.data.startswith("acc_del:"))
async def acc_del(cq: CallbackQuery):
    slot = int(cq.data.split(":")[1])
    sessions_delete(cq.from_user.id, slot)
    await menu_acc(cq)


# ------------------------------------------------
# Groups
# ------------------------------------------------
@dp.callback_query(F.data == "menu_groups")
async def menu_groups(cq: CallbackQuery):
    uid = cq.from_user.id
    groups = list_groups(uid)

    if not groups:
        return await cq.message.edit_text(
            "🎯 No groups added.\nUse <code>.addgc</code>",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu_home")]]
            )
        )

    rows = []
    for g in groups:
        rows.append([
            InlineKeyboardButton(g, callback_data="noop"),
            InlineKeyboardButton("❌", callback_data=f"g_del:{g}")
        ])

    rows.append([InlineKeyboardButton("⬅ Back", callback_data="menu_home")])

    await cq.message.edit_text("🎯 <b>Your Groups</b>\nTap ❌ to remove.",
                               reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))


@dp.callback_query(F.data.startswith("g_del:"))
async def g_del(cq: CallbackQuery):
    # simply clear & re-add except removed
    uid = cq.from_user.id
    g = cq.data[6:]

    all_g = list_groups(uid)
    new_g = [x for x in all_g if x != g]

    set_setting(f"groups_cap:{uid}", groups_cap(uid))  # keep cap
    set_setting(f"del_tmp:{uid}", 1)

    set_setting(f"groups_tmp:{uid}", new_g)
    from core.db import db
    db().groups.update_one(
        {"user_id": uid},
        {"$set": {"targets": new_g}},
        upsert=True
    )

    await menu_groups(cq)


# ------------------------------------------------
# Commands Menu
# ------------------------------------------------
@dp.callback_query(F.data == "menu_cmds")
async def menu_cmds(cq: CallbackQuery):
    txt = (
        "🧭 <b>Self Commands</b>\n\n"
        "• .help\n"
        "• .status\n"
        "• .time 30|45|60\n"
        "• .gc\n"
        "• .addgc @group\n"
        "• .cleargc\n"
        "• .adreset\n"
    )
    await cq.message.edit_text(txt,
                               reply_markup=InlineKeyboardMarkup(
                                   inline_keyboard=[[InlineKeyboardButton("⬅ Back",
                                                                          callback_data="menu_home")]]
                               ))


# ------------------------------------------------
# Stats
# ------------------------------------------------
@dp.callback_query(F.data == "menu_stats")
async def menu_stats(cq: CallbackQuery):
    txt = (
        "📊 <b>Global Stats</b>\n\n"
        f"• Users: {users_count()}\n"
        f"• Sent: {get_total_sent_ok()}"
    )
    await cq.message.edit_text(txt,
                               reply_markup=InlineKeyboardMarkup(
                                   inline_keyboard=[[InlineKeyboardButton("⬅ Back",
                                                                          callback_data="menu_home")]]
                               ))


# ------------------------------------------------
# Top users
# ------------------------------------------------
@dp.callback_query(F.data == "menu_top")
async def menu_top(cq: CallbackQuery):
    rows = top_users(10)
    if not rows:
        txt = "🏆 No ranking yet."

    else:
        txt = "🏆 <b>Top Users</b>\n\n" + "\n".join(
            f"{i+1}. <code>{r['user_id']}</code> — {r['sent_ok']}"
            for i, r in enumerate(rows)
        )

    await cq.message.edit_text(txt,
                               reply_markup=InlineKeyboardMarkup(
                                   inline_keyboard=[[InlineKeyboardButton("⬅ Back",
                                                                          callback_data="menu_home")]]
                               ))


# ------------------------------------------------
# Unlock GC
# ------------------------------------------------
@dp.callback_query(F.data == "menu_unlock")
async def menu_unlock(cq: CallbackQuery):
    uid = cq.from_user.id
    cap = groups_cap(uid)

    txt = (
        "🔓 <b>Unlock Group Limit</b>\n"
        "Join the GC below to unlock <b>20 slots</b>.\n\n"
        f"Current: <b>{cap}</b>"
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton("🔗 Join Unlock GC", url=UNLOCK_GC_LINK)],
            [InlineKeyboardButton("I Joined ✔️", callback_data="unlock_ok")],
            [InlineKeyboardButton("⬅ Back", callback_data="menu_home")]
        ]
    )

    await cq.message.edit_text(txt, reply_markup=kb)


@dp.callback_query(F.data == "unlock_ok")
async def unlock_ok(cq: CallbackQuery):
    uid = cq.from_user.id

    ok, _ = await _check_gate(uid)
    if not ok:
        return await cq.answer("❌ First join required channels", show_alert=True)

    set_setting(f"groups_cap:{uid}", 20)

    await cq.message.edit_text(
        "✅ Unlocked! New limit: <b>20</b>",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu_home")]]
        )
    )


# ------------------------------------------------
# Broadcast
# ------------------------------------------------
@dp.callback_query(F.data == "menu_bcast")
async def menu_bcast(cq: CallbackQuery):
    if not is_owner(cq.from_user.id):
        return

    await cq.message.edit_text(
        "📣 Send broadcast text now.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu_home")]]
        )
    )


# ------------------------------------------------
# Back → Home
# ------------------------------------------------
@dp.callback_query(F.data == "menu_home")
async def menu_home(cq: CallbackQuery):
    await show_home(cq, cq.from_user.id)


# ------------------------------------------------
# Entrypoint
# ------------------------------------------------
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
