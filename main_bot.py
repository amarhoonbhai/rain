# main_bot.py — A1 Compact UI + All Fixes
import os, asyncio, logging
from datetime import datetime, timezone

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)

from dotenv import load_dotenv

from core.db import (
    init_db, ensure_user,
    sessions_list, sessions_delete,
    list_groups, groups_cap, add_group, clear_groups,
    get_interval, users_count, get_total_sent_ok, top_users,
    get_gate_channels_effective, get_setting, set_setting,
    get_last_sent_at
)

# ---------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------
load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("main-bot")

TOKEN = os.getenv("MAIN_BOT_TOKEN", "").strip()
if not TOKEN:
    raise RuntimeError("MAIN_BOT_TOKEN missing")

OWNER_ID = int(os.getenv("OWNER_ID", "0"))
UNLOCK_GC_LINK = os.getenv("UNLOCK_GC_LINK", "")
DEVELOPER_TAG = "@Spinify"

bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
init_db()


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def _gate_channels():
    c1, c2 = get_gate_channels_effective()
    return [x for x in (c1, c2) if x]


async def _check_gate(uid):
    missing = []
    for ch in _gate_channels():
        try:
            st = await bot.get_chat_member(ch, uid)
            if str(st.status).lower() in {"left", "kicked"}:
                missing.append(ch)
        except:
            missing.append(ch)
    return (not missing), missing


def _kb_main(uid):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton("👤 Accounts", callback_data="menu:acc"),
                InlineKeyboardButton("🎯 Groups", callback_data="menu:groups"),
            ],
            [
                InlineKeyboardButton("📊 Stats", callback_data="menu:stats"),
                InlineKeyboardButton("🏆 Top", callback_data="menu:top"),
            ],
            [
                InlineKeyboardButton("📣 Broadcast", callback_data="menu:bcast"),
                InlineKeyboardButton("🧭 Commands", callback_data="menu:cmds"),
            ],
            [
                InlineKeyboardButton("🔒 Unlock GC", callback_data="menu:unlock"),
                InlineKeyboardButton("⚙ Developer", callback_data="menu:dev"),
            ],
            [InlineKeyboardButton("🔄 Refresh", callback_data="menu:home")],
        ]
    )


def _format_eta(uid):
    last = get_last_sent_at(uid)
    interval = get_interval(uid)
    if not last:
        return f"in ~{interval}m"
    now = int(datetime.now(timezone.utc).timestamp())
    left = interval * 60 - (now - last)
    if left <= 0:
        return "now"
    m, s = divmod(left, 60)
    return f"in ~{m}m"


# ---------------------------------------------------------
# Home Screen
# ---------------------------------------------------------
async def show_home(m, uid: int):
    gs = len(list_groups(uid))
    ss = len(sessions_list(uid))
    interval = get_interval(uid)
    eta = "—" if gs == 0 or ss == 0 else _format_eta(uid)

    text = (
        "📘 <b>Spinify Ads Dashboard</b>\n\n"
        "Add account using <b>@SpinifyLoginBot</b>.\n"
        "Send your ad in <b>Saved Messages</b>.\n\n"
        f"👤 Sessions: <b>{ss}</b>\n"
        f"🎯 Groups: <b>{gs}/{groups_cap(uid)}</b>\n"
        f"⏱ Interval: <b>{interval}m</b>\n"
        f"📤 Next Send: <b>{eta}</b>\n"
    )

    if isinstance(m, Message):
        await m.answer(text, reply_markup=_kb_main(uid))
    else:
        try:
            await m.message.edit_text(text, reply_markup=_kb_main(uid))
        except TelegramBadRequest:
            pass


# ---------------------------------------------------------
# Start + Gate
# ---------------------------------------------------------
@dp.message(Command("start"))
async def start_cmd(msg: Message):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.username)

    ok, _ = await _check_gate(uid)
    if not ok:
        return await msg.answer(
            "📘 <b>Join the required channels first</b>\n" +
            "\n".join(f"• {c}" for c in _gate_channels()) +
            "\n\nTap <b>Joined</b> after doing it.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(f"🔗 {c}", url=f"https://t.me/{c.lstrip('@')}")] for c in _gate_channels()
                ] + [
                    [InlineKeyboardButton("✔ Joined", callback_data="gate:chk")]
                ]
            )
        )

    await show_home(msg, uid)


@dp.callback_query(F.data == "gate:chk")
async def gate_chk(cq: CallbackQuery):
    ok, _ = await _check_gate(cq.from_user.id)
    if not ok:
        return await cq.answer("Join all channels first", show_alert=True)
    await show_home(cq, cq.from_user.id)


# ---------------------------------------------------------
# Accounts Manager
# ---------------------------------------------------------
@dp.callback_query(F.data == "menu:acc")
async def menu_acc(cq: CallbackQuery):
    uid = cq.from_user.id
    rows = sessions_list(uid)

    if not rows:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton("➕ Open Login Bot", url="https://t.me/SpinifyLoginBot")],
                [InlineKeyboardButton("⬅ Back", callback_data="menu:home")],
            ]
        )
        return await cq.message.edit_text("👤 <b>No accounts added.</b>", reply_markup=kb)

    text = "👤 <b>Your Accounts</b>\n\n" + "\n".join(
        f"• Slot {r['slot']} — API {r['api_id']}" for r in rows
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(f"🗑 Delete Slot {r['slot']}", callback_data=f"acc:del:{r['slot']}")] for r in rows
        ] + [[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]]
    )

    await cq.message.edit_text(text, reply_markup=kb)


@dp.callback_query(F.data.startswith("acc:del:"))
async def acc_del(cq: CallbackQuery):
    slot = int(cq.data.split(":")[2])
    sessions_delete(cq.from_user.id, slot)
    await menu_acc(cq)


# ---------------------------------------------------------
# Groups Manager
# ---------------------------------------------------------
@dp.callback_query(F.data == "menu:groups")
async def menu_groups(cq: CallbackQuery):
    uid = cq.from_user.id
    groups = list_groups(uid)

    if not groups:
        return await cq.message.edit_text(
            "🎯 <b>No groups</b>\nAdd via `.addgc @link`",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]]
            )
        )

    kb = []
    for g in groups:
        kb.append([
            InlineKeyboardButton(g, callback_data="noop"),
            InlineKeyboardButton("❌", callback_data=f"gdel:{g}")
        ])
    kb.append([InlineKeyboardButton("⬅ Back", callback_data="menu:home")])

    await cq.message.edit_text("🎯 <b>Your Groups</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@dp.callback_query(F.data.startswith("gdel:"))
async def gdel(cq: CallbackQuery):
    clear_groups(cq.from_user.id)
    await menu_groups(cq)


# ---------------------------------------------------------
# Unlock Groups (20 slots)
# ---------------------------------------------------------
@dp.callback_query(F.data == "menu:unlock")
async def unlock_menu(cq: CallbackQuery):
    uid = cq.from_user.id
    cap = groups_cap(uid)

    await cq.message.edit_text(
        f"🔓 <b>Unlock 20 Groups</b>\nCurrent limit: <b>{cap}</b>",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton("🔗 Unlock GC", url=UNLOCK_GC_LINK)],
                [InlineKeyboardButton("✔ Joined", callback_data="unlock:chk")],
                [InlineKeyboardButton("⬅ Back", callback_data="menu:home")]
            ]
        )
    )


@dp.callback_query(F.data == "unlock:chk")
async def unlock_chk(cq: CallbackQuery):
    set_setting(f"groups_cap:{cq.from_user.id}", 20)
    await cq.message.edit_text("✅ Unlocked to <b>20</b> groups.",
                               reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                   [InlineKeyboardButton("⬅ Back", callback_data="menu:home")]
                               ]))


# ---------------------------------------------------------
# Commands Menu
# ---------------------------------------------------------
@dp.callback_query(F.data == "menu:cmds")
async def menu_cmds(cq: CallbackQuery):
    await cq.message.edit_text(
        "🧭 <b>Self Commands</b>\n"
        "• .help\n"
        "• .status\n"
        "• .gc\n"
        "• .addgc @group\n"
        "• .cleargc\n"
        "• .time 30|45|60\n"
        "• .adreset\n",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton("⬅ Back", callback_data="menu:home")]]
        )
    )


# ---------------------------------------------------------
# Stats
# ---------------------------------------------------------
@dp.callback_query(F.data == "menu:stats")
async def menu_stats(cq: CallbackQuery):
    txt = (
        "📊 <b>Statistics</b>\n\n"
        f"Users: {users_count()}\n"
        f"Total Sent: {get_total_sent_ok()}\n"
    )
    await cq.message.edit_text(txt,
                               reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                   [InlineKeyboardButton("⬅ Back", callback_data="menu:home")]
                               ]))


# ---------------------------------------------------------
# Leaderboard
# ---------------------------------------------------------
@dp.callback_query(F.data == "menu:top")
async def menu_top(cq: CallbackQuery):
    rows = top_users(10)

    if not rows:
        t = "🏆 No ranking yet."
    else:
        t = "🏆 <b>Top Users</b>\n" + "\n".join(
            f"{i+1}. {r['user_id']} — {r['sent_ok']}" for i, r in enumerate(rows)
        )

    await cq.message.edit_text(t,
                               reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                   [InlineKeyboardButton("⬅ Back", callback_data="menu:home")]
                               ]))


# ---------------------------------------------------------
# Broadcast
# ---------------------------------------------------------
@dp.callback_query(F.data == "menu:bcast")
async def menu_bcast(cq: CallbackQuery):
    if cq.from_user.id != OWNER_ID:
        return await cq.answer("Only owner", show_alert=True)

    await cq.message.edit_text(
        "📣 Send broadcast text.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton("⬅ Back", callback_data="menu:home")]
        ])
    )


# ---------------------------------------------------------
# Developer
# ---------------------------------------------------------
@dp.callback_query(F.data == "menu:dev")
async def menu_dev(cq: CallbackQuery):
    await cq.message.edit_text(
        f"🛠 <b>Developer</b>\n{DEVELOPER_TAG}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton("⬅ Back", callback_data="menu:home")]
        ])
    )


# ---------------------------------------------------------
# Entry
# ---------------------------------------------------------
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
