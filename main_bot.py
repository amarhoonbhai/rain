import os, asyncio, logging
from datetime import datetime, timezone
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv

from core.db import (
    init_db, ensure_user, get_conn,
    sessions_list, sessions_delete, sessions_count_user, sessions_count,
    list_groups, groups_cap, get_interval, get_last_sent_at,
    users_count, get_total_sent_ok, top_users,
    get_gate_channels_effective, set_setting,
    night_enabled, set_night_enabled
)

# -----------------------------
# Bootstrap
# -----------------------------
load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("main-bot")

TOKEN = os.getenv("MAIN_BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
UNLOCK_GC_LINK = os.getenv("UNLOCK_GC_LINK", "")
DEV_CONTACT = "@Spinify"
DEVELOPER_BUTTON = "👨‍💻 Developer"

bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
init_db()

# ---------------------------------------------------
# Helpers
# ---------------------------------------------------
def is_owner(uid: int) -> bool:
    return uid == OWNER_ID

def _gate_channels():
    ch1, ch2 = get_gate_channels_effective()
    return [c for c in (ch1, ch2) if c]

async def _check_gate(user_id: int):
    missing = []
    for ch in _gate_channels():
        try:
            m = await bot.get_chat_member(ch, user_id)
            status = str(getattr(m, "status", "left")).lower()
            if status in {"left", "kicked"}:
                missing.append(ch)
        except Exception:
            missing.append(ch)
    return (len(missing) == 0), missing

def _gate_text():
    rows = "\n".join(f"• {c}" for c in _gate_channels())
    return (
        "🔵 <b>Welcome to Spinify Ads</b>\n\n"
        "To continue, please join the required channels:\n"
        f"{rows}\n\n"
        "Click <b>I've Joined</b> when done."
    )

def _gate_kb():
    rows = [
        [InlineKeyboardButton(text=f"🔗 {c}", url=f"https://t.me/{c.lstrip('@')}")]
        for c in _gate_channels()
    ]
    rows.append([InlineKeyboardButton(text="✅ I've Joined", callback_data="gate:check")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def _format_eta(uid: int) -> str:
    last = get_last_sent_at(uid)
    interval = get_interval(uid) or 30

    if last is None:
        return f"in ~{interval}m"

    now = int(datetime.now(timezone.utc).timestamp())
    left = interval * 60 - (now - int(last))
    if left <= 0:
        return "now"

    m, s = divmod(left, 60)
    return f"in ~{m}m"

# ---------------------------
# UI (Blue Theme)
# ---------------------------
def kb_main(uid: int):
    rows = [
        [InlineKeyboardButton(text="👤 Accounts", callback_data="menu:acc")],
        [
            InlineKeyboardButton(text="🎯 Target Groups", callback_data="menu:groups"),
            InlineKeyboardButton(text="ℹ️ Commands", callback_data="menu:cmds"),
        ],
        [
            InlineKeyboardButton(text="🔓 Unlock GC", callback_data="menu:unlock"),
            InlineKeyboardButton(text="⚠️ Disclaimer", callback_data="menu:disc"),
        ],
        [InlineKeyboardButton(text=DEVELOPER_BUTTON, url="https://t.me/Spinify")],
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="menu:home")],
    ]
    if is_owner(uid):
        rows.insert(3, [
            InlineKeyboardButton(
                text=("🌙 Night: ON" if night_enabled() else "🌙 Night: OFF"),
                callback_data="owner:night"
            )
        ])
        rows.append([
            InlineKeyboardButton(text="📊 Stats", callback_data="owner:stats"),
            InlineKeyboardButton(text="🏆 Top 10", callback_data="owner:top"),
        ])
        rows.append([
            InlineKeyboardButton(text="📣 Broadcast", callback_data="owner:bcast")
        ])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def cmds_text():
    return (
        "🔵 <b>Spinify Self Commands</b>\n\n"
        "Send these from your logged-in account:\n\n"
        "• <b>.help</b> — list commands\n"
        "• <b>.status</b> — show interval, delay, groups\n"
        "• <b>.info</b> — account details\n"
        "• <b>.addgroup &lt;link/@user&gt;</b>\n"
        "• <b>.delgroup &lt;link/@user&gt;</b>\n"
        "• <b>.groups</b> — list groups\n"
        "• <b>.time 30</b> / <b>.time 45</b> / <b>.time 60</b>\n\n"
        "⏳ Delay is fixed at <b>100 seconds</b>\n"
        "🛡 Commands starting with <b>.</b> never forward to groups."
    )

# ---------------------------
# Home UI
# ---------------------------
async def home(m, uid: int):
    gs = len(list_groups(uid))
    ss = sessions_count_user(uid)
    interval = get_interval(uid)

    text = (
        "🔵 <b>Spinify Ads Dashboard</b>\n\n"
        "Use @SpinifyLoginBot to add accounts.\n"
        "Then send .help from your own account.\n\n"
        f"📦 Sessions: {ss}\n"
        f"🎯 Groups: {gs}/{groups_cap(uid)}\n"
        f"⏱ Interval: {interval}m\n"
        f"⏩ Next send: {('—' if ss == 0 or gs == 0 else _format_eta(uid))}\n"
        f"🌙 Night Mode: {'ON' if night_enabled() else 'OFF'}\n"
    )

    if isinstance(m, Message):
        await m.answer(text, reply_markup=kb_main(uid))
    else:
        try:
            await m.message.edit_text(text, reply_markup=kb_main(uid))
        except:
            pass

# ---------------------------------------------------
# Start + Gate Enforcement
# ---------------------------------------------------
@dp.message(Command("start"))
async def start(msg: Message):
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.username)

    if _gate_channels():
        ok, _ = await _check_gate(uid)
        if not ok:
            await msg.answer(_gate_text(), reply_markup=_gate_kb())
            return

    await home(msg, uid)

@dp.callback_query(F.data == "gate:check")
async def gate_check(cq: CallbackQuery):
    ok, _ = await _check_gate(cq.from_user.id)
    if ok:
        await home(cq, cq.from_user.id)
    else:
        await cq.message.edit_text(_gate_text(), reply_markup=_gate_kb())

# ---------------------------------------------------
# Accounts
# ---------------------------------------------------
@dp.callback_query(F.data == "menu:acc")
async def cb_acc(cq: CallbackQuery):
    uid = cq.from_user.id
    rows = sessions_list(uid)

    if not rows:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="➕ Open Login Bot", url="https://t.me/SpinifyLoginBot")],
                [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]
            ]
        )
        await cq.message.edit_text("👤 No sessions yet.", reply_markup=kb)
        return

    text = "👤 <b>Your Accounts</b>\n" + "\n".join(
        f"• Slot {r['slot']} — API_ID {r['api_id']}" for r in rows
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"🗑 Delete S{r['slot']}", callback_data=f"acc:del:{r['slot']}")] 
            for r in rows
        ] + [[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]
    )
    await cq.message.edit_text(text, reply_markup=kb)

@dp.callback_query(F.data.startswith("acc:del:"))
async def acc_del(cq: CallbackQuery):
    slot = int(cq.data.split(":")[2])
    sessions_delete(cq.from_user.id, slot)
    await cb_acc(cq)

# ---------------------------------------------------
# Groups Menu
# ---------------------------------------------------
@dp.callback_query(F.data == "menu:groups")
async def groups_menu(cq: CallbackQuery):
    uid = cq.from_user.id
    gs = list_groups(uid)

    text = (
        "🎯 <b>Your Target Groups</b>\n\n" +
        ("\n".join(gs) if gs else "No groups added.") +
        "\n\nUse <b>.addgroup</b> and <b>.delgroup</b> inside your logged-in account."
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]
    )
    await cq.message.edit_text(text, reply_markup=kb)

# ---------------------------------------------------
# Unlock GC
# ---------------------------------------------------
@dp.callback_query(F.data == "menu:unlock")
async def cb_unlock(cq: CallbackQuery):
    uid = cq.from_user.id
    cap = groups_cap(uid)

    text = (
        "🔓 <b>Unlock GC</b>\n\n"
        "Join the Unlock GC to increase your max groups to 20.\n"
        f"Current cap: {cap}\n\n"
        "Leaving the Unlock GC resets cap to 5."
    )

    kb = [
        [InlineKeyboardButton(text="🔗 Join GC", url=UNLOCK_GC_LINK)],
        [InlineKeyboardButton(text="I've Joined", callback_data="unlock:ok")],
        [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]
    ]
    await cq.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data == "unlock:ok")
async def unlock_ok(cq: CallbackQuery):
    uid = cq.from_user.id
    set_setting(f"groups_cap:{uid}", 20)

    await cq.message.edit_text(
        "✅ Unlock complete. You can now add up to <b>20 groups</b>.\n"
        "⚠️ If you leave the Unlock GC, cap resets to 5.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]
        )
    )

# ---------------------------------------------------
# Commands List
# ---------------------------------------------------
@dp.callback_query(F.data == "menu:cmds")
async def cb_cmds(cq: CallbackQuery):
    await cq.message.edit_text(
        cmds_text(),
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]
        )
    )

# ---------------------------------------------------
# Disclaimer
# ---------------------------------------------------
@dp.callback_query(F.data == "menu:disc")
async def cb_disc(cq: CallbackQuery):
    text = (
        "⚠️ <b>Disclaimer</b>\n\n"
        "This tool forwards your own saved messages using your Telegram account.\n"
        "Use responsibly. We are not responsible for bans, spam issues, or misuse."
    )
    await cq.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]
        )
    )

# ---------------------------------------------------
# Stats & Owner Tools
# ---------------------------------------------------
@dp.callback_query(F.data == "owner:night")
async def owner_night(cq: CallbackQuery):
    if not is_owner(cq.from_user.id):
        return
    set_night_enabled(not night_enabled())
    await home(cq, cq.from_user.id)

@dp.callback_query(F.data == "owner:stats")
async def owner_stats(cq: CallbackQuery):
    total = users_count()
    active = sessions_count()
    sent = get_total_sent_ok()

    text = (
        "📊 <b>Global Stats</b>\n\n"
        f"Users: {total}\n"
        f"Active Sessions: {active}\n"
        f"Total Forwards: {sent}\n"
    )

    await cq.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]
        )
    )

@dp.callback_query(F.data == "owner:top")
async def owner_top(cq: CallbackQuery):
    rows = top_users(10)
    if not rows:
        text = "🏆 No data yet."
    else:
        text = "🏆 <b>Top Users</b>\n" + "\n".join(
            f"{i+1}. {r['user_id']} — {r['sent_ok']}" for i, r in enumerate(rows)
        )

    await cq.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]
        )
    )

# ---------------------------------------------------
# Broadcast
# ---------------------------------------------------
class OwnerBcast(StatesGroup):
    text = State()

@dp.callback_query(F.data == "owner:bcast")
async def owner_bcast(cq: CallbackQuery, state: FSMContext):
    if not is_owner(cq.from_user.id): return
    await state.set_state(OwnerBcast.text)
    await cq.message.edit_text(
        "📣 Send broadcast message:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]
        )
    )

@dp.message(OwnerBcast.text)
async def do_broadcast(msg: Message, state: FSMContext):
    if not is_owner(msg.from_user.id):
        await state.clear()
        return

    uids = [r["user_id"] for r in get_conn().execute("SELECT user_id FROM users")]
    ok = fail = 0

    for uid in uids:
        try:
            await bot.send_message(uid, msg.text)
            ok += 1
        except:
            fail += 1

    await msg.answer(f"📣 Broadcast sent.\n✔ {ok} OK\n✖ {fail} Failed")
    await state.clear()

# ---------------------------------------------------
# Entrypoint
# ---------------------------------------------------
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
