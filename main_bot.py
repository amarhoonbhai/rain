# main_bot.py — Aiogram v3.x (compact iOS-style UI)
# Features:
# • Channel gate (@PhiloBots, @TheTrafficZone by default; override via env/settings)
# • Manage Accounts (up to 3; removal; add via @SpinifyLoginBot)
# • Groups (up to 5) with add/clear
# • Intervals: 30/45/60 minutes
# • Message (ad) setter + parse mode + preview
# • Disclaimer screen
# • Owner-only: Night Mode toggle (00:00–07:00 IST), Stats, Top 10, Broadcast, Upgrade/Downgrade name-lock
# • Referrals: /ref /refstats /reftop + deep-link /start ref_<id>
# • Buttons are non-sticky (auto-ack + safe edit)

import os, asyncio, logging
from aiogram import Bot, Dispatcher, F, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from dotenv import load_dotenv

from core.db import (
    init_db, ensure_user, get_conn,
    # sessions
    sessions_list, sessions_delete, sessions_count, sessions_count_user, first_free_slot,
    # groups/interval
    list_groups, add_group, clear_groups, groups_cap,
    set_interval, get_interval,
    # stats
    get_total_sent_ok, users_count, top_users,
    # night mode
    night_enabled, set_night_enabled,
    # gate channels
    get_gate_channels_effective, set_setting, get_setting,
    # premium name-lock
    set_name_lock,
    # ads
    set_ad, get_ad,
)

# ---------------- ENV / BOOT ----------------
load_dotenv()
TOKEN = (os.getenv("MAIN_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
if not TOKEN or ":" not in TOKEN:
    raise RuntimeError("MAIN_BOT_TOKEN missing/malformed.")

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
init_db()

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"))
log = logging.getLogger("main_bot")

BOT_USERNAME = None  # cached on first /start

# -------------- Helpers / Gate --------------
def is_owner(uid: int) -> bool:
    return OWNER_ID and int(uid) == OWNER_ID

async def safe_edit_text(message, text, **kw):
    try:
        return await message.edit_text(text, **kw)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            return None
        raise

def _defaults_gate_if_empty(chs: list[str]) -> list[str]:
    # If no channels configured in settings/env, default to these two
    if chs: return chs
    return ["@PhiloBots", "@TheTrafficZone"]

def _gate_channels() -> list[str]:
    ch1, ch2 = get_gate_channels_effective()
    chs = [c for c in (ch1, ch2) if c]
    return _defaults_gate_if_empty(chs)

async def _check_gate(user_id: int):
    missing = []
    for ch in _gate_channels():
        try:
            m = await bot.get_chat_member(ch, user_id)
            if str(getattr(m, "status", "left")).lower() in {"left","kicked"}:
                missing.append(ch)
        except Exception:
            missing.append(ch)
    return (len(missing)==0), missing

def _gate_kb():
    rows = []
    for ch in _gate_channels():
        rows.append([InlineKeyboardButton(text=f"🔗 {ch}", url=f"https://t.me/{ch.lstrip('@')}")])
    rows.append([InlineKeyboardButton(text="✅ I've Joined", callback_data="gate:check")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

GATE_TEXT = (
    "✇ Access required\n"
    "✇ Join the channels below to use the bot:\n"
    + "\n".join([f"  • {ch}" for ch in _gate_channels()]) +
    "\n\n✇ After joining, tap <b>I've Joined</b>."
)

# -------------- Middlewares --------------
class AutoAckMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        if isinstance(event, CallbackQuery):
            try: await event.answer()
            except Exception: pass
        return await handler(event, data)

class GateGuardMiddleware(BaseMiddleware):
    """Block everything except /start and gate:* until joined channels."""
    async def __call__(self, handler, event, data):
        uid = getattr(getattr(event, "from_user", None), "id", None)
        allow = False
        if isinstance(event, Message) and event.text and event.text.startswith("/start"):
            allow = True
        if isinstance(event, CallbackQuery) and (event.data or "").startswith("gate:"):
            allow = True
        if allow or not _gate_channels() or not uid:
            return await handler(event, data)
        ok, _ = await _check_gate(uid)
        if ok:
            return await handler(event, data)
        # show gate prompt
        if isinstance(event, CallbackQuery):
            try:
                await safe_edit_text(event.message, GATE_TEXT, reply_markup=_gate_kb())
            except Exception:
                await bot.send_message(uid, GATE_TEXT, reply_markup=_gate_kb())
        else:
            await bot.send_message(uid, GATE_TEXT, reply_markup=_gate_kb())
        return

dp.update.middleware(AutoAckMiddleware())
dp.update.middleware(GateGuardMiddleware())

# -------------- Referrals --------------
def _ref_key_by(user_id: int) -> str:  # who referred this user
    return f"ref:by:{user_id}"

def _ref_key_count(user_id: int) -> str:  # how many this user referred
    return f"ref:count:{user_id}"

def _ref_set_if_absent(user_id: int, referrer_id: int) -> bool:
    if referrer_id == user_id or referrer_id <= 0:
        return False
    if get_setting(_ref_key_by(user_id), None) is not None:
        return False
    set_setting(_ref_key_by(user_id), int(referrer_id))
    cur = int(get_setting(_ref_key_count(referrer_id), 0) or 0)
    set_setting(_ref_key_count(referrer_id), cur + 1)
    return True

async def _ensure_bot_username():
    global BOT_USERNAME
    if not BOT_USERNAME:
        me = await bot.get_me()
        BOT_USERNAME = me.username

# -------------- Keyboards --------------
def kb_main(uid: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="👤 Manage Accounts", callback_data="menu:accounts")],
        [InlineKeyboardButton(text="👥 Groups",           callback_data="menu:groups"),
         InlineKeyboardButton(text="⏱ Interval",         callback_data="menu:interval")],
        [InlineKeyboardButton(text="📝 Message",          callback_data="menu:msg")],
        [InlineKeyboardButton(text="ℹ️ Disclaimer",       callback_data="menu:disc")],
    ]
    if is_owner(uid):
        rows.append([InlineKeyboardButton(text=("🌙 Night: ON" if night_enabled() else "🌙 Night: OFF"),
                                          callback_data="owner:night:toggle")])
        rows.append([InlineKeyboardButton(text="📊 Stats", callback_data="owner:stats"),
                     InlineKeyboardButton(text="🏆 Top 10", callback_data="owner:top")])
        rows.append([InlineKeyboardButton(text="📣 Broadcast", callback_data="owner:broadcast"),
                     InlineKeyboardButton(text="💎 Upgrade/Downgrade", callback_data="owner:upgrade")])
    rows.append([InlineKeyboardButton(text="🔄 Refresh", callback_data="menu:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_intervals(cur: int|None) -> InlineKeyboardMarkup:
    def chip(v):
        label = f"{v}m" + (" ✅" if cur==v else "")
        return InlineKeyboardButton(text=label, callback_data=f"interval:set:{v}")
    return InlineKeyboardMarkup(inline_keyboard=[
        [chip(30), chip(45), chip(60)],
        [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]
    ])

def kb_groups(uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add Group", callback_data="groups:add"),
         InlineKeyboardButton(text="🧹 Clear",     callback_data="groups:clear")],
        [InlineKeyboardButton(text="🔄 Refresh",   callback_data="menu:groups"),
         InlineKeyboardButton(text="⬅ Back",       callback_data="menu:home")],
    ])

def kb_accounts(slots) -> InlineKeyboardMarkup:
    row1 = []
    for s in slots:
        row1.append(InlineKeyboardButton(text=f"🗑 Remove S{s['slot']}", callback_data=f"acct:del:{s['slot']}"))
    if not row1:
        row1 = [InlineKeyboardButton(text="➕ Add via @SpinifyLoginBot", url="https://t.me/SpinifyLoginBot")]
    rows = [row1] if row1 else []
    rows.append([InlineKeyboardButton(text="🔄 Refresh", callback_data="menu:accounts"),
                 InlineKeyboardButton(text="⬅ Back",   callback_data="menu:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_owner_upgrade_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💎 Upgrade",   callback_data="owner:upgrade:do")],
        [InlineKeyboardButton(text="🧹 Downgrade", callback_data="owner:downgrade:do")],
        [InlineKeyboardButton(text="⬅ Back",      callback_data="menu:home")]
    ])

def kb_msg_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✍️ Set / Update", callback_data="msg:set"),
         InlineKeyboardButton(text="👁 Preview",      callback_data="msg:show")],
        [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]
    ])

def kb_msg_modes() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Plain",    callback_data="msg:mode:none"),
         InlineKeyboardButton(text="Markdown", callback_data="msg:mode:md"),
         InlineKeyboardButton(text="HTML",     callback_data="msg:mode:html")],
        [InlineKeyboardButton(text="⬅ Cancel", callback_data="menu:msg")]
    ])

# -------------- Views --------------
async def view_home(msg_or_cq, uid: int):
    # Reminder if no sessions
    have_sessions = sessions_count_user(uid) > 0
    session_line = "✇ Sessions: ✅" if have_sessions else "✇ Sessions: ❌ (Add via @SpinifyLoginBot)"
    HOWTO = (
        "✇ How to use\n"
        "  1) ✇ Open @SpinifyLoginBot and add up to 3 accounts\n"
        "  2) ✇ Set interval (30/45/60 min)\n"
        "  3) ✇ Add up to 5 groups\n"
        "  4) ✇ Set your 📝 Message\n"
        "  5) ✇ Worker will forward on schedule\n\n"
        f"{session_line}\n"
        "✇ Owner can enable Night Mode (00:00–07:00 IST).\n"
        "✇ Use /ref to get your referral link."
    )
    if isinstance(msg_or_cq, Message):
        await msg_or_cq.answer(HOWTO, reply_markup=kb_main(uid))
    else:
        await safe_edit_text(msg_or_cq.message, HOWTO, reply_markup=kb_main(uid))

async def view_accounts(cq: CallbackQuery):
    uid = cq.from_user.id
    slots = sessions_list(uid)
    if not slots:
        text = ("👤 Manage Accounts\n"
                "✇ No sessions found.\n"
                "✇ Use @SpinifyLoginBot to add up to 3 accounts.")
    else:
        lines = [f"• Slot {r['slot']} — API_ID {r['api_id']}" for r in slots]
        text = "👤 Manage Accounts\n" + "\n".join(lines)
    await safe_edit_text(cq.message, text, reply_markup=kb_accounts(slots))

async def view_groups(cq: CallbackQuery):
    uid = cq.from_user.id
    gs = list_groups(uid)
    if gs:
        text = "👥 Groups (max {cap})\n".format(cap=groups_cap()) + "\n".join(f"• {g}" for g in gs)
    else:
        text = f"👥 Groups (max {groups_cap()})\n✇ No groups yet. Add one."
    await safe_edit_text(cq.message, text, reply_markup=kb_groups(uid))

async def view_interval(cq: CallbackQuery):
    uid = cq.from_user.id
    cur = get_interval(uid)
    text = "⏱ Interval\n✇ Choose how often to forward:"
    await safe_edit_text(cq.message, text, reply_markup=kb_intervals(cur))

async def view_disclaimer(cq: CallbackQuery):
    text = (
        "⚠️ Disclaimer (Free Version)\n"
        "✇ Use at your own risk.\n"
        "✇ If your Telegram ID gets terminated, I am not responsible.\n"
        "✇ You must comply with Telegram’s Terms and local laws.\n"
        "✇ Excessive spam/abuse may lead to account limitations."
    )
    await safe_edit_text(cq.message, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]
    ]))

# -------------- FSM --------------
class G(StatesGroup):
    adding = State()

class OwnerFlow(StatesGroup):
    broadcast = State()
    upgrade_user = State()
    upgrade_name = State()
    downgrade_user = State()

class MsgFlow(StatesGroup):
    text = State()

# -------------- Handlers --------------
@dp.message(Command("start"))
async def on_start(msg: Message):
    global BOT_USERNAME
    uid = msg.from_user.id
    ensure_user(uid, getattr(msg.from_user, "username", None))

    # Capture referral from deep-link
    try:
        parts = msg.text.split(maxsplit=1)
        if len(parts) == 2 and parts[1].startswith("ref_"):
            ref_id = int(parts[1][4:])
            # record referral
            if _ref_set_if_absent(uid, ref_id):
                try: await bot.send_message(ref_id, f"🎉 New referral joined: <code>{uid}</code>")
                except Exception: pass
    except Exception:
        pass

    if not BOT_USERNAME:
        await _ensure_bot_username()

    # Gate first
    if _gate_channels():
        ok, _ = await _check_gate(uid)
        if not ok:
            await msg.answer(GATE_TEXT, reply_markup=_gate_kb())
            return

    await view_home(msg, uid)

@dp.callback_query(F.data == "gate:check")
async def on_gate_check(cq: CallbackQuery):
    uid = cq.from_user.id
    ok, _ = await _check_gate(uid)
    if ok: await view_home(cq, uid)
    else:  await safe_edit_text(cq.message, GATE_TEXT, reply_markup=_gate_kb())

@dp.callback_query(F.data == "menu:home")
async def cb_home(cq: CallbackQuery):
    await view_home(cq, cq.from_user.id)

@dp.callback_query(F.data == "menu:accounts")
async def cb_accounts(cq: CallbackQuery):
    await view_accounts(cq)

@dp.callback_query(F.data == "menu:groups")
async def cb_groups(cq: CallbackQuery):
    await view_groups(cq)

@dp.callback_query(F.data == "menu:interval")
async def cb_interval(cq: CallbackQuery):
    await view_interval(cq)

@dp.callback_query(F.data == "menu:disc")
async def cb_disc(cq: CallbackQuery):
    await view_disclaimer(cq)

# Accounts delete slot
@dp.callback_query(F.data.startswith("acct:del:"))
async def cb_acct_del(cq: CallbackQuery):
    uid = cq.from_user.id
    try:
        slot = int(cq.data.split(":")[-1])
        sessions_delete(uid, slot)
    except Exception as e:
        log.error(f"acct del err: {e}")
    await view_accounts(cq)

# Groups add / clear
@dp.callback_query(F.data == "groups:add")
async def cb_groups_add(cq: CallbackQuery, state: FSMContext):
    await state.set_state(G.adding)
    await safe_edit_text(cq.message, "✇ Send a group username or invite link (e.g., @MyGroup or https://t.me/xyz)")

@dp.message(G.adding)
async def on_group_text(msg: Message, state: FSMContext):
    uid = msg.from_user.id
    try:
        n = add_group(uid, msg.text)
        if n:
            await msg.answer("✅ Added.")
        else:
            await msg.answer("ℹ️ No slot available or already added (max 5).")
    except Exception as e:
        await msg.answer(f"❌ Failed: <code>{e}</code>")
    await state.clear()
    # back to groups view
    gs = list_groups(uid)
    text = ("👥 Groups (max {cap})\n".format(cap=groups_cap()) + "\n".join(f"• {g}" for g in gs)) if gs else f"👥 Groups (max {groups_cap()})\n✇ No groups yet. Add one."
    await msg.answer(text, reply_markup=kb_groups(uid))

@dp.callback_query(F.data == "groups:clear")
async def cb_groups_clear(cq: CallbackQuery):
    clear_groups(cq.from_user.id)
    await view_groups(cq)

# Interval set
@dp.callback_query(F.data.startswith("interval:set:"))
async def cb_set_interval(cq: CallbackQuery):
    uid = cq.from_user.id
    mins = int(cq.data.split(":")[-1])
    if mins not in (30,45,60):
        await safe_edit_text(cq.message, "❌ Allowed: 30, 45, 60 minutes", reply_markup=kb_intervals(get_interval(uid))); return
    set_interval(uid, mins)
    await safe_edit_text(cq.message, f"⏱ Interval set to {mins} minutes ✅", reply_markup=kb_intervals(mins))

# Message (ad) flow
@dp.callback_query(F.data == "menu:msg")
async def menu_msg(cq: CallbackQuery):
    uid = cq.from_user.id
    text, mode = get_ad(uid)
    curr = text if text else "— (not set)"
    mode_str = {"Markdown":"Markdown", "HTML":"HTML", None:"Plain"}.get(mode, str(mode or "Plain"))
    await safe_edit_text(
        cq.message,
        "📝 Message (the text your worker forwards)\n"
        f"✇ Current mode: <b>{mode_str}</b>\n"
        "✇ Current text:\n"
        f"<code>{(curr[:900] + '…') if len(curr)>900 else curr}</code>",
        reply_markup=kb_msg_menu()
    )

@dp.callback_query(F.data == "msg:set")
async def msg_set(cq: CallbackQuery, state: FSMContext):
    await state.set_state(MsgFlow.text)
    await safe_edit_text(
        cq.message,
        "✇ Send the message text now (next message).\n"
        "• You can include formatting; you’ll choose Plain/Markdown/HTML after this.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅ Cancel", callback_data="menu:msg")]])
    )

@dp.message(MsgFlow.text)
async def msg_text_save(msg: Message, state: FSMContext):
    await state.update_data(pending_text=msg.text)
    await msg.answer("✇ Choose parse mode:", reply_markup=kb_msg_modes())

@dp.callback_query(F.data.startswith("msg:mode:"))
async def msg_mode_choose(cq: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    pending = data.get("pending_text")
    if not pending:
        await safe_edit_text(cq.message, "❌ Nothing to save. Tap “Set / Update”.", reply_markup=kb_msg_menu())
        await state.clear()
        return
    code = cq.data.split(":")[-1]
    mode = None
    if code == "md": mode = "Markdown"
    elif code == "html": mode = "HTML"
    set_ad(cq.from_user.id, pending, mode)
    await state.clear()
    await safe_edit_text(cq.message, "✅ Saved. Use Preview to test.", reply_markup=kb_msg_menu())

@dp.callback_query(F.data == "msg:show")
async def msg_show(cq: CallbackQuery):
    uid = cq.from_user.id
    text, mode = get_ad(uid)
    if not text:
        await cq.message.answer("ℹ️ No message set. Tap “Set / Update”.")
        return
    try:
        await bot.send_message(uid, text, parse_mode=mode)
    except Exception:
        await bot.send_message(uid, text)

# Quick commands to set/show ad
@dp.message(Command("setad"))
async def cmd_setad(msg: Message, state: FSMContext):
    await msg.answer("✇ Send the message text now.")
    await state.set_state(MsgFlow.text)

@dp.message(Command("showad"))
async def cmd_showad(msg: Message):
    text, mode = get_ad(msg.from_user.id)
    if not text:
        await msg.answer("ℹ️ No message set."); return
    try: await bot.send_message(msg.chat.id, text, parse_mode=mode)
    except Exception: await bot.send_message(msg.chat.id, text)

# Owner panel: stats/top/night/broadcast/upgrade
@dp.callback_query(F.data == "owner:stats")
async def cb_owner_stats(cq: CallbackQuery):
    if not is_owner(cq.from_user.id): return
    text = (f"📊 Stats\n"
            f"✇ Users: {users_count()}\n"
            f"✇ Active (≥1 session): {sessions_count()}\n"
            f"✇ Total forwarded: {get_total_sent_ok()}")
    await safe_edit_text(cq.message, text, reply_markup=kb_main(cq.from_user.id))

@dp.callback_query(F.data == "owner:top")
async def cb_owner_top(cq: CallbackQuery):
    if not is_owner(cq.from_user.id): return
    rows = top_users(10)
    if not rows:
        text = "🏆 Top Users (forwards)\n✇ No data yet."
    else:
        lines = [f"{i+1}. {r['user_id']} — {r['sent_ok']} msgs" for i,r in enumerate(rows)]
        text = "🏆 Top Users (forwards)\n" + "\n".join(lines)
    await safe_edit_text(cq.message, text, reply_markup=kb_main(cq.from_user.id))

@dp.callback_query(F.data == "owner:night:toggle")
async def cb_night_toggle(cq
