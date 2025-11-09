# main_bot.py — Aiogram v3.x (compact iOS-style UI)
# Features:
# • Channel gate (@PhiloBots, @TheTrafficZone defaults)
# • Accounts (remove slot; add via @SpinifyLoginBot)
# • Groups (accept any links/IDs; remind to join manually; max 5 by default)
# • Unlock Gc → raise cap to 10 if user joined UNLOCK_GC_CHAT_ID
# • Intervals: 30/45/60 minutes
# • Message flow = instructions only (worker sends pinned message from Saved Messages)
# • Pause/Resume + /fstats with next-send countdown
# • Disclaimer
# • Owner-only: Night Mode toggle, Stats, Top 10, Broadcast, Upgrade/Downgrade (name-lock)
# • Referrals: /ref /refstats /reftop + /start ref_<id>

import os, asyncio, logging, math
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
    sessions_list, sessions_delete, sessions_count, sessions_count_user,
    # groups & interval
    list_groups, add_group, clear_groups,
    set_interval, get_interval, get_last_sent_at,
    # stats
    get_total_sent_ok, users_count, top_users,
    # night mode
    night_enabled, set_night_enabled,
    # KV/settings
    set_setting, get_setting,
    # premium name-lock
    set_name_lock,
)

# ---------------- ENV / BOOT ----------------
load_dotenv()
TOKEN = (os.getenv("MAIN_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
if not TOKEN or ":" not in TOKEN:
    raise RuntimeError("MAIN_BOT_TOKEN missing/malformed.")

# Unlock Gc config:
# UNLOCK_GC_LINK: t.me/xxx or invite link (used for button)
# UNLOCK_GC_CHAT_ID: numeric chat id to verify membership (required for hard verification)
UNLOCK_GC_LINK = os.getenv("UNLOCK_GC_LINK", "").strip()
try:
    UNLOCK_GC_CHAT_ID = int(os.getenv("UNLOCK_GC_CHAT_ID", "0"))
except Exception:
    UNLOCK_GC_CHAT_ID = 0

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
init_db()

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"))
log = logging.getLogger("main_bot")

BOT_USERNAME = None  # cached after first /start

# ---------------- Gate helpers ----------------
def _defaults_gate_if_empty(chs: list[str]) -> list[str]:
    return chs or ["@PhiloBots", "@TheTrafficZone"]

def _gate_channels() -> list[str]:
    ch1 = os.getenv("GATE_CH1", "").strip()
    ch2 = os.getenv("GATE_CH2", "").strip()
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

def _gate_text() -> str:
    lines = "\n".join(f"  • {ch}" for ch in _gate_channels())
    return (
        "✇ Access required\n"
        "✇ Join the channels below to use the bot:\n"
        f"{lines}\n\n"
        "✇ After joining, tap <b>I've Joined</b>."
    )

# ---------------- Pause helpers ----------------
def _pause_key(uid:int) -> str:
    return f"user:paused:{uid}"

def _is_paused(uid: int) -> bool:
    val = get_setting(_pause_key(uid), 0)
    try:
        return bool(int(val))
    except Exception:
        return str(val).lower() in {"1","true","yes","on"}

def _set_paused(uid: int, v: bool):
    set_setting(_pause_key(uid), 1 if v else 0)

# ---------------- Groups cap helpers ----------------
def _groups_cap(uid: int) -> int:
    # default 5; if unlocked → 10
    raw = get_setting(f"groups_cap:{uid}", None)
    try:
        return int(raw) if raw is not None else 5
    except Exception:
        return 5

def _set_groups_cap(uid: int, cap: int):
    set_setting(f"groups_cap:{uid}", int(cap))

# ---------------- Middlewares ----------------
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
                await event.message.edit_text(_gate_text(), reply_markup=_gate_kb())
            except Exception:
                await bot.send_message(uid, _gate_text(), reply_markup=_gate_kb())
        else:
            await bot.send_message(uid, _gate_text(), reply_markup=_gate_kb())
        return

dp.update.middleware(AutoAckMiddleware())
dp.update.middleware(GateGuardMiddleware())

# ---------------- Referrals ----------------
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

# ---------------- Keyboards ----------------
def kb_main(uid: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="👤 Manage Accounts", callback_data="menu:accounts")],
        [InlineKeyboardButton(text="👥 Groups",           callback_data="menu:groups"),
         InlineKeyboardButton(text="⏱ Interval",         callback_data="menu:interval")],
        [InlineKeyboardButton(text="📝 Message",          callback_data="menu:msg")],
        [InlineKeyboardButton(text="🔓 Unlock Gc",        callback_data="groups:unlock")],
        [InlineKeyboardButton(text=("⏸ Pause" if not _is_paused(uid) else "▶️ Resume"),
                              callback_data="user:pause:toggle")],
        [InlineKeyboardButton(text="ℹ️ Disclaimer",       callback_data="menu:disc")],
    ]
    if OWNER_ID and uid == OWNER_ID:
        rows.append([InlineKeyboardButton(text=("🌙 Night: ON" if night_enabled() else "🌙 Night: OFF"),
                                          callback_data="owner:night:toggle")])
        rows.append([InlineKeyboardButton(text="📊 Stats", callback_data="owner:stats"),
                     InlineKeyboardButton(text="🏆 Top 10", callback_data="owner:top")])
        rows.append([InlineKeyboardButton(text="📣 Broadcast", callback_data="owner:broadcast"),
                     InlineKeyboardButton(text="💎 Upgrade/Downgrade", callback_data="owner:upgrade")])
    rows.append([InlineKeyboardButton(text="🔄 Refresh", callback_data="menu:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_groups(uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add Group", callback_data="groups:add"),
         InlineKeyboardButton(text="🧹 Clear",     callback_data="groups:clear")],
        [InlineKeyboardButton(text="🔓 Unlock Gc", callback_data="groups:unlock")],
        [InlineKeyboardButton(text="🔄 Refresh",   callback_data="menu:groups"),
         InlineKeyboardButton(text="⬅ Back",       callback_data="menu:home")],
    ])

def kb_intervals(cur: int|None) -> InlineKeyboardMarkup:
    def chip(v):
        label = f"{v}m" + (" ✅" if cur==v else "")
        return InlineKeyboardButton(text=label, callback_data=f"interval:set:{v}")
    return InlineKeyboardMarkup(inline_keyboard=[
        [chip(30), chip(45), chip(60)],
        [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]
    ])

def kb_msg_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📖 How it works", callback_data="msg:howto"),
         InlineKeyboardButton(text="👁 Preview Tip",  callback_data="msg:previewtip")],
        [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]
    ])

def kb_owner_upgrade_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💎 Upgrade",   callback_data="owner:upgrade:do")],
        [InlineKeyboardButton(text="🧹 Downgrade", callback_data="owner:downgrade:do")],
        [InlineKeyboardButton(text="⬅ Back",      callback_data="menu:home")]
    ])

def kb_unlock_gc():
    rows = []
    if UNLOCK_GC_LINK:
        rows.append([InlineKeyboardButton(text="🔗 Join Unlock GC", url=UNLOCK_GC_LINK)])
    rows.append([InlineKeyboardButton(text="✅ I've Joined", callback_data="groups:unlock:check")])
    rows.append([InlineKeyboardButton(text="⬅ Back", callback_data="menu:groups")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ---------------- Views ----------------
async def view_home(msg_or_cq, uid: int):
    have_sessions = sessions_count_user(uid) > 0
    session_line = "✇ Sessions: ✅" if have_sessions else "✇ Sessions: ❌ (Add via @SpinifyLoginBot)"
    HOWTO = (
        "✇ How to use\n"
        "  1) ✇ Open @SpinifyLoginBot and add up to 3 accounts\n"
        "  2) ✇ Set interval (30/45/60 min)\n"
        "  3) ✇ Add up to {cap} groups (any link/ID; join them manually)\n"
        "  4) ✇ In each account: save your ad in Saved Messages and PIN it\n"
        "  5) ✇ Worker forwards pinned ad on schedule\n\n"
        f"{session_line}\n"
        "✇ Owner can enable Night Mode (00:00–07:00 IST).\n"
        "✇ Use /fstats for next send. Use /ref for referral link."
    ).format(cap=_groups_cap(uid))
    if isinstance(msg_or_cq, Message):
        await msg_or_cq.answer(HOWTO, reply_markup=kb_main(uid))
    else:
        try:
            await msg_or_cq.message.edit_text(HOWTO, reply_markup=kb_main(uid))
        except TelegramBadRequest as e:
            if "message is not modified" not in str(e).lower():
                raise

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
    await cq.message.edit_text(text, reply_markup=_kb_accounts(slots))

def _kb_accounts(slots):
    row1 = [InlineKeyboardButton(text=f"🗑 Remove S{s['slot']}", callback_data=f"acct:del:{s['slot']}") for s in slots]
    if not row1:
        row1 = [InlineKeyboardButton(text="➕ Add via @SpinifyLoginBot", url="https://t.me/SpinifyLoginBot")]
    rows = [row1]
    rows.append([InlineKeyboardButton(text="🔄 Refresh", callback_data="menu:accounts"),
                 InlineKeyboardButton(text="⬅ Back",   callback_data="menu:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def view_groups(cq: CallbackQuery):
    uid = cq.from_user.id
    gs = list_groups(uid)
    if gs:
        text = f"👥 Groups (max {_groups_cap(uid)})\n" + "\n".join(f"• {g}" for g in gs)
    else:
        text = f"👥 Groups (max {_groups_cap(uid)})\n✇ No groups yet. Add one."
    await cq.message.edit_text(text, reply_markup=kb_groups(uid))

async def view_interval(cq: CallbackQuery):
    uid = cq.from_user.id
    cur = get_interval(uid)
    text = "⏱ Interval\n✇ Choose how often to forward:"
    await cq.message.edit_text(text, reply_markup=kb_intervals(cur))

async def view_disclaimer(cq: CallbackQuery):
    text = (
        "⚠️ Disclaimer (Free Version)\n"
        "✇ Use at your own risk.\n"
        "✇ If your Telegram ID gets terminated, I am not responsible.\n"
        "✇ You must comply with Telegram’s Terms and local laws.\n"
        "✇ Excessive spam/abuse may lead to account limitations."
    )
    await cq.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]
    ]))

# ---------------- FSM ----------------
class G(StatesGroup):
    adding = State()

class OwnerFlow(StatesGroup):
    broadcast = State()
    upgrade_user = State()
    upgrade_name = State()
    downgrade_user = State()

# ---------------- Handlers ----------------
@dp.message(Command("start"))
async def on_start(msg: Message):
    global BOT_USERNAME
    uid = msg.from_user.id
    ensure_user(uid, getattr(msg.from_user, "username", None))

    # Referral capture
    try:
        parts = msg.text.split(maxsplit=1)
        if len(parts) == 2 and parts[1].startswith("ref_"):
            ref_id = int(parts[1][4:])
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
            await msg.answer(_gate_text(), reply_markup=_gate_kb()); return

    await view_home(msg, uid)

@dp.callback_query(F.data == "gate:check")
async def on_gate_check(cq: CallbackQuery):
    uid = cq.from_user.id
    ok, _ = await _check_gate(uid)
    if ok: await view_home(cq, uid)
    else:  await cq.message.edit_text(_gate_text(), reply_markup=_gate_kb())

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

# Pause toggle
@dp.callback_query(F.data == "user:pause:toggle")
async def cb_pause_toggle(cq: CallbackQuery):
    uid = cq.from_user.id
    _set_paused(uid, not _is_paused(uid))
    await view_home(cq, uid)

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
    cap = _groups_cap(cq.from_user.id)
    await cq.message.edit_text(
        "✇ Send a group link/username/numeric ID (public or private invite). I only save it for forwarding.\n"
        "✇ Reminder: join those groups manually with your account.\n"
        f"✇ Limit: {cap} entries.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅ Cancel", callback_data="menu:groups")]
        ])
    )

@dp.message(G.adding)
async def on_group_text(msg: Message, state: FSMContext):
    uid = msg.from_user.id
    try:
        n = add_group(uid, msg.text)
        if n:
            await msg.answer("✅ Added.")
        else:
            await msg.answer(f"ℹ️ No slot available or already added (max {_groups_cap(uid)}).")
    except Exception as e:
        await msg.answer(f"❌ Failed: <code>{e}</code>")
    await state.clear()
    # back to groups view
    gs = list_groups(uid)
    text = (f"👥 Groups (max {_groups_cap(uid)})\n" + "\n".join(f"• {g}" for g in gs)) if gs else f"👥 Groups (max {_groups_cap(uid)})\n✇ No groups yet. Add one."
    await msg.answer(text, reply_markup=kb_groups(uid))

@dp.callback_query(F.data == "groups:clear")
async def cb_groups_clear(cq: CallbackQuery):
    clear_groups(cq.from_user.id)
    await view_groups(cq)

# Unlock Gc flow
@dp.callback_query(F.data == "groups:unlock")
async def cb_groups_unlock(cq: CallbackQuery):
    uid = cq.from_user.id
    cap = _groups_cap(uid)
    if cap >= 10:
        await cq.message.edit_text("🔓 Unlock Gc\n✇ You already have 10 slots.", reply_markup=kb_groups(uid))
        return
    txt = (
        "🔓 Unlock Gc\n"
        "✇ Join the special group to unlock 10 groups.\n"
        "✇ After joining, tap “I've Joined”."
    )
    await cq.message.edit_text(txt, reply_markup=kb_unlock_gc())

@dp.callback_query(F.data == "groups:unlock:check")
async def cb_groups_unlock_check(cq: CallbackQuery):
    uid = cq.from_user.id
    if UNLOCK_GC_CHAT_ID == 0:
        # Soft-unlock fallback if no chat ID configured
        _set_groups_cap(uid, 10)
        await cq.message.edit_text("✅ Unlocked! You can now add up to 10 groups.", reply_markup=kb_groups(uid))
        return
    try:
        m = await bot.get_chat_member(UNLOCK_GC_CHAT_ID, uid)
        if str(getattr(m, "status", "left")).lower() in {"left","kicked"}:
            raise RuntimeError("not joined")
        _set_groups_cap(uid, 10)
        await cq.message.edit_text("✅ Unlocked! You can now add up to 10 groups.", reply_markup=kb_groups(uid))
    except Exception:
        await cq.message.edit_text(
            "❌ I could not verify your membership in the unlock group.\n"
            "✇ Join using the button, then tap “I've Joined”.",
            reply_markup=kb_unlock_gc()
        )

# Interval set
@dp.callback_query(F.data.startswith("interval:set:"))
async def cb_set_interval(cq: CallbackQuery):
    uid = cq.from_user.id
    mins = int(cq.data.split(":")[-1])
    if mins not in (30,45,60):
        await cq.message.edit_text("❌ Allowed: 30, 45, 60 minutes", reply_markup=kb_intervals(get_interval(uid))); return
    set_interval(uid, mins)
    await cq.message.edit_text(f"⏱ Interval set to {mins} minutes ✅", reply_markup=kb_intervals(mins))

# Message (instructions only)
@dp.callback_query(F.data == "menu:msg")
async def menu_msg(cq: CallbackQuery):
    uid = cq.from_user.id
    txt = (
        "📝 Message — Pinned Saved Message\n"
        "✇ The worker forwards the <b>pinned message</b> from each account’s <b>Saved Messages</b>.\n"
        "✇ Steps (per account):\n"
        "  • Send your ad to “Saved Messages”\n"
        "  • Include premium emojis / media / links as you like\n"
        "  • <b>Pin</b> that message\n"
        "✇ That’s it — nothing to set here.\n"
        "✇ Use <b>👁 Preview Tip</b> for a quick check idea."
    )
    await cq.message.edit_text(txt, reply_markup=kb_msg_menu())

@dp.callback_query(F.data == "msg:howto")
async def msg_howto(cq: CallbackQuery):
    await cq.message.edit_text(
        "📖 How it works\n"
        "✇ We don’t store ad text/media in DB.\n"
        "✇ Your session posts the pinned Saved Message directly.\n"
        "✇ Change the ad? Just edit or re-pin in Saved Messages.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:msg")]])
    )

@dp.callback_query(F.data == "msg:previewtip")
async def msg_preview_tip(cq: CallbackQuery):
    await cq.message.edit_text(
        "👁 Preview tip\n"
        "✇ Forward that pinned Saved Message to a private test group/channel where your account has access.\n"
        "✇ Confirm media, formatting, and premium emojis look correct.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:msg")]])
    )

# Disclaimer
@dp.callback_query(F.data == "menu:disc")
async def cb_disc(cq: CallbackQuery):
    await view_disclaimer(cq)

# Owner panel: stats/top/night/broadcast/upgrade
def _is_owner(uid:int)->bool: return OWNER_ID and uid == OWNER_ID

@dp.callback_query(F.data == "owner:stats")
async def cb_owner_stats(cq: CallbackQuery):
    if not _is_owner(cq.from_user.id): return
    text = (f"📊 Stats\n"
            f"✇ Users: {users_count()}\n"
            f"✇ Active (≥1 session): {sessions_count()}\n"
            f"✇ Total forwarded: {get_total_sent_ok()}")
    await cq.message.edit_text(text, reply_markup=kb_main(cq.from_user.id))

@dp.callback_query(F.data == "owner:top")
async def cb_owner_top(cq: CallbackQuery):
    if not _is_owner(cq.from_user.id): return
    rows = top_users(10)
    if not rows:
        text = "🏆 Top Users (forwards)\n✇ No data yet."
    else:
        lines = [f"{i+1}. {r['user_id']} — {r['sent_ok']} msgs" for i,r in enumerate(rows)]
        text = "🏆 Top Users (forwards)\n" + "\n".join(lines)
    await cq.message.edit_text(text, reply_markup=kb_main(cq.from_user.id))

@dp.callback_query(F.data == "owner:night:toggle")
async def cb_night_toggle(cq: CallbackQuery):
    if not _is_owner(cq.from_user.id): return
    set_night_enabled(not night_enabled())
    await view_home(cq, cq.from_user.id)

# Owner: Broadcast (UI)
@dp.callback_query(F.data == "owner:broadcast")
async def cb_owner_broadcast(cq: CallbackQuery, state: FSMContext):
    if not _is_owner(cq.from_user.id): return
    await state.set_state(OwnerFlow.broadcast)
    await cq.message.edit_text(
        "📣 Broadcast\n"
        "✇ Send the message text now. It will be sent to all users.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅ Cancel", callback_data="menu:home")]
        ])
    )

@dp.message(OwnerFlow.broadcast)
async def on_broadcast_text(msg: Message, state: FSMContext):
    if not _is_owner(msg.from_user.id):
        await state.clear(); return
    text = msg.text
    await msg.answer("📤 Broadcasting…")
    conn = get_conn()
    uids = [r["user_id"] for r in conn.execute("SELECT user_id FROM users").fetchall()]
    conn.close()
    ok = bad = 0
    for i, uid in enumerate(uids, 1):
        try:
            await bot.send_message(uid, text)
            ok += 1
        except Exception:
            bad += 1
        if i % 25 == 0:
            await asyncio.sleep(1.2)
    await state.clear()
    await msg.answer(f"✅ Done. Sent: {ok} | Failed: {bad}")

# Owner: Upgrade/Downgrade (UI)
@dp.callback_query(F.data == "owner:upgrade")
async def cb_owner_upgrade_menu(cq: CallbackQuery):
    if not _is_owner(cq.from_user.id): return
    await cq.message.edit_text("💎 Premium Controls", reply_markup=kb_owner_upgrade_menu())

@dp.callback_query(F.data == "owner:upgrade:do")
async def cb_owner_upgrade_do(cq: CallbackQuery, state: FSMContext):
    if not _is_owner(cq.from_user.id): return
    await state.set_state(OwnerFlow.upgrade_user)
    await cq.message.edit_text(
        "✇ Send the <code>user_id</code> to upgrade (next message).\n(Then I'll ask for an optional locked name.)",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]])
    )

@dp.message(OwnerFlow.upgrade_user)
async def on_upgrade_user(msg: Message, state: FSMContext):
    if not _is_owner(msg.from_user.id):
        await state.clear(); return
    try:
        target = int(msg.text.strip())
    except Exception:
        await msg.answer("❌ user_id must be an integer. Try again or /cancel."); return
    await state.update_data(target=target)
    await state.set_state(OwnerFlow.upgrade_name)
    await msg.answer("✇ Send locked display name (or send '-' to skip):")

@dp.message(OwnerFlow.upgrade_name)
async def on_upgrade_name(msg: Message, state: FSMContext):
    if not _is_owner(msg.from_user.id):
        await state.clear(); return
    data = await state.get_data()
    target = data.get("target")
    locked = None if msg.text.strip() == "-" else msg.text.strip()
    set_name_lock(target, True, name=locked)
    await state.clear()
    await msg.answer(f"✅ Premium name-lock enabled for <code>{target}</code>{' with name “'+locked+'”' if locked else ''}.")

@dp.callback_query(F.data == "owner:downgrade:do")
async def cb_owner_downgrade_do(cq: CallbackQuery, state: FSMContext):
    if not _is_owner(cq.from_user.id): return
    await state.set_state(OwnerFlow.downgrade_user)
    await cq.message.edit_text(
        "✇ Send the <code>user_id</code> to downgrade (next message).",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]])
    )

@dp.message(OwnerFlow.downgrade_user)
async def on_downgrade_user(msg: Message, state: FSMContext):
    if not _is_owner(msg.from_user.id):
        await state.clear(); return
    try:
        target = int(msg.text.strip())
    except Exception:
        await msg.answer("❌ user_id must be integer"); return
    set_name_lock(target, False)
    await state.clear()
    await msg.answer(f"✅ Premium name-lock disabled for <code>{target}</code>.")

# Owner shortcuts
@dp.message(Command("stats"))
async def cmd_stats(msg: Message):
    if not _is_owner(msg.from_user.id): return
    await msg.answer(f"📊 Users: {users_count()} | Active (≥1 session): {sessions_count()} | Forwarded: {get_total_sent_ok()}")

@dp.message(Command("top"))
async def cmd_top(msg: Message):
    if not _is_owner(msg.from_user.id): return
    try:
        n = int((msg.text.split(maxsplit=1)[1]).strip())
    except Exception:
        n = 10
    rows = top_users(n)
    if not rows:
        await msg.answer("🏆 No data yet."); return
    lines = [f"{i+1}. <code>{r['user_id']}</code> — {r['sent_ok']} msgs" for i,r in enumerate(rows)]
    await msg.answer("🏆 Top Users (forwards)\n" + "\n".join(lines))

@dp.message(Command("broadcast"))
async def cmd_broadcast(msg: Message):
    if not _is_owner(msg.from_user.id): return
    try:
        text = msg.text.split(maxsplit=1)[1]
    except Exception:
        await msg.answer("Usage:\n/broadcast your message text"); return
    await msg.answer("📤 Broadcasting…")
    conn = get_conn()
    uids = [r["user_id"] for r in conn.execute("SELECT user_id FROM users").fetchall()]
    conn.close()
    ok = bad = 0
    for i, uid in enumerate(uids, 1):
        try:
            await bot.send_message(uid, text)
            ok += 1
        except Exception:
            bad += 1
        if i % 25 == 0:
            await asyncio.sleep(1.2)
    await msg.answer(f"✅ Done. Sent: {ok} | Failed: {bad}")

# Referrals
@dp.message(Command("ref"))
async def cmd_ref(msg: Message):
    await _ensure_bot_username()
    uid = msg.from_user.id
    count = int(get_setting(_ref_key_count(uid), 0) or 0)
    link = f"https://t.me/{BOT_USERNAME}?start=ref_{uid}"
    txt = (f"🔗 Your referral link:\n{link}\n\n"
           f"✇ Referrals credited: {count}\n"
           f"✇ Share this link; when users start the bot, they’ll count for you.")
    await msg.answer(txt)

@dp.message(Command("refstats"))
async def cmd_refstats(msg: Message):
    uid = msg.from_user.id
    who = get_setting(_ref_key_by(uid), None)
    count = int(get_setting(_ref_key_count(uid), 0) or 0)
    txt = "👥 Referral Stats\n"
    txt += f"✇ Referred by: <code>{who}</code>\n" if who is not None else "✇ Referred by: —\n"
    txt += f"✇ You referred: {count}\n"
    await msg.answer(txt)

@dp.message(Command("reftop"))
async def cmd_reftop(msg: Message):
    try:
        n = int((msg.text.split(maxsplit=1)[1]).strip())
    except Exception:
        n = 10
    n = max(1, min(50, n))
    conn = get_conn()
    rows = conn.execute("SELECT key, val FROM settings WHERE key LIKE 'ref:count:%'").fetchall()
    conn.close()
    pairs = []
    for r in rows:
        try:
            uid = int(r["key"].split(":")[-1])
            cnt_val = r["val"]
            cnt = int(cnt_val) if isinstance(cnt_val, (int, float)) else int(str(cnt_val).strip('"'))
            pairs.append((uid, cnt))
        except Exception:
            continue
    pairs.sort(key=lambda x: x[1], reverse=True)
    pairs = pairs[:n]
    if not pairs:
        await msg.answer("🏆 Referral leaderboard is empty."); return
    lines = [f"{i+1}. <code>{uid}</code> — {cnt}" for i,(uid,cnt) in enumerate(pairs)]
    await msg.answer("🏆 Referral Leaderboard\n" + "\n".join(lines))

# /fstats (per-user forward status)
def _fmt_next(delta_sec: int) -> str:
    if delta_sec <= 0:
        return "soon"
    m, s = divmod(delta_sec, 60)
    h, m = divmod(m, 60)
    parts = []
    if h: parts.append(f"{h}h")
    if m: parts.append(f"{m}m")
    if s and not h: parts.append(f"{s}s")
    return " ".join(parts) if parts else "soon"

@dp.message(Command("fstats"))
async def cmd_fstats(msg: Message):
    uid = msg.from_user.id
    interval = int(get_interval(uid) or 30)
    groups_n = len(list_groups(uid))
    sessions_n = sessions_count_user(uid)
    paused = _is_paused(uid)
    last_ts = get_last_sent_at(uid)
    import time as _t
    now = int(_t.time())
    due = 0
    if last_ts is None:
        due = 0
    else:
        gap = interval*60 - (now - last_ts)
        due = max(0, gap)
    status = "PAUSED" if paused else "RUNNING"
    night = "ON" if night_enabled() else "OFF"
    txt = (
        "📟 Forward Stats\n"
        f"✇ {'⏸' if paused else '▶️'} Worker: {status}\n"
        f"✇ Interval: {interval} min\n"
        f"✇ Sessions: {sessions_n}  |  Groups: {groups_n}\n"
        f"✇ Next send: {_fmt_next(due)}\n"
        f"🌙 Night Mode {night}"
    )
    await msg.answer(txt)

# ---------------- Runner ----------------
async def main():
    try:
        await dp.start_polling(bot)
    except Exception as e:
        log.error(f"polling stopped: {e}")

if __name__ == "__main__":
    asyncio.run(main())
