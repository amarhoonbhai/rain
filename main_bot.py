# main_bot.py — compact iOS-style UI (Aiogram v3.x)
# Features:
# • Gate on /start (defaults: @PhiloBots, @TheTrafficZone) — set via REQUIRED_CHANNELS CSV
# • Start/Pause toggle + /pause /resume
# • /fstats shows next send ETA (hh:mm:ss), sessions, groups, night-mode state
# • Intervals: 30/45/60 minutes
# • Groups: save ANY handle/ID/link (no validation); private invites require you to join manually
# • Unlock 10 groups if user joins UNLOCK_GC_USERNAME (env)
# • “Saved → PINNED” ad flow (worker forwards pinned message from each session)
# • Owner: Night toggle, Stats, Top 10 (UI + /stats /top)
# • Referrals: /ref /refstats /reftop + start payload ref_<id>

import os, asyncio, logging
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, StateFilter
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from dotenv import load_dotenv

from core.db import (
    init_db, ensure_user, get_conn,
    sessions_list, sessions_delete, sessions_count, sessions_count_user,
    list_groups, add_group, clear_groups,
    set_interval, get_interval,
    get_total_sent_ok, users_count, top_users,
    night_enabled, set_night_enabled,
    set_setting, get_setting,
    get_last_sent_at,
)

# ---------------- ENV / BOOT ----------------
load_dotenv()
TOKEN = (os.getenv("MAIN_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
UNLOCK_GC_USERNAME = os.getenv("UNLOCK_GC_USERNAME", "").strip()  # e.g. @YourPremiumGC
REQUIRED_CHANNELS = [c.strip() for c in os.getenv("REQUIRED_CHANNELS", "@PhiloBots,@TheTrafficZone").split(",") if c.strip()]

if not TOKEN or ":" not in TOKEN:
    raise RuntimeError("MAIN_BOT_TOKEN missing/malformed.")

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
init_db()

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"))
log = logging.getLogger("main_bot")

IST = ZoneInfo("Asia/Kolkata")
NIGHT_START = time(0, 0)
NIGHT_END   = time(7, 0)

BOT_USERNAME = None  # lazy-cached for /ref link

# -------------- Helpers --------------
def is_owner(uid: int) -> bool:
    return OWNER_ID and int(uid) == OWNER_ID

async def safe_edit_text(message, text, **kw):
    try:
        return await message.edit_text(text, **kw)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            return None
        raise

async def _is_member(chat: str, user_id: int) -> bool:
    if not chat: return False
    try:
        m = await bot.get_chat_member(chat, user_id)
        st = str(getattr(m, "status", "left")).lower()
        return st not in {"left", "kicked"}
    except Exception:
        return False

async def groups_cap_for(uid: int) -> int:
    # Default 5; unlock 10 if member of UNLOCK_GC_USERNAME
    if UNLOCK_GC_USERNAME and await _is_member(UNLOCK_GC_USERNAME, uid):
        return 10
    return 5

def _is_paused(uid: int) -> bool:
    v = str(get_setting(f"user:{uid}:paused", "0")).lower()
    return v in ("1","true","yes","on")

def _set_paused(uid: int, on: bool):
    set_setting(f"user:{uid}:paused", "1" if on else "0")

def _is_night_now_ist() -> bool:
    now = datetime.now(IST).time()
    return NIGHT_START <= now < NIGHT_END

def _next_7am_ist_from(now_ist: datetime) -> datetime:
    target = now_ist.replace(hour=7, minute=0, second=0, microsecond=0)
    if now_ist.time() >= NIGHT_END:
        return target + timedelta(days=1)
    return target

def _fmt_hms(sec: int) -> str:
    sec = max(0, int(sec))
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    if h: return f"{h}h {m}m {s}s"
    if m: return f"{m}m {s}s"
    return f"{s}s"

# -------------- Gate (required channels on /start) --------------
async def _gate_ok(uid: int):
    missing = []
    for ch in REQUIRED_CHANNELS:
        if not await _is_member(ch, uid):
            missing.append(ch)
    return (len(missing) == 0), missing

def _gate_kb(missing: list[str]):
    rows = [[InlineKeyboardButton(text=f"🔗 {ch}", url=f"https://t.me/{ch.lstrip('@')}")] for ch in missing]
    rows.append([InlineKeyboardButton(text="✅ I've Joined", callback_data="gate:check")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def _gate_text(missing: list[str]) -> str:
    lines = "\n".join(f"  • {ch}" for ch in missing)
    return (
        "✇ Access required\n"
        "✇ Join the channels below to use the bot:\n"
        f"{lines}\n\n"
        "✇ After joining, tap <b>I've Joined</b>."
    )

class AutoAckMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        if isinstance(event, CallbackQuery):
            try: await event.answer()
            except Exception: pass
        return await handler(event, data)

class GateGuardMiddleware(BaseMiddleware):
    """Block everything except /start and gate:* until joined REQUIRED_CHANNELS."""
    async def __call__(self, handler, event, data):
        uid = getattr(getattr(event, "from_user", None), "id", None)
        allow = False
        if isinstance(event, Message) and event.text and event.text.startswith("/start"):
            allow = True
        if isinstance(event, CallbackQuery) and (event.data or "").startswith("gate:"):
            allow = True
        if allow or not uid:
            return await handler(event, data)
        ok, miss = await _gate_ok(uid)
        if ok:
            return await handler(event, data)
        # Show gate
        if isinstance(event, CallbackQuery):
            try:
                await safe_edit_text(event.message, _gate_text(miss), reply_markup=_gate_kb(miss))
            except Exception:
                await bot.send_message(uid, _gate_text(miss), reply_markup=_gate_kb(miss))
        else:
            await bot.send_message(uid, _gate_text(miss), reply_markup=_gate_kb(miss))
        return

dp.update.middleware(AutoAckMiddleware())
dp.update.middleware(GateGuardMiddleware())

# -------------- Referrals --------------
def _ref_key_by(user_id: int) -> str:      # who referred this user
    return f"ref:by:{user_id}"

def _ref_key_count(user_id: int) -> str:   # how many this user referred
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
    paused = _is_paused(uid)
    rows = [
        [InlineKeyboardButton(text=("▶️ Start" if paused else "⏸ Pause"), callback_data="user:togglepause"),
         InlineKeyboardButton(text="📟 Forward Stats", callback_data="menu:fstats")],
        [InlineKeyboardButton(text="👤 Manage Accounts", callback_data="menu:accounts")],
        [InlineKeyboardButton(text="👥 Groups",           callback_data="menu:groups"),
         InlineKeyboardButton(text="⏱ Interval",         callback_data="menu:interval")],
        [InlineKeyboardButton(text="📝 Set Message (PINNED)", callback_data="menu:msginfo")],
        [InlineKeyboardButton(text="ℹ️ Disclaimer",       callback_data="menu:disc")],
    ]
    if is_owner(uid):
        rows.append([InlineKeyboardButton(text=("🌙 Night: ON" if night_enabled() else "🌙 Night: OFF"),
                                          callback_data="owner:night:toggle")])
        rows.append([InlineKeyboardButton(text="📊 Stats", callback_data="owner:stats"),
                     InlineKeyboardButton(text="🏆 Top 10", callback_data="owner:top")])
    rows.append([InlineKeyboardButton(text="🔄 Refresh", callback_data="menu:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_groups(uid: int, cap: int, unlocked: bool) -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton(text="➕ Add Group", callback_data="groups:add"),
         InlineKeyboardButton(text="🧹 Clear",     callback_data="groups:clear")],
    ]
    if UNLOCK_GC_USERNAME:
        label = "🔓 Unlock 10 Groups" if not unlocked else "✅ 10 Groups Unlocked"
        row = [InlineKeyboardButton(text=label, url=f"https://t.me/{UNLOCK_GC_USERNAME.lstrip('@')}")]
        if not unlocked:
            row.append(InlineKeyboardButton(text="✅ I've Joined", callback_data="unlock:check"))
        kb.append(row)
    kb.append([InlineKeyboardButton(text="🔄 Refresh",   callback_data="menu:groups"),
               InlineKeyboardButton(text="⬅ Back",       callback_data="menu:home")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def kb_intervals(cur: int|None) -> InlineKeyboardMarkup:
    def chip(v):
        label = f"{v}m" + (" ✅" if cur==v else "")
        return InlineKeyboardButton(text=label, callback_data=f"interval:set:{v}")
    return InlineKeyboardMarkup(inline_keyboard=[
        [chip(30), chip(45), chip(60)],
        [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]
    ])

def kb_msg_info() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ I’ve pinned my ad", callback_data="msg:saved-confirm")],
        [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]
    ])

# -------------- Views --------------
async def view_home(msg_or_cq, uid: int):
    have_sessions = sessions_count_user(uid) > 0
    session_line = "✇ Sessions: ✅" if have_sessions else "✇ Sessions: ❌ (Add via @SpinifyLoginBot)"
    HOWTO = (
        "✇ How it works\n"
        "  1) Login accounts in @SpinifyLoginBot (up to 3)\n"
        "  2) Add groups (ANY handle/ID/link). We just save it.\n"
        "  3) Set interval 30/45/60 minutes\n"
        "  4) In Saved Messages, send your ad and PIN it\n"
        "  5) Press ▶ Start to begin forwarding\n\n"
        f"{session_line}\n"
        "✇ Private invites require YOU to join with the sender account(s) first."
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
    kb = [
        ([InlineKeyboardButton(text=f"🗑 Remove S{s['slot']}", callback_data=f"acct:del:{s['slot']}") for s in slots]
         if slots else [InlineKeyboardButton(text="➕ Add via @SpinifyLoginBot", url="https://t.me/SpinifyLoginBot")]),
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="menu:accounts"),
         InlineKeyboardButton(text="⬅ Back",   callback_data="menu:home")]
    ]
    await safe_edit_text(cq.message, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

async def view_groups(cq: CallbackQuery):
    uid = cq.from_user.id
    gs = list_groups(uid)
    cap = await groups_cap_for(uid)
    unlocked = (cap >= 10)
    if gs:
        text = f"👥 Groups (max {cap})\n" + "\n".join(f"• {g}" for g in gs)
    else:
        text = f"👥 Groups (max {cap})\n✇ No groups yet. Add one."
    await safe_edit_text(cq.message, text, reply_markup=kb_groups(uid, cap, unlocked))

async def view_interval(cq: CallbackQuery):
    uid = cq.from_user.id
    cur = get_interval(uid)
    text = "⏱ Interval\n✇ Choose how often to forward:"
    await safe_edit_text(cq.message, text, reply_markup=kb_intervals(cur))

async def view_disclaimer(cq: CallbackQuery):
    text = (
        "⚠️ Disclaimer (Free Version)\n"
        "✇ Use at your own risk.\n"
        "✇ If your Telegram account is limited/terminated, we are not responsible.\n"
        "✇ Follow Telegram terms and local laws. Avoid spam/abuse."
    )
    await safe_edit_text(cq.message, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]
    ]))

async def view_msg_info(cq: CallbackQuery):
    txt = (
        "📝 Set Message (Saved → PINNED)\n"
        "✇ Open your Telegram account (sender).\n"
        "✇ Go to <b>Saved Messages</b>.\n"
        "✇ Send your ad text OR media+caption (premium emoji OK).\n"
        "✇ <b>PIN</b> that message. The worker forwards EXACTLY this.\n\n"
        "✇ After you pin, tap “I’ve pinned my ad”."
    )
    await safe_edit_text(cq.message, txt, reply_markup=kb_msg_info())

# -------------- FSM --------------
class G(StatesGroup):
    adding = State()

# -------------- Forward Stats --------------
def _forward_stats_text(uid: int) -> str:
    paused = _is_paused(uid)
    interval = get_interval(uid) or 30
    last = get_last_sent_at(uid)
    now = int(datetime.utcnow().timestamp())

    # Night mode
    if night_enabled() and _is_night_now_ist():
        now_ist = datetime.now(IST)
        nxt = _next_7am_ist_from(now_ist)
        wait = int((nxt - now_ist).total_seconds())
        nm = f"🌙 Night Mode ON — resumes at 07:00 IST (~{_fmt_hms(wait)})"
    else:
        nm = "🌙 Night Mode OFF"

    if paused:
        status = "⏸ Worker: PAUSED"
        next_txt = "—"
    else:
        status = "▶️ Worker: RUNNING"
        if last is None:
            next_txt = "Due now"
        else:
            remain = interval*60 - (now - int(last))
            next_txt = "Due now" if remain <= 0 else f"in ~{_fmt_hms(remain)}"

    ses = sessions_count_user(uid)
    gs  = len(list_groups(uid))
    return (
        "📟 Forward Stats\n"
        f"✇ {status}\n"
        f"✇ Interval: {interval} min\n"
        f"✇ Sessions: {ses}  |  Groups: {gs}\n"
        f"✇ Next send: {next_txt}\n"
        f"{nm}"
    )

# -------------- Handlers --------------
@dp.message(Command("start"))
async def on_start(msg: Message):
    uid = msg.from_user.id
    ensure_user(uid, getattr(msg.from_user, "username", None))

    # capture referral payload: /start ref_<id>
    try:
        parts = msg.text.split(maxsplit=1)
        if len(parts) == 2 and parts[1].startswith("ref_"):
            ref_id = int(parts[1][4:])
            if _ref_set_if_absent(uid, ref_id):
                try: await bot.send_message(ref_id, f"🎉 New referral joined: <code>{uid}</code>")
                except Exception: pass
    except Exception:
        pass

    ok, miss = await _gate_ok(uid)
    if not ok:
        await msg.answer(_gate_text(miss), reply_markup=_gate_kb(miss))
        return
    await view_home(msg, uid)

@dp.callback_query(F.data == "gate:check")
async def on_gate_check(cq: CallbackQuery):
    uid = cq.from_user.id
    ok, miss = await _gate_ok(uid)
    if ok: await view_home(cq, uid)
    else:  await safe_edit_text(cq.message, _gate_text(miss), reply_markup=_gate_kb(miss))

@dp.callback_query(F.data == "menu:home")
async def cb_home(cq: CallbackQuery):
    await view_home(cq, cq.from_user.id)

@dp.callback_query(F.data == "menu:fstats")
async def cb_fstats(cq: CallbackQuery):
    uid = cq.from_user.id
    await safe_edit_text(cq.message, _forward_stats_text(uid), reply_markup=kb_main(uid))

@dp.callback_query(F.data == "user:togglepause")
async def cb_toggle_pause(cq: CallbackQuery):
    uid = cq.from_user.id
    _set_paused(uid, not _is_paused(uid))
    await view_home(cq, uid)

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

@dp.callback_query(F.data == "menu:msginfo")
async def cb_msginfo(cq: CallbackQuery):
    await view_msg_info(cq)

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
    cap = await groups_cap_for(cq.from_user.id)
    gs = list_groups(cq.from_user.id)
    if len(gs) >= cap:
        await safe_edit_text(cq.message, f"👥 Groups (max {cap})\n✇ Limit reached. Unlock 10 groups to add more.",
                             reply_markup=kb_groups(cq.from_user.id, cap, cap>=10))
        return
    await state.set_state(G.adding)
    await safe_edit_text(cq.message,
        "✇ Send ANY group reference to save (we do not validate):\n"
        "• @username  |  numeric ID  |  https://t.me/username  |  private invite link\n\n"
        "⚠️ <b>Reminder</b>: Private links require you to JOIN the group with your sender account(s)."
    )

@dp.message(StateFilter(G.adding))
async def on_group_text(msg: Message, state: FSMContext):
    uid = msg.from_user.id
    cap = await groups_cap_for(uid)
    gs = list_groups(uid)
    if len(gs) >= cap:
        await state.clear()
        await msg.answer(f"👥 Groups (max {cap})\n✇ Limit reached.")
        return
    value = (msg.text or "").strip()
    try:
        added = add_group(uid, value)
        if added:
            await msg.answer(
                "✅ Saved.\n"
                "⚠️ If this is a private invite, you must manually join with the sending account."
            )
        else:
            await msg.answer("ℹ️ Already saved (or empty).")
    except Exception as e:
        await msg.answer(f"❌ Failed: <code>{e}</code>")
    await state.clear()
    gs2 = list_groups(uid)
    cap2 = await groups_cap_for(uid)
    unlocked = (cap2 >= 10)
    text = (f"👥 Groups (max {cap2})\n" + "\n".join(f"• {g}" for g in gs2)) if gs2 else f"👥 Groups (max {cap2})\n✇ No groups yet. Add one."
    await msg.answer(text, reply_markup=kb_groups(uid, cap2, unlocked))

@dp.callback_query(F.data == "groups:clear")
async def cb_groups_clear(cq: CallbackQuery):
    clear_groups(cq.from_user.id)
    await view_groups(cq)

# Unlock 10 groups verify
@dp.callback_query(F.data == "unlock:check")
async def cb_unlock_check(cq: CallbackQuery):
    uid = cq.from_user.id
    cap = await groups_cap_for(uid)
    unlocked = (cap >= 10)
    gs = list_groups(uid)
    text = (f"👥 Groups (max {cap})\n" + "\n".join(f"• {g}" for g in gs)) if gs else f"👥 Groups (max {cap})\n✇ No groups yet. Add one."
    await safe_edit_text(cq.message, text, reply_markup=kb_groups(uid, cap, unlocked))

# Interval set
@dp.callback_query(F.data.startswith("interval:set:"))
async def cb_set_interval(cq: CallbackQuery):
    uid = cq.from_user.id
    mins = int(cq.data.split(":")[-1])
    if mins not in (30,45,60):
        await safe_edit_text(cq.message, "❌ Allowed: 30, 45, 60 minutes", reply_markup=kb_intervals(get_interval(uid))); return
    set_interval(uid, mins)
    await safe_edit_text(cq.message, f"⏱ Interval set to {mins} minutes ✅", reply_markup=kb_intervals(mins))

# Owner panel
@dp.callback_query(F.data == "owner:night:toggle")
async def cb_night_toggle(cq: CallbackQuery):
    if not is_owner(cq.from_user.id): return
    set_night_enabled(not night_enabled())
    await view_home(cq, cq.from_user.id)

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

# Commands: pause/resume/fstats + owner stats/top
@dp.message(Command("pause"))
async def cmd_pause(msg: Message):
    uid = msg.from_user.id
    _set_paused(uid, True)
    await msg.answer("⏸ Paused. Use /resume to continue.", reply_markup=kb_main(uid))

@dp.message(Command("resume"))
async def cmd_resume(msg: Message):
    uid = msg.from_user.id
    _set_paused(uid, False)
    await msg.answer("▶️ Resumed. Worker will follow your interval.", reply_markup=kb_main(uid))

@dp.message(Command("fstats"))
async def cmd_fstats(msg: Message):
    uid = msg.from_user.id
    await msg.answer(_forward_stats_text(uid), reply_markup=kb_main(uid))

@dp.message(Command("stats"))
async def cmd_stats(msg: Message):
    if not is_owner(msg.from_user.id): return
    await msg.answer(f"📊 Users: {users_count()} | Active (≥1 session): {sessions_count()} | Forwarded: {get_total_sent_ok()}")

@dp.message(Command("top"))
async def cmd_top(msg: Message):
    if not is_owner(msg.from_user.id): return
    try:
        n = int((msg.text.split(maxsplit=1)[1]).strip())
    except Exception:
        n = 10
    rows = top_users(n)
    if not rows:
        await msg.answer("🏆 No data yet."); return
    lines = [f"{i+1}. <code>{r['user_id']}</code> — {r['sent_ok']} msgs" for i,r in enumerate(rows)]
    await msg.answer("🏆 Top Users (forwards)\n" + "\n".join(lines))

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

# -------------- Runner --------------
async def main():
    try:
        await dp.start_polling(bot)
    except Exception as e:
        log.error(f"polling stopped: {e}")

if __name__ == "__main__":
    asyncio.run(main())
