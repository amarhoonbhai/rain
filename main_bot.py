# main_bot.py — Aiogram v3.x
# Compact iOS-style UI, channel-gate, auto-ack (no spinner),
# Accounts / Groups (cap 5) / Intervals (30/45/60),
# Disclaimer, Owner panel (Night Mode, Stats, Top forwards),
# Owner Broadcast + Upgrade/Downgrade (UI + commands),
# Referral system (/ref /refstats /reftop + /start ref_<id>).
import os, asyncio, logging
from datetime import datetime
from aiogram import Bot, Dispatcher, F, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramBadRequest
from dotenv import load_dotenv

from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext

from core.db import (
    init_db, ensure_user, get_conn,
    # sessions
    sessions_list, sessions_delete, sessions_count, sessions_count_user,
    # groups/interval
    list_groups, add_group, clear_groups, groups_cap,
    set_interval, get_interval,
    # stats
    get_total_sent_ok, users_count, top_users,
    # night mode
    night_enabled, set_night_enabled,
    # gate
    get_gate_channels_effective,
    # settings (for referrals)
    get_setting, set_setting,
    # premium name-lock
    set_name_lock,
)

# -------------------- ENV --------------------
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

BOT_USERNAME = None  # populated on first /start

# -------------------- utils --------------------
def is_owner(uid: int) -> bool:
    return OWNER_ID and int(uid) == OWNER_ID

async def safe_edit_text(message, text, **kw):
    try:
        return await message.edit_text(text, **kw)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            return None
        raise

def _gate_channels():
    ch1, ch2 = get_gate_channels_effective()
    return [c for c in (ch1, ch2) if c]

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
    chs = _gate_channels()
    rows = []
    if len(chs)>=1:
        rows.append([InlineKeyboardButton(text=f"🔗 {chs[0]}", url=f"https://t.me/{chs[0].lstrip('@')}")])
    if len(chs)>=2:
        rows.append([InlineKeyboardButton(text=f"🔗 {chs[1]}", url=f"https://t.me/{chs[1].lstrip('@')}")])
    rows.append([InlineKeyboardButton(text="✅ I've Joined", callback_data="gate:check")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

GATE_TEXT = (
    "✇ Access required\n"
    "✇ Join the channels below to use the bot:\n"
    "  • {ch1}\n"
    "  • {ch2}\n\n"
    "✇ After joining, tap <b>I've Joined</b>."
)

# -------------------- middlewares --------------------
class AutoAckMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        if isinstance(event, CallbackQuery):
            try: await event.answer()
            except Exception: pass
        return await handler(event, data)

class GateGuardMiddleware(BaseMiddleware):
    """Blocks everything except /start and gate:* until user joins channels"""
    async def __call__(self, handler, event, data):
        uid = getattr(getattr(event, "from_user", None), "id", None)
        allow = False
        if isinstance(event, Message):
            if event.text and event.text.startswith("/start"):
                allow = True
        if isinstance(event, CallbackQuery):
            if (event.data or "").startswith("gate:"):
                allow = True
        if allow or not _gate_channels() or not uid:
            return await handler(event, data)
        ok, _ = await _check_gate(uid)
        if ok:
            return await handler(event, data)
        chs = _gate_channels()
        txt = GATE_TEXT.format(
            ch1=chs[0] if len(chs)>0 else "—",
            ch2=chs[1] if len(chs)>1 else "—",
        )
        if isinstance(event, CallbackQuery):
            try:
                await safe_edit_text(event.message, txt, reply_markup=_gate_kb())
            except Exception:
                await bot.send_message(uid, txt, reply_markup=_gate_kb())
        else:
            await bot.send_message(uid, txt, reply_markup=_gate_kb())
        return

dp.update.middleware(AutoAckMiddleware())
dp.update.middleware(GateGuardMiddleware())

# -------------------- referral helpers --------------------
def _ref_key_by(user_id: int) -> str:  # who referred this user
    return f"ref:by:{user_id}"

def _ref_key_count(user_id: int) -> str:  # how many this user referred
    return f"ref:count:{user_id}"

def _ref_set_if_absent(user_id: int, referrer_id: int) -> bool:
    """Return True if recorded now; False if already had a referrer or invalid."""
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

# -------------------- keyboards --------------------
def kb_main(uid: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="👤 Manage Accounts", callback_data="menu:accounts")],
        [InlineKeyboardButton(text="👥 Groups",           callback_data="menu:groups"),
         InlineKeyboardButton(text="⏱ Interval",         callback_data="menu:interval")],
        [InlineKeyboardButton(text="ℹ️ Disclaimer",       callback_data="menu:disc")],
    ]
    if is_owner(uid):
        rows.append([InlineKeyboardButton(text=("🌙 Night: ON" if night_enabled() else "🌙 Night: OFF"),
                                          callback_data="owner:night:toggle")])
        rows.append([InlineKeyboardButton(text="📊 Stats", callback_data="owner:stats"),
                     InlineKeyboardButton(text="🏆 Top 10 (forwards)", callback_data="owner:top")])
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

# -------------------- views --------------------
async def view_home(msg_or_cq, uid: int):
    HOWTO = (
        "✇ How to use\n"
        "  1) ✇ Open @SpinifyLoginBot and add up to 3 accounts\n"
        "  2) ✇ Set interval (30/45/60 min)\n"
        "  3) ✇ Add up to 5 groups\n"
        "  4) ✇ Worker will forward on schedule\n\n"
        "✇ Note: Owner can enable Night Mode (00:00–07:00 IST).\n"
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

# -------------------- FSM for groups and owner flows --------------------
class G(StatesGroup):
    adding = State()

class OwnerFlow(StatesGroup):
    broadcast = State()
    upgrade_user = State()
    upgrade_name = State()
    downgrade_user = State()

# -------------------- handlers --------------------
@dp.message(Command("start"))
async def on_start(msg: Message):
    global BOT_USERNAME
    uid = msg.from_user.id
    ensure_user(uid, getattr(msg.from_user, "username", None))

    # referral capture
    try:
        parts = msg.text.split(maxsplit=1)
        if len(parts) == 2 and parts[1].startswith("ref_"):
            ref_id = int(parts[1][4:])
            if _ref_set_if_absent(uid, ref_id):
                try:
                    await bot.send_message(ref_id, f"🎉 New referral joined: <code>{uid}</code>")
                except Exception:
                    pass
    except Exception:
        pass

    if not BOT_USERNAME:
        await _ensure_bot_username()

    # gate
    if _gate_channels():
        ok, _ = await _check_gate(uid)
        if not ok:
            chs = _gate_channels()
            txt = GATE_TEXT.format(
                ch1=chs[0] if len(chs)>0 else "—",
                ch2=chs[1] if len(chs)>1 else "—",
            )
            await msg.answer(txt, reply_markup=_gate_kb()); return

    await view_home(msg, uid)

@dp.callback_query(F.data == "gate:check")
async def on_gate_check(cq: CallbackQuery):
    uid = cq.from_user.id
    ok, _ = await _check_gate(uid)
    if ok: await view_home(cq, uid)
    else:
        chs=_gate_channels()
        await safe_edit_text(cq.message, GATE_TEXT.format(
            ch1=chs[0] if len(chs)>0 else "—",
            ch2=chs[1] if len(chs)>1 else "—",
        ), reply_markup=_gate_kb())

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

# Accounts: delete slot
@dp.callback_query(F.data.startswith("acct:del:"))
async def cb_acct_del(cq: CallbackQuery):
    uid = cq.from_user.id
    try:
        slot = int(cq.data.split(":")[-1])
        sessions_delete(uid, slot)
    except Exception as e:
        log.error(f"acct del err: {e}")
    await view_accounts(cq)

# Groups flow
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
    # show groups view again
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

# Owner panel core
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
async def cb_night_toggle(cq: CallbackQuery):
    if not is_owner(cq.from_user.id): return
    set_night_enabled(not night_enabled())
    await view_home(cq, cq.from_user.id)

# Owner: Broadcast (UI)
@dp.callback_query(F.data == "owner:broadcast")
async def cb_owner_broadcast(cq: CallbackQuery, state: FSMContext):
    if not is_owner(cq.from_user.id): return
    await state.set_state(OwnerFlow.broadcast)
    await safe_edit_text(cq.message,
        "📣 Broadcast\n"
        "✇ Send the message text now. It will be sent to all users.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅ Cancel", callback_data="menu:home")]
        ]))

@dp.message(OwnerFlow.broadcast)
async def on_broadcast_text(msg: Message, state: FSMContext):
    if not is_owner(msg.from_user.id):
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
    if not is_owner(cq.from_user.id): return
    await safe_edit_text(cq.message, "💎 Premium Controls", reply_markup=kb_owner_upgrade_menu())

@dp.callback_query(F.data == "owner:upgrade:do")
async def cb_owner_upgrade_do(cq: CallbackQuery, state: FSMContext):
    if not is_owner(cq.from_user.id): return
    await state.set_state(OwnerFlow.upgrade_user)
    await safe_edit_text(cq.message, "✇ Send the <code>user_id</code> to upgrade (next message).\n(Then I'll ask for an optional locked name.)",
                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]))

@dp.message(OwnerFlow.upgrade_user)
async def on_upgrade_user(msg: Message, state: FSMContext):
    if not is_owner(msg.from_user.id):
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
    if not is_owner(msg.from_user.id):
        await state.clear(); return
    data = await state.get_data()
    target = data.get("target")
    locked = None if msg.text.strip() == "-" else msg.text.strip()
    set_name_lock(target, True, name=locked)
    await state.clear()
    await msg.answer(f"✅ Premium name-lock enabled for <code>{target}</code>{' with name “'+locked+'”' if locked else ''}.")

@dp.callback_query(F.data == "owner:downgrade:do")
async def cb_owner_downgrade_do(cq: CallbackQuery, state: FSMContext):
    if not is_owner(cq.from_user.id): return
    await state.set_state(OwnerFlow.downgrade_user)
    await safe_edit_text(cq.message, "✇ Send the <code>user_id</code> to downgrade (next message).",
                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]))

@dp.message(OwnerFlow.downgrade_user)
async def on_downgrade_user(msg: Message, state: FSMContext):
    if not is_owner(msg.from_user.id):
        await state.clear(); return
    try:
        target = int(msg.text.strip())
    except Exception:
        await msg.answer("❌ user_id must be an integer. Try again or /cancel."); return
    set_name_lock(target, False)
    await state.clear()
    await msg.answer(f"✅ Premium name-lock disabled for <code>{target}</code>.")

# -------------------- Commands (owner shortcuts) --------------------
@dp.message(Command("broadcast"))
async def cmd_broadcast(msg: Message):
    if not is_owner(msg.from_user.id): return
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

@dp.message(Command("upgrade"))
async def cmd_upgrade(msg: Message):
    if not is_owner(msg.from_user.id): return
    parts = msg.text.split(maxsplit=2)
    if len(parts) < 2:
        await msg.answer("Usage:\n/upgrade <user_id> [locked_name]"); return
    try:
        target = int(parts[1])
    except Exception:
        await msg.answer("❌ user_id must be integer"); return
    locked_name = parts[2] if len(parts) == 3 else None
    set_name_lock(target, True, name=locked_name)
    await msg.answer(f"✅ Premium name-lock enabled for <code>{target}</code>{' with name “'+locked_name+'”' if locked_name else ''}.")

@dp.message(Command("downgrade"))
async def cmd_downgrade(msg: Message):
    if not is_owner(msg.from_user.id): return
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        await msg.answer("Usage:\n/downgrade <user_id>"); return
    try:
        target = int(parts[1])
    except Exception:
        await msg.answer("❌ user_id must be integer"); return
    set_name_lock(target, False)
    await msg.answer(f"✅ Premium name-lock disabled for <code>{target}</code>.")

# -------------------- Referral commands --------------------
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
            # each settings.val is JSON-encoded or raw
            cnt = int(r["val"]) if isinstance(r["val"], (int, float)) else int(str(r["val"]).strip('"'))
            pairs.append((uid, cnt))
        except Exception:
            continue
    pairs.sort(key=lambda x: x[1], reverse=True)
    pairs = pairs[:n]
    if not pairs:
        await msg.answer("🏆 Referral leaderboard is empty."); return
    lines = [f"{i+1}. <code>{uid}</code> — {cnt}" for i,(uid,cnt) in enumerate(pairs)]
    await msg.answer("🏆 Referral Leaderboard\n" + "\n".join(lines))

# -------------------- runner --------------------
async def main():
    try:
        await dp.start_polling(bot)
    except Exception as e:
        log.error(f"polling stopped: {e}")

if __name__ == "__main__":
    asyncio.run(main())
