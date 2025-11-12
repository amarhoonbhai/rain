# main_bot.py — Compact iOS-style UI (Aiogram v3.x)
# Mode: Saved-All via user self-commands (.addgc/.gc/.time/.adreset)
# - Channel gate: requires joining @PhiloBots and @TheTrafficZone (or your custom pair)
# - Manage Accounts: delete slots; add via @SpinifyLoginBot
# - Unlock GC: show private/public invite link; on "I've Joined" -> raises groups cap to 10
# - Commands view: how to use self-commands from logged-in account
# - /fstats (everyone), /stats (owner), /top N, owner Night toggle, Broadcast, Premium (name-lock + 50 GC cap)
# - Disclaimer

import os
import asyncio
import logging
from datetime import datetime, timezone

from aiogram import Bot, Dispatcher, F, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from dotenv import load_dotenv

from core.db import (
    init_db, get_conn,
    ensure_user,
    # sessions
    sessions_list, sessions_delete, sessions_count_user, sessions_count,
    # saved groups cap
    list_groups, groups_cap,
    # schedule
    get_interval, get_last_sent_at,
    # stats
    users_count, get_total_sent_ok, top_users,
    # gate & settings
    get_gate_channels_effective, set_setting, get_setting,
    # night mode
    night_enabled, set_night_enabled,
    # premium (name lock flag only; enforcement done elsewhere)
    set_name_lock,
)

# ---------------- ENV / BOOT ----------------
load_dotenv()
TOKEN = (os.getenv("MAIN_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
UNLOCK_GC_LINK = os.getenv("UNLOCK_GC_LINK", "").strip()  # can be private invite; we trust manual join

if not TOKEN or ":" not in TOKEN:
    raise RuntimeError("MAIN_BOT_TOKEN missing/malformed.")

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
init_db()

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("main_bot")

BOT_USERNAME = None  # filled on first /start

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

def _defaults_gate_if_empty(chs: list[str]) -> list[str]:
    return chs or ["@PhiloBots", "@TheTrafficZone"]

def _gate_channels() -> list[str]:
    ch1, ch2 = get_gate_channels_effective()
    chs = [c for c in (ch1, ch2) if c]
    return _defaults_gate_if_empty(chs)

async def _check_gate(user_id: int):
    missing = []
    for ch in _gate_channels():
        try:
            m = await bot.get_chat_member(ch, user_id)
            if str(getattr(m, "status", "left")).lower() in {"left", "kicked"}:
                missing.append(ch)
        except Exception:
            # if bot can't access (private/blocked), still require join by showing link
            missing.append(ch)
    return (len(missing) == 0), missing

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

# ---------- Next-send ETA helpers ----------
def _fmt_eta(seconds: int) -> str:
    if seconds <= 0:
        return "now"
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    parts = []
    if h: parts.append(f"{h}h")
    if m: parts.append(f"{m}m")
    if s or not parts: parts.append(f"{s}s")
    return "in ~" + " ".join(parts)

def _next_send_eta(uid: int) -> str:
    try:
        last = get_last_sent_at(uid)  # epoch (UTC) or None
        interval_min = get_interval(uid) or 30
        now = int(datetime.now(timezone.utc).timestamp())
        if last is None:
            return "now"
        remain = interval_min * 60 - (now - int(last))
        return _fmt_eta(remain)
    except Exception:
        return "—"

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
                await safe_edit_text(event.message, _gate_text(), reply_markup=_gate_kb())
            except Exception:
                await bot.send_message(uid, _gate_text(), reply_markup=_gate_kb())
        else:
            await bot.send_message(uid, _gate_text(), reply_markup=_gate_kb())
        return

dp.update.middleware(AutoAckMiddleware())
dp.update.middleware(GateGuardMiddleware())

# -------------- Keyboards --------------
def kb_main(uid: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="👤 Manage Accounts", callback_data="menu:accounts")],
        [InlineKeyboardButton(text="📜 Commands",        callback_data="menu:cmds"),
         InlineKeyboardButton(text="🔓 Unlock GC",      callback_data="menu:unlock")],
        [InlineKeyboardButton(text="ℹ️ Disclaimer",      callback_data="menu:disc")],
    ]
    if is_owner(uid):
        rows.append([InlineKeyboardButton(text=("🌙 Night: ON" if night_enabled() else "🌙 Night: OFF"),
                                          callback_data="owner:night:toggle")])
        rows.append([InlineKeyboardButton(text="📊 Stats", callback_data="owner:stats"),
                     InlineKeyboardButton(text="🏆 Top 10", callback_data="owner:top")])
        rows.append([InlineKeyboardButton(text="📣 Broadcast", callback_data="owner:broadcast"),
                     InlineKeyboardButton(text="💎 Premium",   callback_data="owner:upgrade")])
    rows.append([InlineKeyboardButton(text="🔄 Refresh", callback_data="menu:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_accounts(slots) -> InlineKeyboardMarkup:
    row1 = [InlineKeyboardButton(text=f"🗑 Remove S{s['slot']}", callback_data=f"acct:del:{s['slot']}") for s in slots]
    if not row1:
        row1 = [InlineKeyboardButton(text="➕ Add via @SpinifyLoginBot", url="https://t.me/SpinifyLoginBot")]
    rows = [row1,
            [InlineKeyboardButton(text="🔄 Refresh", callback_data="menu:accounts"),
             InlineKeyboardButton(text="⬅ Back",   callback_data="menu:home")]]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_unlock() -> InlineKeyboardMarkup:
    rows = []
    if UNLOCK_GC_LINK:
        rows.append([InlineKeyboardButton(text="🔗 Join Unlock GC", url=UNLOCK_GC_LINK)])
    rows.append([InlineKeyboardButton(text="✅ I've Joined", callback_data="unlock:confirm")])
    rows.append([InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_owner_upgrade_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💎 Upgrade User",   callback_data="owner:upgrade:do")],
        [InlineKeyboardButton(text="🧹 Downgrade User", callback_data="owner:downgrade:do")],
        [InlineKeyboardButton(text="⬅ Back",           callback_data="menu:home")]
    ])

# -------------- Views --------------
async def _ensure_bot_username():
    global BOT_USERNAME
    if not BOT_USERNAME:
        me = await bot.get_me()
        BOT_USERNAME = me.username

def _commands_text() -> str:
    return (
        "📜 Commands (use from your <b>logged-in account</b>)\n"
        "✇ <code>.help</code> — show help\n"
        "✇ <code>.addgc</code> &lt;targets&gt; — add @usernames, numeric IDs, or ANY t.me link (private invites are stored; join manually)\n"
        "✇ <code>.gc</code> — list saved targets (cap 5, or 10 after Unlock)\n"
        "✇ <code>.time</code> 30m|45m|60m — set interval\n"
        "✇ <code>.adreset</code> — restart the Saved-messages cycle\n\n"
        "✇ Put your ads (text/media, premium emoji OK) in <b>Saved Messages</b>. The worker cycles through them every interval and copies to all targets with ~10s gap."
    )

async def view_home(msg_or_cq, uid: int):
    have_sessions = sessions_count_user(uid) > 0
    groups = len(list_groups(uid))
    interval = get_interval(uid) or 30
    next_line = "now" if groups == 0 or not have_sessions else _next_send_eta(uid)
    info = (
        "✇ Welcome\n"
        "✇ Use @SpinifyLoginBot to add up to 3 accounts.\n"
        "✇ Then send <code>.addgc</code>, <code>.gc</code>, <code>.time</code> etc. from your logged-in account.\n\n"
        f"✇ Sessions: {'✅' if have_sessions else '❌'}\n"
        f"✇ Groups saved: {groups}/{groups_cap(uid)}\n"
        f"✇ Interval: {interval} min\n"
        f"✇ Next send: {next_line}\n"
        f"🌙 Night Mode: {'ON' if night_enabled() else 'OFF'}"
    )
    if isinstance(msg_or_cq, Message):
        await msg_or_cq.answer(info, reply_markup=kb_main(uid))
    else:
        await safe_edit_text(msg_or_cq.message, info, reply_markup=kb_main(uid))

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

async def view_unlock(cq: CallbackQuery):
    cap_now = groups_cap(cq.from_user.id)
    txt = (
        "🔓 Unlock GC\n"
        "✇ Join the GC below to unlock 10 target slots (from 5).\n"
        "✇ Private invites are supported — just join manually, then tap “I've Joined”.\n\n"
        f"✇ Your current cap: {cap_now}"
    )
    await safe_edit_text(cq.message, txt, reply_markup=kb_unlock())

async def view_commands(cq: CallbackQuery):
    await safe_edit_text(cq.message, _commands_text(), reply_markup=InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]
    ))

async def view_disclaimer(cq: CallbackQuery):
    text = (
        "⚠️ Disclaimer (Free Version)\n"
        "✇ Use at your own risk.\n"
        "✇ If your Telegram ID gets terminated, I am not responsible.\n"
        "✇ You must comply with Telegram’s Terms and local laws.\n"
        "✇ Excessive spam/abuse may lead to account limitations."
    )
    await safe_edit_text(cq.message, text, reply_markup=InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]
    ))

# -------------- FSM (Owner flows) --------------
class OwnerFlow(StatesGroup):
    broadcast = State()
    upgrade_user = State()
    upgrade_name = State()
    downgrade_user = State()

# -------------- Commands/Callbacks --------------
@dp.message(Command("start"))
async def on_start(msg: Message):
    global BOT_USERNAME
    uid = msg.from_user.id
    ensure_user(uid, getattr(msg.from_user, "username", None))
    if not BOT_USERNAME:
        await _ensure_bot_username()

    # Channel gate first
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
    else:  await safe_edit_text(cq.message, _gate_text(), reply_markup=_gate_kb())

@dp.callback_query(F.data == "menu:home")
async def cb_home(cq: CallbackQuery):
    await view_home(cq, cq.from_user.id)

@dp.callback_query(F.data == "menu:accounts")
async def cb_accounts(cq: CallbackQuery):
    await view_accounts(cq)

@dp.callback_query(F.data == "menu:cmds")
async def cb_cmds(cq: CallbackQuery):
    await view_commands(cq)

@dp.callback_query(F.data == "menu:unlock")
async def cb_unlock(cq: CallbackQuery):
    await view_unlock(cq)

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

# Unlock confirm (trust-based; supports private invites)
@dp.callback_query(F.data == "unlock:confirm")
async def cb_unlock_confirm(cq: CallbackQuery):
    uid = cq.from_user.id
    # Raise personal cap to 10; groups_cap() should read this
    set_setting(f"groups_cap:{uid}", 10)
    await safe_edit_text(cq.message, f"✅ Unlocked. Your group cap is now {groups_cap(uid)}.", reply_markup=InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]
    ))

# ---------- /fstats (everyone) ----------
@dp.message(Command("fstats"))
async def cmd_fstats(msg: Message):
    uid = msg.from_user.id
    try:
        sessions = sessions_count_user(uid)
    except Exception:
        sessions = 0
    try:
        groups = len(list_groups(uid))
    except Exception:
        groups = 0
    interval = get_interval(uid) or 30
    status = "RUNNING" if sessions > 0 else "IDLE"
    eta = "—" if sessions == 0 or groups == 0 else _next_send_eta(uid)
    night_line = "🌙 Night Mode OFF" if not night_enabled() else "🌙 Night Mode ON"

    text = (
        "📟 Forward Stats\n"
        f"✇ ▶️ Worker: {status}\n"
        f"✇ Interval: {interval} min\n"
        f"✇ Sessions: {sessions}  |  Groups: {groups}\n"
        f"✇ Next send: {eta}\n"
        f"{night_line}"
    )
    await msg.answer(text)

# ---------- /stats (owner) ----------
@dp.message(Command("stats"))
async def cmd_stats(msg: Message):
    if not is_owner(msg.from_user.id):
        return
    try:
        total_users = users_count()
    except Exception:
        total_users = 0
    try:
        active_users = sessions_count()  # users with ≥1 session
    except Exception:
        active_users = 0
    try:
        forwarded = get_total_sent_ok()
    except Exception:
        forwarded = 0
    text = (
        "📊 Global Stats\n"
        f"✇ Users: {total_users}\n"
        f"✇ Active (≥1 session): {active_users}\n"
        f"✇ Total forwarded: {forwarded}\n"
        f"🌙 Night Mode: {'ON' if night_enabled() else 'OFF'}"
    )
    await msg.answer(text)

# ---------- /top (owner) ----------
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
    lines = [f"{i+1}. <code>{r['user_id']}</code> — {r['sent_ok']} msgs" for i, r in enumerate(rows)]
    await msg.answer("🏆 Top Users (forwards)\n" + "\n".join(lines))

# ---------- Owner toggles ----------
@dp.callback_query(F.data == "owner:night:toggle")
async def cb_night_toggle(cq: CallbackQuery):
    if not is_owner(cq.from_user.id): return
    set_night_enabled(not night_enabled())
    await view_home(cq, cq.from_user.id)

# ---------- Owner Broadcast ----------
@dp.callback_query(F.data == "owner:broadcast")
async def cb_owner_broadcast(cq: CallbackQuery, state: FSMContext):
    if not is_owner(cq.from_user.id): return
    await state.set_state(OwnerFlow.broadcast)
    await safe_edit_text(
        cq.message,
        "📣 Broadcast\n✇ Send the message text now. It will be sent to all users.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]
        )
    )

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
            await asyncio.sleep(1.0)
    await state.clear()
    await msg.answer(f"✅ Done. Sent: {ok} | Failed: {bad}")

# ---------- Owner Premium (name-lock + 50 cap) ----------
@dp.callback_query(F.data == "owner:upgrade")
async def cb_owner_upgrade_menu(cq: CallbackQuery):
    if not is_owner(cq.from_user.id): return
    await safe_edit_text(cq.message, "💎 Premium Controls", reply_markup=kb_owner_upgrade_menu())

@dp.callback_query(F.data == "owner:upgrade:do")
async def cb_owner_upgrade_do(cq: CallbackQuery, state: FSMContext):
    if not is_owner(cq.from_user.id): return
    await state.set_state(OwnerFlow.upgrade_user)
    await safe_edit_text(
        cq.message,
        "✇ Send the <code>user_id</code> to upgrade (next message).\n(Then I'll ask for an optional locked name.)",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]])
    )

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
    # raise cap to 50 for premium
    set_setting(f"groups_cap:{target}", 50)
    await state.clear()
    await msg.answer(f"✅ Premium enabled for <code>{target}</code> (cap=50){' with name “'+locked+'”' if locked else ''}.")

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
        await msg.answer("❌ user_id must be integer"); return
    set_name_lock(target, False)
    # reset to default free cap (5)
    set_setting(f"groups_cap:{target}", 5)
    await state.clear()
    await msg.answer(f"✅ Premium disabled for <code>{target}</code> (cap=5).")

# -------------- Runner --------------
async def main():
    try:
        await dp.start_polling(bot)
    except Exception as e:
        log.error(f"polling stopped: {e}")

if __name__ == "__main__":
    asyncio.run(main())
