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
    init_db, get_conn, ensure_user,
    sessions_list, sessions_delete, sessions_count_user, sessions_count,
    list_groups, groups_cap, get_interval, get_last_sent_at,
    users_count, get_total_sent_ok, top_users,
    get_gate_channels_effective, set_setting, get_setting,
    night_enabled, set_night_enabled, set_name_lock,
    is_premium, set_premium,
)

# =========================
# Bootstrap
# =========================
load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("main-bot")

TOKEN = (os.getenv("MAIN_BOT_TOKEN") or "").strip()
if not TOKEN or ":" not in TOKEN:
    raise RuntimeError("MAIN_BOT_TOKEN missing")

OWNER_ID = int(os.getenv("OWNER_ID", "0"))
UNLOCK_GC_LINK = os.getenv("UNLOCK_GC_LINK", "").strip()
# Contact shown everywhere for premium upgrades
PREMIUM_CONTACT = os.getenv("PREMIUM_CONTACT", "@spinify")

bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
init_db()


# =========================
# Helpers
# =========================
def is_owner(uid: int) -> bool:
    return OWNER_ID and uid == OWNER_ID


def _gate_channels():
    ch1, ch2 = get_gate_channels_effective()
    return [c for c in (ch1, ch2) if c]


async def _check_gate(user_id: int):
    missing = []
    for ch in _gate_channels():
        try:
            m = await bot.get_chat_member(ch, user_id)
            if str(getattr(m, "status", "left")).lower() in {"left", "kicked"}:
                missing.append(ch)
        except Exception:
            missing.append(ch)
    return (len(missing) == 0), missing


def _gate_text():
    lines = "\n".join(f"  • {c}" for c in _gate_channels())
    return (
        "✇ Access required\n"
        "✇ Join these channels then tap <b>I've Joined</b>:\n"
        f"{lines}"
    )


def _gate_kb():
    rows = [
        [InlineKeyboardButton(text=f"🔗 {c}", url=f"https://t.me/{c.lstrip('@')}")]
        for c in _gate_channels()
    ]
    rows.append([InlineKeyboardButton(text="✅ I've Joined", callback_data="gate:check")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _format_eta(uid: int) -> str:
    """
    Human-friendly ETA for "Next send":
      - If never sent: "in ~Xm (first cycle)"
      - If due / almost due: "very soon"
      - Else: "in ~Xm Ys"
    """
    last = get_last_sent_at(uid)
    interval = get_interval(uid) or 30  # minutes

    # Never sent yet
    if last is None:
        return f"in ~{interval}m (first cycle)"

    now = int(datetime.now(timezone.utc).timestamp())
    left = interval * 60 - (now - int(last))

    # Already due or almost due
    if left <= 5:
        return "very soon"

    h, rem = divmod(left, 3600)
    m, s = divmod(rem, 60)
    parts = []
    if h:
        parts.append(f"{h}h")
    if m:
        parts.append(f"{m}m")
    if s and not parts:
        parts.append(f"{s}s")
    return "in ~" + " ".join(parts)


def kb_main(uid: int):
    rows = [
        [InlineKeyboardButton(text="👤 Manage Accounts", callback_data="menu:acc")],
        [
            InlineKeyboardButton(text="📜 Commands", callback_data="menu:cmds"),
            InlineKeyboardButton(text="💎 Premium",  callback_data="menu:prem"),
        ],
        [
            InlineKeyboardButton(text="🔓 Unlock GC",  callback_data="menu:unlock"),
            InlineKeyboardButton(text="ℹ️ Disclaimer", callback_data="menu:disc"),
        ],
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="menu:home")],
    ]
    if is_owner(uid):
        rows.insert(
            3,
            [
                InlineKeyboardButton(
                    text=("🌙 Night: ON" if night_enabled() else "🌙 Night: OFF"),
                    callback_data="owner:night",
                )
            ],
        )
        rows.append(
            [
                InlineKeyboardButton(text="📊 Stats", callback_data="owner:stats"),
                InlineKeyboardButton(text="🏆 Top 10", callback_data="owner:top"),
            ]
        )
        rows.append(
            [
                InlineKeyboardButton(text="📣 Broadcast", callback_data="owner:bcast"),
                InlineKeyboardButton(text="⚙️ Owner Premium", callback_data="owner:prem"),
            ]
        )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _cmds_text():
    return (
        "📜 <b>Self-Commands</b> (send from your <b>logged-in account</b>, not this bot)\n\n"
        "Basic (free plan):\n"
        "• <code>.help</code> – show all commands\n"
        "• <code>.status</code> – show plan, interval, delay & Auto-Night status\n"
        "• <code>.info</code> – detailed info (name, phone, groups, plan, expiry)\n"
        "• <code>.addgroup &lt;link/@user&gt;</code> – add target groups/channels\n"
        "  ▸ you can also reply to a message containing multiple t.me links\n"
        "• <code>.delgroup &lt;link/@user&gt;</code> – remove a target\n"
        "• <code>.groups</code> – list all added groups/channels\n"
        "• <code>.time 30</code> / <code>.time 45</code> / <code>.time 60</code> – set basic interval (minutes)\n"
        "• <code>.upgrade</code> – get your Telegram ID to request premium\n\n"
        "Premium extras (after upgrade):\n"
        "• <code>.time &lt;value&gt;[m|h]</code> – full custom interval "
        "(e.g. <code>.time 10</code>, <code>.time 90</code>, <code>.time 2h</code>)\n"
        "• <code>.delay &lt;sec&gt;</code> – custom per-message delay between forwards\n"
        "• <code>.night</code> / <code>.night on</code> / <code>.night off</code> / "
        "<code>.night 23:00-07:00</code> – Auto-Night quiet hours\n\n"
        f"💎 To upgrade, use <code>.upgrade</code> from your own account and send your ID to {PREMIUM_CONTACT}."
    )


async def home(m, uid: int):
    gs = len(list_groups(uid))
    ss = sessions_count_user(uid)
    interval = get_interval(uid)
    plan = "Premium 💎" if is_premium(uid) else "Free ⚪"
    text = (
        "✇ <b>Spinify Ads Panel</b>\n"
        "Use <b>@SpinifyLoginBot</b> to add up to 3 accounts.\n"
        "Then, from your own Telegram account, type <code>.help</code> to see all self-commands\n"
        "(.addgroup, .groups, .time, .upgrade, etc.).\n\n"
        f"👤 Plan: {plan}\n"
        f"Sessions: {ss} | Groups: {gs}/{groups_cap(uid)} | Interval: {interval}m\n"
        f"Next send: {('—' if ss == 0 or gs == 0 else _format_eta(uid))}\n"
        f"Night: {'ON' if night_enabled() else 'OFF'}\n\n"
        f"💎 For Premium plans, contact {PREMIUM_CONTACT}"
    )
    if isinstance(m, Message):
        await m.answer(text, reply_markup=kb_main(uid))
    else:
        try:
            await m.message.edit_text(text, reply_markup=kb_main(uid))
        except TelegramBadRequest:
            pass


# =========================
# FSM for owner broadcast
# =========================
class OwnerFSM(StatesGroup):
    broadcast = State()


# =========================
# Handlers
# =========================

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


@dp.callback_query(F.data == "menu:home")
async def cb_home(cq: CallbackQuery):
    await home(cq, cq.from_user.id)


@dp.callback_query(F.data == "menu:acc")
async def cb_acc(cq: CallbackQuery):
    uid = cq.from_user.id
    rows = sessions_list(uid)
    if not rows:
        text = "👤 <b>Manage Accounts</b>\nNo sessions. Add via @SpinifyLoginBot."
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="➕ Open @SpinifyLoginBot",
                        url="https://t.me/SpinifyLoginBot",
                    )
                ],
                [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")],
            ]
        )
        try:
            await cq.message.edit_text(text, reply_markup=kb)
        except TelegramBadRequest:
            pass
        return

    lines = [f"• Slot {r['slot']} — API_ID {r['api_id']}" for r in rows]
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"🗑 Remove S{r['slot']}",
                    callback_data=f"acc:del:{r['slot']}",
                )
            ]
            for r in rows
        ]
        + [[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]
    )
    try:
        await cq.message.edit_text(
            "👤 <b>Manage Accounts</b>\n" + "\n".join(lines), reply_markup=kb
        )
    except TelegramBadRequest:
        pass


@dp.callback_query(F.data.startswith("acc:del:"))
async def acc_del(cq: CallbackQuery):
    try:
        slot = int(cq.data.split(":")[-1])
        sessions_delete(cq.from_user.id, slot)
    except Exception as e:
        log.error("del slot %s", e)
    await cb_acc(cq)


@dp.callback_query(F.data == "menu:unlock")
async def cb_unlock(cq: CallbackQuery):
    cap = groups_cap(cq.from_user.id)
    rows = []
    if UNLOCK_GC_LINK:
        rows.append(
            [InlineKeyboardButton(text="🔗 Join Unlock GC", url=UNLOCK_GC_LINK)]
        )
    rows.append([InlineKeyboardButton(text="✅ I've Joined", callback_data="unlock:ok")])
    rows.append([InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")])
    try:
        await cq.message.edit_text(
            f"🔓 <b>Unlock GC</b>\nJoin the GC to unlock 10 targets.\nCurrent cap: {cap}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
        )
    except TelegramBadRequest:
        pass


@dp.callback_query(F.data == "unlock:ok")
async def unlock_ok(cq: CallbackQuery):
    set_setting(f"groups_cap:{cq.from_user.id}", 10)
    try:
        await cq.message.edit_text(
            f"✅ Unlocked. Cap is now {groups_cap(cq.from_user.id)}.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]
                ]
            ),
        )
    except TelegramBadRequest:
        pass


@dp.callback_query(F.data == "menu:cmds")
async def cb_cmds(cq: CallbackQuery):
    try:
        await cq.message.edit_text(
            _cmds_text(),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]
                ]
            ),
        )
    except TelegramBadRequest:
        pass


@dp.callback_query(F.data == "menu:disc")
async def cb_disc(cq: CallbackQuery):
    text = (
        "⚠️ <b>Disclaimer</b>\n"
        "This tool automates message forwarding using your own Telegram account.\n"
        "Use at your own risk. Always follow Telegram TOS and local laws.\n"
        "We are not responsible for bans, blocks, or any misuse."
    )
    try:
        await cq.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]
                ]
            ),
        )
    except TelegramBadRequest:
        pass


@dp.callback_query(F.data == "menu:prem")
async def cb_prem(cq: CallbackQuery):
    plan = "Premium 💎" if is_premium(cq.from_user.id) else "Free ⚪"
    text = (
        "💎 <b>Premium Plan</b>\n"
        f"Your current plan: {plan}\n\n"
        "Premium unlocks extra features in the self-commands:\n"
        "  • Any interval value (.time N)\n"
        "  • Custom per-message delay (.delay)\n"
        "  • Auto-Night scheduling (.night)\n"
        "  • Higher group caps (more targets)\n\n"
        f"To upgrade, contact {PREMIUM_CONTACT} on Telegram and share your user ID "
        "(you can see it via <code>.upgrade</code> in your own account)."
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]
        ]
    )
    try:
        await cq.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        pass


# /fstats — for everyone
@dp.message(Command("fstats"))
async def fstats(msg: Message):
    uid = msg.from_user.id
    ss = sessions_count_user(uid)
    gs = len(list_groups(uid))
    interval = get_interval(uid)
    eta = "—" if ss == 0 or gs == 0 else _format_eta(uid)
    plan = "Premium 💎" if is_premium(uid) else "Free ⚪"
    await msg.answer(
        "📟 <b>Forward Stats</b>\n"
        f"Plan: {plan}\n"
        f"▶ Worker: {'RUNNING' if ss>0 else 'IDLE'}\n"
        f"Interval: {interval} min\n"
        f"Sessions: {ss} | Groups: {gs}/{groups_cap(uid)}\n"
        f"Next send: {eta}\n"
        f"{'🌙 Night ON' if night_enabled() else '🌙 Night OFF'}\n\n"
        f"💎 Upgrade: contact {PREMIUM_CONTACT}"
    )


# =========================
# Owner-only tools
# =========================

@dp.callback_query(F.data == "owner:night")
async def owner_night(cq: CallbackQuery):
    if not is_owner(cq.from_user.id):
        return
    set_night_enabled(not night_enabled())
    await home(cq, cq.from_user.id)


@dp.callback_query(F.data == "owner:stats")
async def owner_stats(cq: CallbackQuery):
    if not is_owner(cq.from_user.id):
        return
    total = users_count()
    active = sessions_count()
    sent = get_total_sent_ok()
    await cq.message.edit_text(
        f"📊 <b>Global Stats</b>\n"
        f"Users: {total}\n"
        f"Active sessions: {active}\n"
        f"Total forwarded: {sent}",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]
            ]
        ),
    )


@dp.callback_query(F.data == "owner:top")
async def owner_top(cq: CallbackQuery):
    if not is_owner(cq.from_user.id):
        return
    rows = top_users(10)
    if not rows:
        text = "🏆 No data."
    else:
        text = "🏆 <b>Top Users</b>\n" + "\n".join(
            f"{i+1}. <code>{r['user_id']}</code> — {r['sent_ok']}"
            for i, r in enumerate(rows)
        )
    await cq.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]
            ]
        ),
    )


class OwnerBroadcastFSM(StatesGroup):
    broadcast = State()


@dp.callback_query(F.data == "owner:bcast")
async def owner_bcast(cq: CallbackQuery, state: FSMContext):
    if not is_owner(cq.from_user.id):
        return
    await state.set_state(OwnerBroadcastFSM.broadcast)
    await cq.message.edit_text(
        "📣 Send broadcast text now.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]
            ]
        ),
    )


@dp.message(OwnerBroadcastFSM.broadcast)
async def do_bcast(msg: Message, state: FSMContext):
    if not is_owner(msg.from_user.id):
        await state.clear()
        return
    uids = [
        r["user_id"] for r in get_conn().execute("SELECT user_id FROM users").fetchall()
    ]
    sent = fail = 0
    for i, uid in enumerate(uids, 1):
        try:
            await bot.send_message(uid, msg.html_text or msg.text)
            sent += 1
        except Exception:
            fail += 1
        if i % 25 == 0:
            await asyncio.sleep(1)
    await state.clear()
    await msg.answer(f"✅ Done. Sent {sent}, failed {fail}")


@dp.callback_query(F.data == "owner:prem")
async def owner_prem_menu(cq: CallbackQuery):
    """
    Just show instructions for /upgrade and /downgrade.
    """
    if not is_owner(cq.from_user.id):
        return
    text = (
        "⚙️ <b>Owner Premium Controls</b>\n\n"
        "Use commands in this bot:\n"
        "  • <code>/upgrade &lt;user_id&gt;</code> – enable premium for a user\n"
        "  • <code>/downgrade &lt;user_id&gt;</code> – disable premium for a user\n\n"
        "Premium flag is stored in DB and used by the forwarder.\n"
        "You can also use name-lock via internal tools if needed."
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]
        ]
    )
    try:
        await cq.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        pass


@dp.message(Command("upgrade"))
async def owner_upgrade(msg: Message):
    """
    /upgrade <user_id> — OWNER ONLY
    Sets premium flag and raises group cap to 50.
    """
    if not is_owner(msg.from_user.id):
        return
    parts = msg.text.split()
    if len(parts) != 2:
        await msg.answer("Usage: <code>/upgrade &lt;user_id&gt;</code>")
        return
    try:
        target = int(parts[1])
    except Exception:
        await msg.answer("❌ <code>user_id</code> must be integer.")
        return
    set_premium(target, True)
    set_setting(f"groups_cap:{target}", 50)
    await msg.answer(
        f"💎 Premium enabled for <code>{target}</code> (cap=50).\n"
        "Name-lock can be managed separately if you want."
    )


@dp.message(Command("downgrade"))
async def owner_downgrade(msg: Message):
    """
    /downgrade <user_id> — OWNER ONLY
    Clears premium flag and lowers cap to 5.
    """
    if not is_owner(msg.from_user.id):
        return
    parts = msg.text.split()
    if len(parts) != 2:
        await msg.answer("Usage: <code>/downgrade &lt;user_id&gt;</code>")
        return
    try:
        target = int(parts[1])
    except Exception:
        await msg.answer("❌ <code>user_id</code> must be integer.")
        return
    set_premium(target, False)
    set_setting(f"groups_cap:{target}", 5)
    await msg.answer(
        f"🧹 Premium disabled for <code>{target}</code> (cap=5)."
    )


# =========================
# Entrypoint
# =========================
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
