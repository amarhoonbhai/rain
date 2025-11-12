# main_bot.py — compact UI: Accounts, Commands & Setup, Unlock, Disclaimer, Owner tools, /fstats
import os, asyncio, logging, re
from aiogram import Bot, Dispatcher, F, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramBadRequest
from dotenv import load_dotenv

from core.db import (
    init_db, ensure_user, get_conn,
    # sessions / users
    sessions_list, sessions_delete, sessions_count, sessions_count_user, users_count,
    # groups & caps (for status text)
    list_groups, groups_cap,
    # stats
    top_users, get_total_sent_ok, last_sent_at_for,
    # schedule (interval saved by .time from the user account)
    get_interval,
    # night mode
    night_enabled, set_night_enabled,
    # gate channels + KV
    get_gate_channels_effective, set_setting, get_setting,
    # unlock/premium
    set_gc_unlock, is_gc_unlocked,
    set_premium, is_premium, list_premium_users,
)

# ---------------- boot ----------------
load_dotenv()
TOKEN = (os.getenv("MAIN_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
UNLOCK_GC_LINK = (os.getenv("UNLOCK_GC_LINK") or "").strip()  # can be @public or t.me/+invite or empty

if not TOKEN or ":" not in TOKEN:
    raise RuntimeError("MAIN_BOT_TOKEN missing/malformed.")

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
init_db()

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"))
log = logging.getLogger("main_bot")

BOT_USERNAME = None

# ---------------- helpers ----------------
def is_owner(uid: int) -> bool:
    return OWNER_ID and int(uid) == OWNER_ID

async def safe_edit(message, text, **kw):
    try:
        return await message.edit_text(text, **kw)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower(): return None
        raise

def _gate_defaults(chs): return chs or ["@PhiloBots", "@TheTrafficZone"]

def _gate_channels():
    ch1, ch2 = get_gate_channels_effective()
    return _gate_defaults([c for c in (ch1, ch2) if c])

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

def _gate_kb():
    rows = [[InlineKeyboardButton(text=f"🔗 {ch}", url=f"https://t.me/{ch.lstrip('@')}")] for ch in _gate_channels()]
    rows.append([InlineKeyboardButton(text="✅ I've Joined", callback_data="gate:check")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def _gate_text():
    items = "\n".join(f"  • {ch}" for ch in _gate_channels())
    return "✇ Access required\n✇ Join the channels below to use the bot:\n" + items + "\n\n✇ After joining, tap <b>I've Joined</b>."

async def _ensure_bot_username():
    global BOT_USERNAME
    if not BOT_USERNAME:
        me = await bot.get_me()
        BOT_USERNAME = me.username

# ---- Unlock GC helpers ----
# Accepts @public channel, t.me/channel, or t.me/+invite (unverifiable).
def _unlock_kind_and_button():
    link = UNLOCK_GC_LINK.strip()
    if not link:
        return ("none", None, None)  # no link configured
    # normalize
    if link.startswith("@"):
        uname = link
        url = f"https://t.me/{uname.lstrip('@')}"
        return ("public", uname, url)
    if "t.me/" in link:
        # extract username if any
        m = re.match(r"https?://t\.me/([A-Za-z0-9_]+)$", link.strip("/"))
        if m:
            uname = "@" + m.group(1)
            return ("public", uname, f"https://t.me/{m.group(1)}")
        # invite link (unverifiable)
        return ("invite", link, link)
    # fallback
    return ("unknown", link, link)

def kb_main(uid: int):
    rows = [
        [InlineKeyboardButton(text="👤 Accounts", callback_data="menu:accounts")],
        [InlineKeyboardButton(text="⚙ Commands & Setup", callback_data="menu:cmds")],
        [InlineKeyboardButton(text="🔓 Unlock 10 Groups", callback_data="menu:unlock")],
        [InlineKeyboardButton(text="ℹ️ Disclaimer", callback_data="menu:disc")],
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="menu:home")],
    ]
    if is_owner(uid):
        rows.append([InlineKeyboardButton(text=("🌙 Night: ON" if night_enabled() else "🌙 Night: OFF"),
                                          callback_data="owner:night")])
        rows.append([InlineKeyboardButton(text="📊 Stats", callback_data="owner:stats"),
                     InlineKeyboardButton(text="🏆 Top 10", callback_data="owner:top")])
        rows.append([InlineKeyboardButton(text="📣 Broadcast", callback_data="owner:broadcast")])
        rows.append([InlineKeyboardButton(text="💎 Premium", callback_data="owner:premium")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def view_home(target, uid: int):
    s = sessions_count_user(uid)
    gs = list_groups(uid)
    cap = groups_cap(uid)
    it = get_interval(uid) or 30
    txt = (
        "✇ Welcome\n"
        "✇ After login, control everything from your <b>Saved Messages</b>.\n"
        "✇ Pin your ad in <b>Saved Messages</b> (only one pinned).\n\n"
        f"✇ Sessions linked: {s or 0}\n"
        f"✇ Groups saved: {len(gs)} / {cap}\n"
        f"✇ Interval: {it} min (set via <code>.time</code>)\n"
        f"✇ Unlock10: {'ON' if is_gc_unlocked(uid) else 'OFF'} | Premium: {'ON' if is_premium(uid) else 'OFF'}"
    )
    if isinstance(target, Message):
        await target.answer(txt, reply_markup=kb_main(uid))
    else:
        await safe_edit(target.message, txt, reply_markup=kb_main(uid))

# ---------------- middlewares ----------------
class AutoAck(BaseMiddleware):
    async def __call__(self, handler, event, data):
        if isinstance(event, CallbackQuery):
            try: await event.answer()
            except Exception: pass
        return await handler(event, data)

class GateGuard(BaseMiddleware):
    async def __call__(self, handler, event, data):
        uid = getattr(getattr(event, "from_user", None), "id", None)
        allow = False
        if isinstance(event, Message) and event.text and event.text.startswith("/start"): allow = True
        if isinstance(event, CallbackQuery) and (event.data or "").startswith("gate:"): allow = True
        if allow or not _gate_channels() or not uid:
            return await handler(event, data)
        ok, _ = await _check_gate(uid)
        if ok: return await handler(event, data)
        if isinstance(event, CallbackQuery):
            try: await safe_edit(event.message, _gate_text(), reply_markup=_gate_kb())
            except Exception: await bot.send_message(uid, _gate_text(), reply_markup=_gate_kb())
        else:
            await bot.send_message(uid, _gate_text(), reply_markup=_gate_kb())
        return

dp.update.middleware(AutoAck())
dp.update.middleware(GateGuard())

# ---------------- keyboards/views ----------------
def kb_accounts(slots):
    row1 = [InlineKeyboardButton(text=f"🗑 Remove S{s['slot']}", callback_data=f"acct:del:{s['slot']}") for s in slots]
    if not row1:
        row1 = [InlineKeyboardButton(text="➕ Add via @SpinifyLoginBot", url="https://t.me/SpinifyLoginBot")]
    rows = [row1,
            [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home"),
             InlineKeyboardButton(text="🔄 Refresh", callback_data="menu:accounts")]]
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.message(Command("start"))
async def on_start(msg: Message):
    ensure_user(msg.from_user.id, getattr(msg.from_user, "username", None))
    ok, _ = await _check_gate(msg.from_user.id)
    if not ok:
        await msg.answer(_gate_text(), reply_markup=_gate_kb()); return
    await view_home(msg, msg.from_user.id)

@dp.callback_query(F.data == "gate:check")
async def on_gate_check(cq: CallbackQuery):
    ok, _ = await _check_gate(cq.from_user.id)
    if ok: await view_home(cq, cq.from_user.id)
    else:  await safe_edit(cq.message, _gate_text(), reply_markup=_gate_kb())

@dp.callback_query(F.data == "menu:home")
async def cb_home(cq: CallbackQuery):
    await view_home(cq, cq.from_user.id)

@dp.callback_query(F.data == "menu:accounts")
async def cb_accounts(cq: CallbackQuery):
    slots = sessions_list(cq.from_user.id)
    if not slots:
        text = ("👤 Accounts\n"
                "✇ No sessions found.\n"
                "✇ Use @SpinifyLoginBot to add up to 3 accounts.")
    else:
        text = "👤 Accounts\n" + "\n".join([f"• Slot {r['slot']} — API_ID {r['api_id']}" for r in slots])
    await safe_edit(cq.message, text, reply_markup=kb_accounts(slots))

@dp.callback_query(F.data.startswith("acct:del:"))
async def cb_acct_del(cq: CallbackQuery):
    try:
        slot = int(cq.data.split(":")[-1])
        sessions_delete(cq.from_user.id, slot)
    except Exception as e:
        log.error(f"acct del err: {e}")
    await cb_accounts(cq)

@dp.callback_query(F.data == "menu:cmds")
async def cb_cmds(cq: CallbackQuery):
    await _ensure_bot_username()
    txt = (
        "⚙ Commands & Setup\n"
        "✇ Pin your ad in <b>Saved Messages</b> (only one pinned).\n"
        "✇ Type these commands from your account (any chat). You’ll get a reply there.\n\n"
        "• <code>.addgc</code>  (then up to 5 lines: id/@user/t.me/...)\n"
        "• <code>.listgc</code> / <code>.cleargc</code>\n"
        "• <code>.time 30m</code> | <code>45m</code> | <code>60m</code>  (premium: 1–360m or e.g., 2h)\n"
        "• <code>.status</code> / <code>.help</code>\n\n"
        "🔓 <b>Unlock 10 Groups</b>: tap the button below.\n"
        "💎 <b>Premium</b>: up to 50 groups & custom intervals."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔓 Unlock 10 Groups", callback_data="menu:unlock")],
        [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]
    ])
    await safe_edit(cq.message, txt, reply_markup=kb)

# ---------- Unlock flow (uses UNLOCK_GC_LINK; tries to verify if public) ----------
def _unlock_view_text(kind, ident):
    base = "🔓 Unlock 10 Groups\n"
    if kind == "public":
        return (base +
                f"✇ Join {ident} and then tap <b>I've Joined</b> below to unlock.\n"
                "✇ If your join is private and I can’t verify, you can still confirm.")
    if kind in ("invite", "unknown"):
        return (base +
                "✇ Join the group using the link below, then tap <b>Confirm</b>.\n"
                "✇ I may not be able to verify private joins; confirmation will unlock.")
    return (base +
            "✇ No unlock link configured. You can still unlock now by confirming.")

@dp.callback_query(F.data == "menu:unlock")
async def cb_unlock(cq: CallbackQuery):
    kind, ident, url = _unlock_kind_and_button()
    rows = []
    if url:
        rows.append([InlineKeyboardButton(text="🔗 Join Link", url=url)])
    rows.append([InlineKeyboardButton(text=("✅ I've Joined" if kind=="public" else "✅ Confirm"),
                                      callback_data=f"unlock:check:{kind}:{ident or ''}")])
    rows.append([InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")])
    await safe_edit(cq.message, _unlock_view_text(kind, ident), reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

@dp.callback_query(F.data.startswith("unlock:check:"))
async def cb_unlock_check(cq: CallbackQuery):
    _, _, kind, ident = (cq.data.split(":", 3) + ["", ""])[:4]
    uid = cq.from_user.id
    ok = False
    if kind == "public" and ident:
        try:
            m = await bot.get_chat_member(ident, uid)
            ok = str(getattr(m, "status", "left")).lower() not in {"left", "kicked"}
        except Exception:
            ok = False
    else:
        # private/invite/unknown -> allow confirm
        ok = True
    if ok:
        set_gc_unlock(uid, True)
        await safe_edit(cq.message, "✅ Unlocked. You can now store up to <b>10</b> groups.\n✇ Use <code>.addgc</code> again if needed.",
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]))
    else:
        await safe_edit(cq.message, "❌ I couldn’t verify your join yet. Please join first and tap “I’ve Joined” again.",
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="🔗 Open Link", url=f"https://t.me/{ident.lstrip('@')}")] if ident else [],
                            [InlineKeyboardButton(text="🔄 Try Again", callback_data="menu:unlock")],
                            [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")],
                        ]))

@dp.callback_query(F.data == "menu:disc")
async def cb_disc(cq: CallbackQuery):
    txt = (
        "⚠️ Disclaimer (Free Version)\n"
        "✇ Use at your own risk.\n"
        "✇ If your Telegram ID gets terminated, I am not responsible.\n"
        "✇ You must comply with Telegram’s Terms and local laws.\n"
        "✇ Excessive spam/abuse may lead to account limitations."
    )
    await safe_edit(cq.message, txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]))

# ---------------- owner tools ----------------
@dp.callback_query(F.data == "owner:night")
async def cb_night(cq: CallbackQuery):
    if not is_owner(cq.from_user.id): return
    set_night_enabled(not night_enabled())
    await view_home(cq, cq.from_user.id)

@dp.callback_query(F.data == "owner:stats")
async def cb_stats(cq: CallbackQuery):
    if not is_owner(cq.from_user.id): return
    txt = (f"📊 Stats\n"
           f"✇ Users: {users_count()}\n"
           f"✇ Active (≥1 session): {sessions_count()}\n"
           f"✇ Total forwarded: {get_total_sent_ok()}")
    await safe_edit(cq.message, txt, reply_markup=kb_main(cq.from_user.id))

@dp.callback_query(F.data == "owner:top")
async def cb_top(cq: CallbackQuery):
    if not is_owner(cq.from_user.id): return
    rows = top_users(10)
    txt = "🏆 Top Users (forwards)\n" + ("\n".join(f"{i+1}. {r['user_id']} — {r['sent_ok']} msgs" for i,r in enumerate(rows)) if rows else "✇ No data yet.")
    await safe_edit(cq.message, txt, reply_markup=kb_main(cq.from_user.id))

@dp.callback_query(F.data == "owner:broadcast")
async def cb_broadcast(cq: CallbackQuery):
    if not is_owner(cq.from_user.id): return
    await safe_edit(cq.message, "📣 Send the broadcast text as a reply to this message.\nUse /cancel to abort.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]))

@dp.callback_query(F.data == "owner:premium")
async def cb_premium(cq: CallbackQuery):
    if not is_owner(cq.from_user.id): return
    ids = list_premium_users()
    txt = "💎 Premium Users\n" + ("\n".join(f"• <code>{i}</code>" for i in ids) if ids else "✇ None")
    await safe_edit(cq.message, txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Upgrade user", callback_data="owner:up")],
        [InlineKeyboardButton(text="➖ Downgrade user", callback_data="owner:down")],
        [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]
    ]))

@dp.message(Command("upgrade"))
async def cmd_upgrade(msg: Message):
    if not is_owner(msg.from_user.id): return
    try:
        uid = int(msg.text.split(maxsplit=1)[1])
    except Exception:
        await msg.answer("Usage: /upgrade <user_id>"); return
    set_premium(uid, True)
    await msg.answer(f"✅ Upgraded <code>{uid}</code> to Premium (50 groups, custom intervals).")

@dp.message(Command("downgrade"))
async def cmd_downgrade(msg: Message):
    if not is_owner(msg.from_user.id): return
    try:
        uid = int(msg.text.split(maxsplit=1)[1])
    except Exception:
        await msg.answer("Usage: /downgrade <user_id>"); return
    set_premium(uid, False)
    await msg.answer(f"✅ Downgraded <code>{uid}</code> to Free.")

@dp.message(Command("premium"))
async def cmd_premium_list(msg: Message):
    if not is_owner(msg.from_user.id): return
    ids = list_premium_users()
    await msg.answer("💎 Premium Users\n" + ("\n".join(f"• <code>{i}</code>" for i in ids) if ids else "✇ None"))

# ---------------- quick user status (/fstats) ----------------
@dp.message(Command("fstats"))
async def cmd_fstats(msg: Message):
    uid = msg.from_user.id
    s_cnt = sessions_count_user(uid)
    gs = list_groups(uid)
    it = get_interval(uid) or 30
    last = last_sent_at_for(uid)
    eta = "—"
    if last is not None:
        import time as _time
        remain = (last + it * 60) - int(_time.time())
        if remain > 0:
            mm, ss = divmod(remain, 60); hh, mm = divmod(mm, 60)
            eta = f"in ~{hh}h {mm}m {ss}s" if hh else f"in ~{mm}m {ss}s"
        else:
            eta = "due now"
    txt = (
        "📟 Forward Stats\n"
        f"✇ Sessions: {s_cnt}  |  Groups: {len(gs)}\n"
        f"✇ Interval: {it} min\n"
        f"✇ Next send: {eta}\n"
        f"{'🌙 Night Mode ON' if night_enabled() else '🌙 Night Mode OFF'}"
    )
    await msg.answer(txt)

# ---------------- runner ----------------
async def main():
    try:
        await dp.start_polling(bot)
    except Exception as e:
        log.error(f"polling stopped: {e}")

if __name__ == "__main__":
    asyncio.run(main())
