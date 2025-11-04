# main_bot.py — Aiogram v3.7+ (Main control bot for Spinify Ads)
# Works with DB tables: user_sessions, user_settings
import asyncio, os, sys, hashlib
from datetime import datetime

import portalocker
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv

from core.db import init_db, get_conn

load_dotenv()
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN") or os.getenv("ADS_BOT_TOKEN", "")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
REQUIRED_CHANNELS = ["@PhiloBots", "@ThetrafficZone"]

if not MAIN_BOT_TOKEN:
    raise RuntimeError("MAIN_BOT_TOKEN (or ADS_BOT_TOKEN) missing in .env")

# ---- single-instance lock per token (avoid getUpdates conflicts)
_key = hashlib.sha256(MAIN_BOT_TOKEN.encode()).hexdigest()[:16]
_lockf = open(f"/tmp/spinify-main-{_key}.lock", "a+")
try:
    portalocker.lock(_lockf, portalocker.LOCK_EX | portalocker.LOCK_NB)
    _lockf.seek(0); _lockf.truncate(0); _lockf.write(str(os.getpid())); _lockf.flush()
except portalocker.exceptions.LockException:
    print("[guard] Another main bot instance is running. Exiting.")
    sys.exit(0)

bot = Bot(MAIN_BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
init_db()

DISCLAIMER = (
    "⚠️ <b>Disclaimer</b>\n"
    "We are not responsible for account limitations/termination. "
    "This is a free version. For paid support, contact @Spinify."
)

# ---------- DB helpers ----------
def ensure_tables():
    conn = get_conn()
    conn.execute("""
    CREATE TABLE IF NOT EXISTS user_settings(
      user_id          INTEGER PRIMARY KEY,
      interval_minutes INTEGER DEFAULT 60,
      ad_text          TEXT    DEFAULT '',
      groups_text      TEXT    DEFAULT '',
      updated_at       TEXT
    )
    """)
    conn.commit(); conn.close()

def has_session(user_id: int) -> bool:
    conn = get_conn()
    ok = conn.execute("SELECT 1 FROM user_sessions WHERE user_id=?", (user_id,)).fetchone() is not None
    conn.close()
    return ok

def get_settings(user_id: int):
    conn = get_conn()
    row = conn.execute("SELECT interval_minutes, ad_text, groups_text FROM user_settings WHERE user_id=?",
                       (user_id,)).fetchone()
    if not row:
        conn.execute("INSERT INTO user_settings(user_id, updated_at) VALUES (?, ?)",
                     (user_id, datetime.utcnow().isoformat()))
        conn.commit()
        row = (60, "", "")
    conn.close()
    return {"interval": int(row[0]), "ad": row[1], "groups": row[2]}

def set_settings(user_id: int, **kw):
    cols, vals = [], []
    for k, v in kw.items():
        if k == "interval": cols.append("interval_minutes=?"); vals.append(int(v))
        if k == "ad":       cols.append("ad_text=?");         vals.append(str(v))
        if k == "groups":   cols.append("groups_text=?");     vals.append(str(v))
    cols.append("updated_at=?"); vals.append(datetime.utcnow().isoformat())
    vals.append(user_id)
    conn = get_conn()
    conn.execute(f"UPDATE user_settings SET {', '.join(cols)} WHERE user_id=?", vals)
    conn.commit(); conn.close()

# ---------- join check ----------
async def check_joined(user_id: int) -> bool:
    for ch in REQUIRED_CHANNELS:
        try:
            m = await bot.get_chat_member(ch, user_id)
            if m.status in ("left", "kicked"):  # not a member
                return False
        except Exception:
            return False
    return True

# ---------- UI ----------
def main_menu():
    kb = [
        [InlineKeyboardButton(text="1️⃣ Set interval", callback_data="set_interval")],
        [InlineKeyboardButton(text="2️⃣ Set message",  callback_data="set_message")],
        [InlineKeyboardButton(text="3️⃣ Add groups (max 5)", callback_data="add_groups")],
        [InlineKeyboardButton(text="📊 Status",      callback_data="status")],
        [InlineKeyboardButton(text="⚠️ Disclaimer",  callback_data="disclaimer")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

# ---------- preflight ----------
async def _preflight():
    try:
        await bot.delete_webhook(drop_pending_updates=True)  # prevent webhook/getUpdates conflict
    except Exception:
        pass
    me = await bot.get_me()
    print(f"[main preflight] @{me.username} (id={me.id}) ready")

# ---------- commands & callbacks ----------
@dp.message(Command("start"))
async def start(msg: Message):
    ensure_tables()
    user_id = msg.from_user.id

    # gate: must join required channels
    if not await check_joined(user_id):
        join_btns = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Join @PhiloBots",      url="https://t.me/PhiloBots")],
            [InlineKeyboardButton(text="Join @TheTrafficZone", url="https://t.me/TheTrafficZone")],
            [InlineKeyboardButton(text="✅ I joined",           callback_data="recheck_join")]
        ])
        await msg.answer("👋 Join both channels to use the bot.", reply_markup=join_btns)
        return

    # gate: must have a saved user session
    if not has_session(user_id):
        await msg.answer("🔑 No session found.\nLogin at @SpinifyLoginBot first, then /start again.")
        return

    await msg.answer("✅ Main menu:", reply_markup=main_menu())

@dp.callback_query(F.data == "recheck_join")
async def recheck_join(cb: CallbackQuery):
    await cb.answer()
    if not await check_joined(cb.from_user.id):
        await cb.message.edit_text("❌ Still not joined. Join and press again.")
        return
    if not has_session(cb.from_user.id):
        await cb.message.edit_text("✅ Join OK, but no session.\nLogin at @SpinifyLoginBot.")
        return
    await cb.message.edit_text("✅ Main menu:", reply_markup=main_menu())

# ---- Set interval
@dp.callback_query(F.data == "set_interval")
async def cb_set_int(cb: CallbackQuery):
    await cb.answer()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="45 minutes", callback_data="int_45")],
        [InlineKeyboardButton(text="60 minutes", callback_data="int_60")],
    ])
    await cb.message.edit_text("⏱ Choose interval:", reply_markup=kb)

@dp.callback_query(F.data.in_(["int_45", "int_60"]))
async def cb_int_set(cb: CallbackQuery):
    await cb.answer()
    mins = 45 if cb.data == "int_45" else 60
    set_settings(cb.from_user.id, interval=mins)
    await cb.message.edit_text(f"✅ Interval set to <b>{mins} minutes</b>.", reply_markup=main_menu())

# ---- Set message
class SetMsg(StatesGroup):
    waiting = State()

@dp.callback_query(F.data == "set_message")
async def cb_set_msg(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await cb.message.answer("📝 Send your ad message.\n(New one will replace old.)")
    await state.set_state(SetMsg.waiting)

@dp.message(StateFilter(SetMsg.waiting))
async def save_msg(msg: Message, state: FSMContext):
    ad = (msg.text or "").strip()
    if not ad:
        await msg.answer("❌ Empty. Send plain text for the ad.")
        return
    set_settings(msg.from_user.id, ad=ad)
    await state.clear()
    await msg.answer("✅ Ad message saved.", reply_markup=main_menu())

# ---- Add groups (replace)
class AddGroups(StatesGroup):
    waiting = State()

@dp.callback_query(F.data == "add_groups")
async def cb_add_groups(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await cb.message.answer("📥 Send group links/usernames, one per line (max 5). New list will REPLACE old.")
    await state.set_state(AddGroups.waiting)

@dp.message(StateFilter(AddGroups.waiting))
async def save_groups(msg: Message, state: FSMContext):
    lines = [l.strip() for l in (msg.text or "").splitlines() if l.strip()]
    # normalize, cap 5, store as newline text for worker_forward.py
    norm = []
    for link in lines:
        x = link.strip()
        if x.startswith("https://t.me/"):
            x = x.split("https://t.me/", 1)[1].strip("/")
        if x.startswith("@"):
            x = x[1:]
        if x and x not in norm:
            norm.append("https://t.me/" + x)
        if len(norm) == 5:
            break
    set_settings(msg.from_user.id, groups="\n".join(norm))
    await state.clear()
    await msg.answer(f"✅ Saved {len(norm)}/5 groups.", reply_markup=main_menu())

# ---- Status
@dp.callback_query(F.data == "status")
async def cb_status(cb: CallbackQuery):
    await cb.answer()
    s = get_settings(cb.from_user.id)
    groups = [g for g in s["groups"].splitlines() if g.strip()]
    ad_preview = (s["ad"][:180] + "…") if len(s["ad"]) > 180 else s["ad"]
    text = (
        "<b>Your Settings</b>\n"
        f"• Interval: <b>{s['interval']} min</b>\n"
        f"• Groups ({len(groups)}/5):\n" + ("\n".join(groups) if groups else "—") + "\n"
        f"• Ad:\n<code>{ad_preview or '—'}</code>\n\n" + DISCLAIMER
    )
    await cb.message.edit_text(text, reply_markup=main_menu())

# ---- Disclaimer
@dp.callback_query(F.data == "disclaimer")
async def cb_disclaimer(cb: CallbackQuery):
    await cb.answer()
    text = (
        "⚠️ We are NOT responsible for your account / session termination.\n"
        "This is FREE version.\n"
        "For upgrade contact: @Spinify"
    )
    await cb.message.edit_text(text, reply_markup=main_menu())

# ---- Owner broadcast
@dp.message(Command("broadcast"))
async def broadcast(msg: Message):
    if OWNER_ID and msg.from_user.id != OWNER_ID:
        return
    parts = msg.text.split(maxsplit=1)
    if len(parts) == 1:
        await msg.answer("Usage: /broadcast <text>")
        return
    text = parts[1]
    conn = get_conn()
    users = [r[0] for r in conn.execute("SELECT DISTINCT user_id FROM user_sessions").fetchall()]
    conn.close()
    ok, fail = 0, 0
    for uid in users:
        try:
            await bot.send_message(uid, "📢 " + text)
            ok += 1
            await asyncio.sleep(0.05)
        except Exception:
            fail += 1
    await msg.answer(f"📣 Broadcast done.\n✅ {ok}\n❌ {fail}")

# ---- runner
async def main():
    await _preflight()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
    
