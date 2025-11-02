import asyncio, os
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from dotenv import load_dotenv
from core.db import init_db, get_conn

load_dotenv()
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
REQUIRED_CHANNELS = ["@PhiloBots", "@ThetrafficZone"]

bot = Bot(MAIN_BOT_TOKEN)
dp = Dispatcher()
init_db()

def has_session(user_id: int) -> bool:
    conn = get_conn()
    cur = conn.execute("SELECT 1 FROM user_sessions WHERE user_id = ?", (user_id,))
    ok = cur.fetchone() is not None
    conn.close()
    return ok

async def check_joined(user_id: int) -> bool:
    for ch in REQUIRED_CHANNELS:
        try:
            m = await bot.get_chat_member(ch, user_id)
            if m.status in ("left", "kicked"):
                return False
        except:
            return False
    conn = get_conn()
    conn.execute("UPDATE users SET joined_ok = 1 WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()
    return True

def main_menu():
    kb = [
        [InlineKeyboardButton(text="1️⃣ Set interval", callback_data="set_interval")],
        [InlineKeyboardButton(text="2️⃣ Set message", callback_data="set_message")],
        [InlineKeyboardButton(text="3️⃣ Add groups (max 5)", callback_data="add_groups")],
        [InlineKeyboardButton(text="⚠️ Disclaimer", callback_data="disclaimer")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)

@dp.message(Command("start"))
async def start(msg: Message):
    user_id = msg.from_user.id
    username = msg.from_user.username or ""
    conn = get_conn()
    conn.execute("""
    INSERT INTO users(user_id, username)
    VALUES (?, ?)
    ON CONFLICT(user_id) DO UPDATE SET username=excluded.username
    """, (user_id, username))
    conn.commit()
    conn.close()

    ok = await check_joined(user_id)
    if not ok:
        join_btns = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Join @PhiloBots", url="https://t.me/PhiloBots")],
            [InlineKeyboardButton(text="Join @TheTrafficZone", url="https://t.me/TheTrafficZone")],
            [InlineKeyboardButton(text="✅ I joined", callback_data="recheck_join")]
        ])
        return await msg.answer("👋 Join both channels to use the bot.", reply_markup=join_btns)

    if not has_session(user_id):
        return await msg.answer("🔑 No session found.\nLogin at @SpinifyLoginBot first, then /start again.")

    await msg.answer("✅ Main menu:", reply_markup=main_menu())

@dp.callback_query(F.data == "recheck_join")
async def recheck_join(cb: CallbackQuery):
    ok = await check_joined(cb.from_user.id)
    if not ok:
        return await cb.message.edit_text("❌ Still not joined. Join and press again.")
    if not has_session(cb.from_user.id):
        return await cb.message.edit_text("✅ Join OK, but no session.\nLogin at @SpinifyLoginBot.")
    await cb.message.edit_text("✅ Main menu:", reply_markup=main_menu())

# set interval
@dp.callback_query(F.data == "set_interval")
async def cb_set_int(cb: CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="45 minutes", callback_data="int_45")],
        [InlineKeyboardButton(text="60 minutes", callback_data="int_60")],
    ])
    await cb.message.edit_text("⏱ Choose interval:", reply_markup=kb)

@dp.callback_query(F.data.in_(["int_45", "int_60"]))
async def cb_int_set(cb: CallbackQuery):
    mins = 45 if cb.data == "int_45" else 60
    conn = get_conn()
    conn.execute("UPDATE users SET interval_minutes = ? WHERE user_id = ?", (mins, cb.from_user.id))
    conn.commit()
    conn.close()
    await cb.message.edit_text(f"✅ Interval set to {mins} minutes.", reply_markup=main_menu())

# set message
class SetMsg(StatesGroup):
    waiting = State()

@dp.callback_query(F.data == "set_message")
async def cb_set_msg(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("📝 Send your ad message.\n(New one will replace old.)")
    await state.set_state(SetMsg.waiting)

@dp.message(SetMsg.waiting)
async def save_msg(msg: Message, state: FSMContext):
    ad = msg.text.strip()
    conn = get_conn()
    conn.execute("UPDATE users SET ad_message = ? WHERE user_id = ?", (ad, msg.from_user.id))
    conn.commit()
    conn.close()
    await msg.answer("✅ Ad message saved.", reply_markup=main_menu())
    await state.clear()

# add groups (replace)
class AddGroups(StatesGroup):
    waiting = State()

@dp.callback_query(F.data == "add_groups")
async def cb_add_groups(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("📥 Send group links, line by line (max 5). New list will REPLACE old.")
    await state.set_state(AddGroups.waiting)

@dp.message(AddGroups.waiting)
async def save_groups(msg: Message, state: FSMContext):
    lines = [l.strip() for l in msg.text.splitlines() if l.strip()]
    lines = lines[:5]
    conn = get_conn()
    conn.execute("DELETE FROM user_groups WHERE user_id = ?", (msg.from_user.id,))
    for link in lines:
        conn.execute("INSERT INTO user_groups(user_id, group_link) VALUES (?, ?)", (msg.from_user.id, link))
    conn.commit()
    conn.close()
    await msg.answer(f"✅ Saved {len(lines)}/5 groups.", reply_markup=main_menu())
    await state.clear()

# disclaimer
@dp.callback_query(F.data == "disclaimer")
async def cb_disclaimer(cb: CallbackQuery):
    text = (
        "⚠️ We are NOT responsible for your account / session termination.\n"
        "This is FREE version.\n"
        "For upgrade contact: @Spinify"
    )
    await cb.message.edit_text(text, reply_markup=main_menu())

# owner broadcast
@dp.message(Command("broadcast"))
async def broadcast(msg: Message):
    if msg.from_user.id != OWNER_ID:
        return
    parts = msg.text.split(maxsplit=1)
    if len(parts) == 1:
        return await msg.answer("Usage: /broadcast <text>")
    text = parts[1]
    conn = get_conn()
    cur = conn.execute("SELECT user_id FROM users")
    rows = cur.fetchall()
    ok, fail = 0, 0
    for r in rows:
        uid = r["user_id"]
        try:
            await bot.send_message(uid, text)
            ok += 1
        except:
            fail += 1
    conn.close()
    await msg.answer(f"📣 Broadcast done.\n✅ {ok}\n❌ {fail}")

async def main():
    print("Main bot running...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
  
