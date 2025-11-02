import asyncio, os, sqlite3
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from pyrogram import Client
from dotenv import load_dotenv
from core.db import init_db, get_conn

load_dotenv()
LOGIN_BOT_TOKEN = os.getenv("LOGIN_BOT_TOKEN")

bot = Bot(LOGIN_BOT_TOKEN)
dp = Dispatcher()
init_db()

class Login(StatesGroup):
    api_id = State()
    api_hash = State()
    phone = State()
    otp = State()

def otp_kb():
    rows = [
        [KeyboardButton(text="1"), KeyboardButton(text="2"), KeyboardButton(text="3")],
        [KeyboardButton(text="4"), KeyboardButton(text="5"), KeyboardButton(text="6")],
        [KeyboardButton(text="7"), KeyboardButton(text="8"), KeyboardButton(text="9")],
        [KeyboardButton(text="0"), KeyboardButton(text="⬅️"), KeyboardButton(text="❌ Cancel")],
    ]
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

@dp.message(Command("start"))
async def start(msg: Message, state: FSMContext):
    await msg.answer("🔐 Send your *API ID* (number)", parse_mode="Markdown")
    await state.set_state(Login.api_id)

@dp.message(Login.api_id)
async def get_api_id(msg: Message, state: FSMContext):
    try:
        api_id = int(msg.text.strip())
    except:
        return await msg.answer("❗ API ID must be a number. Send again.")
    await state.update_data(api_id=api_id)
    await msg.answer("✅ Got API ID.\nNow send your *API HASH*.", parse_mode="Markdown")
    await state.set_state(Login.api_hash)

@dp.message(Login.api_hash)
async def get_api_hash(msg: Message, state: FSMContext):
    api_hash = msg.text.strip()
    await state.update_data(api_hash=api_hash)
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Send phone", request_contact=True)]],
        resize_keyboard=True
    )
    await msg.answer("📞 Now send your phone number (with country code), or tap below.", reply_markup=kb)
    await state.set_state(Login.phone)

@dp.message(Login.phone)
async def get_phone(msg: Message, state: FSMContext):
    data = await state.get_data()
    api_id = data["api_id"]
    api_hash = data["api_hash"]

    if msg.contact:
        phone = msg.contact.phone_number
    else:
        phone = msg.text.strip()

    client = Client(
        name=f"login-{msg.from_user.id}",
        api_id=api_id,
        api_hash=api_hash,
        in_memory=True
    )
    await client.connect()
    sent = await client.send_code(phone)
    await state.update_data(phone=phone,
                            phone_code_hash=sent.phone_code_hash)
    await client.disconnect()

    await msg.answer("📨 Code sent. Tap digits 👇", reply_markup=otp_kb())
    await state.update_data(code="")
    await state.set_state(Login.otp)

@dp.message(Login.otp)
async def otp_input(msg: Message, state: FSMContext):
    data = await state.get_data()
    code = data.get("code", "")

    txt = msg.text.strip()
    if txt == "❌ Cancel":
        await state.clear()
        return await msg.answer("❌ Cancelled.", reply_markup=None)
    if txt == "⬅️":
        code = code[:-1]
    elif txt.isdigit():
        code += txt

    await state.update_data(code=code)
    await msg.answer(f"Code: `{code}`", parse_mode="Markdown", reply_markup=otp_kb())

    if len(code) >= 5:
        api_id = data["api_id"]
        api_hash = data["api_hash"]
        phone = data["phone"]
        phone_code_hash = data["phone_code_hash"]

        client = Client(
            name=f"login-{msg.from_user.id}",
            api_id=api_id,
            api_hash=api_hash,
            in_memory=True
        )
        await client.connect()
        try:
            await client.sign_in(
                phone_number=phone,
                phone_code_hash=phone_code_hash,
                phone_code=code
            )
        except Exception as e:
            await client.disconnect()
            await state.clear()
            return await msg.answer(f"⚠️ Login failed: {e}\n/start again", reply_markup=None)

        session_str = await client.export_session_string()

        # enforce branding
        try:
            await client.update_profile(bio="#1 Free Ads Bot — Join @PhiloBots")
            me = await client.get_me()
            base = me.first_name.split(" — ")[0]
            await client.update_profile(first_name=base + " — via @SpinifyAdsBot")
        except:
            pass

        await client.disconnect()

        conn = get_conn()
        conn.execute("""
        INSERT INTO user_sessions(user_id, api_id, api_hash, session_string)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            api_id=excluded.api_id,
            api_hash=excluded.api_hash,
            session_string=excluded.session_string
        """, (msg.from_user.id, api_id, api_hash, session_str))
        conn.commit()
        conn.close()

        await msg.answer("✅ Logged in & session saved.\nGo to @SpinifyAdsBot", reply_markup=None)
        await state.clear()

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
      
