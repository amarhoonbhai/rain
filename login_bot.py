import asyncio
import os
from aiogram import Bot, Dispatcher
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from dotenv import load_dotenv
from pyrogram import Client
from pyrogram.errors import PhoneCodeExpired, PhoneCodeInvalid
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


def otp_keyboard() -> ReplyKeyboardMarkup:
    # like your screenshot: 1-9, 0, then Back/Clear/Submit
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="1"), KeyboardButton(text="2"), KeyboardButton(text="3")],
            [KeyboardButton(text="4"), KeyboardButton(text="5"), KeyboardButton(text="6")],
            [KeyboardButton(text="7"), KeyboardButton(text="8"), KeyboardButton(text="9")],
            [KeyboardButton(text="0")],
            [KeyboardButton(text="⬅️ Back"), KeyboardButton(text="🧹 Clear"), KeyboardButton(text="✅ Submit")],
        ],
        resize_keyboard=True
    )


@dp.message(Command("start"))
async def start(msg: Message, state: FSMContext):
    await msg.answer("Send your API ID (number).")
    await state.set_state(Login.api_id)


@dp.message(Login.api_id)
async def get_api_id(msg: Message, state: FSMContext):
    try:
        api_id = int(msg.text.strip())
    except ValueError:
        await msg.answer("API ID must be a number. Send again.")
        return
    await state.update_data(api_id=api_id)
    await msg.answer("OK. Now send your API HASH.")
    await state.set_state(Login.api_hash)


@dp.message(Login.api_hash)
async def get_api_hash(msg: Message, state: FSMContext):
    api_hash = msg.text.strip()
    await state.update_data(api_hash=api_hash)
    await msg.answer("Now send your phone (with country code). Example: +918000000000")
    await state.set_state(Login.phone)


@dp.message(Login.phone)
async def get_phone(msg: Message, state: FSMContext):
    data = await state.get_data()
    api_id = data["api_id"]
    api_hash = data["api_hash"]
    phone = msg.text.strip()

    client = Client(
        name=f"login-{msg.from_user.id}",
        api_id=api_id,
        api_hash=api_hash,
        in_memory=True,
    )
    await client.connect()
    sent = await client.send_code(phone)
    await client.disconnect()

    await state.update_data(phone=phone, phone_code_hash=sent.phone_code_hash, code="")
    await msg.answer("Verification Code\nUse the keypad below.", reply_markup=otp_keyboard())
    await state.set_state(Login.otp)


@dp.message(Login.otp)
async def otp_input(msg: Message, state: FSMContext):
    data = await state.get_data()
    api_id = data["api_id"]
    api_hash = data["api_hash"]
    phone = data["phone"]
    phone_code_hash = data["phone_code_hash"]
    code = data.get("code", "")
    txt = msg.text.strip()

    # buttons
    if txt == "🧹 Clear":
        code = ""
        await state.update_data(code=code)
        await msg.answer("Verification Code\nUse the keypad below.", reply_markup=otp_keyboard())
        return

    if txt == "⬅️ Back":
        code = code[:-1]
        await state.update_data(code=code)
        await msg.answer(f"Code: {code}", reply_markup=otp_keyboard())
        return

    if txt == "✅ Submit":
        if not code:
            await msg.answer("Enter the code first.", reply_markup=otp_keyboard())
            return
        # go to sign-in below
    elif txt.isdigit():
        code += txt
        await state.update_data(code=code)
        await msg.answer(f"Code: {code}", reply_markup=otp_keyboard())
        return
    else:
        return

    # try sign in
    client = Client(
        name=f"login-{msg.from_user.id}",
        api_id=api_id,
        api_hash=api_hash,
        in_memory=True,
    )
    await client.connect()
    try:
        await client.sign_in(
            phone_number=phone,
            phone_code_hash=phone_code_hash,
            phone_code=code
        )
    except PhoneCodeExpired:
        new_sent = await client.send_code(phone)
        await client.disconnect()
        await state.update_data(phone_code_hash=new_sent.phone_code_hash, code="")
        await msg.answer("Code expired. New code sent.\nVerification Code\nUse the keypad below.", reply_markup=otp_keyboard())
        return
    except PhoneCodeInvalid:
        await client.disconnect()
        await state.update_data(code="")
        await msg.answer("Wrong code. Try again.\nVerification Code\nUse the keypad below.", reply_markup=otp_keyboard())
        return
    except Exception as e:
        await client.disconnect()
        await state.clear()
        await msg.answer(f"Login failed: {e}\n/start again")
        return

    # success
    session_str = await client.export_session_string()

    # brand
    try:
        await client.update_profile(bio="#1 Free Ads Bot — Join @PhiloBots")
        me = await client.get_me()
        base = me.first_name.split(" — ")[0]
        await client.update_profile(first_name=base + " — via @SpinifyAdsBot")
    except:
        pass

    await client.disconnect()

    conn = get_conn()
    conn.execute(
        "INSERT INTO user_sessions(user_id, api_id, api_hash, session_string) "
        "VALUES (?, ?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET api_id=excluded.api_id, api_hash=excluded.api_hash, session_string=excluded.session_string",
        (msg.from_user.id, api_id, api_hash, session_str)
    )
    conn.commit()
    conn.close()

    await state.clear()
    await msg.answer("✅ Session saved.\nYou can go back to the main bot now.", reply_markup=None)


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
