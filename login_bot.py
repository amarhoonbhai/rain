# login_bot.py
# style same as screenshot: Step 1/2/3 + big reply-keypad for OTP

import asyncio
import os

from aiogram import Bot, Dispatcher
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
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


def otp_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="1"), KeyboardButton(text="2"), KeyboardButton(text="3")],
            [KeyboardButton(text="4"), KeyboardButton(text="5"), KeyboardButton(text="6")],
            [KeyboardButton(text="7"), KeyboardButton(text="8"), KeyboardButton(text="9")],
            [KeyboardButton(text="0")],
            [KeyboardButton(text="⬅ Back"), KeyboardButton(text="🧹 Clear"), KeyboardButton(text="✔ Submit")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
        is_persistent=True,
    )


@dp.message(Command("start"))
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer(
        "Step 1 — Send your <b>API_ID</b>.\n"
        "Get it at <a href=\"https://my.telegram.org\">my.telegram.org</a> → API Development Tools.",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(Login.api_id)


@dp.message(Login.api_id)
async def get_api_id(msg: Message, state: FSMContext):
    text = msg.text.strip()
    try:
        api_id = int(text)
    except ValueError:
        await msg.answer("API_ID must be a number. Send again.")
        return
    await state.update_data(api_id=api_id)
    await msg.answer("Step 2 — Paste your <b>API_HASH</b>.", parse_mode="HTML")
    await state.set_state(Login.api_hash)


@dp.message(Login.api_hash)
async def get_api_hash(msg: Message, state: FSMContext):
    api_hash = msg.text.strip()
    await state.update_data(api_hash=api_hash)
    await msg.answer("Step 3 — Send your phone as +countrycode number.")
    await state.set_state(Login.phone)


@dp.message(Login.phone)
async def get_phone(msg: Message, state: FSMContext):
    data = await state.get_data()
    api_id = data["api_id"]
    api_hash = data["api_hash"]
    phone = msg.text.strip()

    # send code via pyrogram
    client = Client(
        name=f"login-{msg.from_user.id}",
        api_id=api_id,
        api_hash=api_hash,
        in_memory=True,
    )
    await client.connect()
    sent = await client.send_code(phone)
    await client.disconnect()

    await state.update_data(
        phone=phone,
        phone_code_hash=sent.phone_code_hash,
        code=""  # collected digits
    )

    await msg.answer("Verification Code\nUse the keypad below.", reply_markup=otp_kb())
    await state.set_state(Login.otp)


@dp.message(Login.otp)
async def get_otp(msg: Message, state: FSMContext):
    data = await state.get_data()
    api_id = data["api_id"]
    api_hash = data["api_hash"]
    phone = data["phone"]
    phone_code_hash = data["phone_code_hash"]
    code = data.get("code", "")
    txt = msg.text.strip()

    # special buttons
    if txt == "🧹 Clear":
        await state.update_data(code="")
        await msg.answer("Verification Code\nUse the keypad below.", reply_markup=otp_kb())
        return

    if txt == "⬅ Back":
        code = code[:-1]
        await state.update_data(code=code)
        # no reply so user can keep tapping fast
        return

    # digits: just store, don't reply -> faster input, less spam, less chance of expiry
    if txt.isdigit():
        if len(code) < 8:
            code += txt
            await state.update_data(code=code)
        return

    # only submit actually tries login
    if txt != "✔ Submit":
        return

    # final combined code
    code = (await state.get_data()).get("code", "")
    if not (4 <= len(code) <= 8):
        await msg.answer("Enter 4–8 digits, then press Submit.", reply_markup=otp_kb())
        return

    # try sign-in
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
        # resend new code and ask again
        new_sent = await client.send_code(phone)
        await client.disconnect()
        await state.update_data(phone_code_hash=new_sent.phone_code_hash, code="")
        await msg.answer("Code expired. New code sent.\nVerification Code\nUse the keypad below.", reply_markup=otp_kb())
        return
    except PhoneCodeInvalid:
        await client.disconnect()
        await state.update_data(code="")
        await msg.answer("❌ Wrong code. Try again.\nVerification Code\nUse the keypad below.", reply_markup=otp_kb())
        return
    except Exception as e:
        await client.disconnect()
        await state.clear()
        await msg.answer(f"Login failed: {e}\n/start again", reply_markup=ReplyKeyboardRemove())
        return

    # success — export session
    session_str = await client.export_session_string()

    # branding (best effort)
    try:
        await client.update_profile(bio="#1 Free Ads Bot — Join @PhiloBots")
        me = await client.get_me()
        base = me.first_name.split(" — ")[0]
        await client.update_profile(first_name=base + " — via @SpinifyAdsBot")
    except:
        pass

    await client.disconnect()

    # save to DB
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
    await msg.answer("✅ Session saved.\nYou can go back to the main bot now.", reply_markup=ReplyKeyboardRemove())


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
