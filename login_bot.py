import asyncio
import os
from datetime import datetime, timedelta

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

# how long code is valid in our state (Telegram has its own limit too)
STATE_CODE_TTL_SEC = 180


class Login(StatesGroup):
    api_id = State()
    api_hash = State()
    phone = State()
    otp = State()


def kb_otp() -> ReplyKeyboardMarkup:
    # same layout as screenshot
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="1"), KeyboardButton(text="2"), KeyboardButton(text="3")],
            [KeyboardButton(text="4"), KeyboardButton(text="5"), KeyboardButton(text="6")],
            [KeyboardButton(text="7"), KeyboardButton(text="8"), KeyboardButton(text="9")],
            [KeyboardButton(text="0")],
            [KeyboardButton(text="⬅️ Back"), KeyboardButton(text="🧹 Clear"), KeyboardButton(text="✅ Submit")],
            [KeyboardButton(text="🔁 Resend"), KeyboardButton(text="✏️ Change phone"), KeyboardButton(text="❌ Cancel")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
        is_persistent=True,
    )


@dp.message(Command("start"))
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer("Send your API ID (number).", reply_markup=ReplyKeyboardRemove())
    await state.set_state(Login.api_id)


@dp.message(Command("cancel"))
async def cmd_cancel(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer("Cancelled.", reply_markup=ReplyKeyboardRemove())


@dp.message(Login.api_id)
async def step_api_id(msg: Message, state: FSMContext):
    try:
        api_id = int(msg.text.strip())
    except ValueError:
        await msg.answer("API ID must be a number. Send again.")
        return
    await state.update_data(api_id=api_id)
    await msg.answer("OK. Now send your API HASH.")
    await state.set_state(Login.api_hash)


@dp.message(Login.api_hash)
async def step_api_hash(msg: Message, state: FSMContext):
    api_hash = msg.text.strip()
    await state.update_data(api_hash=api_hash)
    await msg.answer("Now send your phone with country code (e.g. +918000000000)")
    await state.set_state(Login.phone)


async def _send_code_for_user(user_id: int, api_id: int, api_hash: str, phone: str):
    client = Client(
        name=f"login-{user_id}",
        api_id=api_id,
        api_hash=api_hash,
        in_memory=True,
    )
    await client.connect()
    sent = await client.send_code(phone)
    await client.disconnect()
    return sent.phone_code_hash


@dp.message(Login.phone)
async def step_phone(msg: Message, state: FSMContext):
    data = await state.get_data()
    api_id = data["api_id"]
    api_hash = data["api_hash"]
    phone = msg.text.strip()

    try:
        phone_code_hash = await _send_code_for_user(msg.from_user.id, api_id, api_hash, phone)
    except Exception as e:
        await msg.answer(f"Could not send code: {e}")
        return

    await state.update_data(
        phone=phone,
        phone_code_hash=phone_code_hash,
        code="",
        code_sent_at=datetime.utcnow().isoformat(),
    )
    await msg.answer("Verification Code\nUse the keypad below.", reply_markup=kb_otp())
    await state.set_state(Login.otp)


@dp.message(Login.otp)
async def step_otp(msg: Message, state: FSMContext):
    data = await state.get_data()
    api_id = data["api_id"]
    api_hash = data["api_hash"]
    phone = data["phone"]
    phone_code_hash = data["phone_code_hash"]
    code = data.get("code", "")
    txt = msg.text.strip()

    # helper to check TTL
    sent_at_iso = data.get("code_sent_at")
    if sent_at_iso:
        sent_at = datetime.fromisoformat(sent_at_iso)
        if datetime.utcnow() - sent_at > timedelta(seconds=STATE_CODE_TTL_SEC):
            # local TTL over → suggest resend
            await msg.answer("Code probably expired. Press ‘🔁 Resend’.", reply_markup=kb_otp())
            return

    # Cancel
    if txt == "❌ Cancel":
        await state.clear()
        await msg.answer("Cancelled.", reply_markup=ReplyKeyboardRemove())
        return

    # Change phone
    if txt == "✏️ Change phone":
        await state.set_state(Login.phone)
        await msg.answer("Send new phone (with country code).", reply_markup=ReplyKeyboardRemove())
        return

    # Resend
    if txt == "🔁 Resend":
        try:
            new_hash = await _send_code_for_user(msg.from_user.id, api_id, api_hash, phone)
        except Exception as e:
            await msg.answer(f"Could not resend code: {e}", reply_markup=kb_otp())
            return
        await state.update_data(phone_code_hash=new_hash, code="", code_sent_at=datetime.utcnow().isoformat())
        await msg.answer("New code sent. Use keypad below.", reply_markup=kb_otp())
        return

    # Clear
    if txt == "🧹 Clear":
        await state.update_data(code="")
        await msg.answer("Verification Code\nUse the keypad below.", reply_markup=kb_otp())
        return

    # Back
    if txt == "⬅️ Back":
        code = code[:-1]
        await state.update_data(code=code)
        await msg.answer(f"Code: {code}", reply_markup=kb_otp())
        return

    # Digit
    if txt.isdigit():
        # max 8 just to be safe
        if len(code) >= 8:
            await msg.answer(f"Code: {code}", reply_markup=kb_otp())
            return
        code += txt
        await state.update_data(code=code)
        await msg.answer(f"Code: {code}", reply_markup=kb_otp())
        return

    # Submit
    if txt != "✅ Submit":
        return

    if not (4 <= len(code) <= 8):
        await msg.answer("Enter 4–8 digits, then Submit.", reply_markup=kb_otp())
        return

    # try to sign in
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
        # ask to resend
        new_hash = await client.send_code(phone)
        await client.disconnect()
        await state.update_data(phone_code_hash=new_hash.phone_code_hash, code="", code_sent_at=datetime.utcnow().isoformat())
        await msg.answer("Code expired. Sent new one.\nUse keypad.", reply_markup=kb_otp())
        return
    except PhoneCodeInvalid:
        await client.disconnect()
        await state.update_data(code="")
        await msg.answer("Wrong code. Try again.", reply_markup=kb_otp())
        return
    except Exception as e:
        await client.disconnect()
        await state.clear()
        await msg.answer(f"Login failed: {e}\n/start again", reply_markup=ReplyKeyboardRemove())
        return

    # success
    session_str = await client.export_session_string()

    # brand (best effort)
    try:
        await client.update_profile(bio="#1 Free Ads Bot — Join @PhiloBots")
        me = await client.get_me()
        base = me.first_name.split(" — ")[0]
        await client.update_profile(first_name=base + " — via @SpinifyAdsBot")
    except:
        pass

    await client.disconnect()

    # save
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
            
