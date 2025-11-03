# login_bot.py
# Aiogram v3 + Pyrogram
# OTP keypad attached UNDER the "Verification Code" message (INLINE keyboard)

import asyncio
import os
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command, StateFilter
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

# local guard (Telegram has its own TTL too)
LOCAL_CODE_TTL_SEC = 65


class Login(StatesGroup):
    api_id = State()
    api_hash = State()
    phone = State()
    otp = State()


def otp_inline_kb() -> InlineKeyboardMarkup:
    # 1-9, 0, then Back / Clear / Submit (INLINE under the message)
    btns = [
        [InlineKeyboardButton(text="1", callback_data="d:1"),
         InlineKeyboardButton(text="2", callback_data="d:2"),
         InlineKeyboardButton(text="3", callback_data="d:3")],
        [InlineKeyboardButton(text="4", callback_data="d:4"),
         InlineKeyboardButton(text="5", callback_data="d:5"),
         InlineKeyboardButton(text="6", callback_data="d:6")],
        [InlineKeyboardButton(text="7", callback_data="d:7"),
         InlineKeyboardButton(text="8", callback_data="d:8"),
         InlineKeyboardButton(text="9", callback_data="d:9")],
        [InlineKeyboardButton(text="0", callback_data="d:0")],
        [InlineKeyboardButton(text="⬅ Back", callback_data="act:back"),
         InlineKeyboardButton(text="🧹 Clear", callback_data="act:clear"),
         InlineKeyboardButton(text="✔ Submit", callback_data="act:submit")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=btns)


async def send_telegram_code(user_id: int, api_id: int, api_hash: str, phone: str) -> str:
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


@dp.message(Command("start"))
async def start(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer("Step 1 — Send your API_ID.")
    await state.set_state(Login.api_id)


@dp.message(StateFilter(Login.api_id))
async def step_api_id(msg: Message, state: FSMContext):
    try:
        api_id = int(msg.text.strip())
    except ValueError:
        await msg.answer("API_ID must be a number. Send again.")
        return
    await state.update_data(api_id=api_id)
    await msg.answer("Step 2 — Paste your API_HASH.")
    await state.set_state(Login.api_hash)


@dp.message(StateFilter(Login.api_hash))
async def step_api_hash(msg: Message, state: FSMContext):
    api_hash = msg.text.strip()
    await state.update_data(api_hash=api_hash)
    await msg.answer("Step 3 — Send your phone as +countrycode number.")
    await state.set_state(Login.phone)


@dp.message(StateFilter(Login.phone))
async def step_phone(msg: Message, state: FSMContext):
    data = await state.get_data()
    api_id = data["api_id"]
    api_hash = data["api_hash"]
    phone = msg.text.strip()

    # request code
    phone_code_hash = await send_telegram_code(msg.from_user.id, api_id, api_hash, phone)

    # send the OTP prompt with INLINE keypad
    sent_msg = await msg.answer("Verification Code\nUse the keypad below.", reply_markup=otp_inline_kb())

    await state.update_data(
        phone=phone,
        phone_code_hash=phone_code_hash,
        code="",
        code_sent_at=datetime.utcnow().isoformat(),
        otp_msg_id=sent_msg.message_id,
    )
    await state.set_state(Login.otp)


# --------- INLINE keypad handlers ---------

@dp.callback_query(StateFilter(Login.otp), F.data.startswith("d:"))
async def otp_digit(cq: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    code = data.get("code", "")
    digit = cq.data.split(":", 1)[1]
    if len(code) < 8:
        code += digit
        await state.update_data(code=code)
    # keep it fast: no edits on each tap
    await cq.answer()


@dp.callback_query(StateFilter(Login.otp), F.data == "act:back")
async def otp_back(cq: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    code = data.get("code", "")
    if code:
        code = code[:-1]
        await state.update_data(code=code)
    await cq.answer("Back")
    # optional: briefly show remaining length
    # (we skip editing to keep it fast)


@dp.callback_query(StateFilter(Login.otp), F.data == "act:clear")
async def otp_clear(cq: CallbackQuery, state: FSMContext):
    await state.update_data(code="")
    await cq.answer("Cleared")
    # refresh the prompt text (not required, but nice)
    try:
        await bot.edit_message_text(
            chat_id=cq.message.chat.id,
            message_id=cq.message.message_id,
            text="Verification Code\nUse the keypad below.",
            reply_markup=otp_inline_kb()
        )
    except Exception:
        pass


@dp.callback_query(StateFilter(Login.otp), F.data == "act:submit")
async def otp_submit(cq: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    api_id = data["api_id"]
    api_hash = data["api_hash"]
    phone = data["phone"]
    phone_code_hash = data["phone_code_hash"]
    code = data.get("code", "")
    sent_at = datetime.fromisoformat(data.get("code_sent_at"))

    # quick local TTL guard
    if datetime.utcnow() - sent_at > timedelta(seconds=LOCAL_CODE_TTL_SEC):
        new_hash = await send_telegram_code(cq.from_user.id, api_id, api_hash, phone)
        await state.update_data(phone_code_hash=new_hash, code="", code_sent_at=datetime.utcnow().isoformat())
        await cq.answer("Code expired. New code sent.")
        try:
            await bot.edit_message_text(
                chat_id=cq.message.chat.id,
                message_id=cq.message.message_id,
                text="Verification Code\nUse the keypad below.",
                reply_markup=otp_inline_kb()
            )
        except Exception:
            pass
        return

    if not (4 <= len(code) <= 8):
        await cq.answer("Enter 4–8 digits", show_alert=False)
        return

    # try to sign in
    client = Client(
        name=f"login-{cq.from_user.id}",
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
        new_hash = await client.send_code(phone)
        await client.disconnect()
        await state.update_data(
            phone_code_hash=new_hash.phone_code_hash,
            code="",
            code_sent_at=datetime.utcnow().isoformat()
        )
        await cq.answer("Code expired. New code sent.")
        try:
            await bot.edit_message_text(
                chat_id=cq.message.chat.id,
                message_id=cq.message.message_id,
                text="Verification Code\nUse the keypad below.",
                reply_markup=otp_inline_kb()
            )
        except Exception:
            pass
        return
    except PhoneCodeInvalid:
        await client.disconnect()
        await state.update_data(code="")
        await cq.answer("Wrong code.", show_alert=False)
        return
    except Exception as e:
        await client.disconnect()
        await state.clear()
        await cq.answer("Login failed.", show_alert=False)
        await bot.send_message(cq.message.chat.id, f"Login failed: {e}\n/start again")
        return

    # success
    session_str = await client.export_session_string()

    # brand (best effort)
    try:
        await client.update_profile(bio="#1 Free Ads Bot — Join @PhiloBots")
        me = await client.get_me()
        base = me.first_name.split(" — ")[0]
        await client.update_profile(first_name=base + " — via @SpinifyAdsBot")
    except Exception:
        pass

    await client.disconnect()

    # save session
    conn = get_conn()
    conn.execute(
        "INSERT INTO user_sessions(user_id, api_id, api_hash, session_string) "
        "VALUES (?, ?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET api_id=excluded.api_id, api_hash=excluded.api_hash, session_string=excluded.session_string",
        (cq.from_user.id, api_id, api_hash, session_str)
    )
    conn.commit()
    conn.close()

    await state.clear()
    await cq.answer("Logged in.")
    # edit the keypad message and drop the keypad
    try:
        await bot.edit_message_text(
            chat_id=cq.message.chat.id,
            message_id=cq.message.message_id,
            text="✅ Session saved.\nYou can go back to the main bot now."
        )
    except Exception:
        await bot.send_message(cq.message.chat.id, "✅ Session saved.\nYou can go back to the main bot now.")


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
    
