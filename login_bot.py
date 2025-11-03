# login_bot.py
# Aiogram v3 + Pyrogram
# Inline keypad under "Verification Code" + 2FA password flow (no force_sms)

import asyncio
import os
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, StateFilter
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from dotenv import load_dotenv
from pyrogram import Client
from pyrogram.errors import PhoneCodeExpired, PhoneCodeInvalid, FloodWait, SessionPasswordNeeded

from core.db import init_db, get_conn

# ---------- setup ----------
load_dotenv()
LOGIN_BOT_TOKEN = os.getenv("LOGIN_BOT_TOKEN")

bot = Bot(LOGIN_BOT_TOKEN)
dp = Dispatcher()
init_db()

# Keep a connected Pyrogram client when 2FA password is needed
LOGIN_CLIENTS: dict[int, Client] = {}

# Local TTL guard (Telegram has own TTL as well)
LOCAL_CODE_TTL_SEC = 65


class Login(StatesGroup):
    api_id = State()
    api_hash = State()
    phone = State()
    otp = State()
    password = State()


def otp_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
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
        [InlineKeyboardButton(text="🔁 Resend", callback_data="act:resend")],
    ])


async def send_code(user_id: int, api_id: int, api_hash: str, phone: str) -> str:
    """Send Telegram login code. Compatible with Pyrogram versions without force_sms."""
    app = Client(name=f"login-{user_id}", api_id=api_id, api_hash=api_hash, in_memory=True)
    await app.connect()
    sent = await app.send_code(phone)  # no force_sms here
    await app.disconnect()
    return sent.phone_code_hash


# ---------- flow ----------

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
    d = await state.get_data()
    api_id, api_hash = d["api_id"], d["api_hash"]
    phone = msg.text.strip()

    try:
        pch = await send_code(msg.from_user.id, api_id, api_hash, phone)
    except Exception as e:
        await msg.answer(f"Could not send code: {e}")
        return

    prompt = await msg.answer("Verification Code\nUse the keypad below.", reply_markup=otp_inline_kb())

    await state.update_data(
        phone=phone,
        phone_code_hash=pch,
        code="",
        code_sent_at=datetime.utcnow().isoformat(),
        otp_msg_id=prompt.message_id,
    )
    await state.set_state(Login.otp)


# ---- inline keypad handlers ----

@dp.callback_query(StateFilter(Login.otp), F.data.startswith("d:"))
async def otp_digit(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    code = d.get("code", "")
    digit = cq.data.split(":", 1)[1]
    if len(code) < 8:
        code += digit
        await state.update_data(code=code)
    await cq.answer()  # silent & fast


@dp.callback_query(StateFilter(Login.otp), F.data == "act:back")
async def otp_back(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    code = d.get("code", "")
    if code:
        await state.update_data(code=code[:-1])
    await cq.answer("Back")


@dp.callback_query(StateFilter(Login.otp), F.data == "act:clear")
async def otp_clear(cq: CallbackQuery, state: FSMContext):
    await state.update_data(code="")
    await cq.answer("Cleared")
    try:
        await bot.edit_message_text(
            chat_id=cq.message.chat.id,
            message_id=cq.message.message_id,
            text="Verification Code\nUse the keypad below.",
            reply_markup=otp_inline_kb(),
        )
    except Exception:
        pass


@dp.callback_query(StateFilter(Login.otp), F.data == "act:resend")
async def otp_resend(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    try:
        new_hash = await send_code(cq.from_user.id, d["api_id"], d["api_hash"], d["phone"])
    except Exception as e:
        await cq.answer("Resend failed")
        await bot.send_message(cq.message.chat.id, f"Could not resend code: {e}")
        return

    await state.update_data(phone_code_hash=new_hash, code="", code_sent_at=datetime.utcnow().isoformat())
    await cq.answer("New code sent")
    try:
        await bot.edit_message_text(
            chat_id=cq.message.chat.id,
            message_id=cq.message.message_id,
            text="Verification Code\nUse the keypad below.",
            reply_markup=otp_inline_kb(),
        )
    except Exception:
        pass


@dp.callback_query(StateFilter(Login.otp), F.data == "act:submit")
async def otp_submit(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    user_id = cq.from_user.id
    api_id, api_hash, phone = d["api_id"], d["api_hash"], d["phone"]
    phone_code_hash = d["phone_code_hash"]
    code = d.get("code", "")
    sent_at = datetime.fromisoformat(d["code_sent_at"])

    # Local TTL guard before trying
    if datetime.utcnow() - sent_at > timedelta(seconds=LOCAL_CODE_TTL_SEC):
        try:
            new_hash = await send_code(user_id, api_id, api_hash, phone)
        except Exception as e:
            await cq.answer("Resend failed")
            await bot.send_message(cq.message.chat.id, f"Could not resend code: {e}")
            return
        await state.update_data(phone_code_hash=new_hash, code="", code_sent_at=datetime.utcnow().isoformat())
        await cq.answer("Code expired. New code sent.")
        try:
            await bot.edit_message_text(
                chat_id=cq.message.chat.id,
                message_id=cq.message.message_id,
                text="Verification Code\nUse the keypad below.",
                reply_markup=otp_inline_kb(),
            )
        except Exception:
            pass
        return

    if not (4 <= len(code) <= 8):
        await cq.answer("Enter 4–8 digits")
        return

    app = Client(name=f"login-{user_id}", api_id=api_id, api_hash=api_hash, in_memory=True)
    await app.connect()
    try:
        await app.sign_in(phone_number=phone, phone_code_hash=phone_code_hash, phone_code=code)
    except SessionPasswordNeeded:
        # Keep the connected client to finish password step later
        LOGIN_CLIENTS[user_id] = app
        await state.set_state(Login.password)
        await cq.answer()
        await bot.send_message(cq.message.chat.id, "This account has 2-step verification.\nSend your password now:")
        return
    except PhoneCodeExpired:
        new_hash = await app.send_code(phone)
        await app.disconnect()
        await state.update_data(phone_code_hash=new_hash.phone_code_hash, code="", code_sent_at=datetime.utcnow().isoformat())
        await cq.answer("Code expired. New code sent.")
        try:
            await bot.edit_message_text(
                chat_id=cq.message.chat.id,
                message_id=cq.message.message_id,
                text="Verification Code\nUse the keypad below.",
                reply_markup=otp_inline_kb(),
            )
        except Exception:
            pass
        return
    except PhoneCodeInvalid:
        await app.disconnect()
        await state.update_data(code="")
        await cq.answer("Wrong code")
        return
    except FloodWait as fw:
        await app.disconnect()
        await cq.answer(f"Wait {fw.value}s", show_alert=True)
        return
    except Exception as e:
        await app.disconnect()
        await state.clear()
        await cq.answer("Login failed")
        await bot.send_message(cq.message.chat.id, f"Login failed: {e}\n/start again")
        return

    # success (no 2FA)
    session_str = await app.export_session_string()
    try:
        await app.update_profile(bio="#1 Free Ads Bot — Join @PhiloBots")
        me = await app.get_me()
        base = me.first_name.split(" — ")[0]
        await app.update_profile(first_name=base + " — via @SpinifyAdsBot")
    except Exception:
        pass
    await app.disconnect()

    conn = get_conn()
    conn.execute(
        "INSERT INTO user_sessions(user_id, api_id, api_hash, session_string) "
        "VALUES (?, ?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET api_id=excluded.api_id, api_hash=excluded.api_hash, session_string=excluded.session_string",
        (user_id, api_id, api_hash, session_str)
    )
    conn.commit()
    conn.close()

    await state.clear()
    await cq.answer("Logged in")
    try:
        await bot.edit_message_text(
            chat_id=cq.message.chat.id,
            message_id=cq.message.message_id,
            text="✅ Session saved.\nYou can go back to the main bot now.",
        )
    except Exception:
        await bot.send_message(cq.message.chat.id, "✅ Session saved.\nYou can go back to the main bot now.")


# ---- 2FA password step ----

@dp.message(StateFilter(Login.password))
async def step_password(msg: Message, state: FSMContext):
    d = await state.get_data()
    user_id = msg.from_user.id
    api_id, api_hash = d["api_id"], d["api_hash"]
    password = msg.text

    app = LOGIN_CLIENTS.get(user_id)
    fresh = False
    if app is None:
        # Rare fallback: open a fresh client; Telegram will immediately ask for password
        app = Client(name=f"login-{user_id}", api_id=api_id, api_hash=api_hash, in_memory=True)
        await app.connect()
        fresh = True

    try:
        await app.check_password(password)
    except FloodWait as fw:
        await app.disconnect()
        LOGIN_CLIENTS.pop(user_id, None)
        await state.set_state(Login.otp)  # stay in flow
        await msg.answer(f"Too many attempts. Try again after {fw.value}s.")
        return
    except Exception:
        if fresh:
            await app.disconnect()
        await msg.answer("❌ Wrong password. Send the 2FA password again.")
        return

    # success with 2FA
    session_str = await app.export_session_string()
    try:
        await app.update_profile(bio="#1 Free Ads Bot — Join @PhiloBots")
        me = await app.get_me()
        base = me.first_name.split(" — ")[0]
        await app.update_profile(first_name=base + " — via @SpinifyAdsBot")
    except Exception:
        pass
    await app.disconnect()
    LOGIN_CLIENTS.pop(user_id, None)

    conn = get_conn()
    conn.execute(
        "INSERT INTO user_sessions(user_id, api_id, api_hash, session_string) "
        "VALUES (?, ?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET api_id=excluded.api_id, api_hash=excluded.api_hash, session_string=excluded.session_string",
        (user_id, api_id, api_hash, session_str)
    )
    conn.commit()
    conn.close()

    await state.clear()
    await msg.answer("✅ Session saved.\nYou can go back to the main bot now.")


# ---------- runner ----------

async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
