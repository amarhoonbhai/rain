#!/usr/bin/env python3
import os
import logging
import asyncio

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, StateFilter
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardButton, InlineKeyboardMarkup,
)
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from dotenv import load_dotenv

from pyrogram import Client
from pyrogram.errors import (
    PhoneNumberInvalid, PhoneNumberFlood, PhoneNumberBanned,
    PhoneCodeInvalid, PhoneCodeExpired, SessionPasswordNeeded,
    FloodWait, ApiIdInvalid
)

from core.db import (
    init_db, ensure_user,
    first_free_slot, sessions_upsert_slot
)

# -------------------------------------------------------------------
# Bootstrap
# -------------------------------------------------------------------
load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("login-bot")

TOKEN = (os.getenv("LOGIN_BOT_TOKEN") or "").strip()
if not TOKEN:
    raise RuntimeError("LOGIN_BOT_TOKEN missing")

BIO = os.getenv("ENFORCE_BIO", "Managed by @SpinifyAdsBot")
NAME_SUFFIX = os.getenv("ENFORCE_NAME_SUFFIX", " • Spinify Ads")

bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
init_db()


# -------------------------------------------------------------------
# FSM States
# -------------------------------------------------------------------
class S(StatesGroup):
    api_id = State()
    api_hash = State()
    phone = State()
    otp = State()
    pwd = State()


# -------------------------------------------------------------------
# OTP Keyboard
# -------------------------------------------------------------------
def kb_otp():
    rows = [
        [InlineKeyboardButton(str(i), callback_data=f"d:{i}") for i in (1, 2, 3)],
        [InlineKeyboardButton(str(i), callback_data=f"d:{i}") for i in (4, 5, 6)],
        [InlineKeyboardButton(str(i), callback_data=f"d:{i}") for i in (7, 8, 9)],
        [InlineKeyboardButton("0", callback_data="d:0")],
        [
            InlineKeyboardButton("←", callback_data="act:back"),
            InlineKeyboardButton("C", callback_data="act:clear"),
            InlineKeyboardButton("✔ Login", callback_data="act:go"),
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


# -------------------------------------------------------------------
# /start
# -------------------------------------------------------------------
@dp.message(Command("start"))
async def start(msg: Message, state: FSMContext):
    ensure_user(msg.from_user.id, msg.from_user.username)
    await state.clear()

    await msg.answer(
        "🔐 <b>Spinify Login Panel</b>\n\n"
        "We'll connect your Telegram account.\n"
        "Send your <b>API ID</b>."
    )
    await state.set_state(S.api_id)


# -------------------------------------------------------------------
# API ID
# -------------------------------------------------------------------
@dp.message(StateFilter(S.api_id))
async def step_apiid(msg: Message, state: FSMContext):
    try:
        aid = int(msg.text.strip())
    except:
        return await msg.answer("❌ Send a valid number.")

    await state.update_data(api_id=aid)
    await state.set_state(S.api_hash)
    await msg.answer("Send your <b>API HASH</b>.")


# -------------------------------------------------------------------
# API HASH
# -------------------------------------------------------------------
@dp.message(StateFilter(S.api_hash))
async def step_apihash(msg: Message, state: FSMContext):
    ah = msg.text.strip()
    if not ah:
        return await msg.answer("❌ API HASH cannot be empty.")

    await state.update_data(api_hash=ah)
    await state.set_state(S.phone)
    await msg.answer("Now send your phone:\n<b>+91xxxxxxxxxx</b>")


# -------------------------------------------------------------------
# Send code
# -------------------------------------------------------------------
async def send_code(aid, ah, phone):
    app = Client("login", api_id=aid, api_hash=ah, in_memory=True)
    await app.connect()
    sent = await app.send_code(phone)
    return app, sent.phone_code_hash


@dp.message(StateFilter(S.phone))
async def step_phone(msg: Message, state: FSMContext):
    phone = msg.text.strip()
    d = await state.get_data()
    aid, ah = d["api_id"], d["api_hash"]

    if not phone.startswith("+"):
        return await msg.answer("❌ Include + country code.")

    m = await msg.answer("Sending code…")

    try:
        app, pch = await send_code(aid, ah, phone)
    except ApiIdInvalid:
        await m.edit_text("❌ API credentials invalid.")
        return await state.clear()
    except PhoneNumberInvalid:
        await m.edit_text("❌ Invalid phone number.")
        return await state.clear()
    except PhoneNumberFlood:
        await m.edit_text("⏳ Too many attempts. Try later.")
        return await state.clear()
    except PhoneNumberBanned:
        await m.edit_text("❌ Number banned.")
        return await state.clear()
    except FloodWait as e:
        await m.edit_text(f"⏳ Wait {e.value}s.")
        return await state.clear()
    except Exception as e:
        log.error(e)
        await m.edit_text("Unexpected error. Try again.")
        return await state.clear()

    await state.update_data(app=app, pch=pch, phone=phone, code="")
    await state.set_state(S.otp)

    await m.edit_text("Enter code via keypad:", reply_markup=kb_otp())


# -------------------------------------------------------------------
# OTP Keypad
# -------------------------------------------------------------------
@dp.callback_query(StateFilter(S.otp), F.data.startswith("d:"))
async def otp_digit(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    code = d["code"] + cq.data.split(":")[1]
    await state.update_data(code=code)
    await cq.answer(code)


@dp.callback_query(StateFilter(S.otp), F.data == "act:back")
async def otp_back(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    code = d["code"][:-1]
    await state.update_data(code=code)
    await cq.answer(code or "empty")


@dp.callback_query(StateFilter(S.otp), F.data == "act:clear")
async def otp_clear(cq: CallbackQuery, state: FSMContext):
    await state.update_data(code="")
    await cq.answer("Cleared")


@dp.callback_query(StateFilter(S.otp), F.data == "act:go")
async def otp_go(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    app: Client = d["app"]

    try:
        await app.sign_in(
            phone_number=d["phone"],
            phone_code_hash=d["pch"],
            phone_code=d["code"],
        )
    except PhoneCodeInvalid:
        return await cq.message.answer("❌ Wrong code.")
    except PhoneCodeExpired:
        await state.clear()
        return await cq.message.answer("❌ Code expired. /start again.")
    except SessionPasswordNeeded:
        await state.set_state(S.pwd)
        return await cq.message.answer("Send your 2FA password.")

    session = await app.export_session_string()
    await app.disconnect()
    await finish_login(cq.message.chat.id, cq.from_user.id, d, session, state)


# -------------------------------------------------------------------
# 2FA Password
# -------------------------------------------------------------------
@dp.message(StateFilter(S.pwd))
async def step_pwd(msg: Message, state: FSMContext):
    d = await state.get_data()
    app: Client = d["app"]

    try:
        await app.check_password(msg.text)
    except FloodWait as e:
        return await msg.answer(f"⏳ Wait {e.value}s.")

    session = await app.export_session_string()
    await app.disconnect()
    await finish_login(msg.chat.id, msg.from_user.id, d, session, state)


# -------------------------------------------------------------------
# Finish login
# -------------------------------------------------------------------
async def finish_login(chat_id, uid, d, session_str, state):
    api_id = d["api_id"]
    api_hash = d["api_hash"]

    # --- cosmetic branding ---
    try:
        tmp = Client("cosmetic", api_id=api_id, api_hash=api_hash, session_string=session_str)
        await tmp.start()

        try:
            await tmp.update_profile(bio=BIO)
        except: pass

        try:
            me = await tmp.get_me()
            base = me.first_name or "User"
            if NAME_SUFFIX not in base:
                await tmp.update_profile(first_name=base + NAME_SUFFIX)
        except: pass

        await tmp.stop()
    except Exception as e:
        log.warning("Cosmetic update failed: %s", e)

    # save session
    slot = first_free_slot(uid)
    sessions_upsert_slot(uid, slot, api_id, api_hash, session_str)
    await state.clear()

    await bot.send_message(
        chat_id,
        f"✅ <b>Session Connected</b>\nSlot: <b>{slot}</b>\n\n"
        "Now from your logged-in account:\n"
        "• Put ads in <b>Saved Messages</b>\n"
        "• .addgc @group\n"
        "• .time 30|45|60\n"
        "• The worker will forward automatically."
    )


# -------------------------------------------------------------------
# Entrypoint
# -------------------------------------------------------------------
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
