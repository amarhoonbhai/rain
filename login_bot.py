#!/usr/bin/env python3
import os
import asyncio
import logging

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from dotenv import load_dotenv

from pyrogram import Client
from pyrogram.errors import (
    PhoneCodeExpired,
    PhoneCodeInvalid,
    FloodWait,
    SessionPasswordNeeded,
    ApiIdInvalid,
    PhoneNumberInvalid,
    PhoneNumberFlood,
    PhoneNumberBanned,
)

from core.db import init_db, ensure_user, first_free_slot, sessions_upsert_slot

# ---------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------
load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("login-bot")

TOKEN = (os.getenv("LOGIN_BOT_TOKEN") or "").strip()
if not TOKEN:
    raise RuntimeError("LOGIN_BOT_TOKEN missing")

BIO = os.getenv("ENFORCE_BIO", "Managed by @SpinifyAdsBot")
SUFFIX = os.getenv("ENFORCE_NAME_SUFFIX", " | Spinify Ads")

bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
init_db()


# ---------------------------------------------------------
# FSM Flow
# ---------------------------------------------------------
class S(StatesGroup):
    api_id = State()
    api_hash = State()
    phone = State()
    otp = State()
    pwd = State()


def keypad():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(str(i), callback_data=f"d:{i}") for i in (1, 2, 3)],
            [InlineKeyboardButton(str(i), callback_data=f"d:{i}") for i in (4, 5, 6)],
            [InlineKeyboardButton(str(i), callback_data=f"d:{i}") for i in (7, 8, 9)],
            [InlineKeyboardButton("0", callback_data="d:0")],
            [
                InlineKeyboardButton("⬅", callback_data="act:back"),
                InlineKeyboardButton("🧹", callback_data="act:clear"),
                InlineKeyboardButton("✔ Login", callback_data="act:go"),
            ],
        ]
    )


# ---------------------------------------------------------
# Start
# ---------------------------------------------------------
@dp.message(Command("start"))
async def start_cmd(msg: Message, state: FSMContext):
    ensure_user(msg.from_user.id, msg.from_user.username)
    await state.clear()

    await msg.answer(
        "✹ <b>Spinify Login Panel</b>\n\n"
        "We will connect your Telegram account.\n"
        "Steps:\n"
        "1) Send API ID\n"
        "2) Send API HASH\n"
        "3) Send Phone Number\n"
        "4) Enter Login Code",
    )
    await state.set_state(S.api_id)


# ---------------------------------------------------------
# API ID
# ---------------------------------------------------------
@dp.message(StateFilter(S.api_id))
async def api_id_step(msg: Message, state: FSMContext):
    try:
        aid = int(msg.text.strip())
    except:
        return await msg.answer("Send a valid NUMBER for API ID.")

    await state.update_data(api_id=aid)
    await state.set_state(S.api_hash)
    await msg.answer("Good. Now send <b>API HASH</b>")


# ---------------------------------------------------------
# API HASH
# ---------------------------------------------------------
@dp.message(StateFilter(S.api_hash))
async def api_hash_step(msg: Message, state: FSMContext):
    ah = msg.text.strip()
    if not ah:
        return await msg.answer("API HASH can't be empty.")

    await state.update_data(api_hash=ah)
    await state.set_state(S.phone)
    await msg.answer("Send your phone number in +91 format.")


# ---------------------------------------------------------
# Send Code
# ---------------------------------------------------------
async def _send_code(aid: int, ah: str, phone: str):
    app = Client("login", api_id=aid, api_hash=ah, in_memory=True)
    await app.connect()
    sent = await app.send_code(phone)
    return app, sent


@dp.message(StateFilter(S.phone))
async def phone_step(msg: Message, state: FSMContext):
    phone = msg.text.strip()
    if not phone.startswith("+"):
        return await msg.answer("Phone must include +CountryCode.")

    d = await state.get_data()
    aid, ah = d["api_id"], d["api_hash"]

    m = await msg.answer("Sending login code…")

    try:
        app, sent = await _send_code(aid, ah, phone)
    except ApiIdInvalid:
        return await m.edit_text("API ID / HASH invalid. Restart with /start")
    except PhoneNumberInvalid:
        return await m.edit_text("Invalid phone number.")
    except PhoneNumberFlood:
        return await m.edit_text("Too many attempts. Try later.")
    except PhoneNumberBanned:
        return await m.edit_text("This number is banned.")
    except FloodWait as fw:
        return await m.edit_text(f"Wait {fw.value}s and try again.")
    except Exception as e:
        log.error(f"Code send error: {e}")
        return await m.edit_text("Unexpected error. Try again later.")

    await state.update_data(app=app, phone=phone, pch=sent.phone_code_hash, code="")
    await state.set_state(S.otp)

    await m.edit_text("Enter OTP using keypad:", reply_markup=keypad())


# ---------------------------------------------------------
# OTP Keypad
# ---------------------------------------------------------
@dp.callback_query(StateFilter(S.otp), F.data.startswith("d:"))
async def otp_digit(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    code = d.get("code", "") + cq.data.split(":")[1]
    await state.update_data(code=code)
    await cq.answer(code)


@dp.callback_query(StateFilter(S.otp), F.data == "act:back")
async def otp_back(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    code = d.get("code", "")[:-1]
    await state.update_data(code=code)
    await cq.answer(code or "empty")


@dp.callback_query(StateFilter(S.otp), F.data == "act:clear")
async def otp_clear(cq: CallbackQuery, state: FSMContext):
    await state.update_data(code="")
    await cq.answer("Cleared")


# ---------------------------------------------------------
# OTP Submit
# ---------------------------------------------------------
@dp.callback_query(StateFilter(S.otp), F.data == "act:go")
async def otp_submit(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    app: Client = d["app"]

    try:
        await app.sign_in(
            phone_number=d["phone"],
            phone_code_hash=d["pch"],
            phone_code=d.get("code", "")
        )
    except PhoneCodeInvalid:
        return await cq.answer("Wrong code.", show_alert=True)
    except PhoneCodeExpired:
        return await cq.message.edit_text("Code expired. Restart /start")
    except SessionPasswordNeeded:
        await state.set_state(S.pwd)
        return await cq.message.edit_text("Send your 2FA password:")

    session = await app.export_session_string()
    await app.disconnect()

    await finish_login(
        cq.message.chat.id, cq.from_user.id,
        d["api_id"], d["api_hash"], session, state
    )


# ---------------------------------------------------------
# 2FA Password
# ---------------------------------------------------------
@dp.message(StateFilter(S.pwd))
async def pwd_step(msg: Message, state: FSMContext):
    d = await state.get_data()
    app: Client = d["app"]

    try:
        await app.check_password(msg.text)
    except FloodWait as fw:
        return await msg.answer(f"Wait {fw.value}s and retry.")

    session = await app.export_session_string()
    await app.disconnect()

    await finish_login(
        msg.chat.id, msg.from_user.id,
        d["api_id"], d["api_hash"], session, state
    )


# ---------------------------------------------------------
# Final Save + Branding
# ---------------------------------------------------------
async def finish_login(chat_id, uid, api_id, api_hash, session_str, state: FSMContext):

    # Branding cosmetic
    try:
        tmp = Client(
            "final",
            api_id=api_id,
            api_hash=api_hash,
            session_string=session_str,
        )
        await tmp.start()

        try:
            await tmp.update_profile(bio=BIO)
        except:
            pass

        try:
            me = await tmp.get_me()
            base = me.first_name or "User"
            clean = base.split("|")[0].strip()
            await tmp.update_profile(first_name=f"{clean}{SUFFIX}")
        except:
            pass

        await tmp.stop()
    except Exception as e:
        log.warning(f"Branding failed: {e}")

    # Save
    slot = first_free_slot(uid)
    sessions_upsert_slot(uid, slot, api_id, api_hash, session_str)

    await state.clear()

    await bot.send_message(
        chat_id,
        f"✅ <b>Session Connected</b>\nSlot: {slot}\n\n"
        "Now:\n"
        "• Put your Ad in Saved Messages\n"
        "• Add groups using .addgc @link\n"
        "• Set interval with .time 30|45|60\n"
        "• Check status with .status\n"
    )


# ---------------------------------------------------------
# Entry
# ---------------------------------------------------------
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
