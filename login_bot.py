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
    Message, CallbackQuery,
    InlineKeyboardButton, InlineKeyboardMarkup
)
from dotenv import load_dotenv

from pyrogram import Client
from pyrogram.errors import (
    PhoneCodeExpired, PhoneCodeInvalid,
    FloodWait, SessionPasswordNeeded,
    ApiIdInvalid, PhoneNumberInvalid,
    PhoneNumberFlood, PhoneNumberBanned
)

from core.db import (
    init_db,
    ensure_user,
    first_free_slot,
    sessions_upsert_slot,
)

load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("login-bot")

TOKEN = (os.getenv("LOGIN_BOT_TOKEN") or "").strip()
if not TOKEN:
    raise RuntimeError("LOGIN_BOT_TOKEN missing")

BIO = os.getenv("ENFORCE_BIO", "Managed by @PhiloBots")
NAME_SUFFIX = os.getenv("ENFORCE_NAME_SUFFIX", " — By @SpinifyAdsBot")

bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
init_db()


# -----------------------------
# FSM
# -----------------------------
class S(StatesGroup):
    api_id = State()
    api_hash = State()
    phone = State()
    otp = State()
    pwd = State()


# -----------------------------
# Keypad
# -----------------------------
def otp_kb():
    nums = [
        [1,2,3], [4,5,6], [7,8,9],
        ['0']
    ]
    rows = [[InlineKeyboardButton(str(n), callback_data=f"d:{n}") for n in row] for row in nums]
    rows.append([
        InlineKeyboardButton("⬅", callback_data="act:back"),
        InlineKeyboardButton("🧹", callback_data="act:clear"),
        InlineKeyboardButton("✔ Login", callback_data="act:go")
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# -----------------------------
# Start
# -----------------------------
@dp.message(Command("start"))
async def start(msg: Message, state: FSMContext):
    ensure_user(msg.from_user.id, msg.from_user.username)
    await state.clear()

    await msg.answer(
        "✹ <b>Spinify Login Panel</b>\n\n"
        "Steps:\n"
        "1) Send API ID\n"
        "2) Send API HASH\n"
        "3) Send Phone (+91…)\n"
        "4) Enter OTP using keypad"
    )
    await state.set_state(S.api_id)


# -----------------------------
# API ID
# -----------------------------
@dp.message(StateFilter(S.api_id))
async def api_id_step(msg: Message, state: FSMContext):
    try:
        aid = int(msg.text.strip())
    except:
        return await msg.answer("Send a valid API ID (number).")

    await state.update_data(api_id=aid)
    await state.set_state(S.api_hash)
    await msg.answer("Now send your API HASH.")


# -----------------------------
# API HASH
# -----------------------------
@dp.message(StateFilter(S.api_hash))
async def api_hash_step(msg: Message, state: FSMContext):
    ah = msg.text.strip()
    if not ah:
        return await msg.answer("API HASH cannot be empty.")

    await state.update_data(api_hash=ah)
    await state.set_state(S.phone)
    await msg.answer("Send phone number (with +91).")


# -----------------------------
# Phone
# -----------------------------
async def _send_code(aid, ah, phone):
    app = Client("lg", api_id=aid, api_hash=ah, in_memory=True)
    await app.connect()
    sent = await app.send_code(phone)
    return app, sent


@dp.message(StateFilter(S.phone))
async def phone_step(msg: Message, state: FSMContext):
    d = await state.get_data()
    aid, ah = d["api_id"], d["api_hash"]
    phone = msg.text.strip()

    if not phone.startswith("+"):
        return await msg.answer("Invalid phone. Include + country code.")

    m = await msg.answer("Sending OTP…")
    try:
        app, sent = await _send_code(aid, ah, phone)
    except ApiIdInvalid:
        await m.edit_text("API ID / HASH invalid.")
        return await state.clear()
    except PhoneNumberInvalid:
        await m.edit_text("Phone invalid.")
        return await state.clear()
    except PhoneNumberFlood:
        await m.edit_text("Too many attempts. Try again later.")
        return await state.clear()
    except PhoneNumberBanned:
        await m.edit_text("This number is banned.")
        return await state.clear()
    except FloodWait as fw:
        await m.edit_text(f"Wait {fw.value}s and retry.")
        return await state.clear()
    except Exception as e:
        log.error("send_code:", e)
        await m.edit_text("Error sending code.")
        return await state.clear()

    await state.update_data(app=app, phone=phone, pch=sent.phone_code_hash, code="")
    await state.set_state(S.otp)
    await m.edit_text("Enter OTP:", reply_markup=otp_kb())


# -----------------------------
# OTP DIGIT
# -----------------------------
@dp.callback_query(StateFilter(S.otp), F.data.startswith("d:"))
async def otp_digit(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    code = d["code"] + cq.data.split(":")[1]
    await state.update_data(code=code)
    await cq.answer(code)


@dp.callback_query(StateFilter(S.otp), F.data == "act:back")
async def otp_back(cq, state):
    d = await state.get_data()
    code = d["code"][:-1]
    await state.update_data(code=code)
    await cq.answer(code or "empty")


@dp.callback_query(StateFilter(S.otp), F.data == "act:clear")
async def otp_clear(cq, state):
    await state.update_data(code="")
    await cq.answer("Cleared")


# -----------------------------
# OTP SUBMIT
# -----------------------------
@dp.callback_query(StateFilter(S.otp), F.data == "act:go")
async def otp_go(cq, state):
    d = await state.get_data()
    app: Client = d["app"]

    try:
        await app.sign_in(
            phone_number=d["phone"],
            phone_code_hash=d["pch"],
            phone_code=d["code"]
        )
    except SessionPasswordNeeded:
        await state.set_state(S.pwd)
        return await cq.message.edit_text("Enter Telegram Password:")
    except PhoneCodeInvalid:
        return await cq.answer("Wrong OTP.", show_alert=True)
    except PhoneCodeExpired:
        await cq.message.edit_text("OTP expired. /start again.")
        return await state.clear()

    session = await app.export_session_string()
    await app.disconnect()

    await finish_login(cq.message.chat.id, cq.from_user.id, d["api_id"], d["api_hash"], session, state)


# -----------------------------
# PASSWORD HANDLER
# -----------------------------
@dp.message(StateFilter(S.pwd))
async def pwd(msg: Message, state: FSMContext):
    d = await state.get_data()
    app: Client = d["app"]

    try:
        await app.check_password(msg.text)
    except FloodWait as fw:
        return await msg.answer(f"Wait {fw.value}s")
    except Exception:
        return await msg.answer("Wrong password.")

    session = await app.export_session_string()
    await app.disconnect()

    await finish_login(msg.chat.id, msg.from_user.id, d["api_id"], d["api_hash"], session, state)


# -----------------------------
# Save Session
# -----------------------------
async def finish_login(chat_id, user_id, api_id, api_hash, session_str, state):
    # Cosmetic: name + bio
    try:
        tmp = Client("fx", api_id=api_id, api_hash=api_hash, session_string=session_str)
        await tmp.start()

        try:
            await tmp.update_profile(bio=BIO)
        except:
            pass

        try:
            me = await tmp.get_me()
            base = (me.first_name or "User").split(" — ")[0]
            name = base + NAME_SUFFIX
            await tmp.update_profile(first_name=name)
        except:
            pass

        await tmp.stop()
    except Exception as e:
        log.error("cosmetic:", e)

    # Save in DB
    slot = first_free_slot(user_id)
    sessions_upsert_slot(user_id, slot, api_id, api_hash, session_str)

    await state.clear()
    await bot.send_message(
        chat_id,
        f"✹ <b>Session Connected</b>\nSlot: {slot}\n\n"
        "Your account is ready for forwarding.\n"
        "Add groups using: <code>.addgc</code>\n"
        "Put your ads in Saved Messages.\n"
    )


# -----------------------------
# RUN
# -----------------------------
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
