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

from core.db import (
    init_db,
    ensure_user,
    first_free_slot,
    sessions_upsert_slot
)

# ============================================================
# Bootstrap
# ============================================================
load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("login-bot")

TOKEN = (os.getenv("LOGIN_BOT_TOKEN") or "").strip()
if not TOKEN or ":" not in TOKEN:
    raise RuntimeError("LOGIN_BOT_TOKEN missing")

BIO = os.getenv("ENFORCE_BIO", "#1 Free Ads Bot — Managed By @PhiloBots")
NAME_SUFFIX = os.getenv("ENFORCE_NAME_SUFFIX", " Hosted By — @SpinifyAdsBot")

bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
init_db()


# ============================================================
# FSM
# ============================================================
class S(StatesGroup):
    api_id = State()
    api_hash = State()
    phone = State()
    otp = State()
    pwd = State()


def _kb_otp():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(str(i), callback_data=f"d:{i}") for i in (1, 2, 3)],
            [InlineKeyboardButton(str(i), callback_data=f"d:{i}") for i in (4, 5, 6)],
            [InlineKeyboardButton(str(i), callback_data=f"d:{i}") for i in (7, 8, 9)],
            [InlineKeyboardButton("0", callback_data="d:0")],
            [
                InlineKeyboardButton("⬅", callback_data="act:back"),
                InlineKeyboardButton("🧹", callback_data="act:clear"),
                InlineKeyboardButton("✔️ Login", callback_data="act:go"),
            ],
        ]
    )


# ============================================================
# /start
# ============================================================
@dp.message(Command("start"))
async def start(msg: Message, state: FSMContext):
    ensure_user(msg.from_user.id, msg.from_user.username)
    await state.clear()

    await msg.answer(
        "✹ <b>Spinify Login Panel</b>\n\n"
        "We will connect your Telegram account.\n\n"
        "Steps:\n"
        " 1) Send your <b>API ID</b>\n"
        " 2) Send your <b>API HASH</b>\n"
        " 3) Send your <b>Phone Number</b>\n"
        " 4) Enter OTP using keypad\n\n"
        "Get API credentials from my.telegram.org → API Development Tools."
    )
    await state.set_state(S.api_id)


# ============================================================
# API ID
# ============================================================
@dp.message(StateFilter(S.api_id))
async def api_id_step(msg: Message, state: FSMContext):
    try:
        aid = int((msg.text or "").strip())
    except:
        return await msg.answer("✹ API ID must be a number.")
    await state.update_data(api_id=aid)
    await state.set_state(S.api_hash)
    await msg.answer("✹ Good. Now send your <b>API HASH</b>.")


# ============================================================
# API HASH
# ============================================================
@dp.message(StateFilter(S.api_hash))
async def api_hash_step(msg: Message, state: FSMContext):
    t = (msg.text or "").strip()
    if not t:
        return await msg.answer("✹ API HASH cannot be empty.")
    await state.update_data(api_hash=t)
    await state.set_state(S.phone)
    await msg.answer(
        "✹ Send your phone number with country code.\n"
        "Example: <code>+919876543210</code>"
    )


# ============================================================
# Send Code
# ============================================================
async def _send_code(aid, ah, phone):
    app = Client("login", api_id=aid, api_hash=ah, in_memory=True)
    await app.connect()
    sent = await app.send_code(phone)
    return app, sent


@dp.message(StateFilter(S.phone))
async def phone_step(msg: Message, state: FSMContext):
    d = await state.get_data()
    aid, ah = d["api_id"], d["api_hash"]

    phone = (msg.text or "").strip()
    if not phone.startswith("+"):
        return await msg.answer("✹ Phone must include +country code.")

    info = await msg.answer("✹ Sending OTP…")

    try:
        app, sent = await _send_code(aid, ah, phone)

    except ApiIdInvalid:
        await info.edit_text("✹ API ID / HASH invalid. Restart with /start")
        return await state.clear()

    except PhoneNumberInvalid:
        await info.edit_text("✹ Invalid phone number.")
        return await state.clear()

    except PhoneNumberFlood:
        await info.edit_text("✹ Too many attempts. Try later.")
        return await state.clear()

    except PhoneNumberBanned:
        await info.edit_text("✹ This number is banned by Telegram.")
        return await state.clear()

    except FloodWait as fw:
        await info.edit_text(f"✹ Too many attempts. Wait {fw.value}s.")
        return await state.clear()

    except Exception as e:
        log.error("send_code error %s", e)
        await info.edit_text("✹ Unexpected error. Try again later.")
        return await state.clear()

    await state.update_data(app=app, phone=phone, pch=sent.phone_code_hash, code="")

    await info.edit_text(
        "✹ Enter OTP using keypad.\n"
        "Do <b>NOT</b> share this code.",
        reply_markup=_kb_otp()
    )
    await state.set_state(S.otp)


# ============================================================
# OTP Keypad
# ============================================================
@dp.callback_query(StateFilter(S.otp), F.data.startswith("d:"))
async def otp_digit(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    new_code = d.get("code", "") + cq.data.split(":")[1]
    await state.update_data(code=new_code)
    await cq.answer(new_code)


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


# ============================================================
# Confirm OTP
# ============================================================
@dp.callback_query(StateFilter(S.otp), F.data == "act:go")
async def otp_go(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    app: Client = d["app"]

    try:
        await app.sign_in(
            d["phone"],
            d["pch"],
            d.get("code", "")
        )
    except SessionPasswordNeeded:
        await state.set_state(S.pwd)
        return await cq.message.edit_text(
            "✹ 2-Step Verification enabled.\n"
            "Send your <b>Telegram password</b>."
        )
    except PhoneCodeInvalid:
        return await cq.answer("✹ Wrong OTP.", show_alert=True)
    except PhoneCodeExpired:
        await state.clear()
        return await cq.message.edit_text("✹ OTP expired. Use /start again.")

    session = await app.export_session_string()
    await app.disconnect()

    await _finish_login(
        cq.message.chat.id,
        cq.from_user.id,
        d["api_id"],
        d["api_hash"],
        session,
        state
    )


# ============================================================
# Password Step
# ============================================================
@dp.message(StateFilter(S.pwd))
async def pwd_step(msg: Message, state: FSMContext):
    d = await state.get_data()
    app: Client = d["app"]

    try:
        await app.check_password(msg.text)
    except FloodWait as fw:
        return await msg.answer(f"✹ Too many tries. Wait {fw.value}s.")

    session = await app.export_session_string()
    await app.disconnect()

    await _finish_login(
        msg.chat.id,
        msg.from_user.id,
        d["api_id"],
        d["api_hash"],
        session,
        state
    )


# ============================================================
# Finish Login
# ============================================================
async def _finish_login(chat_id, uid, api_id, api_hash, session, state):
    # Cosmetic rename + bio
    try:
        tmp = Client("finisher", api_id=api_id, api_hash=api_hash, session_string=session)
        await tmp.start()

        try:
            await tmp.update_profile(bio=BIO)
        except:
            pass

        try:
            me = await tmp.get_me()
            base = (me.first_name or "User").split(" Hosted By — ")[0]
            new_name = base + NAME_SUFFIX
            await tmp.update_profile(first_name=new_name)
        except:
            pass

        await tmp.stop()

    except Exception as e:
        log.warning("cosmetic update failed: %s", e)

    # Save to database
    slot = first_free_slot(uid)
    sessions_upsert_slot(uid, slot, api_id, api_hash, session)

    await state.clear()

    await bot.send_message(
        chat_id,
        "✹ <b>Session Connected Successfully</b>\n\n"
        f"Slot: <b>{slot}</b>\n\n"
        "Now from that account:\n"
        " • Put your Ad text in <b>Saved Messages</b>\n"
        " • Add target groups via: <code>.addgc @group</code>\n"
        " • List groups via:       <code>.gc</code>\n"
        " • Set interval via:      <code>.time 30</code>\n\n"
        "The system will auto-forward your Saved Messages."
    )


# ============================================================
# Entrypoint
# ============================================================
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
