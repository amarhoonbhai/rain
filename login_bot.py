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

# =========================
# Bootstrap
# =========================
load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("login-bot")

TOKEN = (os.getenv("LOGIN_BOT_TOKEN") or "").strip()
if not TOKEN or ":" not in TOKEN:
    raise RuntimeError("LOGIN_BOT_TOKEN missing")

# Branding (used when we finish login)
BIO = os.getenv(
    "ENFORCE_BIO",
    "#1 Free Ads Bot — Managed By @PhiloBots"
)
NAME_SUFFIX = os.getenv(
    "ENFORCE_NAME_SUFFIX",
    " Hosted By — @SpinifyAdsBot"
)

bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
init_db()


# =========================
# FSM for login flow
# =========================
class S(StatesGroup):
    api_id = State()
    api_hash = State()
    phone = State()
    otp = State()
    pwd = State()


def _kb_otp() -> InlineKeyboardMarkup:
    """
    Numeric keypad for OTP entry.
    """
    rows = [
        [InlineKeyboardButton(text=str(i), callback_data=f"d:{i}") for i in (1, 2, 3)],
        [InlineKeyboardButton(text=str(i), callback_data=f"d:{i}") for i in (4, 5, 6)],
        [InlineKeyboardButton(text=str(i), callback_data=f"d:{i}") for i in (7, 8, 9)],
        [InlineKeyboardButton(text="0", callback_data="d:0")],
        [
            InlineKeyboardButton(text="⬅", callback_data="act:back"),
            InlineKeyboardButton(text="🧹", callback_data="act:clear"),
            InlineKeyboardButton(text="✔️ Login", callback_data="act:go"),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


# =========================
# Handlers
# =========================
@dp.message(Command("start"))
async def start(msg: Message, state: FSMContext):
    ensure_user(msg.from_user.id, msg.from_user.username)
    await state.clear()
    await msg.answer(
        "✹ <b>Spinify Login Panel</b>\n\n"
        "We will connect your personal Telegram account.\n"
        "Steps:\n"
        " 1) Send your <b>API ID</b>\n"
        " 2) Send your <b>API HASH</b>\n"
        " 3) Send your <b>phone number</b>\n"
        " 4) Enter the login code via keypad\n\n"
        "You can get API ID / HASH from my.telegram.org → API Development Tools."
    )
    await state.set_state(S.api_id)


@dp.message(StateFilter(S.api_id))
async def api_id_step(msg: Message, state: FSMContext):
    text = (msg.text or "").strip()
    try:
        aid = int(text)
    except Exception:
        await msg.answer("✹ Please send a valid <b>number</b> for API ID.")
        return

    await state.update_data(api_id=aid)
    await state.set_state(S.api_hash)
    await msg.answer(
        "✹ Great.\n"
        "Now send your <b>API HASH</b> exactly as given on my.telegram.org."
    )


@dp.message(StateFilter(S.api_hash))
async def api_hash_step(msg: Message, state: FSMContext):
    ah = (msg.text or "").strip()
    if not ah:
        await msg.answer("✹ API HASH cannot be empty. Please send it again.")
        return

    await state.update_data(api_hash=ah)
    await state.set_state(S.phone)
    await msg.answer(
        "✹ Almost done.\n"
        "Send your phone number in international format, for example:\n"
        " +9198xxxxxxxx\n\n"
        "Make sure this is the same account where you want to run the ads."
    )


async def _send_code(aid: int, ah: str, phone: str):
    """
    Use a short-lived Pyrogram client in memory to send login code.
    """
    app = Client(name="login", api_id=aid, api_hash=ah, in_memory=True)
    await app.connect()
    sent = await app.send_code(phone)
    return app, sent


@dp.message(StateFilter(S.phone))
async def phone_step(msg: Message, state: FSMContext):
    d = await state.get_data()
    aid, ah = d["api_id"], d["api_hash"]
    phone = (msg.text or "").strip()

    if not phone.startswith("+"):
        await msg.answer("✹ Phone must include country code, for example +91…")
        return

    m = await msg.answer("✹ Sending login code to your Telegram app…")
    try:
        app, sent = await _send_code(aid, ah, phone)
    except ApiIdInvalid:
        await m.edit_text("✹ API ID / HASH looks invalid. Please /start again.")
        await state.clear()
        return
    except PhoneNumberInvalid:
        await m.edit_text("✹ Phone number is invalid. Please /start again.")
        await state.clear()
        return
    except PhoneNumberFlood:
        await m.edit_text("✹ Too many attempts. Try again later.")
        await state.clear()
        return
    except PhoneNumberBanned:
        await m.edit_text("✹ This number is banned by Telegram.")
        await state.clear()
        return
    except FloodWait as fw:
        await m.edit_text(f"✹ Too many tries. Please wait {fw.value}s and retry.")
        await state.clear()
        return
    except Exception as e:
        log.error("send_code error: %s", e)
        await m.edit_text("✹ Unexpected error while sending code. Try again later.")
        await state.clear()
        return

    await state.update_data(app=app, phone=phone, pch=sent.phone_code_hash, code="")
    await m.edit_text(
        "✹ Enter the login code using the keypad below.\n"
        "Do <b>NOT</b> share this code with anyone.",
        reply_markup=_kb_otp(),
    )
    await state.set_state(S.otp)


# ----- OTP keypad handlers -----
@dp.callback_query(StateFilter(S.otp), F.data.startswith("d:"))
async def otp_digit(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    code = d.get("code", "") + cq.data.split(":")[1]
    await state.update_data(code=code)
    await cq.answer(f"{code}")


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


@dp.callback_query(StateFilter(S.otp), F.data == "act:go")
async def otp_go(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    app: Client = d["app"]

    try:
        await app.sign_in(
            phone_number=d["phone"],
            phone_code_hash=d["pch"],
            phone_code=d.get("code", ""),
        )
    except SessionPasswordNeeded:
        await state.set_state(S.pwd)
        await cq.message.edit_text(
            "✹ 2-Step Verification is enabled.\n"
            "Send your <b>Telegram password</b> here (this message is private)."
        )
        return
    except PhoneCodeInvalid:
        await cq.answer("Wrong code, try again.", show_alert=True)
        return
    except PhoneCodeExpired:
        await cq.message.edit_text("✹ Code expired. Please /start again.")
        await state.clear()
        return

    session = await app.export_session_string()
    await app.disconnect()
    await _finish_login(
        chat_id=cq.message.chat.id,
        user_id=cq.from_user.id,
        api_id=d["api_id"],
        api_hash=d["api_hash"],
        session_str=session,
        state=state,
    )


@dp.message(StateFilter(S.pwd))
async def otp_pwd(msg: Message, state: FSMContext):
    d = await state.get_data()
    app: Client = d["app"]

    try:
        await app.check_password(msg.text)
    except FloodWait as fw:
        await msg.answer(f"✹ Too many tries. Please wait {fw.value}s and retry.")
        return

    session = await app.export_session_string()
    await app.disconnect()
    await _finish_login(
        chat_id=msg.chat.id,
        user_id=msg.from_user.id,
        api_id=d["api_id"],
        api_hash=d["api_hash"],
        session_str=session,
        state=state,
    )


async def _finish_login(
    chat_id: int,
    user_id: int,
    api_id: int,
    api_hash: str,
    session_str: str,
    state: FSMContext,
):
    """
    Cosmetic rename + bio set + save session to Mongo.
    """
    # Cosmetic: set bio + name suffix once, so branding appears quickly.
    try:
        tmp = Client(
            "finish",
            api_id=api_id,
            api_hash=api_hash,
            session_string=session_str,
        )
        await tmp.start()

        # Bio
        try:
            await tmp.update_profile(bio=BIO)
        except Exception:
            pass

        # Name suffix (append after their base name, not spammy)
        try:
            me = await tmp.get_me()
            base = (me.first_name or "User").split(" Hosted By — ")[0]
            desired = base + NAME_SUFFIX
            if (me.first_name or "") != desired:
                await tmp.update_profile(first_name=desired)
        except Exception:
            pass

        await tmp.stop()
    except Exception as e:
        log.warning("finish cosmetic update failed: %s", e)

    # Save to Mongo
    slot = first_free_slot(user_id)
    sessions_upsert_slot(user_id, slot, api_id, api_hash, session_str)
    await state.clear()

    await bot.send_message(
        chat_id,
        "✹ <b>Session Connected</b>\n\n"
        f"Slot: {slot}\n\n"
        "Now from that account:\n"
        " • Put your ads in <b>Saved Messages</b>\n"
        " • Add target groups with:  .addgroup @yourgroup\n"
        " • Check list with:         .groups\n"
        " • Set interval:            .time 30 / .time 45 / .time 60\n\n"
        "The worker will automatically forward your Saved Messages to all added groups in cycles.",
    )


# =========================
# Entrypoint
# =========================
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
