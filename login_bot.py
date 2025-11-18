import os
import asyncio
import logging
import pathlib

from datetime import datetime, timedelta

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
    sessions_upsert_slot,
)

# =========================
# Basic setup
# =========================
load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("login-bot")

TOKEN = os.getenv("LOGIN_BOT_TOKEN", "").strip()
if not TOKEN or ":" not in TOKEN:
    raise RuntimeError("LOGIN_BOT_TOKEN missing")

BIO = os.getenv("ENFORCE_BIO", "#1 Free Ads Bot — Join @PhiloBots")
SUFFIX = os.getenv("ENFORCE_NAME_SUFFIX", " — via @SpinifyAdsBot")

bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

init_db()

# =========================
# FSM states
# =========================
class S(StatesGroup):
    api_id = State()
    api_hash = State()
    phone = State()
    otp = State()
    pwd = State()


# =========================
# Keyboards
# =========================
def _kb_otp() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=str(i), callback_data=f"d:{i}") for i in (1, 2, 3)],
        [InlineKeyboardButton(text=str(i), callback_data=f"d:{i}") for i in (4, 5, 6)],
        [InlineKeyboardButton(text=str(i), callback_data=f"d:{i}") for i in (7, 8, 9)],
        [InlineKeyboardButton(text="0", callback_data="d:0")],
        [
            InlineKeyboardButton(text="⬅", callback_data="act:back"),
            InlineKeyboardButton(text="🧹", callback_data="act:clear"),
            InlineKeyboardButton(text="✔", callback_data="act:go"),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


# =========================
# Helpers
# =========================
async def _send_code(aid: int, ah: str, phone: str):
    """
    Create a temporary Pyrogram client, send login code.
    Returns (app, sent_code).
    """
    app = Client(name="login", api_id=aid, api_hash=ah, in_memory=True)
    await app.connect()
    sent = await app.send_code(phone)
    return app, sent


async def _finish(
    chat_id: int,
    user_id: int,
    api_id: int,
    api_hash: str,
    session_str: str,
    state: FSMContext,
):
    """
    Finalize login:
    - Cosmetically update profile (bio + name suffix)
    - Save session in DB slot
    - Show instructions
    """
    # cosmetic
    try:
        tmp = Client(
            "finish",
            api_id=api_id,
            api_hash=api_hash,
            session_string=session_str,
        )
        await tmp.start()
        try:
            await tmp.update_profile(bio=BIO)
        except Exception:
            pass
        try:
            me = await tmp.get_me()
            base = (me.first_name or "User").split(" — ")[0]
            if not (me.first_name or "").endswith(SUFFIX):
                await tmp.update_profile(first_name=base + SUFFIX)
        except Exception:
            pass
        await tmp.stop()
    except Exception:
        pass

    # store in DB (multi-slot system)
    slot = first_free_slot(user_id)
    sessions_upsert_slot(user_id, slot, api_id, api_hash, session_str)

    await state.clear()

    # instructions for the user (matches worker_forward.py command set)
    text = (
        f"✅ Session saved in Slot {slot}.\n\n"
        "📂 Put your ads in <b>Saved Messages</b> (text/media ok).\n"
        "The worker will forward them in a cycle to your target groups/channels.\n\n"
        "📡 From your own Telegram account (not this bot), use these commands:\n"
        "• <code>.help</code> – show all commands\n"
        "• <code>.status</code> – see plan, interval, and basic stats\n"
        "• <code>.addgroup &lt;link/@user&gt;</code> – add target groups/channels\n"
        "• <code>.delgroup &lt;link/@user&gt;</code> – remove a target\n"
        "• <code>.groups</code> – list all added groups\n"
        "• <code>.time 30|45|60</code> – set basic interval (minutes; free users)\n"
        "• <code>.upgrade</code> – get your Telegram ID to request premium\n\n"
        "After premium upgrade, you will also unlock custom intervals, per-message delay,\n"
        "and advanced Auto-Night scheduling (if enabled in your plan)."
    )

    await bot.send_message(chat_id, text)


# =========================
# Handlers
# =========================
@dp.message(Command("start"))
async def start(msg: Message, state: FSMContext):
    ensure_user(msg.from_user.id, msg.from_user.username)
    await msg.answer(
        "👋 Welcome!\n\n"
        "To link your Telegram account, please send your <b>API_ID</b>.\n"
        "You can find it on https://my.telegram.org under <b>API development tools</b>."
    )
    await state.set_state(S.api_id)


@dp.message(Command("help"))
async def help_cmd(msg: Message, state: FSMContext):
    await msg.answer(
        "ℹ️ This bot only helps you connect your Telegram account.\n\n"
        "Flow:\n"
        "1️⃣ Send your <b>API_ID</b>\n"
        "2️⃣ Send your <b>API_HASH</b>\n"
        "3️⃣ Send your phone in +<code>country</code> format\n"
        "4️⃣ Enter the login code via on-screen keypad\n"
        "5️⃣ If needed, send your 2FA password\n\n"
        "After that, your session is stored and the worker bot will start forwarding\n"
        "ads from your <b>Saved Messages</b> according to your plan."
    )


@dp.message(StateFilter(S.api_id))
async def api_id(msg: Message, state: FSMContext):
    try:
        aid = int(msg.text.strip())
    except Exception:
        await msg.answer("❌ <b>Number required.</b>\nPlease send your <b>API_ID</b> again.")
        return

    await state.update_data(api_id=aid)
    await state.set_state(S.api_hash)
    await msg.answer("✇ Now send your <b>API_HASH</b>.")


@dp.message(StateFilter(S.api_hash))
async def api_hash(msg: Message, state: FSMContext):
    await state.update_data(api_hash=msg.text.strip())
    await state.set_state(S.phone)
    await msg.answer(
        "✇ Send your phone in international format, e.g. <b>+9198xxxxxxx</b>."
    )


@dp.message(StateFilter(S.phone))
async def phone(msg: Message, state: FSMContext):
    d = await state.get_data()
    aid, ah = d["api_id"], d["api_hash"]
    phone = msg.text.strip()

    m = await msg.answer("✇ Sending code…")
    try:
        app, sent = await _send_code(aid, ah, phone)
    except ApiIdInvalid:
        await m.edit_text("❌ API_ID / API_HASH invalid.\nCheck on https://my.telegram.org")
        return
    except PhoneNumberInvalid:
        await m.edit_text("❌ Phone number invalid.\nUse full format, e.g. <b>+91xxxxxxxxxx</b>.")
        return
    except PhoneNumberFlood:
        await m.edit_text("⏳ Too many attempts for this number. Try again later.")
        return
    except PhoneNumberBanned:
        await m.edit_text("❌ This phone number is banned by Telegram.")
        return
    except FloodWait as fw:
        await m.edit_text(f"⏳ Rate limited. Please wait <b>{fw.value}s</b> and try again.")
        return

    await state.update_data(app=app, phone=phone, pch=sent.phone_code_hash, code="")
    await m.edit_text("✇ Enter the code you received:", reply_markup=_kb_otp())
    await state.set_state(S.otp)


@dp.callback_query(StateFilter(S.otp), F.data.startswith("d:"))
async def otp_digit(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    code = d.get("code", "") + cq.data.split(":")[1]
    await state.update_data(code=code)
    await cq.answer()


@dp.callback_query(StateFilter(S.otp), F.data == "act:back")
async def otp_back(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    code = d.get("code", "")[:-1]
    await state.update_data(code=code)
    await cq.answer()


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
        await cq.message.edit_text("🔐 2FA is enabled.\nPlease send your <b>password</b>.")
        return
    except PhoneCodeInvalid:
        await cq.answer("Wrong code. Try again.", show_alert=True)
        return
    except PhoneCodeExpired:
        await cq.message.edit_text("⌛ Code expired. Use /start to begin again.")
        await state.clear()
        return

    session = await app.export_session_string()
    await app.disconnect()
    await _finish(
        cq.message.chat.id,
        cq.from_user.id,
        d["api_id"],
        d["api_hash"],
        session,
        state,
    )


@dp.message(StateFilter(S.pwd))
async def otp_pwd(msg: Message, state: FSMContext):
    d = await state.get_data()
    app: Client = d["app"]

    try:
        await app.check_password(msg.text)
    except FloodWait as fw:
        await msg.answer(f"⏳ Too many attempts. Wait <b>{fw.value}s</b>.")
        return

    session = await app.export_session_string()
    await app.disconnect()
    await _finish(
        msg.chat.id,
        msg.from_user.id,
        d["api_id"],
        d["api_hash"],
        session,
        state,
    )


# =========================
# Entrypoint
# =========================
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
