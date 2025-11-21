import os, asyncio, logging
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    ApiIdInvalidError,
    PhoneNumberInvalidError,
    PhoneNumberFloodError,
    PhoneNumberBannedError,
    FloodWaitError,
    RPCError,
)

from core.db import init_db, ensure_user, first_free_slot, sessions_upsert_slot

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


class S(StatesGroup):
    api_id = State()
    api_hash = State()
    phone = State()
    otp = State()
    pwd = State()


def _kb_otp():
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


@dp.message(Command("start"))
async def start(msg: Message, state: FSMContext):
    ensure_user(msg.from_user.id, msg.from_user.username)
    await msg.answer("✇ Send your <b>API_ID</b> (number)")
    await state.set_state(S.api_id)


@dp.message(StateFilter(S.api_id))
async def api_id(msg: Message, state: FSMContext):
    try:
        aid = int(msg.text.strip())
    except Exception:
        await msg.answer("❌ number required")
        return
    await state.update_data(api_id=aid)
    await state.set_state(S.api_hash)
    await msg.answer("✇ Send your <b>API_HASH</b>")


@dp.message(StateFilter(S.api_hash))
async def api_hash(msg: Message, state: FSMContext):
    await state.update_data(api_hash=msg.text.strip())
    await state.set_state(S.phone)
    await msg.answer("✇ Send phone in international format, e.g. <b>+9198xxxxxxx</b>")


async def _send_code(aid: int, ah: str, phone: str) -> tuple[TelegramClient, object]:
    """
    Create Telethon client in-memory and send login code.
    """
    client = TelegramClient(StringSession(), aid, ah)
    await client.connect()
    sent = await client.send_code_request(phone)
    return client, sent


@dp.message(StateFilter(S.phone))
async def phone(msg: Message, state: FSMContext):
    d = await state.get_data()
    aid, ah = d["api_id"], d["api_hash"]
    phone = msg.text.strip()
    m = await msg.answer("✇ Sending code…")

    try:
        app, sent = await _send_code(aid, ah, phone)
    except ApiIdInvalidError:
        await m.edit_text("❌ API_ID/HASH invalid")
        return
    except PhoneNumberInvalidError:
        await m.edit_text("❌ phone invalid")
        return
    except PhoneNumberFloodError:
        await m.edit_text("⏳ too many attempts, phone is flood-limited")
        return
    except PhoneNumberBannedError:
        await m.edit_text("❌ this number is banned by Telegram")
        return
    except FloodWaitError as fw:
        await m.edit_text(f"⏳ Flood wait: try again after {fw.seconds}s")
        return
    except RPCError as e:
        await m.edit_text(f"❌ Telegram error: {e}")
        return
    except Exception as e:
        log.error("send_code error: %s", e)
        await m.edit_text("❌ Unexpected error while sending code.")
        return

    await state.update_data(app=app, phone=phone, pch=sent.phone_code_hash, code="")
    await m.edit_text("✇ Enter code", reply_markup=_kb_otp())
    await state.set_state(S.otp)


# ===== OTP keypad handlers =====

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
    await cq.answer("cleared")


@dp.callback_query(StateFilter(S.otp), F.data == "act:go")
async def otp_go(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    app: TelegramClient = d["app"]
    phone = d["phone"]
    pch = d["pch"]
    code = d.get("code", "")

    try:
        await app.sign_in(phone=phone, code=code, phone_code_hash=pch)
    except SessionPasswordNeededError:
        await state.set_state(S.pwd)
        await cq.message.edit_text("2FA enabled. Send password.")
        return
    except PhoneCodeInvalidError:
        await cq.answer("wrong code", show_alert=True)
        return
    except PhoneCodeExpiredError:
        await cq.message.edit_text("Code expired. /start")
        await state.clear()
        try:
            await app.disconnect()
        except Exception:
            pass
        return
    except FloodWaitError as fw:
        await cq.message.edit_text(f"⏳ Flood wait: try again after {fw.seconds}s")
        return
    except RPCError as e:
        await cq.message.edit_text(f"❌ Telegram error: {e}")
        return
    except Exception as e:
        log.error("sign_in error: %s", e)
        await cq.message.edit_text("❌ Unexpected error during sign in.")
        return

    # No 2FA, login complete
    session = app.session.save()
    await app.disconnect()
    await _finish(
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
    app: TelegramClient = d["app"]
    try:
        await app.sign_in(password=msg.text)
    except FloodWaitError as fw:
        await msg.answer(f"⏳ Flood wait: try again after {fw.seconds}s")
        return
    except RPCError as e:
        await msg.answer(f"❌ Error: {e}")
        return

    session = app.session.save()
    await app.disconnect()
    await _finish(
        chat_id=msg.chat.id,
        user_id=msg.from_user.id,
        api_id=d["api_id"],
        api_hash=d["api_hash"],
        session_str=session,
        state=state,
    )


async def _finish(chat_id: int, user_id: int, api_id: int, api_hash: str, session_str: str, state: FSMContext):
    """
    Save session in Mongo and do cosmetic bio/name update once.
    """
    # cosmetic: update bio + suffix once
    try:
        tmp = TelegramClient(StringSession(session_str), api_id, api_hash)
        await tmp.connect()
        try:
            await tmp.update_profile(about=BIO)
        except Exception:
            pass
        try:
            me = await tmp.get_me()
            base = (me.first_name or "User").split(" — ")[0]
            desired = base + SUFFIX
            if (me.first_name or "") != desired:
                await tmp.update_profile(first_name=desired)
        except Exception:
            pass
        await tmp.disconnect()
    except Exception as e:
        log.warning("cosmetic profile update failed: %s", e)

    slot = first_free_slot(user_id)
    sessions_upsert_slot(user_id, slot, api_id, api_hash, session_str)
    await state.clear()

    await bot.send_message(
        chat_id,
        f"✅ Session saved in Slot {slot}.\n"
        "Put your ads in <b>Saved Messages</b> (text/media ok).\n"
        "Worker will forward them in cycle.\n"
        "Use commands from your account: <code>.addgc</code>, <code>.gc</code>, "
        "<code>.cleargc</code>, <code>.time</code>, <code>.adreset</code>, <code>.status</code>.",
    )


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
