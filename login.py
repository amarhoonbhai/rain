import os
import re
import time
import logging
import asyncio

from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.utils.exceptions import ChatNotFound, UserNotParticipant, BotBlocked

from dotenv import load_dotenv
from storage import Storage

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError

# --------------------------
# Basics / config
# --------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
log = logging.getLogger("spinify.login")

load_dotenv()

LOGIN_BOT_TOKEN = os.getenv("LOGIN_BOT_TOKEN")
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
DB_PATH = os.getenv("DB_PATH", "data/spinify.sqlite")

# Optional gate (leave blank to disable)
GATE_CHANNEL = (os.getenv("GATE_CHANNEL") or "").strip()  # e.g. @PhiloBots
GATE_GROUP = (os.getenv("GATE_GROUP") or "").strip()      # e.g. @ShadesOfMuse

if not LOGIN_BOT_TOKEN:
    raise RuntimeError("LOGIN_BOT_TOKEN missing in .env")
if not ENCRYPTION_KEY:
    raise RuntimeError("ENCRYPTION_KEY missing in .env")

bot = Bot(token=LOGIN_BOT_TOKEN, parse_mode=types.ParseMode.HTML)
dp = Dispatcher(bot, storage=MemoryStorage())

store = Storage(DB_PATH, ENCRYPTION_KEY)

PHONE_REGEX = re.compile(r"^\+?\d{8,15}$")

# --------------------------
# FSM states
# --------------------------
class LoginStates(StatesGroup):
    waiting_api_id = State()
    waiting_api_hash = State()
    waiting_phone = State()
    waiting_code = State()
    waiting_2fa = State()

# --------------------------
# UI helpers
# --------------------------
def otp_keypad() -> types.ReplyKeyboardMarkup:
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("1", "2", "3")
    kb.row("4", "5", "6")
    kb.row("7", "8", "9")
    kb.row("←", "0", "✓")
    return kb

def join_link(handle: str) -> str:
    if not handle:
        return ""
    return f"https://t.me/{handle.lstrip('@')}"

def gate_keyboard() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    if GATE_CHANNEL:
        kb.add(types.InlineKeyboardButton("🔔 Join Channel", url=join_link(GATE_CHANNEL)))
    if GATE_GROUP:
        kb.add(types.InlineKeyboardButton("👥 Join Group", url=join_link(GATE_GROUP)))
    kb.add(types.InlineKeyboardButton("✅ I joined — Retry", callback_data="retry_gate"))
    return kb

# --------------------------
# Gate checks
# --------------------------
async def is_member(chat: str, user_id: int) -> bool:
    """Return True if user is member; fail-open on unexpected errors."""
    if not chat:
        return True
    try:
        member = await bot.get_chat_member(chat, user_id)
        return member.status not in ("left", "kicked")
    except (ChatNotFound, UserNotParticipant):
        return False
    except Exception as e:
        log.warning("Gate check failed for %s: %s", chat, e)
        return True  # don't hard-lock users if bot lacks rights

async def passes_gate(user_id: int) -> bool:
    need_ch = bool(GATE_CHANNEL)
    need_gr = bool(GATE_GROUP)
    ok_ch = await is_member(GATE_CHANNEL, user_id)
    ok_gr = await is_member(GATE_GROUP, user_id)
    if need_ch and need_gr:
        return ok_ch and ok_gr
    if need_ch:
        return ok_ch
    if need_gr:
        return ok_gr
    return True

# --------------------------
# Commands
# --------------------------
@dp.message_handler(commands=["start"])
async def cmd_start(msg: types.Message, state: FSMContext):
    # Gate first
    if not await passes_gate(msg.from_user.id):
        await msg.answer(
            "To use <b>Spinify Login</b>, please join the required channel/group:",
            reply_markup=gate_keyboard(),
        )
        return

    await store.ensure_user(msg.from_user.id)
    await msg.answer(
        "Welcome to <b>Spinify Login</b> 🔐\n\n"
        "<b>We'll create a secure session for your Telegram account.</b>\n"
        "Steps:\n"
        "1) Send your <b>API_ID</b> (numeric)\n"
        "2) Send your <b>API_HASH</b>\n"
        "3) Send your <b>phone</b> (e.g. +91…)\n"
        "4) Type the <b>OTP</b> using the keypad (and 2FA password if asked)\n\n"
        "After success, return to @SpinifyAdsBot."
    )
    await LoginStates.waiting_api_id.set()

@dp.callback_query_handler(lambda c: c.data == "retry_gate")
async def cb_retry_gate(call: types.CallbackQuery):
    if await passes_gate(call.from_user.id):
        await call.message.edit_text(
            "Thanks! ✅ Access granted.\nSend /start to begin the login flow."
        )
    else:
        await call.answer("Still not a member. Join and try again.", show_alert=True)

@dp.message_handler(commands=["cancel"], state="*")
async def cmd_cancel(msg: types.Message, state: FSMContext):
    await state.finish()
    await msg.answer("❌ Cancelled.", reply_markup=types.ReplyKeyboardRemove())

@dp.message_handler(commands=["help"])
async def cmd_help(msg: types.Message, state: FSMContext):
    await msg.answer(
        "Commands:\n"
        "• /start — begin login flow\n"
        "• /cancel — abort current step\n"
        "• /id — show your Telegram ID\n"
        "• /ping — latency check\n"
    )

@dp.message_handler(commands=["id"])
async def cmd_id(msg: types.Message, state: FSMContext):
    await msg.answer(f"🪪 <b>Your ID:</b> <code>{msg.from_user.id}</code>")

@dp.message_handler(commands=["ping"])
async def cmd_ping(msg: types.Message, state: FSMContext):
    t0 = time.perf_counter()
    m = await msg.answer("Pinging…")
    ms = int((time.perf_counter() - t0) * 1000)
    await m.edit_text(f"🏓 Pong! <b>{ms}ms</b>")

# --------------------------
# FSM: API credentials
# --------------------------
@dp.message_handler(state=LoginStates.waiting_api_id, content_types=types.ContentTypes.TEXT)
async def get_api_id(msg: types.Message, state: FSMContext):
    try:
        api_id = int(msg.text.strip())
        if api_id <= 0:
            raise ValueError
        await state.update_data(api_id=api_id)
        await msg.answer("Great. Now send your <b>API_HASH</b> (32 chars).")
        await LoginStates.waiting_api_hash.set()
    except Exception:
        await msg.answer("Invalid API_ID. Please send a positive number.")

@dp.message_handler(state=LoginStates.waiting_api_hash, content_types=types.ContentTypes.TEXT)
async def get_api_hash(msg: types.Message, state: FSMContext):
    api_hash = msg.text.strip()
    if len(api_hash) < 20:
        await msg.answer("That doesn't look like a valid API_HASH. Try again.")
        return
    await state.update_data(api_hash=api_hash)
    await msg.answer("Now send your <b>phone number</b> (with country code), e.g. +9198xxxxxx.")
    await LoginStates.waiting_phone.set()

@dp.message_handler(state=LoginStates.waiting_phone, content_types=types.ContentTypes.TEXT)
async def get_phone(msg: types.Message, state: FSMContext):
    phone = msg.text.strip().replace(" ", "")
    if not PHONE_REGEX.match(phone):
        await msg.answer("Please send a valid phone number like <code>+9198xxxxxxx</code>.")
        return

    await state.update_data(phone=phone, code="")

    data = await state.get_data()
    api_id = data["api_id"]
    api_hash = data["api_hash"]

    await msg.answer(
        "Sending code… check your Telegram app or SMS.\n\n"
        "Use the <b>keypad</b> below to type the OTP, then press ✓.",
        reply_markup=otp_keypad(),
    )

    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.connect()
    try:
        await client.send_code_request(phone)
        await state.update_data(_tmp_session=client.session.save())
        await LoginStates.waiting_code.set()
    except Exception as e:
        await msg.answer(
            f"Failed to send code: <code>{e}</code>\n"
            "Send /start to try again.",
            reply_markup=types.ReplyKeyboardRemove(),
        )
        await client.disconnect()
        await state.finish()

# --------------------------
# FSM: OTP & 2FA
# --------------------------
@dp.message_handler(state=LoginStates.waiting_code, content_types=types.ContentTypes.TEXT)
async def get_code(msg: types.Message, state: FSMContext):
    text = msg.text.strip()
    data = await state.get_data()
    code = data.get("code", "")

    if text == "←":
        code = code[:-1]
    elif text == "✓":
        api_id = data["api_id"]
        api_hash = data["api_hash"]
        phone = data["phone"]
        tmp_session = data["_tmp_session"]

        client = TelegramClient(StringSession(tmp_session), api_id, api_hash)
        await client.connect()
        try:
            await client.sign_in(phone=phone, code=code)
        except SessionPasswordNeededError:
            await msg.answer(
                "2FA is enabled. Please enter your <b>password</b>.",
                reply_markup=types.ReplyKeyboardRemove(),
            )
            await state.update_data(_client_session=client.session.save())
            await LoginStates.waiting_2fa.set()
            return
        except PhoneCodeInvalidError:
            await msg.answer("❌ Invalid code. Try again or press ← to edit.")
            await client.disconnect()
            return
        except Exception as e:
            await msg.answer(f"Sign-in error: <code>{e}</code>")
            await client.disconnect()
            return

        # Success
        session_str = client.session.save()
        await client.disconnect()
        await store.save_session(msg.from_user.id, api_id, api_hash, session_str)
        await msg.answer(
            "✅ Session saved securely. Now go back to @SpinifyAdsBot and continue.",
            reply_markup=types.ReplyKeyboardRemove(),
        )
        await state.finish()
        return
    else:
        if not text.isdigit() or len(text) != 1:
            await msg.answer("Use the keypad to enter digits, or ✓ to submit.")
            return
        code += text

    # masked preview
    masked = "• " * len(code)
    await state.update_data(code=code)
    await msg.answer(f"OTP: {masked}".strip())

@dp.message_handler(state=LoginStates.waiting_2fa, content_types=types.ContentTypes.TEXT)
async def get_password(msg: types.Message, state: FSMContext):
    pwd = msg.text.strip()
    data = await state.get_data()
    api_id = data["api_id"]
    api_hash = data["api_hash"]

    client = TelegramClient(StringSession(data["_client_session"]), api_id, api_hash)
    await client.connect()
    try:
        await client.sign_in(password=pwd)
    except Exception as e:
        await msg.answer(f"❌ Password error: <code>{e}</code>\nTry again.")
        await client.disconnect()
        return

    session_str = client.session.save()
    await client.disconnect()
    await store.save_session(msg.from_user.id, api_id, api_hash, session_str)
    await msg.answer(
        "✅ Session saved. Return to @SpinifyAdsBot.",
        reply_markup=types.ReplyKeyboardRemove(),
    )
    await state.finish()

# --------------------------
# Startup
# --------------------------
async def on_startup(_):
    # Ensure Storage is ready; create folders if needed
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    await store.init()
    me = await bot.get_me()
    log.info("Login bot ready as @%s (id=%s)", me.username, me.id)
    if GATE_CHANNEL or GATE_GROUP:
        log.info("Gate enabled: channel=%s group=%s", GATE_CHANNEL or "-", GATE_GROUP or "-")
    else:
        log.info("Gate disabled")

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
    
