import os
import re
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
from telethon.errors import SessionPasswordNeededError

# -------------- Logging --------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
log = logging.getLogger("spinify.login")

load_dotenv()

LOGIN_BOT_TOKEN = os.getenv("LOGIN_BOT_TOKEN", "").strip()
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", "").strip()
DB_PATH = os.getenv("DB_PATH", "data/spinify.sqlite")

GATE_CHANNEL = (os.getenv("GATE_CHANNEL") or "").strip()
GATE_GROUP = (os.getenv("GATE_GROUP") or "").strip()

if not LOGIN_BOT_TOKEN:
    raise SystemExit("LOGIN_BOT_TOKEN is required in .env")

bot = Bot(token=LOGIN_BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot, storage=MemoryStorage())
store = Storage(DB_PATH, ENCRYPTION_KEY)

# -------------- Gate helpers --------------
async def _is_member(user_id: int, chat: str) -> bool:
    if not chat:
        return True
    try:
        member = await bot.get_chat_member(chat, user_id)
        status = getattr(member, "status", "left")
        return status in ("member", "administrator", "creator")
    except (ChatNotFound, UserNotParticipant, BotBlocked):
        return False
    except Exception as e:
        log.warning("Gate check failed for %s: %s", chat, e)
        return False

async def check_gate_or_prompt(msg: types.Message) -> bool:
    user_id = msg.from_user.id
    ok_ch = await _is_member(user_id, GATE_CHANNEL)
    ok_gr = await _is_member(user_id, GATE_GROUP)
    if (GATE_CHANNEL and not ok_ch) or (GATE_GROUP and not ok_gr):
        txt = "<b>Access locked</b>.\nPlease join:"
        if GATE_CHANNEL:
            txt += f"\n• Channel: {GATE_CHANNEL}"
        if GATE_GROUP:
            txt += f"\n• Group: {GATE_GROUP}"
        await msg.answer(txt)
        return False
    return True

# -------------- FSM --------------
class LoginStates(StatesGroup):
    waiting_api_id = State()
    waiting_api_hash = State()
    waiting_phone = State()
    waiting_code = State()
    waiting_2fa = State()

@dp.message_handler(commands=["start", "help"])
async def start(msg: types.Message, state: FSMContext):
    if not await check_gate_or_prompt(msg):
        return
    await state.finish()
    await msg.answer(
        "👋 <b>Welcome to Spinify Login Bot</b>\n\n"
        "I'll create a Telegram session for your account.\n"
        "Step 1: Send your <b>API_ID</b> (a number). Get it at https://my.telegram.org\n\n"
        "⚠️ We store everything encrypted."
    )
    await LoginStates.waiting_api_id.set()

@dp.message_handler(state=LoginStates.waiting_api_id, content_types=types.ContentTypes.TEXT)
async def got_api_id(msg: types.Message, state: FSMContext):
    m = re.fullmatch(r"\s*(\d{4,})\s*", msg.text or "")
    if not m:
        await msg.answer("That doesn't look like a valid <b>API_ID</b>. Try again.")
        return
    await state.update_data(api_id=int(m.group(1)))
    await msg.answer("Great. Now send your <b>API_HASH</b> (32 chars).")
    await LoginStates.waiting_api_hash.set()

@dp.message_handler(state=LoginStates.waiting_api_hash, content_types=types.ContentTypes.TEXT)
async def got_api_hash(msg: types.Message, state: FSMContext):
    api_hash = (msg.text or "").strip()
    if len(api_hash) < 20:
        await msg.answer("That doesn't look like a valid <b>API_HASH</b>. Try again.")
        return
    await state.update_data(api_hash=api_hash)
    await msg.answer("Now send your <b>phone number</b> with country code, e.g. +9198xxxxxx")
    await LoginStates.waiting_phone.set()

@dp.message_handler(state=LoginStates.waiting_phone, content_types=types.ContentTypes.TEXT)
async def got_phone(msg: types.Message, state: FSMContext):
    phone = (msg.text or "").strip()
    if not re.fullmatch(r"\+?\d{7,15}", phone):
        await msg.answer("Please send a valid phone number with country code.")
        return
    await state.update_data(phone=phone)

    data = await state.get_data()
    api_id, api_hash = int(data["api_id"]), data["api_hash"]

    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.connect()

    # Send code
    await client.send_code_request(phone)
    await state.update_data(_tmp_session=client.session.save())
    await msg.answer("✅ Code sent! Please enter the <b>login code</b> you received.")
    await LoginStates.waiting_code.set()

@dp.message_handler(state=LoginStates.waiting_code, content_types=types.ContentTypes.TEXT)
async def got_code(msg: types.Message, state: FSMContext):
    code = (msg.text or "").strip().replace(" ", "")
    data = await state.get_data()
    api_id, api_hash = int(data["api_id"]), data["api_hash"]
    phone = data["phone"]
    tmp_sess = data["_tmp_session"]

    client = TelegramClient(StringSession(tmp_sess), api_id, api_hash)
    await client.connect()

    try:
        await client.sign_in(phone=phone, code=code)
    except SessionPasswordNeededError:
        await msg.answer("🔐 Two-step password is enabled. Please send your <b>password</b>.")
        await LoginStates.waiting_2fa.set()
        return

    string_session = client.session.save()
    await store.set_user_session(msg.from_user.id, str(api_id), api_hash, string_session, phone)
    await store.audit(msg.from_user.id, "login_success", {"phone": phone})
    await msg.answer("🎉 <b>Login complete!</b>\nYou can now use the main bot.")
    await state.finish()
    await client.disconnect()

@dp.message_handler(state=LoginStates.waiting_2fa, content_types=types.ContentTypes.TEXT)
async def got_2fa(msg: types.Message, state: FSMContext):
    password = (msg.text or "").strip()
    data = await state.get_data()
    api_id, api_hash = int(data["api_id"]), data["api_hash"]
    phone = data["phone"]
    tmp_sess = data["_tmp_session"]

    client = TelegramClient(StringSession(tmp_sess), api_id, api_hash)
    await client.connect()
    await client.sign_in(password=password)
    string_session = client.session.save()
    await store.set_user_session(msg.from_user.id, str(api_id), api_hash, string_session, phone)
    await store.audit(msg.from_user.id, "login_success_2fa", {"phone": phone})
    await msg.answer("🎉 <b>Login complete!</b>\nYou can now use the main bot.")
    await state.finish()
    await client.disconnect()

async def on_startup(_):
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
    
