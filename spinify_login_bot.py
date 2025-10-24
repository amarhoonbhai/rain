import os, asyncio, logging, re, time
from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from dotenv import load_dotenv
from storage import Storage
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError

logging.basicConfig(level=logging.INFO)
load_dotenv()

LOGIN_BOT_TOKEN = os.getenv("LOGIN_BOT_TOKEN")
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
DB_PATH = os.getenv("DB_PATH", "data/spinify.sqlite")

if not LOGIN_BOT_TOKEN:
    raise RuntimeError("LOGIN_BOT_TOKEN missing in .env")
if not ENCRYPTION_KEY:
    raise RuntimeError("ENCRYPTION_KEY missing in .env")

bot = Bot(token=LOGIN_BOT_TOKEN, parse_mode=types.ParseMode.HTML)
dp = Dispatcher(bot, storage=MemoryStorage())
store = Storage(DB_PATH, ENCRYPTION_KEY)

PHONE_REGEX = re.compile(r"^\+?\d{8,15}$")

class LoginStates(StatesGroup):
    waiting_api_id = State()
    waiting_api_hash = State()
    waiting_phone = State()
    waiting_code = State()
    waiting_2fa = State()

def otp_keypad() -> types.ReplyKeyboardMarkup:
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("1", "2", "3")
    kb.row("4", "5", "6")
    kb.row("7", "8", "9")
    kb.row("←", "0", "✓")
    return kb

@dp.message_handler(commands=["start"])
async def cmd_start(msg: types.Message, state: FSMContext):
    await store.ensure_user(msg.from_user.id)
    await msg.answer(
        "Welcome to <b>Spinify Login</b> 🔐\n\n"
        "<b>We'll create a secure session for your Telegram account.</b>\n"
        "Steps:\n"
        "1) Send your <b>API_ID</b> (numeric)\n"
        "2) Send your <b>API_HASH</b>\n"
        "3) Send your <b>phone</b> (+91…)\n"
        "4) Enter the <b>OTP</b> using the keypad (and 2FA password if asked)\n\n"
        "After success, go back to @SpinifyAdsBot."
    )
    await LoginStates.waiting_api_id.set()

@dp.message_handler(state=LoginStates.waiting_api_id)
async def get_api_id(msg: types.Message, state: FSMContext):
    try:
        api_id = int(msg.text.strip())
        await state.update_data(api_id=api_id)
        await msg.answer("Great. Now send your <b>API_HASH</b> (32 chars).")
        await LoginStates.waiting_api_hash.set()
    except:
        await msg.answer("Invalid API_ID. Please send a number.")

@dp.message_handler(state=LoginStates.waiting_api_hash)
async def get_api_hash(msg: types.Message, state: FSMContext):
    api_hash = msg.text.strip()
    if len(api_hash) < 20:
        await msg.answer("That doesn't look like a valid API_HASH. Try again.")
        return
    await state.update_data(api_hash=api_hash)
    await msg.answer("Now send your <b>phone number</b> (with country code), e.g. +9198xxxxxx.")
    await LoginStates.waiting_phone.set()

@dp.message_handler(state=LoginStates.waiting_phone)
async def get_phone(msg: types.Message, state: FSMContext):
    phone = msg.text.strip().replace(" ", "")
    if not PHONE_REGEX.match(phone):
        await msg.answer("Please send a valid phone number like <code>+9198xxxxxxx</code>.")
        return
    await state.update_data(phone=phone, code="")
    data = await state.get_data()
    api_id = data["api_id"]
    api_hash = data["api_hash"]

    await msg.answer("Sending code… check your Telegram app or SMS.\n\nUse the **keypad** to type the OTP, then press ✓.", reply_markup=otp_keypad())
    # Send code via Telethon
    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.connect()
    try:
        await client.send_code_request(phone)
        await state.update_data(_tmp_session=client.session.save())
        await LoginStates.waiting_code.set()
    except Exception as e:
        await msg.answer(f"Failed to send code: <code>{e}</code>\nPlease re/start to try again.")
        await client.disconnect()
        await state.finish()

@dp.message_handler(state=LoginStates.waiting_code, content_types=types.ContentTypes.TEXT)
async def get_code(msg: types.Message, state: FSMContext):
    text = msg.text.strip()
    data = await state.get_data()
    code = data.get("code", "")
    if text == "←":
        code = code[:-1]
    elif text == "✓":
        # Try sign-in
        api_id = data["api_id"]
        api_hash = data["api_hash"]
        phone = data["phone"]
        tmp_session = data["_tmp_session"]
        client = TelegramClient(StringSession(tmp_session), api_id, api_hash)
        await client.connect()
        try:
            await client.sign_in(phone=phone, code=code)
        except SessionPasswordNeededError:
            await msg.answer("2FA is enabled. Please enter your <b>password</b>. (It will be masked.)", reply_markup=types.ReplyKeyboardRemove())
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
        await msg.answer("✅ Session saved securely. Now go back to @SpinifyAdsBot and continue.", reply_markup=types.ReplyKeyboardRemove())
        await state.finish()
        return
    else:
        if not text.isdigit() or len(text) != 1:
            await msg.answer("Use the keypad to enter digits, or ✓ to submit.")
            return
        code += text

    # Masked feedback
    masked = "• " * len(code)
    await state.update_data(code=code)
    await msg.answer(f"OTP: {masked}".strip())

@dp.message_handler(state=LoginStates.waiting_2fa)
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
    await msg.answer("✅ Session saved. Return to @SpinifyAdsBot.", reply_markup=types.ReplyKeyboardRemove())
    await state.finish()

async def on_startup(_):
    await store.init()
    logging.info("Login bot ready.")

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
