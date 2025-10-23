import logging
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.types import Message
from app.api.settings import settings
import httpx
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError

logging.basicConfig(level=logging.INFO)

bot = Bot(settings.loginbot_token)
dp = Dispatcher()
API_BASE = settings.base_url

class LoginStates(StatesGroup):
    nonce = State()
    api_id = State()
    api_hash = State()
    phone = State()
    otp = State()
    twofa = State()

@dp.message(CommandStart(deep_link=True))
async def start(message: Message, state: FSMContext):
    parts = (message.text or "").split(maxsplit=1)
    nonce = parts[1] if len(parts) > 1 else None
    if not nonce:
        await message.answer("Use the deep-link from ▣ Add Account in the main bot.")
        return
    await state.update_data(nonce=nonce)
    await state.set_state(LoginStates.api_id)
    await message.answer("Send your **API ID** (from https://my.telegram.org).", parse_mode="Markdown")

@dp.message(LoginStates.api_id)
async def step_api_id(message: Message, state: FSMContext):
    try:
        api_id = int(message.text.strip())
    except Exception:
        await message.answer("API ID must be a number. Try again.")
        return
    await state.update_data(api_id=api_id)
    await state.set_state(LoginStates.api_hash)
    await message.answer("Now send your **API Hash**.")

@dp.message(LoginStates.api_hash)
async def step_api_hash(message: Message, state: FSMContext):
    await state.update_data(api_hash=message.text.strip())
    await state.set_state(LoginStates.phone)
    await message.answer("Send your **phone number** in international format (e.g., +9198xxxxxx).")

@dp.message(LoginStates.phone)
async def step_phone(message: Message, state: FSMContext):
    await state.update_data(phone=message.text.strip())
    data = await state.get_data()
    api_id, api_hash, phone = data["api_id"], data["api_hash"], data["phone"]
    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.connect()
    await client.send_code_request(phone)
    await client.disconnect()
    await state.set_state(LoginStates.otp)
    await message.answer("Enter the **OTP** you received (digits only).")

@dp.message(LoginStates.otp)
async def step_otp(message: Message, state: FSMContext):
    code = message.text.strip().replace(" ", "")
    data = await state.get_data()
    api_id, api_hash, phone = data["api_id"], data["api_hash"], data["phone"]
    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.connect()
    try:
        await client.sign_in(phone=phone, code=code)
    except SessionPasswordNeededError:
        await state.set_state(LoginStates.twofa)
        await message.answer("This account has **2FA password**. Send it now.", parse_mode="Markdown")
        await client.disconnect()
        return
    me = await client.get_me()
    sess = client.session.save()
    await client.disconnect()
    await bind_and_finish(message, state, data["nonce"], sess, me, phone, api_id, api_hash)

@dp.message(LoginStates.twofa)
async def step_twofa(message: Message, state: FSMContext):
    pwd = message.text.strip()
    data = await state.get_data()
    api_id, api_hash, phone = data["api_id"], data["api_hash"], data["phone"]
    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.connect()
    await client.sign_in(phone=phone, password=pwd)
    me = await client.get_me()
    sess = client.session.save()
    await client.disconnect()
    await bind_and_finish(message, state, data["nonce"], sess, me, phone, api_id, api_hash)

async def bind_and_finish(message: Message, state: FSMContext, nonce: str, string_session: str, me, phone: str, api_id: int, api_hash: str):
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.post(f"{API_BASE}/session/bind", json={
            "nonce": nonce,
            "string_session": string_session,
            "phone_e164": phone,
            "tg_user_id": me.id,
            "display_name": (me.first_name or "") + (" " + me.last_name if me.last_name else ""),
            "api_id": api_id,
            "api_hash": api_hash
        })
        if r.status_code != 200:
            await message.answer(f"Error saving session: {r.text}")
            await state.clear()
            return
    adsbot_username = getattr(settings, "adsbot_username", "SpinifyAdsBot")
    await message.answer(f"Login complete ✅\nReturn to the main bot: https://t.me/{adsbot_username}?start=back")
    await state.clear()

if __name__ == "__main__":
    import asyncio
    asyncio.run(dp.start_polling(bot))
