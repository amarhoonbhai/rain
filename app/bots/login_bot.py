"""Telegram bot for authenticating users' Telegram accounts.

This module implements the ``SpinifyLoginBot``, which guides users through
Telegram's login flow.  It collects the API ID and hash (from
``my.telegram.org``), prompts for the phone number, sends and verifies the
one‑time password (OTP) and optional two‑factor authentication (2FA), and
finally posts the resulting Telethon ``StringSession`` back to the API.  The
bot uses aiogram's finite state machine (FSM) to track progress through the
steps.

To run the bot locally, ensure the ``LOGINBOT_TOKEN`` environment variable
is set.  When running via Docker, tokens are injected through the
``env_file`` mechanism in ``docker-compose.yml``.  Logging is configured
globally to output INFO‑level messages.
"""

import logging
from aiogram import Bot, Dispatcher
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


class LoginStates(StatesGroup):
    api_id = State()
    api_hash = State()
    phone = State()
    otp = State()
    twofa = State()


@dp.message(CommandStart(deep_link=True))
async def start(message: Message, state: FSMContext):
    """
    Entry point for LoginBot.  Expects a deep link containing a nonce
    provided by the AdsBot.  Stores the nonce and prompts for API ID.
    """
    parts = (message.text or "").split(maxsplit=1)
    nonce = parts[1] if len(parts) > 1 else None
    if not nonce:
        await message.answer("Use the deep-link from \u25a3 Add Account in the main bot.")
        return
    await state.update_data(nonce=nonce)
    await state.set_state(LoginStates.api_id)
    await message.answer(
        "Send your **API ID** (from https://my.telegram.org).",
        parse_mode="Markdown",
    )


@dp.message(LoginStates.api_id)
async def step_api_id(message: Message, state: FSMContext):
    """Handle API ID input."""
    try:
        api_id = int(message.text.strip())
    except Exception:
        await message.answer("API ID must be a number. Try again.")
        return
    await state.update_data(api_id=api_id)
    await state.set_state(LoginStates.api_hash)
    await message.answer("Now send your **API Hash**.", parse_mode="Markdown")


@dp.message(LoginStates.api_hash)
async def step_api_hash(message: Message, state: FSMContext):
    """Handle the API hash input and prompt for the phone number.

    The API hash is the second piece of Telegram API credentials.  After
    storing it, the state transitions to ``LoginStates.phone`` where the
    user will be asked for their phone number.
    """
    await state.update_data(api_hash=message.text.strip())
    await state.set_state(LoginStates.phone)
    await message.answer(
        "Send your **phone number** in international format (e.g., +9198xxxxxx).",
        parse_mode="Markdown",
    )


@dp.message(LoginStates.phone)
async def step_phone(message: Message, state: FSMContext):
    """Send a login code to the specified phone number.

    After storing the phone number, the bot uses Telethon to send a code
    request.  If this fails (e.g., the number is invalid or network issues
    occur), it notifies the user and aborts the login process.
    """
    await state.update_data(phone=message.text.strip())
    data = await state.get_data()
    api_id = data["api_id"]
    api_hash = data["api_hash"]
    phone = data["phone"]
    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.connect()
    try:
        await client.send_code_request(phone)
    except Exception as e:
        await client.disconnect()
        await message.answer(f"Failed to send code: {e}")
        await state.clear(); return
    await client.disconnect()
    await state.set_state(LoginStates.otp)
    await message.answer("Enter the **OTP** you received (digits only).", parse_mode="Markdown")


@dp.message(LoginStates.otp)
async def step_otp(message: Message, state: FSMContext):
    """Validate the OTP and finalise login.

    This step attempts to sign in with the provided one‑time password.  If
    Telegram indicates that a 2FA password is required, the state
    transitions to ``LoginStates.twofa``.  Any other error will abort the
    flow and clear the FSM state.
    """
    code = message.text.strip().replace(" ", "")
    data = await state.get_data()
    api_id = data["api_id"]
    api_hash = data["api_hash"]
    phone = data["phone"]
    nonce = data["nonce"]
    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.connect()
    try:
        await client.sign_in(phone=phone, code=code)
    except SessionPasswordNeededError:
        # 2FA required
        await state.set_state(LoginStates.twofa)
        await message.answer("This account has **2FA password**. Send it now.", parse_mode="Markdown")
        await client.disconnect()
        return
    except Exception as e:
        await client.disconnect()
        await message.answer(f"Login failed: {e}")
        await state.clear(); return
    me = await client.get_me()
    sess = client.session.save()
    await client.disconnect()
    await bind_and_finish(message, state, nonce, sess, me, phone, api_id, data["api_hash"])


@dp.message(LoginStates.twofa)
async def step_twofa(message: Message, state: FSMContext):
    """Handle two‑factor authentication (2FA).

    If Telegram requires a password after the OTP, this handler collects
    it and completes the sign‑in process.  On failure, the user is
    informed and the FSM is cleared.
    """
    pwd = message.text.strip()
    data = await state.get_data()
    api_id = data["api_id"]
    api_hash = data["api_hash"]
    phone = data["phone"]
    nonce = data["nonce"]
    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.connect()
    try:
        await client.sign_in(phone=phone, password=pwd)
    except Exception as e:
        await client.disconnect()
        await message.answer(f"2FA login failed: {e}")
        await state.clear(); return
    me = await client.get_me()
    sess = client.session.save()
    await client.disconnect()
    await bind_and_finish(message, state, nonce, sess, me, phone, api_id, api_hash)


async def bind_and_finish(
    message: Message,
    state: FSMContext,
    nonce: str,
    string_session: str,
    me,
    phone: str,
    api_id: int,
    api_hash: str,
):
    """Helper to call the API and finalize login."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            r = await client.post(
                f"{settings.base_url}/session/bind",
                json={
                    "nonce": nonce,
                    "string_session": string_session,
                    "phone_e164": phone,
                    "tg_user_id": me.id,
                    "display_name": (me.first_name or "") + (" " + me.last_name if me.last_name else ""),
                    "api_id": api_id,
                    "api_hash": api_hash,
                },
            )
        except Exception as e:
            await message.answer(f"Error saving session: {e}")
            await state.clear(); return
        if r.status_code != 200:
            await message.answer(f"Error saving session: {r.text}")
            await state.clear(); return
    await message.answer(
        "Login complete ✅\nReturn to the main bot: https://t.me/" + (
            settings.adsbot_username or "SpinifyAdsBot"
        ) + "?start=back",
        disable_web_page_preview=True,
    )
    await state.clear()


if __name__ == "__main__":
    import asyncio
    asyncio.run(dp.start_polling(bot))
