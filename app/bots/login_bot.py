import logging
from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.types import Message
from app.api.settings import settings

logging.basicConfig(level=logging.INFO)

bot = Bot(settings.loginbot_token)
dp = Dispatcher()

class LoginStates(StatesGroup):
    nonce = State()
    # (S5 will add: api_id, api_hash, phone, otp, twofa)

@dp.message(CommandStart(deep_link=True))
async def start(message: Message, state: FSMContext):
    # /start <nonce>
    parts = (message.text or "").split(maxsplit=1)
    nonce = parts[1] if len(parts) > 1 else None
    if not nonce:
        await message.answer("Use the deep-link from ▣ Add Account in the main bot.")
        return

    await state.update_data(nonce=nonce)
    await state.set_state(LoginStates.nonce)
    await message.answer(
        "Nonce received ✅\n\nSend `CMD: RUN S5` here when you want me to enable the full login flow "
        "(API ID → API hash → phone → OTP/2FA → session bind).",
        parse_mode="Markdown"
    )

if __name__ == "__main__":
    import asyncio
    asyncio.run(dp.start_polling(bot))
