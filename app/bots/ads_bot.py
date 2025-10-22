import logging, httpx
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery
from app.api.settings import settings
from .keyboards import join_gate_kb, main_menu_kb, deep_link_login_kb

logging.basicConfig(level=logging.INFO)

bot = Bot(settings.adsbot_token)
dp = Dispatcher()

CHANNEL = settings.join_channel_username           # e.g., @PhiloBots
GROUP_ID = int(settings.join_group_id)             # -1002424072993
GROUP_INVITE_URL = getattr(settings, "join_group_url", None) or "https://t.me/PhiloBots"  # replace via .env->settings if you add it
LOGIN_BOT_USERNAME = "SpinifyLoginBot"             # set your real @username
API_BASE = settings.base_url                       # e.g., http://api:8000

async def is_member(user_id: int) -> bool:
    try:
        ch_mem = await bot.get_chat_member(chat_id=CHANNEL, user_id=user_id)
        gr_mem = await bot.get_chat_member(chat_id=GROUP_ID, user_id=user_id)
        ok = { "member", "administrator", "creator" }
        return (getattr(ch_mem, "status", None) in ok) and (getattr(gr_mem, "status", None) in ok)
    except Exception:
        return False

@dp.message(CommandStart())
async def start(message: Message):
    # force join-gate every open (you can cache for 24h later)
    if not await is_member(message.from_user.id):
        await message.answer(
            "Welcome to **Spinify Ads**!\nPlease join the channel and group, then tap ▣ Verify.",
            reply_markup=join_gate_kb(CHANNEL, GROUP_INVITE_URL),
            parse_mode="Markdown"
        )
        return
    await message.answer("▣ Main Menu", reply_markup=main_menu_kb())

@dp.callback_query(F.data == "verify")
async def verify(cb: CallbackQuery):
    if await is_member(cb.from_user.id):
        await cb.message.edit_text("Verified ✅")
        await cb.message.answer("▣ Main Menu", reply_markup=main_menu_kb())
    else:
        await cb.answer("Not verified yet. Join both and retry.", show_alert=True)

@dp.callback_query(F.data == "add_account")
async def add_account(cb: CallbackQuery):
    # Ask API for a fresh nonce bound to this AdsBot chat id
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(f"{API_BASE}/nonce/{cb.from_user.id}")
        n = r.json()["nonce"]
    kb = deep_link_login_kb(LOGIN_BOT_USERNAME, n)
    await cb.message.answer("Open the Login bot to connect your Telegram account.", reply_markup=kb)
    await cb.answer()

if __name__ == "__main__":
    import asyncio
    asyncio.run(dp.start_polling(bot))
