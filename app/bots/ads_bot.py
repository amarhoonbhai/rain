import logging, httpx, re
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery
from app.api.settings import settings
from .keyboards import join_gate_kb, main_menu_kb, deep_link_login_kb, del_gc_menu_kb

logging.basicConfig(level=logging.INFO)

bot = Bot(settings.adsbot_token)
dp = Dispatcher()

# Join-gate targets
CHANNEL = settings.join_channel_username  # e.g., @PhiloBots
GROUP_ID = int(settings.join_group_id)    # e.g., -1002424072993
GROUP_INVITE_URL = getattr(settings, "join_group_url", None) or "https://t.me/PhiloBots"
LOGIN_BOT_USERNAME = getattr(settings, "login_bot_username", None) or "SpinifyLoginBot"
API_BASE = settings.base_url

async def is_member(user_id: int) -> bool:
    try:
        ch_mem = await bot.get_chat_member(chat_id=CHANNEL, user_id=user_id)
        gr_mem = await bot.get_chat_member(chat_id=GROUP_ID, user_id=user_id)
        ok = {"member", "administrator", "creator"}
        return (getattr(ch_mem, "status", None) in ok) and (getattr(gr_mem, "status", None) in ok)
    except Exception:
        return False

@dp.message(CommandStart(deep_link=True))
async def start_deeplink(message: Message):
    if not await is_member(message.from_user.id):
        await message.answer(
            f"Welcome to **Spinify Ads**!\nPlease join {CHANNEL} and our community group, then tap \u25A3 Verify.",
            reply_markup=join_gate_kb(CHANNEL, GROUP_INVITE_URL),
            parse_mode="Markdown"
        )
        return
    await show_main_menu(message)

@dp.message(CommandStart())
async def start(message: Message):
    if not await is_member(message.from_user.id):
        await message.answer(
            "Welcome to **Spinify Ads**!\nPlease join the channel and group, then tap \u25A3 Verify.",
            reply_markup=join_gate_kb(CHANNEL, GROUP_INVITE_URL),
            parse_mode="Markdown"
        )
        return
    await show_main_menu(message)

async def show_main_menu(message: Message, text: str | None = None):
    connected = False
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{API_BASE}/me/{message.from_user.id}")
            connected = bool(r.json().get("has_session"))
    except Exception:
        connected = False
    await message.answer(text or "\u25A3 Main Menu", reply_markup=main_menu_kb(connected=connected))

@dp.callback_query(F.data == "verify")
async def verify(cb: CallbackQuery):
    if await is_member(cb.from_user.id):
        await cb.message.edit_text("Verified ✅")
        await cb.message.answer("\u25A3 Main Menu", reply_markup=main_menu_kb(connected=True))
    else:
        await cb.answer("Not verified yet. Join both and retry.", show_alert=True)

@dp.callback_query(F.data == "add_account")
async def add_account(cb: CallbackQuery):
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(f"{API_BASE}/nonce/{cb.from_user.id}")
        n = r.json()["nonce"]
    kb = deep_link_login_kb(LOGIN_BOT_USERNAME, n)
    await cb.message.answer("Open the Login bot to connect your Telegram account.", reply_markup=kb)
    await cb.answer()

@dp.callback_query(F.data == "add_group")
async def add_group(cb: CallbackQuery):
    await cb.message.answer("Send the **t.me group link**, **@username**, or **-100…** chat ID you want to add (limit 5).", parse_mode="Markdown")
    await cb.answer()

# Regex for capturing group link or ID
GROUP_RE = re.compile(r"(?:https?://)?t\.me/(?:\+|joinchat/)?([A-Za-z0-9_\-+]+)|(@[A-Za-z0-9_]+)|(-?\d{5,})")

@dp.message()
async def catch_group_link(message: Message):
    text = (message.text or "").strip()
    if text.startswith("/start") or text.startswith("/help"):
        return
    m = GROUP_RE.search(text)
    if not m:
        return
    link_or_id = m.group(0)
    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.post(f"{API_BASE}/groups/verify_add", json={
            "bot_chat_id": message.from_user.id,
            "link_or_id": link_or_id
        })
    if resp.status_code != 200:
        await message.reply(f"Error: {resp.text}")
        return
    data = resp.json()
    if not data.get("ok"):
        await message.reply(f"Cannot add: {data.get('reason','unknown error')}")
        return
    await message.reply(f"Added \u25A3 *{data['title']}* (`{data['chat_id']}`)", parse_mode="Markdown")

@dp.callback_query(F.data == "del_gc")
async def del_gc(cb: CallbackQuery):
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(f"{API_BASE}/groups/list/{cb.from_user.id}")
        groups = r.json()
    if not groups:
        await cb.message.answer("No groups to delete.")
        await cb.answer()
        return
    pairs = [(g['title'], g['chat_id']) for g in groups]
    await cb.message.answer("Select a group to delete:", reply_markup=del_gc_menu_kb(pairs))
    await cb.answer()

@dp.callback_query(F.data.startswith("del:"))
async def del_gc_pick(cb: CallbackQuery):
    val = cb.data.split(":", 1)[1]
    if val == "cancel":
        await cb.message.edit_text("Delete canceled.")
        await cb.answer()
        return
    try:
        chat_id = int(val)
    except ValueError:
        await cb.answer("Invalid selection", show_alert=True)
        return
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.post(f"{API_BASE}/groups/delete", json={
            "bot_chat_id": cb.from_user.id,
            "chat_id": chat_id
        })
    if r.status_code == 200:
        await cb.message.edit_text(f"Removed group `{chat_id}`.", parse_mode="Markdown")
    else:
        await cb.message.edit_text(f"Error: {r.text}")
    await cb.answer()

if __name__ == "__main__":
    import asyncio
    asyncio.run(dp.start_polling(bot))
