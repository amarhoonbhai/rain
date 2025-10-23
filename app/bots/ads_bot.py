"""Telegram bot for handling the user‑facing portion of Spinify.

This module defines the ``SpinifyAdsBot``, responsible for interacting with
users via Telegram.  It enforces the join gate (making sure users are
subscribed to the channel and group), shows a dynamic main menu, and
coordinates account login and group management by delegating to the API.

The bot is built using aiogram v3 and follows an asynchronous design.  To
start the bot directly (without Docker), ensure that the ``ADSBOT_TOKEN``
environment variable is set in your shell or in a `.env` file.  The bot
will read configuration from :class:`app.api.settings.Settings`.
"""

import logging
import re
import httpx
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery
from app.api.settings import settings
from .keyboards import (
    join_gate_kb,
    main_menu_kb,
    deep_link_login_kb,
    del_gc_menu_kb,
)

logging.basicConfig(level=logging.INFO)

bot = Bot(settings.adsbot_token)
dp = Dispatcher()

CHANNEL = settings.join_channel_username           # e.g., @PhiloBots
GROUP_ID = int(settings.join_group_id)             # -1002424072993
GROUP_INVITE_URL = getattr(settings, "join_group_url", None) or "https://t.me/PhiloBots"
LOGIN_BOT_USERNAME = getattr(settings, "login_bot_username", None) or "SpinifyLoginBot"
API_BASE = settings.base_url

async def is_member(user_id: int) -> bool:
    """Check whether a user is a member of both the channel and the group.

    Args:
        user_id: The Telegram user ID to check.

    Returns:
        True if the user is either a member or administrator in both
        ``CHANNEL`` and ``GROUP_ID``, otherwise False.  Any exceptions
        (e.g., network errors) will cause a False result.
    """
    try:
        ch_mem = await bot.get_chat_member(chat_id=CHANNEL, user_id=user_id)
        gr_mem = await bot.get_chat_member(chat_id=GROUP_ID, user_id=user_id)
        ok = {"member", "administrator", "creator"}
        return (getattr(ch_mem, "status", None) in ok) and (getattr(gr_mem, "status", None) in ok)
    except Exception:
        return False

async def show_main_menu(message: Message, text: str | None = None) -> None:
    """Render the main menu.

    This helper queries the API to determine whether the current user has an
    active session and uses that information to customise the keyboard.  If
    the API call fails (e.g., the API is unreachable), the menu will still
    display but without the connected indicator.

    Args:
        message: The incoming Telegram message to reply to.
        text: Optional header text for the menu; defaults to "\u25a3 Main Menu".
    """
    connected = False
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{API_BASE}/me/{message.from_user.id}")
            if r.status_code == 200:
                data = r.json()
                connected = bool(data.get("has_session"))
    except httpx.RequestError as e:
        logging.warning("Failed to reach API for /me: %s", e)
    except Exception:
        pass
    await message.answer(
        text or "\u25a3 Main Menu",
        reply_markup=main_menu_kb(connected=connected),
        parse_mode="Markdown",
    )

@dp.message(CommandStart(deep_link=True))
async def start_deeplink(message: Message):
    """Handle /start with a deep link parameter.

    If the user has joined both the channel and group, this simply shows
    the main menu.  Otherwise, it prompts them to complete the join gate.
    """
    if not await is_member(message.from_user.id):
        await message.answer(
            f"Welcome to **Spinify Ads**!\nPlease join {CHANNEL} and our community group, then tap \u25a3 Verify.",
            reply_markup=join_gate_kb(CHANNEL, GROUP_INVITE_URL),
            parse_mode="Markdown",
        )
        return
    await show_main_menu(message)

@dp.message(CommandStart())
async def start(message: Message):
    """Handle /start for the general case.

    This forces the join gate on every bot open.  If the user has not
    joined the channel or group, they are prompted to do so.  Otherwise
    the main menu is displayed.
    """
    if not await is_member(message.from_user.id):
        await message.answer(
            "Welcome to **Spinify Ads**!\nPlease join the channel and group, then tap \u25a3 Verify.",
            reply_markup=join_gate_kb(CHANNEL, GROUP_INVITE_URL),
            parse_mode="Markdown",
        )
        return
    await show_main_menu(message)

@dp.callback_query(F.data == "verify")
async def verify(cb: CallbackQuery):
    """Handle the \u25a3 Verify button in the join gate menu.

    When pressed, this callback rechecks membership in both the channel and
    group.  If the user is verified, it edits the gate message and
    displays the main menu.  Otherwise, an alert is shown prompting the
    user to join the required chats.
    """
    if await is_member(cb.from_user.id):
        await cb.message.edit_text("Verified ✅")
        await show_main_menu(cb.message)
    else:
        await cb.answer("Not verified yet. Join both and retry.", show_alert=True)

@dp.callback_query(F.data == "add_account")
async def add_account(cb: CallbackQuery):
    """Deep‑link to the LoginBot to start the login flow.

    A fresh nonce is requested from the API, bound to the current chat ID.
    This nonce is embedded in a URL which opens the LoginBot.  When the
    login flow completes, the user is redirected back to this bot.
    """
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(f"{API_BASE}/nonce/{cb.from_user.id}")
        n = r.json()["nonce"]
    kb = deep_link_login_kb(LOGIN_BOT_USERNAME, n)
    await cb.message.answer(
        "Open the Login bot to connect your Telegram account.", reply_markup=kb
    )
    await cb.answer()

@dp.callback_query(F.data == "add_group")
async def add_group(cb: CallbackQuery):
    """Prompt the user to send a group link, username or ID for addition.

    This callback simply instructs the user on what format to use when
    providing the group they wish to register.  Actual processing happens
    in the generic message handler.
    """
    await cb.message.answer(
        "Send the **t.me group link**, **@username**, or **-100…** chat ID you want to add (limit 5).",
        parse_mode="Markdown",
    )
    await cb.answer()

# Generic message handler to catch group links/IDs
GROUP_RE = re.compile(r"(?:https?://)?t\.me/(?:\+|joinchat/)?([A-Za-z0-9_\-+]+)|(@[A-Za-z0-9_]+)|(-?\d{5,})")

@dp.message()
async def catch_group_link(message: Message):
    """Handle messages containing a group link or ID.

    When the user is prompted to add a group, they can send either a
    ``t.me`` URL, an ``@username`` or a numeric chat ID.  This handler
    searches the incoming message for a matching pattern.  If found, it
    calls the API to verify membership and persist the group.  Errors and
    API responses are relayed back to the user.
    """
    text = (message.text or "").strip()
    if text.startswith("/start") or text.startswith("/help"):
        return
    m = GROUP_RE.search(text)
    if not m:
        return
    link_or_id = m.group(0)
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.post(
                f"{API_BASE}/groups/verify_add",
                json={"bot_chat_id": message.from_user.id, "link_or_id": link_or_id},
            )
    except Exception as e:
        await message.reply(f"Error contacting API: {e}")
        return
    if resp.status_code != 200:
        await message.reply(f"Error: {resp.text}")
        return
    data = resp.json()
    if not data.get("ok"):
        await message.reply(f"Cannot add: {data.get('reason', 'unknown error')}")
        return
    await message.reply(
        f"Added \u25a3 *{data['title']}* (`{data['chat_id']}`)", parse_mode="Markdown"
    )

@dp.callback_query(F.data == "del_gc")
async def del_gc(cb: CallbackQuery):
    """Present a menu of groups for deletion.

    Retrieves the list of registered groups from the API.  If the user
    hasn't added any groups yet, a message is sent indicating that there
    is nothing to delete.  Otherwise, the bot displays a selection menu.
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(f"{API_BASE}/groups/list/{cb.from_user.id}")
            groups = r.json()
    except Exception as e:
        await cb.message.answer(f"Error: {e}")
        await cb.answer()
        return
    if not groups:
        await cb.message.answer("No groups to delete.")
        await cb.answer()
        return
    pairs = [(g['title'], g['chat_id']) for g in groups]
    await cb.message.answer(
        "Select a group to delete:", reply_markup=del_gc_menu_kb(pairs)
    )
    await cb.answer()

@dp.callback_query(F.data.startswith("del:"))
async def del_gc_pick(cb: CallbackQuery):
    """Process a selection from the delete group menu.

    Extracts the chat ID from the callback data.  If the user chooses the
    ``cancel`` option, the deletion is aborted.  Otherwise, an API call
    is made to remove the group.  Success or error messages are sent to
    the user accordingly.
    """
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
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(
                f"{API_BASE}/groups/delete",
                json={"bot_chat_id": cb.from_user.id, "chat_id": chat_id},
            )
    except Exception as e:
        await cb.message.edit_text(f"Error contacting API: {e}")
        await cb.answer()
        return
    if r.status_code == 200:
        await cb.message.edit_text(f"Removed group `{chat_id}`.", parse_mode="Markdown")
    else:
        await cb.message.edit_text(f"Error: {r.text}")
    await cb.answer()

if __name__ == "__main__":
    import asyncio
    asyncio.run(dp.start_polling(bot))
