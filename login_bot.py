# login_bot.py — Spinify Login Bot (Blue Glow UI)
import os
import asyncio
import logging
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from dotenv import load_dotenv

from core.db import (
    init_db, ensure_user,
    sessions_list, sessions_upsert_slot,
    sessions_delete, first_free_slot
)

load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("login-bot")

TOKEN = (os.getenv("LOGIN_BOT_TOKEN") or "").strip()
if not TOKEN or ":" not in TOKEN:
    raise RuntimeError("LOGIN_BOT_TOKEN missing")

OWNER_ID = int(os.getenv("OWNER_ID", "0"))

REQUIRED_CHANNELS = [
    c.strip() for c in (os.getenv("REQUIRED_CHANNELS", "").split(",")) if c.strip()
]

bot = TelegramClient("login-bot", 0, "")
bot.parse_mode = "html"

# --------------------------------------------------------------------
# Blue Glow UI Helpers
# --------------------------------------------------------------------
def ui_title(text: str) -> str:
    return f"✨ <b>{text}</b> ✨"

def ui_section(text: str) -> str:
    return f"<b>{text}</b>"

# --------------------------------------------------------------------
# Channel Gate
# --------------------------------------------------------------------
async def check_gate(user_id: int):
    missing = []
    for ch in REQUIRED_CHANNELS:
        try:
            m = await bot.get_permissions(ch, user_id)
            if m is None:
                missing.append(ch)
        except Exception:
            missing.append(ch)
    return missing

async def gate_message():
    lines = "\n".join(f"• {c}" for c in REQUIRED_CHANNELS)
    return (
        "🔐 <b>Access Required</b>\n\n"
        "You must join these channels to use Spinify Login Bot:\n"
        f"{lines}\n\n"
        "Tap <b>I’ve Joined</b> after joining."
    )

def gate_keyboard():
    rows = []
    for c in REQUIRED_CHANNELS:
        rows.append([("/join_" + c, f"🔗 Join {c}")])
    return None

# --------------------------------------------------------------------
# Memory for login state
# --------------------------------------------------------------------
_pending = {}

# --------------------------------------------------------------------
# /start
# --------------------------------------------------------------------
@bot.on(events.NewMessage(pattern="^/start"))
async def start_handler(event):
    uid = event.sender_id
    ensure_user(uid, event.sender.username)

    # Check gate
    missing = await check_gate(uid)
    if missing:
        buttons = []
        for ch in missing:
            buttons.append([event.builder.button.url(f"🔗 Join {ch}", f"https://t.me/{ch.lstrip('@')}")])
        buttons.append([event.builder.button.callback("✅ I’ve Joined", data=b"gate_check")])
        await event.respond(await gate_message(), buttons=buttons)
        return

    # Main menu
    await event.respond(
        ui_title("SPINIFY LOGIN BOT") + "\n\n"
        "Add your Telegram accounts safely.\n"
        "These accounts will be used for auto-forwarding.\n\n"
        "Choose an option:",
        buttons=[
            [event.builder.button.callback("➕ Add New Account", data=b"add_acc")],
            [event.builder.button.callback("📂 My Sessions", data=b"sessions")],
            [event.builder.button.callback("ℹ️ Help", data=b"help")],
            [event.builder.button.callback("👨‍💻 Developer", data=b"dev")]
        ]
    )

# --------------------------------------------------------------------
# Gate Check
# --------------------------------------------------------------------
@bot.on(events.CallbackQuery(data=b"gate_check"))
async def gate_check_handler(event):
    uid = event.sender_id
    missing = await check_gate(uid)
    if missing:
        await event.answer("❌ Still missing channels.", alert=True)
        return
    await start_handler(event)

# --------------------------------------------------------------------
# Help
# --------------------------------------------------------------------
@bot.on(events.CallbackQuery(data=b"help"))
async def help_handler(event):
    text = (
        ui_title("How to Login") + "\n\n"
        "1️⃣ Go to https://my.telegram.org\n"
        "2️⃣ Create API ID & API Hash\n"
        "3️⃣ Tap <b>Add New Account</b>\n"
        "4️⃣ Enter your API ID & Hash\n"
        "5️⃣ Enter OTP sent by Telegram\n\n"
        "Your session will be securely saved."
    )
    await event.edit(text, buttons=[[event.builder.button.callback("⬅ Back", data=b"back")]])

# --------------------------------------------------------------------
# Developer
# --------------------------------------------------------------------
@bot.on(events.CallbackQuery(data=b"dev"))
async def dev_handler(event):
    await event.edit(
        ui_title("Developer") +
        "\n\n👨‍💻 <b>@SpinifyAdsBot</b>",
        buttons=[[event.builder.button.callback("⬅ Back", data=b"back")]]
    )

# --------------------------------------------------------------------
# Back
# --------------------------------------------------------------------
@bot.on(events.CallbackQuery(data=b"back"))
async def back_handler(event):
    await start_handler(event)

# --------------------------------------------------------------------
# Sessions Screen
# --------------------------------------------------------------------
@bot.on(events.CallbackQuery(data=b"sessions"))
async def sessions_handler(event):
    uid = event.sender_id
    rows = sessions_list(uid)

    if not rows:
        await event.edit(
            ui_section("📂 Your Sessions") +
            "\nNo accounts added yet.",
            buttons=[[event.builder.button.callback("⬅ Back", data=b"back")]]
        )
        return

    lines = []
    buttons = []

    for r in rows:
        lines.append(f"• Slot {r['slot']} — ACTIVE")
        buttons.append([event.builder.button.callback(f"🗑 Remove Slot {r['slot']}", data=f"del_{r['slot']}".encode())])

    buttons.append([event.builder.button.callback("⬅ Back", data=b"back")])

    await event.edit(
        ui_section("📂 Your Sessions") + "\n" + "\n".join(lines),
        buttons=buttons
    )

# --------------------------------------------------------------------
# Delete
# --------------------------------------------------------------------
@bot.on(events.CallbackQuery(pattern=b"del_"))
async def delete_slot(event):
    uid = event.sender_id
    slot = int(event.data.decode().split("_")[1])
    sessions_delete(uid, slot)
    await sessions_handler(event)

# --------------------------------------------------------------------
# Add Account
# --------------------------------------------------------------------
@bot.on(events.CallbackQuery(data=b"add_acc"))
async def add_acc(event):
    uid = event.sender_id
    slot = first_free_slot(uid)

    _pending[uid] = {"step": 1, "slot": slot}
    await event.edit(
        ui_title("Add New Account") +
        "\n\nSend your <b>API ID</b>.",
        buttons=[[event.builder.button.callback("❌ Cancel", data=b"cancel_login")]]
    )

@bot.on(events.CallbackQuery(data=b"cancel_login"))
async def cancel(event):
    _pending.pop(event.sender_id, None)
    await event.edit("❌ Login cancelled.", buttons=[[event.builder.button.callback("⬅ Back", data=b"back")]])

# --------------------------------------------------------------------
# Login Steps
# --------------------------------------------------------------------
@bot.on(events.NewMessage)
async def login_steps(event):
    uid = event.sender_id
    if uid not in _pending:
        return

    state = _pending[uid]

    # Step 1: API ID
    if state["step"] == 1:
        try:
            api_id = int(event.raw_text.strip())
            state["api_id"] = api_id
            state["step"] = 2
            await event.respond("➡ Good. Now send your <b>API Hash</b>.")
        except:
            await event.respond("❌ API ID must be a number.")
        return

    # Step 2: API Hash
    if state["step"] == 2:
        state["api_hash"] = event.raw_text.strip()
        state["step"] = 3
        await event.respond("➡ Perfect. Now enter the <b>code sent by Telegram</b>.")
        return

    # Step 3: OTP
    if state["step"] == 3:
        otp = event.raw_text.strip()
        api_id = state["api_id"]
        api_hash = state["api_hash"]
        slot = state["slot"]

        await event.respond("⏳ Logging in… Please wait…")

        try:
            client = TelegramClient(StringSession(), api_id, api_hash)
            await client.connect()
            result = await client.sign_in(code=otp)

            session_string = client.session.save()
            await client.disconnect()

            sessions_upsert_slot(uid, slot, api_id, api_hash, session_string)
            _pending.pop(uid, None)

            await event.respond("✅ Account logged in successfully!")
        except Exception as e:
            await event.respond(f"❌ Login failed: <code>{e}</code>")
        return


# --------------------------------------------------------------------
# Run
# --------------------------------------------------------------------
async def main():
    init_db()
    await bot.start(bot_token=TOKEN)
    log.info("Spinify Login Bot ready.")
    await bot.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
