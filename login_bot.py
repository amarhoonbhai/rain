# login_bot.py — Aiogram v3.x + Pyrogram 2.x
# ✇ Compact OTP keypad, slot-aware DB save, hard channel gate, ✇ instructions
import os, asyncio, logging
from datetime import datetime

from aiogram import Bot, Dispatcher, F, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, StateFilter
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramBadRequest
from dotenv import load_dotenv

from pyrogram import Client
from pyrogram.errors import (
    PhoneCodeExpired, PhoneCodeInvalid, FloodWait, SessionPasswordNeeded,
    ApiIdInvalid, PhoneNumberInvalid, PhoneNumberFlood, PhoneNumberBanned
)

from core.db import (
    init_db,
    first_free_slot, sessions_upsert_slot,
    get_gate_channels_effective,
)

# ---------------- env / bot ----------------
load_dotenv()
LOGIN_BOT_TOKEN = (os.getenv("LOGIN_BOT_TOKEN") or "").strip()
if not LOGIN_BOT_TOKEN:
    raise RuntimeError("LOGIN_BOT_TOKEN missing in .env")

bot = Bot(token=LOGIN_BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
init_db()
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"))
log = logging.getLogger("login_bot")

# -------------- helpers -------------------
def _gate_channels() -> list[str]:
    ch1, ch2 = get_gate_channels_effective()
    return [c for c in (ch1, ch2) if c]

async def _check_gate(user_id: int):
    missing = []
    for ch in _gate_channels():
        try:
            m = await bot.get_chat_member(ch, user_id)
            if str(getattr(m, "status", "left")).lower() in {"left","kicked"}:
                missing.append(ch)
        except Exception:
            missing.append(ch)
    return (len(missing)==0), missing

def _gate_kb():
    chs = _gate_channels()
    rows = []
    if len(chs)>=1:
        rows.append([InlineKeyboardButton(text=f"🔗 {chs[0]}", url=f"https://t.me/{chs[0].lstrip('@')}")])
    if len(chs)>=2:
        rows.append([InlineKeyboardButton(text=f"🔗 {chs[1]}", url=f"https://t.me/{chs[1].lstrip('@')}")])
    rows.append([InlineKeyboardButton(text="✅ I've Joined", callback_data="gate:check")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def safe_edit_text(message, text, **kw):
    try:
        return await message.edit_text(text, **kw)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            return None
        raise

# auto-ack so buttons never spin
class AutoAckMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        if isinstance(event, CallbackQuery):
            try: await event.answer()
            except Exception: pass
        return await handler(event, data)

dp.update.middleware(AutoAckMiddleware())

# -------------- OTP UI --------------------
def otp_kb():
    rows = [
        [InlineKeyboardButton(text=t, callback_data=f"d:{t}") for t in ("1","2","3")],
        [InlineKeyboardButton(text=t, callback_data=f"d:{t}") for t in ("4","5","6")],
        [InlineKeyboardButton(text=t, callback_data=f"d:{t}") for t in ("7","8","9")],
        [InlineKeyboardButton(text="0", callback_data="d:0")],
        [InlineKeyboardButton(text="⬅ Back", callback_data="act:back"),
         InlineKeyboardButton(text="🧹 Clear", callback_data="act:clear"),
         InlineKeyboardButton(text="✔ Submit", callback_data="act:submit")],
        [InlineKeyboardButton(text="🔁 Resend", callback_data="act:resend")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

def fmt_code(code: str) -> str:
    return "<code>—</code>" if not code else f"<code>{' '.join(list(code))}</code>"

# -------------- FSM -----------------------
class Login(StatesGroup):
    api_id = State()
    api_hash = State()
    phone = State()
    otp = State()
    password = State()

# in-memory Pyrogram client per user during login flow
LOGIN_APPS: dict[int, Client] = {}

async def _get_app(user_id: int, api_id: int, api_hash: str) -> Client:
    app = LOGIN_APPS.get(user_id)
    if not app:
        app = Client(name=f"login-{user_id}", api_id=api_id, api_hash=api_hash, in_memory=True)
        await app.connect()
        LOGIN_APPS[user_id] = app
    return app

async def _disconnect(user_id: int):
    app = LOGIN_APPS.pop(user_id, None)
    if app:
        try: await app.disconnect()
        except Exception: pass

WELCOME = (
    "✇ Spinify Login — Steps\n"
    "  1) ✇ Send your API_ID\n"
    "  2) ✇ Send your API_HASH\n"
    "  3) ✇ Send phone in +countrycode (e.g., +91…)\n"
    "  4) ✇ Enter OTP using keypad (auto-submit on 5 digits)\n"
    "  5) ✇ If 2FA enabled, send password\n\n"
    "✇ After success: Return to main bot to set interval & groups."
)

# -------------- Handlers -------------------
@dp.message(Command("start"))
async def start(msg: Message, state: FSMContext):
    await state.clear()
    if _gate_channels():
        ok, _ = await _check_gate(msg.from_user.id)
        if not ok:
            txt = (
                "✇ Access required for Login Bot\n"
                f"  • {_gate_channels()[0] if len(_gate_channels())>0 else '—'}\n"
                f"  • {_gate_channels()[1] if len(_gate_channels())>1 else '—'}\n\n"
                "✇ Tap <b>I've Joined</b> after joining."
            )
            await msg.answer(txt, reply_markup=_gate_kb()); return
    await msg.answer(WELCOME)
    await msg.answer("✇ Step 1 — Send your API_ID.")
    await state.set_state(Login.api_id)

@dp.callback_query(F.data == "gate:check")
async def gate_check(cq: CallbackQuery, state: FSMContext):
    ok, _ = await _check_gate(cq.from_user.id)
    if ok:
        await safe_edit_text(cq.message, "✅ Thanks! Continue with Step 1 — send your API_ID.")
        await state.set_state(Login.api_id)
    else:
        await safe_edit_text(cq.message, "❌ Still missing. Join required channels and tap again.", reply_markup=_gate_kb())

@dp.message(StateFilter(Login.api_id))
async def step_api_id(msg: Message, state: FSMContext):
    try: api_id = int(msg.text.strip())
    except: await msg.answer("✇ API_ID must be a number. Send again."); return
    await state.update_data(api_id=api_id)
    await msg.answer("✇ Step 2 — Send your API_HASH.")
    await state.set_state(Login.api_hash)

@dp.message(StateFilter(Login.api_hash))
async def step_api_hash(msg: Message, state: FSMContext):
    await state.update_data(api_hash=msg.text.strip())
    await msg.answer("✇ Step 3 — Send your phone (e.g., +91 98xxxxxxx)")
    await state.set_state(Login.phone)

@dp.message(StateFilter(Login.phone))
async def step_phone(msg: Message, state: FSMContext):
    d = await state.get_data()
    phone = msg.text.strip().replace(" ", "")
    if not (phone.startswith("+") and any(ch.isdigit() for ch in phone)):
        await msg.answer("❌ Invalid. Example: +91 98765 43210"); return
    app = await _get_app(msg.from_user.id, d["api_id"], d["api_hash"])
    st = await msg.answer("✇ Sending code...")
    try:
        sent = await app.send_code(phone)
    except ApiIdInvalid:
        await st.edit_text("❌ API_ID/API_HASH invalid (use https://my.telegram.org)."); return
    except PhoneNumberInvalid:
        await st.edit_text("❌ Phone number invalid."); return
    except PhoneNumberFlood:
        await st.edit_text("⏳ Too many attempts. Try later."); return
    except PhoneNumberBanned:
        await st.edit_text("❌ This phone number is banned."); return
    except FloodWait as fw:
        await st.edit_text(f"⏳ Flood wait. Try after {fw.value}s."); return
    except Exception:
        await st.edit_text(f"❌ Could not send code."); return

    await state.update_data(phone=phone, phone_code_hash=sent.phone_code_hash, code="", sent_at=datetime.utcnow().isoformat())
    await st.edit_text(f"✇ Enter the code using keypad\n✇ Code: {fmt_code('')}", reply_markup=otp_kb())
    await state.set_state(Login.otp)

@dp.callback_query(StateFilter(Login.otp), F.data.startswith("d:"))
async def otp_digit(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    code = (d.get("code") or "") + cq.data.split(":")[1]
    code = code[:8]
    await state.update_data(code=code)
    try:
        await safe_edit_text(cq.message, f"✇ Enter the code using keypad\n✇ Code: {fmt_code(code)}", reply_markup=otp_kb())
    except Exception: pass
    if len(code) >= 5:
        await otp_submit(cq, state)

@dp.callback_query(StateFilter(Login.otp), F.data == "act:back")
async def otp_back(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    code = (d.get("code") or "")[:-1]
    await state.update_data(code=code)
    await safe_edit_text(cq.message, f"✇ Enter the code using keypad\n✇ Code: {fmt_code(code)}", reply_markup=otp_kb())

@dp.callback_query(StateFilter(Login.otp), F.data == "act:clear")
async def otp_clear(cq: CallbackQuery, state: FSMContext):
    await state.update_data(code="")
    await safe_edit_text(cq.message, f"✇ Enter the code using keypad\n✇ Code: {fmt_code('')}", reply_markup=otp_kb())

@dp.callback_query(StateFilter(Login.otp), F.data == "act:resend")
async def otp_resend(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    try:
        app = await _get_app(cq.from_user.id, d["api_id"], d["api_hash"])
        sent = await app.send_code(d["phone"])
        await state.update_data(phone_code_hash=sent.phone_code_hash, code="", sent_at=datetime.utcnow().isoformat())
        await safe_edit_text(cq.message, f"✇ New code sent.\n✇ Code: {fmt_code('')}", reply_markup=otp_kb())
    except Exception:
        await safe_edit_text(cq.message, "❌ Resend failed. Try again.", reply_markup=otp_kb())

@dp.callback_query(StateFilter(Login.otp), F.data == "act:submit")
async def otp_submit(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    user_id = cq.from_user.id
    if not (4 <= len(d.get("code","")) <= 8):
        return
    try: await cq.answer("Logging in…")
    except Exception: pass
    app = await _get_app(user_id, d["api_id"], d["api_hash"])
    try:
        await app.sign_in(
            phone_number=d["phone"],
            phone_code_hash=d["phone_code_hash"],
            phone_code=d["code"])
    except SessionPasswordNeeded:
        await bot.send_message(user_id, "✇ 2FA is enabled. Send your password now.")
        await state.set_state(Login.password); return
    except PhoneCodeInvalid:
        await state.update_data(code="")
        await safe_edit_text(cq.message, "❌ Wrong code. Try again.\n✇ Code: {code}".format(code=fmt_code('')), reply_markup=otp_kb()); return
    except PhoneCodeExpired:
        await state.update_data(code="")
        await safe_edit_text(cq.message, "❌ Code expired. Tap Resend.", reply_markup=otp_kb()); return
    except FloodWait as fw:
        await safe_edit_text(cq.message, f"⏳ Flood wait {fw.value}s.", reply_markup=otp_kb()); return
    except Exception:
        await safe_edit_text(cq.message, f"❌ Login failed.", reply_markup=otp_kb()); return

    # success → save slot
    session_str = await app.export_session_string()
    try: await _disconnect(user_id)
    except: pass

    try:
        slot = first_free_slot(user_id) or 1
        sessions_upsert_slot(user_id, slot, d["api_id"], d["api_hash"], session_str)
    except Exception:
        await bot.send_message(user_id, f"❌ DB error while saving session."); return

    await state.clear()
    try:
        await safe_edit_text(cq.message, "✅ Session saved.\n✇ Return to the main bot to set interval & groups.")
    except Exception:
        await bot.send_message(user_id, "✅ Session saved.\n✇ Return to the main bot to set interval & groups.")

@dp.message(StateFilter(Login.password))
async def step_password(msg: Message, state: FSMContext):
    d = await state.get_data()
    user_id = msg.from_user.id
    app = await _get_app(user_id, d["api_id"], d["api_hash"])
    try:
        await app.check_password(msg.text)
    except FloodWait as fw:
        await msg.answer(f"⏳ Flood wait {fw.value}s."); return
    except Exception:
        await msg.answer("❌ Wrong password. Send again."); return

    session_str = await app.export_session_string()
    try: await _disconnect(user_id)
    except: pass

    try:
        slot = first_free_slot(user_id) or 1
        sessions_upsert_slot(user_id, slot, d["api_id"], d["api_hash"], session_str)
    except Exception:
        await msg.answer("❌ DB error while saving session."); return

    await state.clear()
    await msg.answer("✅ Session saved.\n✇ Return to the main bot to set interval & groups.")

# -------------- run ------------------------
async def main():
    await dp.start_polling(bot)

async def login_bot_main():
    await main()

if __name__ == "__main__":
    asyncio.run(main())
