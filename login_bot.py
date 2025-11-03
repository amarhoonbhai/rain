# login_bot.py
# Aiogram v3.7+ + Pyrogram
# OTP inline keypad (under the message) + monospace live preview + Resend/Call/Alt + TTL guard + 2FA
# Saves session to user_sessions for your main bot. ✇-styled texts + robust logging.

import asyncio
import os
import pathlib
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, StateFilter
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from dotenv import load_dotenv

from pyrogram import Client
from pyrogram.errors import (
    PhoneCodeExpired, PhoneCodeInvalid, FloodWait, SessionPasswordNeeded,
    ApiIdInvalid, PhoneNumberInvalid, PhoneNumberFlood, PhoneNumberBanned
)
from pyrogram.raw.functions.auth import SendCode as RawSendCode, ResendCode as RawResendCode
from pyrogram.raw.types import CodeSettings

from core.db import init_db, get_conn


# ---------------- env & bot (Aiogram 3.7+ init) ----------------
load_dotenv()
LOGIN_BOT_TOKEN = os.getenv("LOGIN_BOT_TOKEN")

bot = Bot(
    token=LOGIN_BOT_TOKEN,
    default=DefaultBotProperties(parse_mode="HTML")  # ✅ Aiogram 3.7+ way
)
dp = Dispatcher()
init_db()


# ---------------- logging ----------------
LOG_DIR = pathlib.Path(__file__).with_name("logs")
LOG_DIR.mkdir(exist_ok=True)
_log_f = open(LOG_DIR / "login_bot.log", "a", buffering=1)

def log(*parts):
    ts = datetime.utcnow().isoformat()
    line = "[login_bot] " + ts + " " + " ".join(map(str, parts))
    print(line, flush=True)
    try:
        _log_f.write(line + "\n"); _log_f.flush()
    except Exception:
        pass


# ---------------- state & const ----------------
LOGIN_CLIENTS: dict[int, Client] = {}
LOCAL_CODE_TTL_SEC = 65  # local guard (Telegram has its own TTL)

class Login(StatesGroup):
    api_id = State()
    api_hash = State()
    phone = State()
    otp = State()
    password = State()


# ---------------- UI (inline keypad) ----------------
def otp_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1", callback_data="d:1"),
         InlineKeyboardButton(text="2", callback_data="d:2"),
         InlineKeyboardButton(text="3", callback_data="d:3")],
        [InlineKeyboardButton(text="4", callback_data="d:4"),
         InlineKeyboardButton(text="5", callback_data="d:5"),
         InlineKeyboardButton(text="6", callback_data="d:6")],
        [InlineKeyboardButton(text="7", callback_data="d:7"),
         InlineKeyboardButton(text="8", callback_data="d:8"),
         InlineKeyboardButton(text="9", callback_data="d:9")],
        [InlineKeyboardButton(text="0", callback_data="d:0")],
        [InlineKeyboardButton(text="⬅ Back", callback_data="act:back"),
         InlineKeyboardButton(text="🧹 Clear", callback_data="act:clear"),
         InlineKeyboardButton(text="✔ Submit", callback_data="act:submit")],
        [InlineKeyboardButton(text="🔁 Resend", callback_data="act:resend"),
         InlineKeyboardButton(text="📞 Call",   callback_data="act:call"),
         InlineKeyboardButton(text="🔄 Alt",    callback_data="act:alt")],
        [InlineKeyboardButton(text="ℹ️ Status", callback_data="act:status")],
    ])

def _delivery_label_from_sent(sent) -> str:
    t = getattr(sent, "type", None)
    tname = getattr(t, "_", "") if t else ""
    tn = (tname or "").lower()
    if "sms" in tn: return "SMS"
    if "app" in tn: return "Telegram app"
    if "call" in tn and "flash" not in tn: return "Phone call"
    if "flash" in tn: return "Flash call"
    return "Unknown"

def _format_code_mono(code: str) -> str:
    """Visual check of typed digits in monospace with gaps, e.g. 4 5 6."""
    if not code:
        return "<code>—</code>"
    spaced = " ".join(list(code))
    return f"<code>{spaced}</code>"

def _otp_header(delivery: str | None, timeout: int | None, sent_at_iso: str | None, code: str | None) -> str:
    base = "✇ Verification Code\n✇ Use the keypad below."
    parts = []
    if delivery: parts.append(f"✇ Delivery: {delivery}")
    if sent_at_iso:
        try:
            sent_at = datetime.fromisoformat(sent_at_iso)
            elapsed = int((datetime.utcnow() - sent_at).total_seconds())
            parts.append(f"✇ Elapsed: {elapsed}s")
        except Exception:
            pass
    if timeout: parts.append(f"✇ TTL≈{timeout}s")
    if code is not None:
        parts.append(f"✇ Code: {_format_code_mono(code)}")
    return base + ("\n" + "\n".join(parts) if parts else "")

async def _render_otp(chat_id: int, message_id: int, d: dict):
    try:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=_otp_header(d.get("delivery"), d.get("timeout"), d.get("code_sent_at"), d.get("code", "")),
            reply_markup=otp_inline_kb(),
        )
    except Exception:
        pass


# ---------------- send-code helpers ----------------
async def send_code_app(user_id: int, api_id: int, api_hash: str, phone: str):
    """Standard high-level send_code(). Returns pch, delivery, timeout, raw_sent."""
    app = Client(name=f"login-{user_id}", api_id=api_id, api_hash=api_hash, in_memory=True)
    await app.connect()
    try:
        log("send_code_app: requesting", phone)
        sent = await app.send_code(phone)
        delivery = _delivery_label_from_sent(sent)
        timeout = getattr(sent, "timeout", None)
        log("send_code_app: ok", {"delivery": delivery, "timeout": timeout})
        return sent.phone_code_hash, delivery, timeout, sent
    finally:
        await app.disconnect()

async def send_code_call(user_id: int, api_id: int, api_hash: str, phone: str):
    """Request call/missed-call via raw API; fallback to send_code()."""
    app = Client(name=f"login-{user_id}", api_id=api_id, api_hash=api_hash, in_memory=True)
    await app.connect()
    try:
        try:
            log("send_code_call: raw allow_missed_call", phone)
            sent = await app.invoke(
                RawSendCode(
                    phone_number=phone,
                    api_id=api_id,
                    api_hash=api_hash,
                    settings=CodeSettings(
                        allow_flashcall=False,
                        allow_missed_call=True,
                        current_number=False,
                        allow_app_hash=True,
                    ),
                )
            )
            delivery = "Phone call"
            timeout = getattr(sent, "timeout", None)
            log("send_code_call: ok (raw)", {"timeout": timeout})
            return sent.phone_code_hash, delivery, timeout, sent
        except Exception as e:
            log("send_code_call: raw failed, fallback", repr(e))
            sent = await app.send_code(phone)
            delivery = _delivery_label_from_sent(sent)
            timeout = getattr(sent, "timeout", None)
            log("send_code_call: ok (fallback)", {"delivery": delivery, "timeout": timeout})
            return sent.phone_code_hash, delivery, timeout, sent
    finally:
        await app.disconnect()

async def resend_code_alt(user_id: int, api_id: int, api_hash: str, phone: str, prev_hash: str):
    """Raw auth.resendCode — sometimes switches delivery channel."""
    app = Client(name=f"login-{user_id}", api_id=api_id, api_hash=api_hash, in_memory=True)
    await app.connect()
    try:
        log("resend_code_alt: raw resend", phone)
        sent = await app.invoke(RawResendCode(phone_number=phone, phone_code_hash=prev_hash))
        delivery = _delivery_label_from_sent(sent)
        timeout = getattr(sent, "timeout", None)
        log("resend_code_alt: ok", {"delivery": delivery, "timeout": timeout})
        return sent.phone_code_hash, delivery, timeout, sent
    finally:
        await app.disconnect()


# ---------------- flow ----------------
WELCOME_TXT = (
    "✇ Welcome to Spinify Login\n\n"
    "✇ How to get API_ID & API_HASH:\n"
    "  • Open https://my.telegram.org\n"
    "  • Log in with your phone number\n"
    "  • Go to “API Development Tools” → Create new app\n"
    "  • Copy your API_ID and API_HASH\n\n"
    "✇ Steps\n"
    "  1) Send your API_ID\n"
    "  2) Send your API_HASH\n"
    "  3) Send your phone number in +countrycode format (e.g., +9198XXXXXXX)\n"
    "  4) Enter the code using the keypad below the message\n\n"
    "✇ If code doesn’t arrive: use 🔁 Resend, 📞 Call, or 🔄 Alt.\n"
)

@dp.message(Command("start"))
async def start(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer(WELCOME_TXT)
    await msg.answer("✇ Step 1 — Send your API_ID.")
    await state.set_state(Login.api_id)

@dp.message(StateFilter(Login.api_id))
async def step_api_id(msg: Message, state: FSMContext):
    try:
        api_id = int(msg.text.strip())
    except ValueError:
        await msg.answer("✇ API_ID must be a number. Send again.")
        return
    await state.update_data(api_id=api_id)
    await msg.answer("✇ Step 2 — Paste your API_HASH.")
    await state.set_state(Login.api_hash)

@dp.message(StateFilter(Login.api_hash))
async def step_api_hash(msg: Message, state: FSMContext):
    api_hash = msg.text.strip()
    await state.update_data(api_hash=api_hash)
    await msg.answer("✇ Step 3 — Send your phone in +countrycode format.")
    await state.set_state(Login.phone)

@dp.message(StateFilter(Login.phone))
async def step_phone(msg: Message, state: FSMContext):
    d = await state.get_data()
    api_id, api_hash = d["api_id"], d["api_hash"]
    phone = msg.text.strip()

    if not (phone.startswith("+") and any(ch.isdigit() for ch in phone)):
        await msg.answer("✇ Phone must be like +91XXXXXXXXXX. Send again.")
        return

    status = await msg.answer("✇ Requesting code…")
    try:
        pch, delivery, timeout, _ = await send_code_app(msg.from_user.id, api_id, api_hash, phone)
    except ApiIdInvalid:
        await status.edit_text("❌ API_ID/API_HASH invalid. Use my.telegram.org → API Development Tools."); log("ERROR ApiIdInvalid"); return
    except PhoneNumberInvalid:
        await status.edit_text("❌ Phone number invalid. Use +countrycode (e.g., +9198XXXXXXX)."); log("ERROR PhoneNumberInvalid"); return
    except PhoneNumberFlood:
        await status.edit_text("⏳ Too many attempts. Please wait and try again."); log("ERROR PhoneNumberFlood"); return
    except PhoneNumberBanned:
        await status.edit_text("❌ This phone number is banned by Telegram."); log("ERROR PhoneNumberBanned"); return
    except FloodWait as fw:
        await status.edit_text(f"⏳ Flood wait. Try after {fw.value}s."); log("ERROR FloodWait", fw.value); return
    except Exception as e:
        await status.edit_text(f"❌ Could not send code: {e}"); log("ERROR send_code_app", repr(e)); return

    now_iso = datetime.utcnow().isoformat()
    await status.edit_text(_otp_header(delivery, timeout, now_iso, ""), reply_markup=otp_inline_kb())

    await state.update_data(
        phone=phone, phone_code_hash=pch, code="",
        code_sent_at=now_iso, otp_msg_id=status.message_id,
        delivery=delivery, timeout=timeout
    )
    await state.set_state(Login.otp)


# --------- OTP keypad handlers (live code preview) ---------
@dp.callback_query(StateFilter(Login.otp), F.data.startswith("d:"))
async def otp_digit(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    code = d.get("code", "")
    digit = cq.data.split(":", 1)[1]
    if len(code) < 8:
        code += digit
        await state.update_data(code=code)
        await _render_otp(cq.message.chat.id, cq.message.message_id, {**d, "code": code})
    await cq.answer()

@dp.callback_query(StateFilter(Login.otp), F.data == "act:back")
async def otp_back(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    code = d.get("code", "")
    if code:
        code = code[:-1]
        await state.update_data(code=code)
        await _render_otp(cq.message.chat.id, cq.message.message_id, {**d, "code": code})
    await cq.answer("Back")

@dp.callback_query(StateFilter(Login.otp), F.data == "act:clear")
async def otp_clear(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    await state.update_data(code="")
    await _render_otp(cq.message.chat.id, cq.message.message_id, {**d, "code": ""})
    await cq.answer("Cleared")

@dp.callback_query(StateFilter(Login.otp), F.data == "act:status")
async def otp_status(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    await _render_otp(cq.message.chat.id, cq.message.message_id, d)
    await cq.answer("Updated")

@dp.callback_query(StateFilter(Login.otp), F.data == "act:resend")
async def otp_resend(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    try:
        new_hash, delivery, timeout, _ = await send_code_app(cq.from_user.id, d["api_id"], d["api_hash"], d["phone"])
    except Exception as e:
        await bot.send_message(cq.message.chat.id, f"❌ Resend failed: {e}"); log("ERROR resend app", repr(e)); return
    now_iso = datetime.utcnow().isoformat()
    d.update(phone_code_hash=new_hash, code="", code_sent_at=now_iso, delivery=delivery, timeout=timeout)
    await state.update_data(**d)
    await _render_otp(cq.message.chat.id, cq.message.message_id, d)
    await cq.answer("New code sent")

@dp.callback_query(StateFilter(Login.otp), F.data == "act:call")
async def otp_call(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    try:
        new_hash, delivery, timeout, _ = await send_code_call(cq.from_user.id, d["api_id"], d["api_hash"], d["phone"])
    except Exception as e:
        await bot.send_message(cq.message.chat.id, f"❌ Call request failed: {e}"); log("ERROR call resend", repr(e)); return
    now_iso = datetime.utcnow().isoformat()
    d.update(phone_code_hash=new_hash, code="", code_sent_at=now_iso, delivery=delivery, timeout=timeout)
    await state.update_data(**d)
    await _render_otp(cq.message.chat.id, cq.message.message_id, d)
    await cq.answer("Call requested")

@dp.callback_query(StateFilter(Login.otp), F.data == "act:alt")
async def otp_alt(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    try:
        new_hash, delivery, timeout, _ = await resend_code_alt(
            cq.from_user.id, d["api_id"], d["api_hash"], d["phone"], d["phone_code_hash"]
        )
    except Exception as e:
        await bot.send_message(cq.message.chat.id, f"❌ Alt method failed: {e}"); log("ERROR alt resend", repr(e)); return
    now_iso = datetime.utcnow().isoformat()
    d.update(phone_code_hash=new_hash, code="", code_sent_at=now_iso, delivery=delivery, timeout=timeout)
    await state.update_data(**d)
    await _render_otp(cq.message.chat.id, cq.message.message_id, d)
    await cq.answer("Alternate method requested")

@dp.callback_query(StateFilter(Login.otp), F.data == "act:submit")
async def otp_submit(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    user_id = cq.from_user.id
    api_id, api_hash, phone = d["api_id"], d["api_hash"], d["phone"]
    phone_code_hash = d["phone_code_hash"]
    code = d.get("code", "")
    sent_at = datetime.fromisoformat(d["code_sent_at"])

    if datetime.utcnow() - sent_at > timedelta(seconds=LOCAL_CODE_TTL_SEC):
        try:
            new_hash, delivery, timeout, _ = await send_code_app(user_id, api_id, api_hash, phone)
        except Exception as e:
            await bot.send_message(cq.message.chat.id, f"❌ Resend failed: {e}"); log("ERROR submit-resend", repr(e)); return
        now_iso = datetime.utcnow().isoformat()
        d.update(phone_code_hash=new_hash, code="", code_sent_at=now_iso, delivery=delivery, timeout=timeout)
        await state.update_data(**d)
        await _render_otp(cq.message.chat.id, cq.message.message_id, d)
        await cq.answer("Code expired. New code sent.")
        return

    if not (4 <= len(code) <= 8):
        await cq.answer("Enter 4–8 digits"); return

    app = Client(name=f"login-{user_id}", api_id=api_id, api_hash=api_hash, in_memory=True)
    await app.connect()
    try:
        await app.sign_in(phone_number=phone, phone_code_hash=phone_code_hash, phone_code=code)
    except SessionPasswordNeeded:
        LOGIN_CLIENTS[user_id] = app
        await state.set_state(Login.password)
        await cq.answer()
        await bot.send_message(cq.message.chat.id, "✇ This account has 2-step verification.\n✇ Send your password now:")
        return
    except PhoneCodeExpired:
        new_sent = await app.send_code(phone)
        await app.disconnect()
        now_iso = datetime.utcnow().isoformat()
        d.update(
            phone_code_hash=new_sent.phone_code_hash, code="",
            code_sent_at=now_iso, delivery=_delivery_label_from_sent(new_sent),
            timeout=getattr(new_sent, "timeout", None)
        )
        await state.update_data(**d)
        await _render_otp(cq.message.chat.id, cq.message.message_id, d)
        await cq.answer("Code expired. New code sent."); return
    except PhoneCodeInvalid:
        await app.disconnect()
        await state.update_data(code="")
        await _render_otp(cq.message.chat.id, cq.message.message_id, {**d, "code": ""})
        await cq.answer("Wrong code"); return
    except PhoneNumberFlood:
        await app.disconnect()
        await cq.answer("Too many attempts", show_alert=True); return
    except FloodWait as fw:
        await app.disconnect()
        await cq.answer(f"Wait {fw.value}s", show_alert=True); return
    except Exception as e:
        await app.disconnect()
        await state.clear()
        await cq.answer("Login failed")
        await bot.send_message(cq.message.chat.id, f"❌ Login failed: {e}\n/start again")
        log("ERROR sign_in unknown", repr(e)); return

    # success (no 2FA)
    session_str = await app.export_session_string()
    try:
        await app.update_profile(bio="#1 Free Ads Bot — Join @PhiloBots")
        me = await app.get_me()
        base = me.first_name.split(" — ")[0]
        await app.update_profile(first_name=base + " — via @SpinifyAdsBot")
    except Exception:
        pass
    await app.disconnect()

    # Save session for main bot to use for forwarding
    conn = get_conn()
    conn.execute(
        "INSERT INTO user_sessions(user_id, api_id, api_hash, session_string) "
        "VALUES (?, ?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET api_id=excluded.api_id, api_hash=excluded.api_hash, session_string=excluded.session_string",
        (user_id, api_id, api_hash, session_str)
    )
    conn.commit(); conn.close()

    await state.clear()
    try:
        await bot.edit_message_text(
            chat_id=cq.message.chat.id,
            message_id=cq.message.message_id,
            text="✅ Session saved.\n✇ Return to the main bot to set interval, message, and groups.",
        )
    except Exception:
        await bot.send_message(cq.message.chat.id, "✅ Session saved.\n✇ Return to the main bot to set interval, message, and groups.")


# ---------------- 2FA password ----------------
@dp.message(StateFilter(Login.password))
async def step_password(msg: Message, state: FSMContext):
    d = await state.get_data()
    user_id = msg.from_user.id
    api_id, api_hash = d["api_id"], d["api_hash"]
    password = msg.text

    app = LOGIN_CLIENTS.get(user_id)
    fresh = False
    if app is None:
        app = Client(name=f"login-{user_id}", api_id=api_id, api_hash=api_hash, in_memory=True)
        await app.connect(); fresh = True

    try:
        await app.check_password(password)
    except FloodWait as fw:
        await app.disconnect(); LOGIN_CLIENTS.pop(user_id, None)
        await state.set_state(Login.otp)
        await msg.answer(f"⏳ Too many attempts. Try again after {fw.value}s."); return
    except Exception as e:
        if fresh: await app.disconnect()
