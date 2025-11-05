# login_bot.py — Compact UI · Aiogram v3.7+ · Pyrogram 2.x
# Log TG account → save reusable session (up to 3). Keypad + typed OTP, 2FA, auto-resend.

import asyncio
import logging
import os
import pathlib
import re
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv

from pyrogram import Client
from pyrogram.errors import (
    PhoneCodeExpired, PhoneCodeInvalid, FloodWait, SessionPasswordNeeded,
    ApiIdInvalid, PhoneNumberInvalid, PhoneNumberFlood, PhoneNumberBanned
)
from pyrogram.raw.functions.auth import SendCode as RawSendCode, ResendCode as RawResendCode
from pyrogram.raw.types import CodeSettings

from core.db import (
    init_db, get_conn,
    upsert_session_slot, list_user_sessions, delete_session_slot, first_free_slot,
)

__all__ = ["main", "login_bot_main"]

# ---------- config ----------
AUTO_SUBMIT_LEN = 5
LOCAL_CODE_TTL_SEC = 65
AUTO_RESEND_THRESHOLD = 10
TICK_INTERVAL = 2
DEBUG_ERRORS = True

PHONE_PROMPT = "Send phone in +countrycode format (e.g. +9198XXXXXXX)."
CONFIRM_PROMPT = "We sent a login request. Approve it in Telegram."

BANNER = "🟦 <b>Spinify Login</b> — <code>3 slots • keypad • 2FA</code>"

# ---------- env & bot ----------
load_dotenv()
LOGIN_BOT_TOKEN = os.getenv("LOGIN_BOT_TOKEN", "").strip()
if not LOGIN_BOT_TOKEN or ":" not in LOGIN_BOT_TOKEN:
    raise RuntimeError("LOGIN_BOT_TOKEN is missing/malformed in .env")

bot = Bot(LOGIN_BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
init_db()

# ---------- logging ----------
LOG_DIR = pathlib.Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
_log_f = open(LOG_DIR / "login_bot.log", "a", buffering=1)

logging.basicConfig(level=logging.INFO)
pyro_logger = logging.getLogger("pyrogram")
pyro_logger.setLevel(logging.INFO)
fh = logging.FileHandler(LOG_DIR / "pyrogram-debug.log")
fh.setLevel(logging.DEBUG)
pyro_logger.addHandler(fh)

def log(*parts):
    ts = datetime.utcnow().isoformat()
    line = "[login_bot] " + ts + " " + " ".join(map(str, parts))
    print(line, flush=True)
    try:
        _log_f.write(line + "\n"); _log_f.flush()
    except Exception:
        pass

# ---------- state ----------
LOGIN_APPS: dict[int, Client] = {}
OTP_TICK_TASKS: dict[tuple[int, int], asyncio.Task] = {}

class Login(StatesGroup):
    api_id = State()
    api_hash = State()
    phone = State()
    otp = State()
    password = State()

# ---------- UI ----------
def otp_inline_kb() -> InlineKeyboardMarkup:
    # compact layout: fewer rows, same actions
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
        [InlineKeyboardButton(text="⟵", callback_data="act:back"),
         InlineKeyboardButton(text="✖", callback_data="act:clear"),
         InlineKeyboardButton(text="✔", callback_data="act:submit")],
        [InlineKeyboardButton(text="🔁", callback_data="act:resend"),
         InlineKeyboardButton(text="📞", callback_data="act:call"),
         InlineKeyboardButton(text="🔄", callback_data="act:alt"),
         InlineKeyboardButton(text="ℹ", callback_data="act:status"),
         InlineKeyboardButton(text="🛑", callback_data="act:cancel")],
    ])

def manage_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 Slot 1", callback_data="mg:rm:1"),
         InlineKeyboardButton(text="🗑 Slot 2", callback_data="mg:rm:2"),
         InlineKeyboardButton(text="🗑 Slot 3", callback_data="mg:rm:3")],
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="mg:refresh")]
    ])

def _delivery_label_from_sent(sent) -> str:
    t = getattr(sent, "type", None)
    tn = (getattr(t, "_", "") if t else "").lower()
    if "sms" in tn: return "SMS"
    if "app" in tn: return "Telegram app"
    if "call" in tn and "flash" not in tn: return "Phone call"
    if "flash" in tn: return "Flash call"
    return "Unknown"

def _format_code_mono(code: str) -> str:
    return "<code>—</code>" if not code else f"<code>{' '.join(list(code))}</code>"

def _ttl_left(sent_at_iso: str | None, timeout: int | None) -> int | None:
    if not sent_at_iso or not timeout: return None
    try:
        sent_at = datetime.fromisoformat(sent_at_iso)
        elapsed = int((datetime.utcnow() - sent_at).total_seconds())
        return max(0, timeout - elapsed)
    except Exception:
        return None

def _otp_header(delivery: str | None, timeout: int | None, sent_at_iso: str | None, code: str | None) -> str:
    left = _ttl_left(sent_at_iso, timeout)
    parts = [BANNER, "🔐 <b>Enter OTP</b> (type 4–8 digits or use keypad)"]
    if delivery: parts.append(f"• {delivery}")
    if left is not None: parts.append(f"• TTL: {left}s")
    if code is not None: parts.append(f"• {_format_code_mono(code)}")
    return "\n".join(parts)

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

# ---------- send-code helpers ----------
async def _get_login_app(user_id: int, api_id: int, api_hash: str) -> Client:
    app = LOGIN_APPS.get(user_id)
    if app is None:
        app = Client(name=f"login-{user_id}", api_id=api_id, api_hash=api_hash, in_memory=True)
        await app.connect()
        LOGIN_APPS[user_id] = app
    return app

async def send_code_app(user_id: int, api_id: int, api_hash: str, phone: str):
    app = await _get_login_app(user_id, api_id, api_hash)
    phone = phone.replace(" ", "")
    sent = await app.send_code(phone)
    return sent.phone_code_hash, _delivery_label_from_sent(sent), getattr(sent, "timeout", None), sent

async def send_code_call(user_id: int, api_id: int, api_hash: str, phone: str):
    app = await _get_login_app(user_id, api_id, api_hash)
    phone = phone.replace(" ", "")
    try:
        sent = await app.invoke(
            RawSendCode(
                phone_number=phone,
                api_id=api_id,
                api_hash=api_hash,
                settings=CodeSettings(
                    allow_flashcall=False, allow_missed_call=True,
                    current_number=False, allow_app_hash=True,
                ),
            )
        )
        return sent.phone_code_hash, "Phone call", getattr(sent, "timeout", None), sent
    except Exception:
        sent = await app.send_code(phone)
        return sent.phone_code_hash, _delivery_label_from_sent(sent), getattr(sent, "timeout", None), sent

async def resend_code_alt(user_id: int, api_id: int, api_hash: str, phone: str, prev_hash: str):
    app = await _get_login_app(user_id, api_id, api_hash)
    phone = phone.replace(" ", "")
    sent = await app.invoke(RawResendCode(phone_number=phone, phone_code_hash=prev_hash))
    return sent.phone_code_hash, _delivery_label_from_sent(sent), getattr(sent, "timeout", None), sent

# ---------- OTP tick ----------
async def _otp_tick(chat_id: int, msg_id: int, state: FSMContext):
    key = (chat_id, msg_id)
    while True:
        try:
            d = await state.get_data()
            if "phone_code_hash" not in d: break
            left = _ttl_left(d.get("code_sent_at"), d.get("timeout"))
            if left is not None and left <= AUTO_RESEND_THRESHOLD and not d.get("auto_resent", False):
                try:
                    new_hash, delivery, timeout, _ = await send_code_app(chat_id, d["api_id"], d["api_hash"], d["phone"])
                    now_iso = datetime.utcnow().isoformat()
                    d.update(phone_code_hash=new_hash, code="", code_sent_at=now_iso,
                             delivery=delivery, timeout=timeout, auto_resent=True)
                    await state.update_data(**d)
                    await _render_otp(chat_id, msg_id, d)
                except Exception as e:
                    log("auto-resend", repr(e))
            await _render_otp(chat_id, msg_id, d)
        except Exception as e:
            log("tick", repr(e)); break
        await asyncio.sleep(TICK_INTERVAL)
    OTP_TICK_TASKS.pop(key, None)

def _start_tick(chat_id: int, msg_id: int, state: FSMContext):
    key = (chat_id, msg_id)
    t = OTP_TICK_TASKS.get(key)
    if t and not t.done(): t.cancel()
    OTP_TICK_TASKS[key] = asyncio.create_task(_otp_tick(chat_id, msg_id, state))

def _stop_tick(chat_id: int, msg_id: int):
    key = (chat_id, msg_id)
    t = OTP_TICK_TASKS.pop(key, None)
    if t and not t.done(): t.cancel()

# ---------- management ----------
def _sessions_overview_text(user_id: int) -> str:
    rows = list_user_sessions(user_id)
    used = {r["slot"]: r for r in rows}
    lines = [BANNER, "<b>Accounts</b> (max 3)"]
    for s in (1, 2, 3):
        lines.append(("🟢" if s in used else "⚪") + f" Slot {s}")
    return "\n".join(lines)

@dp.message(Command("status"))
async def cmd_status(msg: Message):
    await msg.answer(_sessions_overview_text(msg.from_user.id), reply_markup=manage_kb())

@dp.callback_query(F.data == "mg:refresh")
async def mg_refresh(cq: CallbackQuery):
    await cq.message.edit_text(_sessions_overview_text(cq.from_user.id), reply_markup=manage_kb())
    await cq.answer("Updated")

@dp.callback_query(F.data.startswith("mg:rm:"))
async def mg_remove(cq: CallbackQuery):
    try: slot = int(cq.data.split(":")[-1])
    except: slot = 0
    if slot not in (1,2,3): return await cq.answer("Invalid")
    n = delete_session_slot(cq.from_user.id, slot)
    await cq.answer("Removed" if n else "Empty")
    await mg_refresh(cq)

# ---------- flow ----------
WELCOME_TXT = (
    BANNER + "\n"
    "1) API_ID  2) API_HASH  3) Phone  4) OTP\n"
    "Type OTP (4–8) or use keypad. Use /status to manage slots."
)

@dp.message(Command("start"))
async def start(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer(WELCOME_TXT)
    await msg.answer("Step 1 — send API_ID")
    await state.set_state(Login.api_id)

@dp.message(StateFilter(Login.api_id))
async def step_api_id(msg: Message, state: FSMContext):
    try: api_id = int(msg.text.strip())
    except ValueError: return await msg.answer("API_ID must be a number.")
    await state.update_data(api_id=api_id)
    await msg.answer("Step 2 — send API_HASH")
    await state.set_state(Login.api_hash)

@dp.message(StateFilter(Login.api_hash))
async def step_api_hash(msg: Message, state: FSMContext):
    await state.update_data(api_hash=msg.text.strip())
    await msg.answer(f"Step 3 — {PHONE_PROMPT}")
    await state.set_state(Login.phone)

@dp.message(StateFilter(Login.phone))
async def step_phone(msg: Message, state: FSMContext):
    d = await state.get_data()
    api_id, api_hash = d["api_id"], d["api_hash"]
    phone = msg.text.strip()
    if not (phone.startswith("+") and any(ch.isdigit() for ch in phone)):
        return await msg.answer("Invalid. Example: +91 98765 43210")
    status = await msg.answer("Sending code…")
    try:
        pch, delivery, timeout, _ = await send_code_app(msg.from_user.id, api_id, api_hash, phone)
    except ApiIdInvalid:
        return await status.edit_text("API creds invalid (my.telegram.org → API Tools).")
    except PhoneNumberInvalid:
        return await status.edit_text("Phone invalid. Use +countrycode.")
    except PhoneNumberFlood:
        return await status.edit_text("Too many attempts. Try later.")
    except PhoneNumberBanned:
        return await status.edit_text("This number is banned.")
    except FloodWait as fw:
        return await status.edit_text(f"Flood wait {fw.value}s.")
    except Exception as e:
        return await status.edit_text(f"Send failed. {e if DEBUG_ERRORS else ''}".strip())

    now_iso = datetime.utcnow().isoformat()
    await status.edit_text(_otp_header(delivery, timeout, now_iso, ""), reply_markup=otp_inline_kb())
    await state.update_data(
        phone=phone, phone_code_hash=pch, code="",
        code_sent_at=now_iso, otp_msg_id=status.message_id,
        delivery=delivery, timeout=timeout, auto_resent=False
    )
    _start_tick(msg.chat.id, status.message_id, state)
    await state.set_state(Login.otp)

    if (delivery or "").lower().startswith("telegram"):
        await msg.answer(CONFIRM_PROMPT)

# ---------- success/save ----------
async def _complete_login_and_save(user_id: int, api_id: int, api_hash: str, chat_id: int, msg_id: int, state: FSMContext, app: Client):
    session_str = await app.export_session_string()
    try:
        await app.update_profile(bio="#1 Free Ads Bot — Join @PhiloBots")
    except Exception: pass
    try:
        await app.disconnect()
    except Exception: pass
    LOGIN_APPS.pop(user_id, None)

    slot = upsert_session_slot(user_id, api_id, api_hash, session_str, slot=None)
    _stop_tick(chat_id, msg_id); await state.clear()
    txt = (f"✅ Saved to Slot {slot}." if slot else
           "✅ Session ready, but slots full. Remove one via /status, then login again.")
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=txt)
    except Exception:
        await bot.send_message(chat_id, txt)

# ---------- typed OTP (compact auto-submit) ----------
@dp.message(StateFilter(Login.otp))
async def typed_otp(m: Message, state: FSMContext):
    v = (m.text or "").strip()
    if not re.fullmatch(r"\d{4,8}", v):
        return
    d = await state.get_data()
    user_id = m.from_user.id
    api_id, api_hash, phone = d["api_id"], d["api_hash"], d["phone"].replace(" ", "")
    phone_code_hash = d["phone_code_hash"]
    chat_id = m.chat.id
    msg_id = d.get("otp_msg_id")

    left = _ttl_left(d.get("code_sent_at"), d.get("timeout"))
    if (left is not None and left <= 0) or (d.get("code_sent_at") and datetime.utcnow() - datetime.fromisoformat(d["code_sent_at"]) > timedelta(seconds=LOCAL_CODE_TTL_SEC)):
        try:
            app = await _get_login_app(user_id, api_id, api_hash)
            new_sent = await app.send_code(phone)
            now_iso = datetime.utcnow().isoformat()
            d.update(phone_code_hash=new_sent.phone_code_hash, code="",
                     code_sent_at=now_iso, delivery=_delivery_label_from_sent(new_sent),
                     timeout=getattr(new_sent, "timeout", None), auto_resent=False)
            await state.update_data(**d)
            if msg_id: await _render_otp(chat_id, msg_id, d)
            return await m.answer("Expired → new code sent.")
        except Exception as e:
            return await m.answer(f"Resend failed. {e if DEBUG_ERRORS else ''}".strip())

    app = await _get_login_app(user_id, api_id, api_hash)
    try:
        await app.sign_in(phone_number=phone, phone_code_hash=phone_code_hash, phone_code=v)
    except SessionPasswordNeeded:
        await state.set_state(Login.password); return await m.answer("2FA enabled. Send password.")
    except PhoneCodeInvalid:
        if msg_id: await _render_otp(chat_id, msg_id, {**d, "code": ""}); return await m.answer("Wrong code.")
    except PhoneCodeExpired:
        try:
            new_sent = await app.send_code(phone)
            now_iso = datetime.utcnow().isoformat()
            d.update(phone_code_hash=new_sent.phone_code_hash, code="",
                     code_sent_at=now_iso, delivery=_delivery_label_from_sent(new_sent),
                     timeout=getattr(new_sent, "timeout", None), auto_resent=False)
            await state.update_data(**d)
            if msg_id: await _render_otp(chat_id, msg_id, d)
            return await m.answer("Expired → new code sent.")
        except Exception as e:
            return await m.answer(f"Refresh failed. {e if DEBUG_ERRORS else ''}".strip())
    except PhoneNumberFlood:
        return await m.answer("Too many attempts. Later.")
    except FloodWait as fw:
        return await m.answer(f"Wait {fw.value}s.")
    except Exception:
        return await m.answer("Login failed. /start again.")

    await _complete_login_and_save(user_id, api_id, api_hash, chat_id, msg_id, state, app)

# ---------- keypad handlers (compact) ----------
@dp.callback_query(StateFilter(Login.otp), F.data.startswith("d:"))
async def otp_digit(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    code = d.get("code", "")
    digit = cq.data.split(":", 1)[1]
    if len(code) < 8:
        code += digit
        await state.update_data(code=code)
        await _render_otp(cq.message.chat.id, cq.message.message_id, {**d, "code": code})
        if len(code) >= AUTO_SUBMIT_LEN:
            await _try_login(cq, state)
    await cq.answer()

@dp.callback_query(StateFilter(Login.otp), F.data == "act:back")
async def otp_back(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    code = d.get("code", "")
    if code:
        code = code[:-1]
        await state.update_data(code=code)
        await _render_otp(cq.message.chat.id, cq.message.message_id, {**d, "code": code})
    await cq.answer()

@dp.callback_query(StateFilter(Login.otp), F.data == "act:clear")
async def otp_clear(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    await state.update_data(code="")
    await _render_otp(cq.message.chat.id, cq.message.message_id, {**d, "code": ""})
    await cq.answer()

@dp.callback_query(StateFilter(Login.otp), F.data == "act:status")
async def otp_status(cq: CallbackQuery, state: FSMContext):
    await cq.message.answer(_sessions_overview_text(cq.from_user.id), reply_markup=manage_kb())
    await cq.answer()

@dp.callback_query(StateFilter(Login.otp), F.data == "act:cancel")
async def otp_cancel(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    msg_id = d.get("otp_msg_id")
    if msg_id: _stop_tick(cq.message.chat.id, msg_id)
    await state.clear()
    await cq.message.answer("Cancelled. /start")
    await cq.answer()

@dp.callback_query(StateFilter(Login.otp), F.data == "act:resend")
async def otp_resend(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    try:
        new_hash, delivery, timeout, _ = await send_code_app(cq.from_user.id, d["api_id"], d["api_hash"], d["phone"])
    except Exception as e:
        return await bot.send_message(cq.message.chat.id, f"Resend failed. {e if DEBUG_ERRORS else ''}".strip())
    now_iso = datetime.utcnow().isoformat()
    d.update(phone_code_hash=new_hash, code="", code_sent_at=now_iso, delivery=delivery, timeout=timeout, auto_resent=False)
    await state.update_data(**d)
    await _render_otp(cq.message.chat.id, cq.message.message_id, d)
    _start_tick(cq.message.chat.id, cq.message.message_id, state)
    await cq.answer()

@dp.callback_query(StateFilter(Login.otp), F.data == "act:call")
async def otp_call(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    try:
        new_hash, delivery, timeout, _ = await send_code_call(cq.from_user.id, d["api_id"], d["api_hash"], d["phone"])
    except Exception as e:
        return await bot.send_message(cq.message.chat.id, f"Call failed. {e if DEBUG_ERRORS else ''}".strip())
    now_iso = datetime.utcnow().isoformat()
    d.update(phone_code_hash=new_hash, code="", code_sent_at=now_iso, delivery=delivery, timeout=timeout, auto_resent=False)
    await state.update_data(**d)
    await _render_otp(cq.message.chat.id, cq.message.message_id, d)
    _start_tick(cq.message.chat.id, cq.message.message_id, state)
    await cq.answer()

@dp.callback_query(StateFilter(Login.otp), F.data == "act:alt")
async def otp_alt(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    try:
        new_hash, delivery, timeout, _ = await resend_code_alt(
            cq.from_user.id, d["api_id"], d["api_hash"], d["phone"], d["phone_code_hash"]
        )
    except Exception as e:
        return await bot.send_message(cq.message.chat.id, f"Alt failed. {e if DEBUG_ERRORS else ''}".strip())
    now_iso = datetime.utcnow().isoformat()
    d.update(phone_code_hash=new_hash, code="", code_sent_at=now_iso, delivery=delivery, timeout=timeout, auto_resent=False)
    await state.update_data(**d)
    await _render_otp(cq.message.chat.id, cq.message.message_id, d)
    _start_tick(cq.message.chat.id, cq.message.message_id, state)
    await cq.answer()

# ---------- keypad submit helper ----------
async def _try_login(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    user_id = cq.from_user.id
    api_id, api_hash, phone = d["api_id"], d["api_hash"], d["phone"].replace(" ", "")
    phone_code_hash = d["phone_code_hash"]
    code = str(d.get("code", "")).strip()
    chat_id = cq.message.chat.id
    msg_id = cq.message.message_id

    left = _ttl_left(d.get("code_sent_at"), d.get("timeout"))
    if (left is not None and left <= 0) or (d.get("code_sent_at") and datetime.utcnow() - datetime.fromisoformat(d["code_sent_at"]) > timedelta(seconds=LOCAL_CODE_TTL_SEC)):
        try:
            app = await _get_login_app(user_id, api_id, api_hash)
            new_sent = await app.send_code(phone)
            now_iso = datetime.utcnow().isoformat()
            d.update(phone_code_hash=new_sent.phone_code_hash, code="",
                     code_sent_at=now_iso, delivery=_delivery_label_from_sent(new_sent),
                     timeout=getattr(new_sent, "timeout", None), auto_resent=False)
            await state.update_data(**d)
            await _render_otp(chat_id, msg_id, d)
            return await cq.answer("New code sent.")
        except Exception as e:
            await bot.send_message(chat_id, f"Resend failed. {e if DEBUG_ERRORS else ''}".strip()); return

    if not (4 <= len(code) <= 8):
        return await cq.answer("Enter 4–8 digits")

    app = await _get_login_app(user_id, api_id, api_hash)
    try:
        await app.sign_in(phone_number=phone, phone_code_hash=phone_code_hash, phone_code=code)
    except SessionPasswordNeeded:
        await state.set_state(Login.password); await cq.answer(); return await bot.send_message(chat_id, "2FA enabled. Send password.")
    except PhoneCodeInvalid:
        await state.update_data(code=""); await _render_otp(chat_id, msg_id, {**d, "code": ""}); return await cq.answer("Wrong code")
    except PhoneCodeExpired:
        try:
            new_sent = await app.send_code(phone)
            now_iso = datetime.utcnow().isoformat()
            d.update(phone_code_hash=new_sent.phone_code_hash, code="",
                     code_sent_at=now_iso, delivery=_delivery_label_from_sent(new_sent),
                     timeout=getattr(new_sent, "timeout", None), auto_resent=False)
            await state.update_data(**d); await _render_otp(chat_id, msg_id, d)
            return await cq.answer("New code sent.")
        except Exception as e:
            await bot.send_message(chat_id, f"Refresh failed. {e if DEBUG_ERRORS else ''}".strip()); return
    except PhoneNumberFlood:
        return await cq.answer("Too many attempts", show_alert=True)
    except FloodWait as fw:
        return await cq.answer(f"Wait {fw.value}s", show_alert=True)
    except Exception as e:
        await cq.answer("Login failed"); await bot.send_message(chat_id, f"Login failed. {e if DEBUG_ERRORS else ''}\n/start".strip()); return

    await _complete_login_and_save(user_id, api_id, api_hash, chat_id, msg_id, state, app)

# ---------- 2FA ----------
@dp.message(StateFilter(Login.password))
async def step_password(msg: Message, state: FSMContext):
    d = await state.get_data()
    user_id = msg.from_user.id
    api_id, api_hash = d["api_id"], d["api_hash"]
    chat_id = msg.chat.id
    msg_id = d.get("otp_msg_id")

    app = await _get_login_app(user_id, api_id, api_hash)
    try:
        await app.check_password(msg.text)
    except FloodWait as fw:
        return await msg.answer(f"Wait {fw.value}s.")
    except Exception:
        return await msg.answer("Wrong password. Try again.")

    await _complete_login_and_save(user_id, api_id, api_hash, chat_id, msg_id, state, app)

# ---------- runner ----------
async def main():
    try:
        import pyrogram, aiogram
        log("versions", {"pyrogram": getattr(pyrogram, "__version__", "?"),
                         "aiogram": getattr(aiogram, "__version__", "?")})
    except Exception:
        pass
    await dp.start_polling(bot)

async def login_bot_main():
    await main()

if __name__ == "__main__":
    asyncio.run(main())
