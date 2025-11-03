# login_bot.py
# Aiogram v3 + Pyrogram
# Inline OTP keypad + live monospace code preview + Resend/Call/Alt + TTL guard + 2FA
# Stylish ✇ texts and robust logs.

import asyncio
import os
import pathlib
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, StateFilter
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
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

# ---------------- env & bot ----------------
load_dotenv()
LOGIN_BOT_TOKEN = os.getenv("LOGIN_BOT_TOKEN")
bot = Bot(LOGIN_BOT_TOKEN)
dp = Dispatcher()
init_db()

PARSE_MODE = "HTML"  # to show <code> 4 5 6 </code> preview

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
LOCAL_CODE_TTL_SEC = 65  # local guard

class Login(StatesGroup):
    api_id = State()
    api_hash = State()
    phone = State()
    otp = State()
    password = State()

# ---------------- UI ----------------
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
         InlineKeyboardButton(
