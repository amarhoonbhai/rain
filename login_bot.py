from datetime import datetime, timedelta
from aiogram import F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message
from aiogram.filters import StateFilter
from pyrogram import Client
from pyrogram.errors import PhoneCodeExpired, PhoneCodeInvalid, FloodWait, SessionPasswordNeeded

LOCAL_CODE_TTL_SEC = 65  # local safety window

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
         InlineKeyboardButton(text="📩 SMS", callback_data="act:sms")],
    ])

async def _send_code(user_id: int, api_id: int, api_hash: str, phone: str, force_sms: bool = False) -> str:
    client = Client(name=f"login-{user_id}", api_id=api_id, api_hash=api_hash, in_memory=True)
    await client.connect()
    sent = await client.send_code(phone, force_sms=force_sms)
    await client.disconnect()
    return sent.phone_code_hash

@dp.message(StateFilter(Login.phone))
async def step_phone(msg: Message, state: FSMContext):
    d = await state.get_data()
    phone = msg.text.strip()
    pch = await _send_code(msg.from_user.id, d["api_id"], d["api_hash"], phone, force_sms=False)
    sent = await msg.answer("Verification Code\nUse the keypad below.", reply_markup=otp_inline_kb())
    await state.update_data(phone=phone, phone_code_hash=pch, code="", code_sent_at=datetime.utcnow().isoformat(), otp_msg_id=sent.message_id)
    await state.set_state(Login.otp)

@dp.callback_query(StateFilter(Login.otp), F.data.startswith("d:"))
async def otp_digit(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    code = d.get("code", "")
    if len(code) < 8:
        code += cq.data.split(":", 1)[1]
        await state.update_data(code=code)
    await cq.answer()  # fast, silent

@dp.callback_query(StateFilter(Login.otp), F.data == "act:back")
async def otp_back(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    code = d.get("code", "")
    if code:
        await state.update_data(code=code[:-1])
    await cq.answer("Back")

@dp.callback_query(StateFilter(Login.otp), F.data == "act:clear")
async def otp_clear(cq: CallbackQuery, state: FSMContext):
    await state.update_data(code="")
    await cq.answer("Cleared")
    try:
        await bot.edit_message_text(cq.message.chat.id, cq.message.message_id,
                                    "Verification Code\nUse the keypad below.", reply_markup=otp_inline_kb())
    except Exception:
        pass

@dp.callback_query(StateFilter(Login.otp), F.data.in_(["act:resend", "act:sms"]))
async def otp_resend(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    force_sms = (cq.data == "act:sms")
    new_hash = await _send_code(cq.from_user.id, d["api_id"], d["api_hash"], d["phone"], force_sms=force_sms)
    await state.update_data(phone_code_hash=new_hash, code="", code_sent_at=datetime.utcnow().isoformat())
    await cq.answer("New code sent" + (" via SMS" if force_sms else ""))
    try:
        await bot.edit_message_text(cq.message.chat.id, cq.message.message_id,
                                    "Verification Code\nUse the keypad below.", reply_markup=otp_inline_kb())
    except Exception:
        pass

@dp.callback_query(StateFilter(Login.otp), F.data == "act:submit")
async def otp_submit(cq: CallbackQuery, state: FSMContext):
    d = await state.get_data()
    api_id, api_hash, phone = d["api_id"], d["api_hash"], d["phone"]
    phone_code_hash = d["phone_code_hash"]
    code = d.get("code", "")
    sent_at = datetime.fromisoformat(d["code_sent_at"])

    # 1) local TTL guard
    if datetime.utcnow() - sent_at > timedelta(seconds=LOCAL_CODE_TTL_SEC):
        new_hash = await _send_code(cq.from_user.id, api_id, api_hash, phone)
        await state.update_data(phone_code_hash=new_hash, code="", code_sent_at=datetime.utcnow().isoformat())
        await cq.answer("Code expired. New code sent.")
        try:
            await bot.edit_message_text(cq.message.chat.id, cq.message.message_id,
                                        "Verification Code\nUse the keypad below.", reply_markup=otp_inline_kb())
        except Exception:
            pass
        return

    # 2) basic validation
    if not (4 <= len(code) <= 8):
        await cq.answer("Enter 4–8 digits")
        return

    # 3) try sign in
    client = Client(name=f"login-{cq.from_user.id}", api_id=api_id, api_hash=api_hash, in_memory=True)
    await client.connect()
    try:
        await client.sign_in(phone_number=phone, phone_code_hash=phone_code_hash, phone_code=code)
    except PhoneCodeExpired:
        new_hash = await client.send_code(phone)
        await client.disconnect()
        await state.update_data(phone_code_hash=new_hash.phone_code_hash, code="", code_sent_at=datetime.utcnow().isoformat())
        await cq.answer("Code expired. New code sent.")
        try:
            await bot.edit_message_text(cq.message.chat.id, cq.message.message_id,
                                        "Verification Code\nUse the keypad below.", reply_markup=otp_inline_kb())
        except Exception:
            pass
        return
    except PhoneCodeInvalid:
        await client.disconnect()
        await state.update_data(code="")
        await cq.answer("Wrong code")
        return
    except FloodWait as fw:
        await client.disconnect()
        await cq.answer(f"Wait {fw.value}s", show_alert=True)
        return
    except SessionPasswordNeeded:
        await client.disconnect()
        # If the account has 2FA password:
        await state.update_data(code="")
        await cq.answer()
        await bot.send_message(cq.message.chat.id, "This account has 2-step password. Send the password now.")
        await state.set_state(Login.otp)  # reuse state; add a separate handler to read password if you want
        return
    except Exception as e:
        await client.disconnect()
        await state.clear()
        await cq.answer("Login failed")
        await bot.send_message(cq.message.chat.id, f"Login failed: {e}\n/start again")
        return

    # 4) success → export session, save, done
    session_str = await client.export_session_string()
    try:
        await client.update_profile(bio="#1 Free Ads Bot — Join @PhiloBots")
        me = await client.get_me()
        base = me.first_name.split(" — ")[0]
        await client.update_profile(first_name=base + " — via @SpinifyAdsBot")
    except Exception:
        pass
    await client.disconnect()

    conn = get_conn()
    conn.execute(
        "INSERT INTO user_sessions(user_id, api_id, api_hash, session_string) "
        "VALUES (?, ?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET api_id=excluded.api_id, api_hash=excluded.api_hash, session_string=excluded.session_string",
        (cq.from_user.id, api_id, api_hash, session_str)
    )
    conn.commit()
    conn.close()

    await state.clear()
    await cq.answer("Logged in")
    try:
        await bot.edit_message_text(cq.message.chat.id, cq.message.message_id,
                                    "✅ Session saved.\nYou can go back to the main bot now.")
    except Exception:
        await bot.send_message(cq.message.chat.id, "✅ Session saved.\nYou can go back to the main bot now.")
