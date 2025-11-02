import asyncio
import os
from aiogram import Bot, Dispatcher
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from dotenv import load_dotenv
from pyrogram import Client
from pyrogram.errors import PhoneCodeExpired, PhoneCodeInvalid
from core.db import init_db, get_conn

load_dotenv()
LOGIN_BOT_TOKEN = os.getenv("LOGIN_BOT_TOKEN")

bot = Bot(LOGIN_BOT_TOKEN)
dp = Dispatcher()
init_db()


class Login(StatesGroup):
    api_id = State()
    api_hash = State()
    phone = State()
    otp = State()


def phone_kb():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Send phone", request_contact=True)]],
        resize_keyboard=True
    )


def otp_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="1"), KeyboardButton(text="2"), KeyboardButton(text="3")],
            [KeyboardButton(text="4"), KeyboardButton(text="5"), KeyboardButton(text="6")],
            [KeyboardButton(text="7"), KeyboardButton(text="8"), KeyboardButton(text="9")],
            [KeyboardButton(text="0")],
            [KeyboardButton(text="⬅️ Back"), KeyboardButton(text="🧹 Clear"), KeyboardButton(text="✅ Submit")],
        ],
        resize_keyboard=True
    )


@dp.message(Command("start"))
async def start(msg: Message, state: FSMContext):
    await msg.answer("Send your API ID (number).")
    await state.set_state(Login.api_id)


@dp.message(Login.api_id)
async def get_api_id(msg: Message, state: FSMContext):
    try:
        api_id = int(msg.text.strip())
    except ValueError:
        return await msg.answer("API ID must be a number. Send again.")
    await state.update_data(api_id=api_id)
    await msg.answer("OK. Now send your API HASH.")
    await state.set_state(Login.api_hash)


@dp.message(Login.api_hash)
async def get_api_hash(msg: Message, state: FSMContext):
    api_hash = msg.text.strip()
    await state.update_data(api_hash=api_hash)
    await msg.answer("Now send you
    api_hash = data["api_hash"]
    phone = data["phone"]
    phone_code_hash = data["phone_code_hash"]
    code = data.get("code", "")

    txt = msg.text.strip()

    # buttons
    if txt == "❌ Cancel":
        await state.clear()
        return await msg.answer("❌ Cancelled.", reply_markup=None)

    if txt == "⬅️":
        code = code[:-1]
        await state.update_data(code=code)
        return await msg.answer(f"Code: `{code}`", parse_mode="Markdown", reply_markup=otp_kb())

    if txt == "✅ OK":
        # try login NOW with whatever we collected
        if not code:
            return await msg.answer("Enter the code first.", reply_markup=otp_kb())
        # fall through to sign-in part below
    elif txt.isdigit():
        code += txt
        await state.update_data(code=code)
        # show current
        await msg.answer(f"Code: `{code}`", parse_mode="Markdown", reply_markup=otp_kb())
        # if length looks enough, we can try auto
        if len(code) < 5:
            return
    else:
        # unknown text
        return

    # ---- try to sign in ----
    client = Client(
        name=f"login-{msg.from_user.id}",
        api_id=api_id,
        api_hash=api_hash,
        in_memory=True,
    )
    await client.connect()
    try:
        await client.sign_in(
            phone_number=phone,
            phone_code_hash=phone_code_hash,
            phone_code=code
        )
    except PhoneCodeExpired:
        # resend code
        new_sent = await client.send_code(phone)
        await client.disconnect()
        await state.update_data(phone_code_hash=new_sent.phone_code_hash, code="")
        return await msg.answer("⏳ Code expired. I sent a **new code**. Enter the NEW one 👇", reply_markup=otp_kb())
    except PhoneCodeInvalid:
        await client.disconnect()
        await state.update_data(code="")
        return await msg.answer("❌ Wrong code. Try again with keypad.", reply_markup=otp_kb())
    except Exception as e:
        await client.disconnect()
        await state.clear()
        return await msg.answer(f"⚠️ Login failed: {e}\n/start again", reply_markup=None)

    # success
    session_str = await client.export_session_string()

    # enforce branding
    try:
        await client.update_profile(bio="#1 Free Ads Bot — Join @PhiloBots")
        me = await client.get_me()
        base = me.first_name.split(" — ")[0]
        await client.update_profile(first_name=base + " — via @SpinifyAdsBot")
    except:
        pass

    await client.disconnect()

    # save session
    conn = get_conn()
    conn.execute("""
        INSERT INTO user_sessions(user_id, api_id, api_hash, session_string)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            api_id=excluded.api_id,
            api_hash=excluded.api_hash,
            session_string=excluded.session_string
    """, (msg.from_user.id, api_id, api_hash, session_str))
    conn.commit()
    conn.close()

    await state.clear()
    await msg.answer("✅ Logged in & session saved.\nGo to @SpinifyAdsBot", reply_markup=None)


async def main():
    print("Login bot running...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
