# main_bot.py — iOS15 inline UI, intervals 30/45/60, /stats(owner), /top(anyone)
import asyncio, os, json
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from dotenv import load_dotenv

from core.db import (
    init_db, upsert_user, set_ad, get_ad, set_interval, get_interval,
    add_groups, list_groups, clear_groups,
    users_count, sessions_count, get_total_sent_ok, top_users
)

load_dotenv()
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN") or os.getenv("ADS_BOT_TOKEN", "")
OWNER_ID = int(os.getenv("OWNER_ID", "0") or "0")
WEBAPP_URL = os.getenv("WEBAPP_URL", "")  # e.g., https://yourdomain/webapp/index.html
if not MAIN_BOT_TOKEN:
    raise RuntimeError("MAIN_BOT_TOKEN (or ADS_BOT_TOKEN) missing in .env")

bot = Bot(MAIN_BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp  = Dispatcher()

VALID_INTERVALS = {30, 45, 60}

class S(StatesGroup):
    set_ad = State()
    add_groups = State()

def is_owner(uid: int) -> bool:
    return OWNER_ID and uid == OWNER_ID

def header(title: str) -> str:
    return f"🟦 <b>{title}</b>\n<code>iOS 15 UI • Figma-ready • Responsive inline menu</code>\n"

def chip(label: str, value: str) -> str:
    return f"• <b>{label}</b>: <code>{value}</code>"

def home_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="📝 Set Ad", callback_data="set_ad"),
         InlineKeyboardButton(text="👥 Add Groups", callback_data="add_groups")],
        [InlineKeyboardButton(text="⏱ Interval", callback_data="interval"),
         InlineKeyboardButton(text="🧹 Clear Groups", callback_data="clear_groups")],
        [InlineKeyboardButton(text="🔁 Refresh", callback_data="refresh")]
    ]
    # WebApp button
    if WEBAPP_URL:
        rows.append([InlineKeyboardButton(text="📱 Open Dashboard", web_app=WebAppInfo(url=WEBAPP_URL))])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def interval_kb(cur: int) -> InlineKeyboardMarkup:
    opts = [30, 45, 60]
    row = [InlineKeyboardButton(text=(f"✅ {m}m" if m==cur else f"• {m}m"), callback_data=f"interval:{m}") for m in opts]
    return InlineKeyboardMarkup(inline_keyboard=[row, [InlineKeyboardButton(text="⬅️ Back", callback_data="home")]])

def back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Done", callback_data="home")]])

async def send_home(msg: Message | CallbackQuery):
    if isinstance(msg, CallbackQuery):
        user = msg.from_user; message = msg.message
    else:
        user = msg.from_user; message = msg
    upsert_user(user.id, user.username)
    ad = get_ad(user.id)
    groups = list_groups(user.id)
    interval = get_interval(user.id) or 30
    if interval not in VALID_INTERVALS: interval = 30
    text = header("Spinify Control Center") + "\n".join([
        chip("Ad saved", "yes" if ad else "no"),
        chip("Groups", str(len(groups))),
        chip("Interval", f"{interval} minutes (30/45/60)"),
        "",
        "Owner-only: /stats • Anyone: /top 10"
    ])
    if isinstance(msg, CallbackQuery):
        await message.edit_text(text, reply_markup=home_kb())
    else:
        await message.answer(text, reply_markup=home_kb())

@dp.message(Command("start"))
async def start_cmd(m: Message, state: FSMContext):
    await state.clear(); await send_home(m)

# /stats — OWNER ONLY
@dp.message(Command("stats"))
async def cmd_stats(m: Message):
    if not is_owner(m.from_user.id): return
    u = users_count()
    a = sessions_count()
    t = get_total_sent_ok()
    await m.answer("\n".join([
        "<b>Stats</b>",
        f"👥 Users: <b>{u}</b>",
        f"🟢 Active users: <b>{a}</b>",
        f"📨 Total messages forwarded: <b>{t}</b>"
    ]))

# /top [N] — open to everyone
@dp.message(Command("top"))
async def cmd_top(m: Message, command: CommandObject):
    n = 10
    try:
        if command.args:
            n = max(1, min(50, int(command.args.strip())))
    except:
        n = 10
    rows = top_users(limit=n)
    if not rows: return await m.answer("No users yet.")
    lines = [f"<b>Top users (by messages sent) — top {n}</b>"]
    rank = 1
    for r in rows:
        lines.append(f"{rank}. <b>{r['user_id']}</b> @{r['username'] or '-'} — sent: <code>{r['sent_ok'] or 0}</code> (last: <code>{r['last_sent_at'] or '—'}</code>)")
        rank += 1
    await m.answer("\n".join(lines))

# ----- callbacks (inline UI) -----
@dp.callback_query(F.data == "set_ad")
async def cb_set_ad(c: CallbackQuery, state: FSMContext):
    await c.answer(); await state.set_state(S.set_ad)
    await c.message.edit_text("📝 <b>Send your Ad text</b>\nIt will be forwarded by your user accounts.", reply_markup=back_kb())

@dp.message(S.set_ad)
async def on_ad_text(m: Message, state: FSMContext):
    text = (m.text or "").strip()
    if not text: return await m.answer("Ad cannot be empty. Try again.")
    set_ad(m.from_user.id, text); await state.clear()
    await m.answer("✅ Saved.", reply_markup=back_kb())

@dp.callback_query(F.data == "add_groups")
async def cb_add_groups(c: CallbackQuery, state: FSMContext):
    await c.answer(); await state.set_state(S.add_groups)
    await c.message.edit_text("👥 <b>Send group links/usernames</b> (one per line)\nExamples:\n<code>@groupname</code>\n<code>https://t.me/groupname</code>", reply_markup=back_kb())

@dp.message(S.add_groups)
async def on_groups(m: Message, state: FSMContext):
    raw = (m.text or "").strip()
    if not raw: return await m.answer("Nothing received. Try again.")
    lines = [x.strip() for x in raw.splitlines() if x.strip()]
    added = add_groups(m.from_user.id, lines); await state.clear()
    await m.answer(f"✅ Added <b>{added}</b> group(s).", reply_markup=back_kb())

@dp.callback_query(F.data == "clear_groups")
async def cb_clear_groups(c: CallbackQuery, state: FSMContext):
    clear_groups(c.from_user.id); await c.answer("Cleared."); await send_home(c)

@dp.callback_query(F.data == "interval")
async def cb_interval(c: CallbackQuery, state: FSMContext):
    await c.answer(); cur = get_interval(c.from_user.id) or 30
    if cur not in VALID_INTERVALS: cur = 30
    await c.message.edit_text(f"⏱ <b>Posting interval</b>\nCurrent: <b>{cur} minutes</b>\nSelect a preset:", reply_markup=interval_kb(cur))

@dp.callback_query(F.data.startswith("interval:"))
async def cb_interval_set(c: CallbackQuery, state: FSMContext):
    _, minutes = c.data.split(":", 1)
    try: minutes = int(minutes)
    except: minutes = 30
    if minutes not in VALID_INTERVALS: minutes = 30
    set_interval(c.from_user.id, minutes); await c.answer(f"Interval set to {minutes}m")
    await cb_interval(c, state)

@dp.callback_query(F.data == "refresh")
async def cb_refresh(c: CallbackQuery, state: FSMContext):
    await c.answer("Refreshed."); await send_home(c)

@dp.callback_query(F.data == "home")
async def cb_home(c: CallbackQuery, state: FSMContext):
    await state.clear(); await send_home(c)

# Optional: handle WebApp sendData events if you keep that path as fallback
@dp.message(F.web_app_data)
async def on_webapp_data(m: Message):
    try:
        payload = json.loads(m.web_app_data.data)
    except Exception:
        return await m.answer("Bad data.")
    action = payload.get("action"); data = payload.get("payload") or {}
    uid = m.from_user.id

    if action == "save_ad":
        set_ad(uid, (data.get("text") or "").strip()); return await m.answer("Ad saved.")
    if action == "set_interval":
        minutes = int(data.get("minutes") or 30)
        if minutes not in {30,45,60}: minutes = 30
        set_interval(uid, minutes); return await m.answer(f"Interval {minutes}m set.")
    if action == "add_group":
        add_groups(uid, [data.get("group")]); return await m.answer("Group added.")
    if action == "add_groups_bulk":
        add_groups(uid, data.get("groups") or []); return await m.answer("Groups added.")
    if action == "clear_groups":
        clear_groups(uid); return await m.answer("Groups cleared.")
    if action == "remove_group":
        gs = [g for g in list_groups(uid) if g != data.get("group")]
        clear_groups(uid); 
        if gs: add_groups(uid, gs)
        return await m.answer("Group removed.")
    if action == "get_top":
        rows = top_users(limit=int(data.get("limit") or 10))
        lines = [f"<b>Top {len(rows)}</b>"] + [
            f"{i+1}. <b>{r['user_id']}</b> @{r['username'] or '-'} — {r['sent_ok'] or 0}"
            for i, r in enumerate(rows)
        ]
        return await m.answer("\n".join(lines))
    await m.answer("Unknown action.")

async def _preflight(): init_db()
async def main(): await _preflight(); await dp.start_polling(bot)
if __name__ == "__main__": asyncio.run(main())
