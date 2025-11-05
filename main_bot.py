# main_bot.py — Compact-first responsive inline UI (aiogram 3.x)
# Features:
# - Interval presets: 30/45/60
# - /stats (owner-only), /top [N] (anyone)
# - Set Ad, Add/Clear Groups, View Groups (pagination), Refresh
# - Layout preference per-user; default locked to COMPACT unless SHOW_LAYOUT_TOGGLE=1

import asyncio, os, json, math
from typing import List, Tuple

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv

from core.db import (
    init_db, upsert_user, set_ad, get_ad, set_interval, get_interval,
    add_groups, list_groups, clear_groups,
    users_count, sessions_count, get_total_sent_ok, top_users, get_conn
)

load_dotenv()

MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN") or os.getenv("ADS_BOT_TOKEN", "")
OWNER_ID = int(os.getenv("OWNER_ID", "0") or "0")

# UI prefs
GLOBAL_DEFAULT_LAYOUT = os.getenv("DEFAULT_LAYOUT", "compact")  # compact|cozy|spacious
SHOW_LAYOUT_TOGGLE = os.getenv("SHOW_LAYOUT_TOGGLE", "0") == "1"

if not MAIN_BOT_TOKEN:
    raise RuntimeError("MAIN_BOT_TOKEN (or ADS_BOT_TOKEN) missing in .env")

bot = Bot(MAIN_BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp  = Dispatcher()

VALID_INTERVALS = (30, 45, 60)
LAYOUTS = ("compact", "cozy", "spacious")  # 2-col, 1-col, 3-col

# ---------- tiny settings helpers (store layout in settings table) ----------
def set_pref(user_id: int, name: str, val: str) -> None:
    key = f"ui:{user_id}:{name}"
    conn = get_conn()
    conn.execute(
        "INSERT INTO settings(key, val) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET val = excluded.val",
        (key, str(val)),
    )
    conn.commit(); conn.close()

def get_pref(user_id: int, name: str, default: str) -> str:
    key = f"ui:{user_id}:{name}"
    conn = get_conn()
    row = conn.execute("SELECT val FROM settings WHERE key = ?", (key,)).fetchone()
    conn.close()
    return row["val"] if row and row["val"] is not None else default

# ---------- FSM ----------
class S(StatesGroup):
    set_ad = State()
    add_groups = State()

# ---------- UI utils ----------
def is_owner(uid: int) -> bool:
    return OWNER_ID and uid == OWNER_ID

def header(title: str) -> str:
    return (
        f"🟦 <b>{title}</b>\n"
        f"<code>Compact inline dashboard • Interval 30/45/60</code>\n"
    )

def chip(label: str, value: str) -> str:
    return f"• <b>{label}</b>: <code>{value}</code>"

def layout_cols(layout: str) -> int:
    if layout == "cozy": return 1
    if layout == "spacious": return 3
    return 2  # compact

def chunk_buttons(btns: List[InlineKeyboardButton], cols: int) -> List[List[InlineKeyboardButton]]:
    if cols <= 1:
        return [[b] for b in btns]
    rows, row = [], []
    for b in btns:
        row.append(b)
        if len(row) == cols:
            rows.append(row); row = []
    if row: rows.append(row)
    return rows

def home_kb(user_id: int) -> InlineKeyboardMarkup:
    layout = get_pref(user_id, "layout", GLOBAL_DEFAULT_LAYOUT)
    cols = layout_cols(layout)

    btns = [
        InlineKeyboardButton(text="📝 Set Ad", callback_data="set_ad"),
        InlineKeyboardButton(text="👥 Add Groups", callback_data="add_groups"),
        InlineKeyboardButton(text="📜 View Groups", callback_data="groups:list:1"),
        InlineKeyboardButton(text="⏱ Interval", callback_data="interval"),
        InlineKeyboardButton(text="🧹 Clear Groups", callback_data="clear_groups"),
        InlineKeyboardButton(text="🔁 Refresh", callback_data="refresh"),
    ]
    if SHOW_LAYOUT_TOGGLE:
        btns.append(InlineKeyboardButton(text=f"🧩 Layout ({layout.title()})", callback_data="layout"))

    rows = chunk_buttons(btns, cols)
    rows.append([InlineKeyboardButton(text="📊 Owner Stats", callback_data="owner_stats")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def interval_kb(cur: int) -> InlineKeyboardMarkup:
    row = [InlineKeyboardButton(text=(f"✅ {m}m" if m==cur else f"{m}m"), callback_data=f"interval:{m}") for m in VALID_INTERVALS]
    return InlineKeyboardMarkup(inline_keyboard=[row, [InlineKeyboardButton(text="⬅️ Back", callback_data="home")]])

def layout_picker_kb(current: str) -> InlineKeyboardMarkup:
    choices = []
    for name in LAYOUTS:
        text = f"✅ {name.title()}" if name == current else name.title()
        choices.append(InlineKeyboardButton(text=text, callback_data=f"layout:set:{name}"))
    rows = chunk_buttons(choices, 3)
    rows.append([InlineKeyboardButton(text="⬅️ Back", callback_data="home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Done", callback_data="home")]])

def groups_page_text(user_id: int, page: int, per_page: int = 8) -> Tuple[str, int]:
    gs = list_groups(user_id)
    total = len(gs)
    pages = max(1, math.ceil(total / per_page))
    page = max(1, min(page, pages))

    start = (page - 1) * per_page
    end = min(start + per_page, total)
    show = gs[start:end]

    lines = [header("Your Groups")]
    lines.append(chip("Total", str(total)))
    lines.append(chip("Page", f"{page}/{pages}"))
    lines.append("")
    if not show:
        lines.append("No groups linked. Use <b>👥 Add Groups</b> to add @links.")
    else:
        for i, g in enumerate(show, start=1 + start):
            lines.append(f"{i:>3}. <code>{g}</code>")
    lines.append("")
    lines.append("Tip: Use <b>🧹 Clear Groups</b> to remove all.")

    return "\n".join(lines), pages

def groups_page_kb(page: int, pages: int) -> InlineKeyboardMarkup:
    nav_row = []
    if page > 1:
        nav_row.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"groups:list:{page-1}"))
    nav_row.append(InlineKeyboardButton(text="📍 Home", callback_data="home"))
    if page < pages:
        nav_row.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"groups:list:{page+1}"))
    return InlineKeyboardMarkup(inline_keyboard=[nav_row])

# ---------- screens ----------
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
        "Owner-only: <code>/stats</code> • Anyone: <code>/top 10</code>"
    ])

    if isinstance(msg, CallbackQuery):
        await message.edit_text(text, reply_markup=home_kb(user.id))
    else:
        await message.answer(text, reply_markup=home_kb(user.id))

# ---------- commands ----------
@dp.message(Command("start"))
async def start_cmd(m: Message, state: FSMContext):
    await state.clear(); await send_home(m)

@dp.message(Command("stats"))
async def cmd_stats(m: Message):
    if not is_owner(m.from_user.id):
        return
    u = users_count(); a = sessions_count(); t = get_total_sent_ok()
    await m.answer("\n".join([
        "<b>Stats</b>",
        f"👥 Users: <b>{u}</b>",
        f"🟢 Active users: <b>{a}</b>",
        f"📨 Total messages forwarded: <b>{t}</b>",
    ]))

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
    for i, r in enumerate(rows, start=1):
        lines.append(
            f"{i}. <b>{r['user_id']}</b> @{r['username'] or '-'} — "
            f"sent: <code>{r['sent_ok'] or 0}</code> (last: <code>{r['last_sent_at'] or '—'}</code>)"
        )
    await m.answer("\n".join(lines))

# ---------- callbacks: owner quick button ----------
@dp.callback_query(F.data == "owner_stats")
async def cb_owner_stats(c: CallbackQuery):
    if not is_owner(c.from_user.id):
        return await c.answer("Owner only.", show_alert=True)
    u = users_count(); a = sessions_count(); t = get_total_sent_ok()
    await c.answer("Stats sent.")
    await c.message.answer("\n".join([
        "<b>Stats</b>",
        f"👥 Users: <b>{u}</b>",
        f"🟢 Active users: <b>{a}</b>",
        f"📨 Total messages forwarded: <b>{t}</b>",
    ]))

# ---------- callbacks: layout (hidden unless SHOW_LAYOUT_TOGGLE=1) ----------
@dp.callback_query(F.data == "layout")
async def cb_layout(c: CallbackQuery):
    if not SHOW_LAYOUT_TOGGLE:
        return await c.answer("Layout locked to Compact.", show_alert=True)
    cur = get_pref(c.from_user.id, "layout", GLOBAL_DEFAULT_LAYOUT)
    await c.message.edit_text(
        "🧩 <b>Choose layout</b>\nCompact (2 col) • Cozy (1 col) • Spacious (3 col)",
        reply_markup=layout_picker_kb(cur)
    )

@dp.callback_query(F.data.startswith("layout:set:"))
async def cb_layout_set(c: CallbackQuery):
    if not SHOW_LAYOUT_TOGGLE:
        return await c.answer("Layout locked.", show_alert=True)
    _, _, val = c.data.partition("layout:set:")
    if val not in LAYOUTS: val = "compact"
    set_pref(c.from_user.id, "layout", val)
    await c.answer(f"Layout: {val.title()}")
    await send_home(c)

# ---------- callbacks: intervals ----------
@dp.callback_query(F.data == "interval")
async def cb_interval(c: CallbackQuery, state: FSMContext):
    cur = get_interval(c.from_user.id) or 30
    if cur not in VALID_INTERVALS: cur = 30
    await c.message.edit_text(
        f"⏱ <b>Posting interval</b>\nCurrent: <b>{cur} minutes</b>\nSelect a preset:",
        reply_markup=interval_kb(cur)
    )

@dp.callback_query(F.data.startswith("interval:"))
async def cb_interval_set(c: CallbackQuery, state: FSMContext):
    _, minutes = c.data.split(":", 1)
    try: m = int(minutes)
    except: m = 30
    if m not in VALID_INTERVALS: m = 30
    set_interval(c.from_user.id, m)
    await c.answer(f"Interval set to {m}m")
    await cb_interval(c, state)

# ---------- callbacks: groups list w/ pagination ----------
@dp.callback_query(F.data.startswith("groups:list:"))
async def cb_groups_list(c: CallbackQuery):
    try:
        page = int(c.data.split(":")[-1])
    except:
        page = 1
    text, pages = groups_page_text(c.from_user.id, page)
    await c.message.edit_text(text, reply_markup=groups_page_kb(page, pages))

# ---------- set ad ----------
@dp.callback_query(F.data == "set_ad")
async def cb_set_ad(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.set_state(S.set_ad)
    await c.message.edit_text(
        "📝 <b>Send your Ad text</b>\nIt will be forwarded by your user accounts.",
        reply_markup=back_kb()
    )

@dp.message(S.set_ad)
async def on_ad_text(m: Message, state: FSMContext):
    text = (m.text or "").strip()
    if not text:
        return await m.answer("Ad cannot be empty. Try again.")
    set_ad(m.from_user.id, text)
    await state.clear()
    await m.answer("✅ Saved.", reply_markup=back_kb())

# ---------- add groups ----------
@dp.callback_query(F.data == "add_groups")
async def cb_add_groups(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.set_state(S.add_groups)
    await c.message.edit_text(
        "👥 <b>Send group links/usernames</b> (one per line)\nExamples:\n"
        "<code>@groupname</code>\n<code>https://t.me/groupname</code>",
        reply_markup=back_kb()
    )

@dp.message(S.add_groups)
async def on_groups(m: Message, state: FSMContext):
    raw = (m.text or "").strip()
    if not raw:
        return await m.answer("Nothing received. Try again.")
    lines = [x.strip() for x in raw.splitlines() if x.strip()]
    added = add_groups(m.from_user.id, lines)
    await state.clear()
    await m.answer(f"✅ Added <b>{added}</b> group(s).", reply_markup=back_kb())

# ---------- clear groups ----------
@dp.callback_query(F.data == "clear_groups")
async def cb_clear_groups(c: CallbackQuery, state: FSMContext):
    clear_groups(c.from_user.id)
    await c.answer("Cleared.")
    await send_home(c)

# ---------- refresh / back / home ----------
@dp.callback_query(F.data == "refresh")
async def cb_refresh(c: CallbackQuery, state: FSMContext):
    await c.answer("Refreshed.")
    await send_home(c)

@dp.callback_query(F.data == "home")
async def cb_home(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await send_home(c)

# ---------- optional: ignore any leftover web_app_data ----------
@dp.message(F.web_app_data)
async def on_webapp_data(m: Message):
    try:
        _ = json.loads(m.web_app_data.data)
    except Exception:
        pass
    await m.answer("WebApp path disabled. Use inline menu.")

# ---------- runner ----------
async def _preflight():
    init_db()

async def main():
    await _preflight()
    await dp.start_polling(bot)

# Expose for run_all.py
__all__ = ["bot", "dp", "_preflight"]

if __name__ == "__main__":
    asyncio.run(main())
