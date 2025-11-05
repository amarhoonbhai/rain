# main_bot.py — Compact inline UI + Channel gate + Manage Accounts + Night Mode + 5-group cap
import asyncio, os, json, math
from typing import List, Tuple
from zoneinfo import ZoneInfo
from datetime import datetime, time as dtime

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
    users_count, sessions_count, get_total_sent_ok, top_users, get_conn,
    count_user_sessions, list_user_sessions, delete_session_slot,
    set_global_night_mode, get_global_night_mode
)

# ---------------- env / bot ----------------
load_dotenv()
MAIN_BOT_TOKEN = (os.getenv("MAIN_BOT_TOKEN") or os.getenv("ADS_BOT_TOKEN","")).strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0") or "0")
if not MAIN_BOT_TOKEN or ":" not in MAIN_BOT_TOKEN:
    raise RuntimeError("MAIN_BOT_TOKEN missing/malformed.")

bot = Bot(MAIN_BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp  = Dispatcher()

VALID_INTERVALS = (30, 45, 60)
GLOBAL_DEFAULT_LAYOUT = os.getenv("DEFAULT_LAYOUT", "compact")
SHOW_LAYOUT_TOGGLE = os.getenv("SHOW_LAYOUT_TOGGLE", "0") == "1"

# Required channels (bot should be admin to check memberships reliably)
REQUIRED_CHANNELS = ("@PhiloBots", "@TheTrafficZone")
LOGIN_BOT_USERNAME = "SpinifyLoginBot"
GROUP_LIMIT = 5

# ---------- tiny prefs ----------
def set_pref(user_id: int, name: str, val: str) -> None:
    key = f"ui:{user_id}:{name}"
    conn = get_conn()
    conn.execute(
        "INSERT INTO settings(key, val) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET val=excluded.val",
        (key, str(val)),
    ); conn.commit(); conn.close()

def get_pref(user_id: int, name: str, default: str) -> str:
    key = f"ui:{user_id}:{name}"
    conn = get_conn()
    row = conn.execute("SELECT val FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    return row["val"] if row and row["val"] is not None else default

# ---------- FSM ----------
class S(StatesGroup):
    set_ad = State()
    add_groups = State()

# ---------- helpers ----------
def is_owner(uid: int) -> bool:
    return OWNER_ID and uid == OWNER_ID

async def ensure_channels_joined(user_id: int) -> bool:
    ok_all = True
    for ch in REQUIRED_CHANNELS:
        try:
            cm = await bot.get_chat_member(ch, user_id)
            if getattr(cm, "status", "left") in ("left", "kicked"):
                ok_all = False
        except Exception:
            ok_all = False
    return ok_all

def header(title: str) -> str:
    return f"🟦 <b>{title}</b>\n<code>Compact inline dashboard • Interval 30/45/60</code>\n"

def chip(label: str, value: str) -> str:
    return f"• <b>{label}</b>: <code>{value}</code>"

def layout_cols(layout: str) -> int:
    return 2 if layout == "compact" else (1 if layout == "cozy" else 3)

def chunk_buttons(btns: List[InlineKeyboardButton], cols: int) -> List[List[InlineKeyboardButton]]:
    if cols <= 1: return [[b] for b in btns]
    rows, row = [], []
    for b in btns:
        row.append(b)
        if len(row) == cols: rows.append(row); row = []
    if row: rows.append(row)
    return rows

def home_kb(user_id: int) -> InlineKeyboardMarkup:
    layout = get_pref(user_id, "layout", GLOBAL_DEFAULT_LAYOUT)
    cols = layout_cols(layout)
    btns = [
        InlineKeyboardButton(text="👤 Manage Accounts", callback_data="acc"),
        InlineKeyboardButton(text="📝 Set Ad", callback_data="set_ad"),
        InlineKeyboardButton(text="👥 Add Groups", callback_data="add_groups"),
        InlineKeyboardButton(text="📜 View Groups", callback_data="groups:list:1"),
        InlineKeyboardButton(text="⏱ Interval", callback_data="interval"),
        InlineKeyboardButton(text="🔗 Verify Channels", callback_data="verify"),
        InlineKeyboardButton(text="🧹 Clear Groups", callback_data="clear_groups"),
        InlineKeyboardButton(text="🔁 Refresh", callback_data="refresh"),
    ]
    if SHOW_LAYOUT_TOGGLE:
        btns.append(InlineKeyboardButton(text=f"🧩 Layout ({layout.title()})", callback_data="layout"))
    rows = chunk_buttons(btns, cols)
    nm = "On" if get_global_night_mode() else "Off"
    rows.append([InlineKeyboardButton(text=f"🌙 Night Mode: {nm} (Owner)", callback_data="owner:night")])
    rows.append([InlineKeyboardButton(text="📊 Owner Stats", callback_data="owner_stats")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def interval_kb(cur: int) -> InlineKeyboardMarkup:
    row = [InlineKeyboardButton(text=(f"✅ {m}m" if m==cur else f"{m}m"), callback_data=f"interval:{m}") for m in VALID_INTERVALS]
    return InlineKeyboardMarkup(inline_keyboard=[row, [InlineKeyboardButton(text="⬅️ Back", callback_data="home")]])

def verify_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Join @PhiloBots", url="https://t.me/PhiloBots")],
        [InlineKeyboardButton(text="Join @TheTrafficZone", url="https://t.me/TheTrafficZone")],
        [InlineKeyboardButton(text="✅ I Joined", callback_data="verify:check")]
    ])

def back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Done", callback_data="home")]])

def groups_page_text(user_id: int, page: int, per_page: int = 8) -> Tuple[str, int]:
    gs = list_groups(user_id)
    total = len(gs); pages = max(1, math.ceil(total / per_page))
    page = max(1, min(page, pages))
    start = (page - 1) * per_page; end = min(start + per_page, total); show = gs[start:end]
    lines = [header("Your Groups"), chip("Total", f"{total} / {GROUP_LIMIT}"), chip("Page", f"{page}/{pages}"), ""]
    if not show:
        lines.append("No groups linked. Use <b>👥 Add Groups</b> to add @links.")
    else:
        for i, g in enumerate(show, start=1 + start):
            lines.append(f"{i:>3}. <code>{g}</code>")
    lines += ["", "Tip: Use <b>🧹 Clear Groups</b> to remove all."]
    return "\n".join(lines), pages

def groups_page_kb(page: int, pages: int) -> InlineKeyboardMarkup:
    nav = []
    if page > 1: nav.append(InlineKeyboardButton(text="⬅️ Prev", callback_data=f"groups:list:{page-1}"))
    nav.append(InlineKeyboardButton(text="📍 Home", callback_data="home"))
    if page < pages: nav.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"groups:list:{page+1}"))
    return InlineKeyboardMarkup(inline_keyboard=[nav])

# ---------- screens ----------
async def send_gate(msg: Message | CallbackQuery):
    text = (
        "🔒 <b>Access Locked</b>\n"
        "Join both channels to continue:\n"
        "1) @PhiloBots\n2) @TheTrafficZone\n\nTap <b>I Joined</b> after subscribing."
    )
    if isinstance(msg, CallbackQuery):
        await msg.message.edit_text(text, reply_markup=verify_kb())
    else:
        await msg.answer(text, reply_markup=verify_kb())

async def send_home(msg: Message | CallbackQuery):
    if isinstance(msg, CallbackQuery):
        user = msg.from_user; message = msg.message
    else:
        user = msg.from_user; message = msg
    upsert_user(user.id, user.username)

    if not await ensure_channels_joined(user.id):
        return await send_gate(msg)

    ad = get_ad(user.id)
    groups = list_groups(user.id)
    interval = get_interval(user.id) or 30
    sessions = count_user_sessions(user.id)

    text = header("Spinify Control Center") + "\n".join([
        chip("Ad saved", "yes" if ad else "no"),
        chip("Groups", f"{len(groups)} / {GROUP_LIMIT}"),
        chip("Interval", f"{interval} minutes (30/45/60)"),
        chip("Accounts", f"{sessions} / 3"),
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
    await state.clear()
    await send_home(m)

@dp.message(Command("stats"))
async def cmd_stats(m: Message):
    if not is_owner(m.from_user.id): return
    u = users_count(); a = sessions_count(); t = get_total_sent_ok()
    nm = "On" if get_global_night_mode() else "Off"
    await m.answer("\n".join([
        "<b>Stats</b>",
        f"👥 Users: <b>{u}</b>",
        f"🟢 Active users: <b>{a}</b>",
        f"📨 Total forwarded: <b>{t}</b>",
        f"🌙 Night Mode: <b>{nm}</b> (00:00–07:00 IST)"
    ]))

@dp.message(Command("top"))
async def cmd_top(m: Message, command: CommandObject):
    n = 10
    try:
        if command.args: n = max(1, min(50, int(command.args.strip())))
    except: n = 10
    rows = top_users(limit=n)
    if not rows: return await m.answer("No users yet.")
    lines = [f"<b>Top users — top {n}</b>"]
    for i, r in enumerate(rows, start=1):
        lines.append(f"{i}. <b>{r['user_id']}</b> @{r['username'] or '-'} — sent: <code>{r['sent_ok'] or 0}</code>")
    await m.answer("\n".join(lines))

# ---------- callbacks ----------
@dp.callback_query(F.data == "verify")
async def cb_verify(c: CallbackQuery):
    await send_gate(c)

@dp.callback_query(F.data == "verify:check")
async def cb_verify_check(c: CallbackQuery):
    ok = await ensure_channels_joined(c.from_user.id)
    if not ok:
        return await c.answer("Still not joined both. Join and tap again.", show_alert=True)
    await send_home(c)

@dp.callback_query(F.data == "interval")
async def cb_interval(c: CallbackQuery, state: FSMContext):
    if not await ensure_channels_joined(c.from_user.id): return await send_gate(c)
    cur = get_interval(c.from_user.id) or 30
    if cur not in VALID_INTERVALS: cur = 30
    await c.message.edit_text(f"⏱ <b>Posting interval</b>\nCurrent: <b>{cur} minutes</b>\nSelect a preset:",
                              reply_markup=interval_kb(cur))

@dp.callback_query(F.data.startswith("interval:"))
async def cb_interval_set(c: CallbackQuery, state: FSMContext):
    _, minutes = c.data.split(":", 1)
    try: m = int(minutes)
    except: m = 30
    if m not in VALID_INTERVALS: m = 30
    set_interval(c.from_user.id, m)
    await c.answer(f"Interval set to {m}m")
    await cb_interval(c, state)

@dp.callback_query(F.data.startswith("groups:list:"))
async def cb_groups_list(c: CallbackQuery):
    if not await ensure_channels_joined(c.from_user.id): return await send_gate(c)
    try: page = int(c.data.split(":")[-1])
    except: page = 1
    text, pages = groups_page_text(c.from_user.id, page)
    await c.message.edit_text(text, reply_markup=groups_page_kb(page, pages))

@dp.callback_query(F.data == "set_ad")
async def cb_set_ad(c: CallbackQuery, state: FSMContext):
    if not await ensure_channels_joined(c.from_user.id): return await send_gate(c)
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
    if not await ensure_channels_joined(c.from_user.id): return await send_gate(c)
    await c.answer(); await state.set_state(S.add_groups)
    cur = len(list_groups(c.from_user.id))
    left = max(0, GROUP_LIMIT - cur)
    await c.message.edit_text(
        f"👥 <b>Send group links/usernames</b> (one per line)\n"
        f"Limit: <b>{GROUP_LIMIT}</b> total. You can add <b>{left}</b> more.",
        reply_markup=back_kb()
    )

@dp.message(S.add_groups)
async def on_groups(m: Message, state: FSMContext):
    cur = len(list_groups(m.from_user.id)); left = max(0, GROUP_LIMIT - cur)
    if left <= 0:
        await state.clear(); return await m.answer("Limit reached (5). Remove some first.", reply_markup=back_kb())
    raw = (m.text or "").strip()
    if not raw: return await m.answer("Nothing received. Try again.")
    lines = [x.strip() for x in raw.splitlines() if x.strip()]
    lines = lines[:left]
    added = add_groups(m.from_user.id, lines)
    await state.clear()
    more = "" if added == len(lines) else f"\n(ignored extras over limit)"
    await m.answer(f"✅ Added <b>{added}</b> group(s).{more}", reply_markup=back_kb())

@dp.callback_query(F.data == "clear_groups")
async def cb_clear_groups(c: CallbackQuery, state: FSMContext):
    clear_groups(c.from_user.id); await c.answer("Cleared."); await send_home(c)

@dp.callback_query(F.data == "refresh")
async def cb_refresh(c: CallbackQuery, state: FSMContext):
    await c.answer("Refreshed."); await send_home(c)

# ----- Manage Accounts (slots 1..3) -----
def accounts_kb(user_id: int) -> InlineKeyboardMarkup:
    rows = []
    rows.append([InlineKeyboardButton(text="➕ Add account (opens @SpinifyLoginBot)", url=f"https://t.me/{LOGIN_BOT_USERNAME}")])
    sess = list_user_sessions(user_id)
    used = {r["slot"] for r in sess}
    rm_row = []
    for s in (1,2,3):
        label = f"🟢 Slot {s}" if s in used else f"⚪ Slot {s} (empty)"
        rows.append([InlineKeyboardButton(text=label, callback_data="noop")])
        rm_row.append(InlineKeyboardButton(text=f"🗑 Remove {s}", callback_data=f"acc:del:{s}"))
    rows.append(rm_row)
    rows.append([InlineKeyboardButton(text="⬅️ Back", callback_data="home"),
                 InlineKeyboardButton(text="🔁 Refresh", callback_data="acc:refresh")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.callback_query(F.data == "acc")
async def cb_acc(c: CallbackQuery):
    if not await ensure_channels_joined(c.from_user.id): return await send_gate(c)
    n = count_user_sessions(c.from_user.id)
    if n == 0:
        await c.message.edit_text(
            "⚠️ <b>No session found.</b>\nLogin in @SpinifyLoginBot to add up to 3 user accounts.",
            reply_markup=accounts_kb(c.from_user.id)
        )
    else:
        await c.message.edit_text("👤 <b>Manage Accounts</b>\nAdd up to 3 accounts.\nUse buttons below.",
                                  reply_markup=accounts_kb(c.from_user.id))

@dp.callback_query(F.data == "acc:refresh")
async def cb_acc_refresh(c: CallbackQuery):
    await cb_acc(c)

@dp.callback_query(F.data.startswith("acc:del:"))
async def cb_acc_del(c: CallbackQuery):
    try: slot = int(c.data.split(":")[-1])
    except: slot = 0
    if slot not in (1,2,3): return await c.answer("Invalid slot")
    n = delete_session_slot(c.from_user.id, slot)
    if n: await c.answer(f"Removed slot {slot}")
    else: await c.answer("Nothing to remove")
    await cb_acc(c)

# ----- Owner night mode toggle -----
@dp.callback_query(F.data == "owner:night")
async def cb_owner_night(c: CallbackQuery):
    if not is_owner(c.from_user.id):
        return await c.answer("Owner only.", show_alert=True)
    new = not get_global_night_mode()
    set_global_night_mode(new)
    await c.answer(f"Night Mode {'On' if new else 'Off'}")
    await send_home(c)

# ---------- ignore old webapp payloads ----------
@dp.message(F.web_app_data)
async def on_webapp_data(m: Message):
    try: _ = json.loads(m.web_app_data.data)
    except Exception: pass
    await m.answer("WebApp path disabled. Use inline menu.")

# ---------- runner ----------
async def _preflight():
    init_db()

async def main():
    await _preflight()
    await dp.start_polling(bot)

__all__ = ["bot", "dp", "_preflight"]

if __name__ == "__main__":
    asyncio.run(main())
