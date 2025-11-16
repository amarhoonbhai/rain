import os, asyncio, logging
from datetime import datetime, timezone
from aiogram import Bot, Dispatcher, F, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv
from core.db import (
    init_db, get_conn, ensure_user,
    sessions_list, sessions_delete, sessions_count_user, sessions_count,
    list_groups, groups_cap, get_interval, get_last_sent_at,
    users_count, get_total_sent_ok, top_users,
    get_gate_channels_effective, set_setting, get_setting,
    night_enabled, set_night_enabled, set_name_lock,
)

load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"))
log = logging.getLogger("main-bot")

TOKEN = (os.getenv("MAIN_BOT_TOKEN") or "").strip()
if not TOKEN or ":" not in TOKEN: raise RuntimeError("MAIN_BOT_TOKEN missing")
OWNER_ID = int(os.getenv("OWNER_ID","0"))
UNLOCK_GC_LINK = os.getenv("UNLOCK_GC_LINK","").strip()

bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
init_db()

def is_owner(uid:int) -> bool: return OWNER_ID and uid==OWNER_ID
def _gate_channels():
    ch1, ch2 = get_gate_channels_effective()
    return [c for c in (ch1, ch2) if c]
async def _check_gate(user_id:int):
    missing=[]
    for ch in _gate_channels():
        try:
            m = await bot.get_chat_member(ch, user_id)
            if str(getattr(m,"status","left")).lower() in {"left","kicked"}: missing.append(ch)
        except Exception: missing.append(ch)
    return (len(missing)==0), missing
def _gate_text():
    lines="\n".join(f"  • {c}" for c in _gate_channels())
    return f"✇ Access required\n✇ Join these channels then tap <b>I've Joined</b>:\n{lines}"
def _gate_kb():
    rows=[[InlineKeyboardButton(text=f"🔗 {c}", url=f"https://t.me/{c.lstrip('@')}")] for c in _gate_channels()]
    rows.append([InlineKeyboardButton(text="✅ I've Joined", callback_data="gate:check")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def _format_eta(uid: int) -> str:
    last = get_last_sent_at(uid); interval = get_interval(uid) or 30
    if last is None: return "now"
    now = int(datetime.now(timezone.utc).timestamp())
    left = interval*60 - (now - int(last))
    if left <= 0: return "now"
    h,m = divmod(left,3600); m,s = divmod(m,60)
    parts=[]
    if h: parts.append(f"{h}h")
    if m: parts.append(f"{m}m")
    if s and not parts: parts.append(f"{s}s")
    return "in ~" + " ".join(parts)

def kb_main(uid:int):
    rows=[
        [InlineKeyboardButton(text="👤 Manage Accounts", callback_data="menu:acc")],
        [InlineKeyboardButton(text="📜 Commands",        callback_data="menu:cmds"),
         InlineKeyboardButton(text="🔓 Unlock GC",       callback_data="menu:unlock")],
        [InlineKeyboardButton(text="ℹ️ Disclaimer",      callback_data="menu:disc")],
        [InlineKeyboardButton(text="🔄 Refresh",         callback_data="menu:home")],
    ]
    if is_owner(uid):
        rows.insert(3,[InlineKeyboardButton(text=("🌙 Night: ON" if night_enabled() else "🌙 Night: OFF"),
                        callback_data="owner:night")])
        rows.append([InlineKeyboardButton(text="📊 Stats", callback_data="owner:stats"),
                     InlineKeyboardButton(text="🏆 Top 10", callback_data="owner:top")])
        rows.append([InlineKeyboardButton(text="📣 Broadcast", callback_data="owner:bcast"),
                     InlineKeyboardButton(text="💎 Premium",   callback_data="owner:prem")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def _cmds_text():
    return (
      "📜 Self-Commands (send from your <b>logged-in account</b>)\n"
      "• <code>.help</code> — show this help\n"
      "• <code>.addgc</code> (paste 1 per line) — add @usernames, numeric IDs, or any t.me link\n"
      "• <code>.gc</code> — list targets | <code>.cleargc</code> — clear\n"
      "• <code>.time</code> 30m|45m|60m|120 — set interval\n"
      "• <code>.adreset</code> — restart Saved-All cycle\n"
      "Saved-All: every interval, the next Saved Message is copied to <b>each</b> target with ~30s gaps. Premium emoji OK."
    )

async def home(m, uid:int):
    gs=len(list_groups(uid)); ss=sessions_count_user(uid); interval=get_interval(uid)
    text=( "✇ Welcome\n"
           "Use @SpinifyLoginBot to add up to 3 accounts.\n"
           "Then use self-commands from your logged-in account.\n\n"
           f"Sessions: {ss} | Groups: {gs}/{groups_cap(uid)} | Interval: {interval}m\n"
           f"Next send: {('—' if ss==0 or gs==0 else _format_eta(uid))}\n"
           f"Night: {'ON' if night_enabled() else 'OFF'}"
    )
    if isinstance(m, Message): await m.answer(text, reply_markup=kb_main(uid))
    else:
        try: await m.message.edit_text(text, reply_markup=kb_main(uid))
        except TelegramBadRequest: pass

class Owner(StatesGroup):
    broadcast=State()
    prem_user=State()
    prem_name=State()
    prem_down=State()

@dp.message(Command("start"))
async def start(msg: Message):
    uid=msg.from_user.id; ensure_user(uid, msg.from_user.username)
    if _gate_channels():
        ok,_=await _check_gate(uid)
        if not ok: await msg.answer(_gate_text(), reply_markup=_gate_kb()); return
    await home(msg, uid)

@dp.callback_query(F.data=="gate:check")
async def gate_check(cq: CallbackQuery):
    ok,_=await _check_gate(cq.from_user.id)
    if ok: await home(cq, cq.from_user.id)
    else:  await cq.message.edit_text(_gate_text(), reply_markup=_gate_kb())

@dp.callback_query(F.data=="menu:home")
async def cb_home(cq: CallbackQuery): await home(cq, cq.from_user.id)

@dp.callback_query(F.data=="menu:acc")
async def cb_acc(cq: CallbackQuery):
    uid=cq.from_user.id; rows=sessions_list(uid)
    if not rows:
        text="👤 Manage Accounts\nNo sessions. Add via @SpinifyLoginBot."
        kb=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Open @SpinifyLoginBot", url="https://t.me/SpinifyLoginBot")],
            [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]])
        try: await cq.message.edit_text(text, reply_markup=kb)
        except TelegramBadRequest: pass
        return
    lines=[f"• Slot {r['slot']} — API_ID {r['api_id']}" for r in rows]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🗑 Remove S{r['slot']}", callback_data=f"acc:del:{r['slot']}")] for r in rows
    ]+[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]])
    try: await cq.message.edit_text("👤 Manage Accounts\n"+"\n".join(lines), reply_markup=kb)
    except TelegramBadRequest: pass

@dp.callback_query(F.data.startswith("acc:del:"))
async def acc_del(cq: CallbackQuery):
    try:
        slot=int(cq.data.split(":")[-1]); sessions_delete(cq.from_user.id, slot)
    except Exception as e: log.error("del slot %s", e)
    await cb_acc(cq)

@dp.callback_query(F.data=="menu:unlock")
async def cb_unlock(cq: CallbackQuery):
    cap=groups_cap(cq.from_user.id)
    rows=[]
    if UNLOCK_GC_LINK: rows.append([InlineKeyboardButton(text="🔗 Join Unlock GC", url=UNLOCK_GC_LINK)])
    rows.append([InlineKeyboardButton(text="✅ I've Joined", callback_data="unlock:ok")])
    rows.append([InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")])
    try:
        await cq.message.edit_text(f"🔓 Unlock GC\nJoin the GC to unlock 10 targets.\nCurrent cap: {cap}", 
                                   reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    except TelegramBadRequest: pass

@dp.callback_query(F.data=="unlock:ok")
async def unlock_ok(cq: CallbackQuery):
    set_setting(f"groups_cap:{cq.from_user.id}", 10)
    try:
        await cq.message.edit_text(f"✅ Unlocked. Cap is now {groups_cap(cq.from_user.id)}.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]))
    except TelegramBadRequest: pass

@dp.callback_query(F.data=="menu:cmds")
async def cb_cmds(cq: CallbackQuery):
    try: await cq.message.edit_text(_cmds_text(), reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]))
    except TelegramBadRequest: pass

@dp.callback_query(F.data=="menu:disc")
async def cb_disc(cq: CallbackQuery):
    text=("⚠️ Disclaimer\nUse at your own risk. Comply with Telegram TOS and local laws.")
    try: await cq.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]))
    except TelegramBadRequest: pass

# /fstats — for everyone
@dp.message(Command("fstats"))
async def fstats(msg: Message):
    uid=msg.from_user.id; ss=sessions_count_user(uid); gs=len(list_groups(uid)); interval=get_interval(uid)
    eta="—" if ss==0 or gs==0 else _format_eta(uid)
    await msg.answer(
        "📟 Forward Stats\n"
        f"▶ Worker: {'RUNNING' if ss>0 else 'IDLE'}\n"
        f"Interval: {interval} min\n"
        f"Sessions: {ss} | Groups: {gs}\n"
        f"Next send: {eta}\n"
        f"{'🌙 Night ON' if night_enabled() else '🌙 Night OFF'}"
    )

# owner tools
@dp.callback_query(F.data=="owner:night")
async def owner_night(cq: CallbackQuery):
    if not is_owner(cq.from_user.id): return
    set_night_enabled(not night_enabled()); await home(cq, cq.from_user.id)

@dp.callback_query(F.data=="owner:stats")
async def owner_stats(cq: CallbackQuery):
    if not is_owner(cq.from_user.id): return
    total=users_count(); active=sessions_count(); sent=get_total_sent_ok()
    await cq.message.edit_text(f"📊 Global Stats\nUsers: {total}\nActive: {active}\nTotal forwarded: {sent}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]))

@dp.callback_query(F.data=="owner:top")
async def owner_top(cq: CallbackQuery):
    if not is_owner(cq.from_user.id): return
    rows=top_users(10)
    if not rows: text="🏆 No data."
    else: text="🏆 Top Users\n"+"\n".join(f"{i+1}. <code>{r['user_id']}</code> — {r['sent_ok']}" for i,r in enumerate(rows))
    await cq.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]))

class OwnerFSM(StatesGroup):
    broadcast=State(); prem_up=State(); prem_name=State(); prem_down=State()

@dp.callback_query(F.data=="owner:bcast")
async def owner_bcast(cq: CallbackQuery, state: FSMContext):
    if not is_owner(cq.from_user.id): return
    await state.set_state(OwnerFSM.broadcast)
    await cq.message.edit_text("📣 Send broadcast text now.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]))
@dp.message(OwnerFSM.broadcast)
async def do_bcast(msg: Message, state: FSMContext):
    if not is_owner(msg.from_user.id): await state.clear(); return
    uids=[r["user_id"] for r in get_conn().execute("SELECT user_id FROM users").fetchall()]
    sent=fail=0
    for i,uid in enumerate(uids,1):
        try: await bot.send_message(uid, msg.html_text or msg.text); sent+=1
        except Exception: fail+=1
        if i%25==0: await asyncio.sleep(1)
    await state.clear(); await msg.answer(f"✅ Done. Sent {sent}, failed {fail}")

@dp.callback_query(F.data=="owner:prem")
async def owner_prem_menu(cq: CallbackQuery):
    if not is_owner(cq.from_user.id): return
    kb=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💎 Upgrade User", callback_data="prem:up")],
        [InlineKeyboardButton(text="🧹 Downgrade User", callback_data="prem:down")],
        [InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]
    ])
    await cq.message.edit_text("💎 Premium Controls", reply_markup=kb)

@dp.callback_query(F.data=="prem:up")
async def prem_up(cq: CallbackQuery, state:FSMContext):
    if not is_owner(cq.from_user.id): return
    await state.set_state(OwnerFSM.prem_up)
    await cq.message.edit_text("Send user_id to upgrade:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]))
@dp.message(OwnerFSM.prem_up)
async def prem_up_id(msg: Message, state:FSMContext):
    try: target=int(msg.text.strip())
    except Exception: await msg.answer("❌ user_id must be integer"); return
    await state.update_data(target=target); await state.set_state(OwnerFSM.prem_name)
    await msg.answer("Send locked display name (or '-' to skip):")
@dp.message(OwnerFSM.prem_name)
async def prem_up_name(msg: Message, state:FSMContext):
    data=await state.get_data(); target=data["target"]; locked=None if msg.text.strip()=="-" else msg.text.strip()
    set_name_lock(target, True, name=locked); set_setting(f"groups_cap:{target}", 50)
    await state.clear(); await msg.answer(f"✅ Premium enabled for {target} (cap=50){' with “'+locked+'”' if locked else ''}.")

@dp.callback_query(F.data=="prem:down")
async def prem_down(cq: CallbackQuery, state:FSMContext):
    if not is_owner(cq.from_user.id): return
    await state.set_state(OwnerFSM.prem_down)
    await cq.message.edit_text("Send user_id to downgrade:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅ Back", callback_data="menu:home")]]))
@dp.message(OwnerFSM.prem_down)
async def prem_down_id(msg: Message, state:FSMContext):
    try: target=int(msg.text.strip())
    except Exception: await msg.answer("❌ user_id must be integer"); return
    set_name_lock(target, False); set_setting(f"groups_cap:{target}", 5)
    await state.clear(); await msg.answer(f"✅ Premium disabled for {target} (cap=5).")

async def main():
    await dp.start_polling(bot)

if __name__=="__main__":
    asyncio.run(main())
