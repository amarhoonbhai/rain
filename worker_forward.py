import os, asyncio, logging
from typing import Dict, List, Optional
from datetime import datetime, timezone

from pyrogram import Client, filters
from pyrogram.errors import FloodWait, Unauthorized, RPCError
from pyrogram.types import Message

from core.db import (
    init_db, users_with_sessions, sessions_list,
    list_groups, add_group, clear_groups, groups_cap,
    get_interval, set_interval, get_last_sent_at, mark_sent_now,
    inc_sent_ok, set_setting, get_setting,
)

LOG_LEVEL=os.getenv("LOG_LEVEL","INFO")
logging.basicConfig(level=LOG_LEVEL)
log=logging.getLogger("worker")

PARALLEL_USERS=int(os.getenv("PARALLEL_USERS","3"))
PER_GROUP_DELAY=float(os.getenv("PER_GROUP_DELAY","30"))
SEND_TIMEOUT=int(os.getenv("SEND_TIMEOUT","60"))
TICK_INTERVAL=int(os.getenv("TICK_INTERVAL","15"))

# Per-user runtime state (cached Saved-All list + current index)
STATE: Dict[int, Dict[str, any]] = {}  # {user_id: {"apps":[Client...], "saved_ids":[int], "idx":int}}

# ---------- Saved-All helpers ----------
async def fetch_saved_ids(app: Client) -> List[int]:
    ids=[]
    async for m in app.get_chat_history("me", limit=1000):  # newest→oldest
        # keep all messages that have text OR media
        if (m.text or m.caption or m.media):
            ids.append(m.id)
    ids.reverse()  # oldest→newest
    return ids

def _next_idx(user_id:int, n:int) -> int:
    st=STATE.setdefault(user_id, {})
    i=int(st.get("idx",0))
    if n<=0: return 0
    i=(i+1) % n
    st["idx"]=i
    return i

def _cur_idx(user_id:int) -> int:
    return int(STATE.get(user_id,{}).get("idx",0))

# ---------- command parsing (from any chat, only for .me sender) ----------
HELP_TEXT=(
"📜 Commands\n"
"<code>.help</code> — this help\n"
"<code>.addgc</code> (paste 1 per line) — add targets (@handle, id, or any t.me link)\n"
"<code>.gc</code> — list saved targets | <code>.cleargc</code> — clear all\n"
"<code>.time</code> 30m|45m|60m|120 — set interval minutes\n"
"<code>.adreset</code> — restart Saved-All cycle"
)

def _parse_time(s: str) -> Optional[int]:
    s=s.strip().lower()
    if s.endswith("m"):
        try: return max(1, int(s[:-1]))
        except Exception: return None
    try: return max(1,int(s))
    except Exception: return None

def register_session_handlers(app: Client, user_id: int):
    @app.on_message(filters.me & filters.text)
    async def my_text(_, msg: Message):
        t=(msg.text or "").strip()
        if not t.startswith("."): return
        try:
            if t.startswith(".help"):
                await msg.reply_text(HELP_TEXT)
            elif t.startswith(".gc"):
                targets=list_groups(user_id); cap=groups_cap(user_id)
                if not targets: await msg.reply_text(f"GC list empty (cap {cap})."); return
                out="\n".join(f"• {x}" for x in targets[:100])
                more = "" if len(targets)<=100 else f"\n… +{len(targets)-100} more"
                await msg.reply_text(f"GC ({len(targets)}/{cap})\n{out}{more}")
            elif t.startswith(".cleargc"):
                clear_groups(user_id); await msg.reply_text("✅ Cleared groups.")
            elif t.startswith(".addgc"):
                # accept after command: whole message body (including reply) lines
                lines=t.splitlines()[1:] or []
                if not lines and msg.reply_to_message and (msg.reply_to_message.text or msg.reply_to_message.caption):
                    body=(msg.reply_to_message.text or msg.reply_to_message.caption)
                    lines=[ln.strip() for ln in body.splitlines()]
                added=0; cap=groups_cap(user_id)
                for ln in lines:
                    if not ln.strip(): continue
                    added += add_group(user_id, ln.strip())
                    if len(list_groups(user_id))>=cap: break
                await msg.reply_text(f"✅ Added {added}. Now {len(list_groups(user_id))}/{cap}.")
            elif t.startswith(".time"):
                arg=t.split(maxsplit=1)[1] if len(t.split())>1 else ""
                mins=_parse_time(arg)
                if not mins: await msg.reply_text("❌ Usage: .time 30m|45m|60m|120"); return
                set_interval(user_id, mins); await msg.reply_text(f"✅ Interval set to {mins}m.")
            elif t.startswith(".adreset"):
                st=STATE.setdefault(user_id,{}); st["idx"]=0
                await msg.reply_text("✅ Saved-All cycle reset to first message.")
        except Exception as e:
            log.error("cmd error u%s: %s", user_id, e)

# ---------- per-user lifecycle ----------
async def build_clients_for_user(uid:int) -> List[Client]:
    apps=[]
    for s in sessions_list(uid):
        try:
            c=Client(name=f"u{uid}s{s['slot']}", api_id=int(s["api_id"]), api_hash=str(s["api_hash"]), session_string=str(s["session_string"]))
            await c.start()
            register_session_handlers(c, uid)
            apps.append(c)
        except Unauthorized:
            # ping via setting for main bot to DM later (auto-rehydrate)
            set_setting(f"rehydrate:{uid}", int(datetime.now(timezone.utc).timestamp()))
            log.warning("[u%s] session unauthorized, flagged for rehydrate", uid)
        except Exception as e:
            log.error("[u%s] start session failed: %s", uid, e)
    return apps

async def ensure_state(uid:int):
    st=STATE.get(uid)
    if st and st.get("apps"): return
    apps=await build_clients_for_user(uid)
    STATE[uid]={"apps": apps, "idx": 0, "saved_ids": []}

async def refresh_saved(uid:int):
    st=STATE.get(uid); 
    if not st or not st.get("apps"): return
    # use first app for Saved Messages list
    try:
        saved=await fetch_saved_ids(st["apps"][0])
        st["saved_ids"]=saved
    except Exception as e:
        log.error("[u%s] fetch_saved_ids error: %s", uid, e)

async def send_copy(app: Client, from_chat, msg_id: int, to_target: str):
    # copy_message supports ids/usernames/links if resolvable by pyrogram internally
    try:
        await app.copy_message(chat_id=to_target, from_chat_id=from_chat, message_id=msg_id)
        return True
    except FloodWait as fw:
        await asyncio.sleep(fw.value); return False
    except Exception as e:
        log.warning("copy fail → %s", e); return False

async def run_cycle(uid:int):
    st=STATE.get(uid); 
    if not st or not st.get("apps"): return
    apps=st["apps"]; targets=list_groups(uid)
    if not targets: return
    if not st["saved_ids"]:
        await refresh_saved(uid)
    if not st["saved_ids"]: 
        log.info("[u%s] no saved messages to send", uid)
        return
    idx=_cur_idx(uid); msg_id=st["saved_ids"][idx]
    # send via each session account to all targets (we’ll just use the first healthy app)
    app=apps[0]
    ok_any=False
    for tg in targets:
        try:
            good = await asyncio.wait_for(send_copy(app, "me", msg_id, tg), timeout=SEND_TIMEOUT)
            if good:
                ok_any=True; inc_sent_ok(uid,1)
        except asyncio.TimeoutError:
            log.warning("[u%s] send timeout to %s", uid, tg)
        await asyncio.sleep(PER_GROUP_DELAY)
    if ok_any:
        mark_sent_now(uid)
        _next_idx(uid, len(st["saved_ids"]))

async def user_loop(uid:int):
    await ensure_state(uid)
    interval=get_interval(uid)
    last=get_last_sent_at(uid)
    now=int(datetime.now(timezone.utc).timestamp())
    if last is None or (now - last) >= interval*60:
        await run_cycle(uid)

# ---------- main loop ----------
async def main_loop():
    init_db()
    log.info("worker started")
    while True:
        uids=users_with_sessions()
        # limit concurrency
        sem=asyncio.Semaphore(PARALLEL_USERS)
        async def run(uid:int):
            async with sem:
                try: await user_loop(uid)
                except Unauthorized:
                    set_setting(f"rehydrate:{uid}", int(datetime.now(timezone.utc).timestamp()))
                except Exception as e:
                    log.error("loop error u%s: %s", uid, e)
        tasks=[asyncio.create_task(run(uid)) for uid in uids]
        if tasks: await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(TICK_INTERVAL)

async def main():
    try: await main_loop()
    except KeyboardInterrupt: pass

if __name__=="__main__":
    asyncio.run(main())
