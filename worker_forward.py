# worker_forward.py — command listener (self-messages) + scheduler (pinned ad) + 10s gaps
import os, asyncio, logging, re, time as _time
from datetime import datetime, time
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import (
    FloodWait, RPCError,
    Unauthorized, AuthKeyUnregistered,
)
try:
    from pyrogram.errors import SessionRevoked, SessionExpired, UserNotParticipant, ChatAdminRequired
except Exception:
    class _Dummy(Exception): pass
    SessionRevoked = SessionExpired = UserNotParticipant = ChatAdminRequired = _Dummy

from core.db import (
    init_db,
    users_with_sessions, sessions_strings,
    list_groups, add_group, clear_groups,
    get_interval, mark_sent_now, last_sent_at_for, inc_sent_ok,
    set_interval,
    is_premium, is_gc_unlocked, groups_cap,
    night_enabled,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"))
log = logging.getLogger("worker")

IST = ZoneInfo("Asia/Kolkata")
NIGHT_START = time(0, 0)
NIGHT_END   = time(7, 0)

SPLIT_RE = re.compile(r"[,\s]+")

def is_night_now_ist() -> bool:
    now = datetime.now(IST).time()
    return NIGHT_START <= now < NIGHT_END

# ---------- utils ----------
def _parse_interval_to_minutes(spec: str) -> int | None:
    s = spec.strip().lower()
    if not s: return None
    if s.endswith("m"):
        return int(s[:-1])
    if s.endswith("h"):
        return int(float(s[:-1]) * 60)
    return int(s)

def _resolve_target_token(token: str):
    """Return (chat_id or username string) — we don't auto-join; send will try."""
    token = token.strip()
    if not token: return None
    if token.lstrip("-").isdigit():
        try: return int(token)
        except Exception: return None
    if token.startswith("@"):
        return token
    if token.startswith("http"):
        u = urlparse(token)
        if u.netloc.lower() == "t.me":
            # either username (/name) or invite (+hash); keep full link for copy_message fallback
            return token
    return token  # raw

async def _find_pinned_in_saved(app: Client) -> Message | None:
    """
    Return pinned Message from 'Saved Messages' if present, else None.
    """
    try:
        # get recent messages; find one with pinned flag
        async for m in app.get_chat_history("me", limit=100):
            if getattr(m, "pinned", False):
                return m
    except Exception as e:
        log.warning(f"[{(await app.get_me()).id}] read pinned failed: {e}")
    return None

async def _send_one(app: Client, target, src_msg: Message) -> bool:
    """
    Copy pinned message to target. target can be int id, @username, or full t.me link.
    """
    try:
        # if it's a t.me link string, pyrogram copy_message accepts username part; for +invite it will fail unless member
        if isinstance(target, str) and target.startswith("http"):
            # try username path if possible
            u = urlparse(target)
            path = u.path.strip("/")
            if path and not path.startswith("+"):
                target = path.split("/")[0]  # username
        await app.copy_message(chat_id=target, from_chat_id="me", message_id=src_msg.id)
        return True
    except FloodWait as fw:
        log.info(f"[send] FloodWait {fw.value}s on {target}")
        await asyncio.sleep(fw.value + 1)
    except (UserNotParticipant, ChatAdminRequired):
        log.info(f"[send] skipped (not a member / admin needed): {target}")
    except Exception as e:
        log.info(f"[send] failed on {target}: {e}")
    return False

# ---------- self-commands (work anywhere you type) ----------
async def handle_addgc(app: Client, msg: Message, text: str):
    me = await app.get_me()
    uid = me.id
    lines = [ln.strip() for ln in text.splitlines()[1:] if ln.strip()]
    if not lines:
        await msg.reply("✇ Usage:\n.addgc\n<id|@user|t.me/link>\n(one per line, up to 5 per command)")
        return
    MAX_PER_CMD = 5
    orig = len(lines)
    lines = lines[:MAX_PER_CMD]

    existing = set(list_groups(uid))
    cap = groups_cap(uid)
    cap_left = max(0, cap - len(existing))

    accepted = []
    dup = 0
    for ln in lines:
        if ln in existing or ln in accepted:
            dup += 1
            continue
        accepted.append(ln)

    if cap_left <= 0:
        await msg.reply(f"⚠️ Capacity full ({cap}). Use .cleargc to free slots.")
        return

    added = 0
    bad = 0
    for tok in accepted:
        t = _resolve_target_token(tok)
        if t is None:
            bad += 1; continue
        if added >= cap_left: break
        if add_group(uid, tok):
            added += 1
        else:
            bad += 1

    extras = max(0, orig - MAX_PER_CMD)
    left = max(0, cap - (len(existing) + added))
    parts = [f"✅ Added: {added}"]
    if dup: parts.append(f"⧗ Duplicates: {dup}")
    if bad: parts.append(f"✖ Invalid/failed: {bad}")
    if extras: parts.append(f"… Ignored extra lines: {extras} (max {MAX_PER_CMD}/cmd)")
    parts.append(f"📦 Capacity left: {left}/{cap}")
    await msg.reply("\n".join(parts))

async def handle_time(app: Client, msg: Message, arg: str):
    me = await app.get_me()
    uid = me.id
    if not arg:
        await msg.reply("✇ Usage: .time 30m | 45m | 60m  (premium: 1–360m or H, e.g., 2h)")
        return
    try:
        mins = _parse_interval_to_minutes(arg)
        if mins is None: raise ValueError()
        if is_premium(uid):
            ok = 1 <= mins <= 360
        else:
            ok = mins in (30, 45, 60)
        if not ok:
            raise ValueError()
        set_interval(uid, int(mins))
        await msg.reply(f"⏱ Interval set to {mins} minutes ✅")
    except Exception:
        await msg.reply("❌ Invalid. Use 30m/45m/60m (premium: 1–360m or e.g., 2h).")

def install_command_handlers(app: Client):
    @app.on_message(filters.me & filters.text)
    async def _on_me_text(_, m: Message):
        t = (m.text or "").strip()
        if not t.startswith("."):
            return
        head = t.split(None, 1)
        cmd = head[0].lower()
        tail = head[1] if len(head) > 1 else ""

        if cmd == ".addgc":
            await handle_addgc(app, m, t)
        elif cmd == ".listgc":
            me = await app.get_me()
            uid = me.id
            gs = list_groups(uid)
            cap = groups_cap(uid)
            if not gs:
                await m.reply(f"👥 Groups: none (cap {cap}). Use .addgc")
            else:
                await m.reply("👥 Groups (cap {}):\n{}".format(cap, "\n".join(f"• {g}" for g in gs)))
        elif cmd == ".cleargc":
            me = await app.get_me(); clear_groups(me.id)
            await m.reply("🧹 Groups cleared.")
        elif cmd == ".time":
            await handle_time(app, m, tail.strip())
        elif cmd == ".status":
            me = await app.get_me()
            uid = me.id
            gs = list_groups(uid)
            it = get_interval(uid) or 30
            last = last_sent_at_for(uid)
            eta = "—"
            if last is not None:
                remain = (last + it * 60) - int(_time.time())
                if remain > 0:
                    mm, ss = divmod(remain, 60)
                    hh, mm = divmod(mm, 60)
                    if hh: eta = f"in ~{hh}h {mm}m {ss}s"
                    else:  eta = f"in ~{mm}m {ss}s"
                else:
                    eta = "due now"
            await m.reply(
                "📟 Status\n"
                f"✇ Interval: {it} min\n"
                f"✇ Groups: {len(gs)} (cap {groups_cap(uid)})\n"
                f"✇ Premium: {'ON' if is_premium(uid) else 'OFF'} | Unlock10: {'ON' if is_gc_unlocked(uid) else 'OFF'}\n"
                f"✇ Next send: {eta}"
            )
        elif cmd == ".help":
            await m.reply(
                "✇ Commands\n"
                "• .addgc  (then up to 5 lines: id/@user/t.me/...)\n"
                "• .listgc / .cleargc\n"
                "• .time 30m|45m|60m  (premium: 1–360m or e.g., 2h)\n"
                "• .status"
            )

# ---------- scheduler loop per user ----------
async def send_cycle_for_user(sess: dict):
    uid = sess["user_id"]
    app = Client(
        name=f"user-{uid}",
        api_id=int(sess["api_id"]),
        api_hash=str(sess["api_hash"]),
        session_string=str(sess["session_string"]),
    )
    try:
        await app.start()
    except (Unauthorized, AuthKeyUnregistered, SessionRevoked, SessionExpired) as e:
        log.error(f"[u{uid}] auth error: {e}")
        return
    except Exception as e:
        log.error(f"[u{uid}] start failed: {e}")
        return

    # install dot-commands
    install_command_handlers(app)

    try:
        while True:
            try:
                # night mode
                if night_enabled() and is_night_now_ist():
                    await asyncio.sleep(30)
                    continue

                interval = get_interval(uid) or 30
                last = last_sent_at_for(uid)
                now = int(_time.time())
                if last is not None and now - last < interval * 60:
                    await asyncio.sleep(10)
                    continue

                # need to send
                pinned = await _find_pinned_in_saved(app)
                if not pinned:
                    try:
                        await app.send_message("me", "✇ No pinned ad found in Saved Messages. Pin your ad and try again.")
                    except Exception:
                        pass
                    await asyncio.sleep(30)
                    continue

                targets = list_groups(uid)
                if not targets:
                    await asyncio.sleep(20)
                    continue

                ok = 0
                for raw in targets:
                    tgt = _resolve_target_token(raw)
                    if tgt is None: continue
                    if await _send_one(app, tgt, pinned):
                        ok += 1
                    await asyncio.sleep(10)  # 10s gap
                if ok > 0:
                    mark_sent_now(uid)
                    inc_sent_ok(uid, ok)
                    log.info(f"[u{uid}] sent_ok+={ok}")
                else:
                    log.info(f"[u{uid}] nothing sent")

            except FloodWait as fw:
                await asyncio.sleep(fw.value + 1)
            except Exception as e:
                log.error(f"[u{uid}] cycle error: {e}")
                await asyncio.sleep(5)
    finally:
        try: await app.stop()
        except Exception: pass

async def main():
    init_db()
    # create a dedicated loop per account (user_id)
    tasks = []
    for uid in users_with_sessions():
        # pick first slot for that uid
        s_rows = sessions_strings(uid)
        if not s_rows:
            continue
        tasks.append(asyncio.create_task(send_cycle_for_user(s_rows[0])))
    if not tasks:
        log.info("no sessions found")
        # idle loop
        while True:
            await asyncio.sleep(60)
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
