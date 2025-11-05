# worker_forward.py — forwarder with channel-gate, night mode, intervals, 3-session rotation, group cap=5
import asyncio
import random
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

from pyrogram import Client
from pyrogram.errors import (
    FloodWait, UserDeactivated, AuthKeyUnregistered,
    ChatWriteForbidden, ChannelPrivate, RPCError
)

from core.db import (
    get_conn, get_ad, list_groups, get_interval,
    get_global_night_mode, count_user_sessions
)

# Reuse the aiogram bot instance to DM users & check membership
from main_bot import bot as control_bot

# -------- constants --------
IST = ZoneInfo("Asia/Kolkata")
QUIET_START = dtime(0, 0)
QUIET_END   = dtime(7, 0)
VALID_INTERVALS = (30, 45, 60)
GROUP_LIMIT = 5
REQUIRED_CHANNELS = ("@PhiloBots", "@TheTrafficZone")

# -------- tiny settings helpers --------
def _get_setting(key: str, default: str | None = None) -> str | None:
    conn = get_conn()
    row = conn.execute("SELECT val FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    return (row["val"] if row and row["val"] is not None else default)

def _set_setting(key: str, val: str) -> None:
    conn = get_conn()
    conn.execute(
        "INSERT INTO settings(key, val) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET val=excluded.val",
        (key, val),
    )
    conn.commit(); conn.close()

def _ensure_user_stats_columns():
    """Add sent_ok and last_sent_at to users if missing (idempotent)."""
    conn = get_conn()
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(users)")]
    if "sent_ok" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN sent_ok INTEGER DEFAULT 0")
    if "last_sent_at" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN last_sent_at TEXT")
    conn.commit(); conn.close()

def _mark_sent_ok(user_id: int, count: int):
    conn = get_conn()
    conn.execute(
        "UPDATE users SET sent_ok=COALESCE(sent_ok,0)+?, last_sent_at=datetime('now') WHERE user_id=?",
        (count, user_id)
    )
    conn.commit(); conn.close()

def _get_all_user_ids() -> list[int]:
    conn = get_conn()
    ids = [r["user_id"] for r in conn.execute("SELECT user_id FROM users").fetchall()]
    conn.close()
    return ids

def _get_user_sessions(user_id: int) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT slot, api_id, api_hash, session_string FROM user_sessions WHERE user_id=? ORDER BY slot",
        (user_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def _get_last_sent_ts(user_id: int) -> datetime | None:
    iso = _get_setting(f"sched:{user_id}:last")
    if not iso: return None
    try: return datetime.fromisoformat(iso)
    except: return None

def _set_last_sent_now(user_id: int) -> None:
    _set_setting(f"sched:{user_id}:last", datetime.utcnow().isoformat())

# reminder cooldowns
def _should_remind(key: str, mins: int = 180) -> bool:
    """Return True if last reminder older than mins."""
    iso = _get_setting(key)
    if not iso: return True
    try:
        last = datetime.fromisoformat(iso)
        return (datetime.utcnow() - last) >= timedelta(minutes=mins)
    except:
        return True

def _touch_remind(key: str) -> None:
    _set_setting(key, datetime.utcnow().isoformat())

# -------- time / gates --------
def _is_quiet_hours_now() -> bool:
    if not get_global_night_mode():
        return False
    now = datetime.now(IST).time()
    return QUIET_START <= now < QUIET_END  # 00:00–07:00 IST

def _is_due(user_id: int, minutes: int) -> bool:
    last = _get_last_sent_ts(user_id)
    if not last:
        return True
    return datetime.utcnow() - last >= timedelta(minutes=minutes)

async def _is_user_subscribed(user_id: int) -> bool:
    """Checks membership in REQUIRED_CHANNELS using the control bot."""
    for ch in REQUIRED_CHANNELS:
        try:
            cm = await control_bot.get_chat_member(ch, user_id)
            if getattr(cm, "status", "left") in ("left", "kicked"):
                return False
        except Exception:
            # if bot can't see membership (not admin / private), treat as not subscribed
            return False
    return True

# -------- pyrogram client pool --------
_client_pool: dict[tuple[int,int], Client] = {}  # (user_id, slot) -> Client

async def _ensure_client(user_id: int, slot: int, api_id: int, api_hash: str, session_string: str) -> Client:
    key = (user_id, slot)
    cli = _client_pool.get(key)
    if cli and cli.is_connected:
        return cli
    cli = Client(
        name=f"acc-{user_id}-{slot}",
        api_id=api_id,
        api_hash=api_hash,
        session_string=session_string,
        in_memory=True,
        no_updates=True,
    )
    await cli.connect()
    _client_pool[key] = cli
    return cli

async def _close_dead_clients():
    dead = []
    for key, cli in _client_pool.items():
        if not cli.is_connected:
            dead.append(key)
    for key in dead:
        _client_pool.pop(key, None)

def _normalize_target(s: str) -> str:
    s = s.strip()
    if s.startswith("https://t.me/") or s.startswith("t.me/"):
        if s.startswith("t.me/"): s = "https://" + s
        return s
    if not s.startswith("@") and s:
        return "@" + s
    return s

async def _maybe_join(cli: Client, target: str):
    try:
        await cli.join_chat(target)
    except RPCError:
        # ignore: already in / invite-only / requires approval
        pass

async def _send_with(client: Client, target: str, text: str) -> bool:
    try:
        await client.send_message(target, text)
        await asyncio.sleep(random.uniform(0.6, 1.4))
        return True
    except FloodWait as fw:
        await asyncio.sleep(fw.value + 3)
        return False
    except (ChatWriteForbidden, ChannelPrivate, UserDeactivated, AuthKeyUnregistered):
        return False
    except RPCError:
        return False

# -------- main loop --------
async def loop_worker():
    _ensure_user_stats_columns()
    await _close_dead_clients()

    while True:
        try:
            if _is_quiet_hours_now():
                await asyncio.sleep(30)
                continue

            user_ids = _get_all_user_ids()
            random.shuffle(user_ids)

            for uid in user_ids:
                # 0) must-join gate — skip forwarding if user isn't subscribed
                if not await _is_user_subscribed(uid):
                    k = f"remind:{uid}:sub"
                    if _should_remind(k, mins=180):
                        try:
                            await control_bot.send_message(
                                uid,
                                "🔒 Please join @PhiloBots and @TheTrafficZone to continue forwarding."
                            )
                            _touch_remind(k)
                        except Exception:
                            pass
                    # don't advance schedule; check again next cycle
                    continue

                # 1) interval / due
                interval = get_interval(uid) or 30
                if interval not in VALID_INTERVALS:
                    interval = 30
                if not _is_due(uid, interval):
                    continue

                # 2) inputs
                ad_text = (get_ad(uid) or "").strip()
                groups  = [ _normalize_target(g) for g in list_groups(uid) ][:GROUP_LIMIT]
                if not groups or not ad_text:
                    # nothing meaningful; advance schedule to avoid hot loop
                    _set_last_sent_now(uid)
                    continue

                # 3) sessions
                sessions = _get_user_sessions(uid)
                if not sessions:
                    k = f"remind:{uid}:session"
                    if _should_remind(k, mins=180):
                        try:
                            await control_bot.send_message(uid, "⚠️ No session found — login into @SpinifyLoginBot to start forwarding.")
                            _touch_remind(k)
                        except Exception:
                            pass
                    # don't advance schedule; we want to remind again later
                    continue

                # 4) connect clients
                clients = []
                for row in sessions:
                    try:
                        slot = int(row["slot"])
                        api_id = int(row["api_id"] or 0)
                        api_hash = str(row["api_hash"] or "")
                        sess = str(row["session_string"] or "")
                        if api_id and api_hash and sess:
                            cli = await _ensure_client(uid, slot, api_id, api_hash, sess)
                            clients.append(cli)
                    except Exception:
                        pass
                if not clients:
                    # all failed to connect
                    k = f"remind:{uid}:session"
                    if _should_remind(k, mins=180):
                        try:
                            await control_bot.send_message(uid, "⚠️ Your session couldn’t connect. Please re-login via @SpinifyLoginBot.")
                            _touch_remind(k)
                        except Exception:
                            pass
                    continue

                # 5) forward round-robin
                ok_count = 0
                for i, g in enumerate(groups):
                    cli = clients[i % len(clients)]
                    await _maybe_join(cli, g)
                    sent = await _send_with(cli, g, ad_text)
                    if sent:
                        ok_count += 1

                if ok_count > 0:
                    _mark_sent_ok(uid, ok_count)

                # 6) schedule next time
                _set_last_sent_now(uid)

            await asyncio.sleep(5)

        except Exception:
            # never crash; light backoff
            await asyncio.sleep(2)
