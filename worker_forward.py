# worker_forward.py — interval forwarder with night mode, 3-session rotation, group cap=5
import asyncio
import random
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

from pyrogram import Client
from pyrogram.errors import FloodWait, UserDeactivated, AuthKeyUnregistered, ChatWriteForbidden, ChannelPrivate, RPCError

from core.db import (
    get_conn, get_ad, list_groups, get_interval,
    get_global_night_mode, count_user_sessions
)

# Reuse the same aiogram bot to DM reminders
from main_bot import bot as control_bot

IST = ZoneInfo("Asia/Kolkata")
QUIET_START = dtime(0, 0)
QUIET_END   = dtime(7, 0)
GROUP_LIMIT = 5

# ---------------- internal DB helpers (settings-based scheduler + stats) ----------------
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
    """Add sent_ok and last_sent_at to users if missing (safe, idempotent)."""
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

# ---------------- time / night mode helpers ----------------
def _is_quiet_hours_now() -> bool:
    if not get_global_night_mode():  # owner toggle
        return False
    now = datetime.now(IST).time()
    return QUIET_START <= now < QUIET_END  # 00:00–07:00 IST

def _is_due(user_id: int, minutes: int) -> bool:
    last = _get_last_sent_ts(user_id)
    if not last:
        return True
    return datetime.utcnow() - last >= timedelta(minutes=minutes)

# ---------------- pyrogram client pool ----------------
_client_pool: dict[tuple[int,int], Client] = {}  # (user_id, slot) -> Client

async def _ensure_client(user_id: int, slot: int, api_id: int, api_hash: str, session_string: str) -> Client:
    key = (user_id, slot)
    cli = _client_pool.get(key)
    if cli and cli.is_connected:
        return cli
    # Build a new client bound to the saved string session
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
    if s.startswith("https://
