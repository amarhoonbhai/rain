# core/db.py  — SQLite helpers for Spinify bots
# Tables: users, user_sessions (slots 1..3), user_groups, settings
# Exposes: init_db(), get_conn(), session & settings helpers used by main/login/worker

from __future__ import annotations
import os, sqlite3, pathlib
from typing import Iterable, List, Dict, Any

DB_PATH = os.getenv("DB_PATH", str(pathlib.Path(__file__).resolve().parent / "bot.db"))

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ---------- bootstrap / migrations ----------
def _colnames(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}

def init_db() -> None:
    conn = get_conn()
    cur = conn.cursor()

    # settings key/value
    cur.execute("""
        CREATE TABLE IF NOT EXISTS settings(
            key TEXT PRIMARY KEY,
            val TEXT
        )
    """)

    # users (one row per bot user)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY,
            is_owner INTEGER DEFAULT 0,
            interval_min INTEGER DEFAULT 30,     -- 30/45/60
            ad_text TEXT,
            sent_ok INTEGER DEFAULT 0,
            last_sent_at TEXT
        )
    """)
    # ensure cols exist if older DB
    have = _colnames(conn, "users")
    if "interval_min" not in have:
        cur.execute("ALTER TABLE users ADD COLUMN interval_min INTEGER DEFAULT 30")
    if "sent_ok" not in have:
        cur.execute("ALTER TABLE users ADD COLUMN sent_ok INTEGER DEFAULT 0")
    if "last_sent_at" not in have:
        cur.execute("ALTER TABLE users ADD COLUMN last_sent_at TEXT")

    # user_sessions (up to 3 slots per user)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_sessions(
            user_id INTEGER,
            slot INTEGER,                         -- 1..3
            api_id INTEGER,
            api_hash TEXT,
            session_string TEXT,
            PRIMARY KEY(user_id, slot)
        )
    """)
    # migrate: add slot if missing (legacy single-session)
    have = _colnames(conn, "user_sessions")
    if "slot" not in have:
        cur.execute("ALTER TABLE user_sessions ADD COLUMN slot INTEGER")
        cur.execute("UPDATE user_sessions SET slot=1 WHERE slot IS NULL")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_user_sessions ON user_sessions(user_id, slot)")

    # groups to forward to (max 5)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_groups(
            user_id INTEGER,
            target TEXT,
            PRIMARY KEY(user_id, target)
        )
    """)

    conn.commit()
    conn.close()

# ---------- generic settings ----------
def get_setting(key: str, default: str | None = None) -> str | None:
    conn = get_conn()
    row = conn.execute("SELECT val FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    return row["val"] if row and row["val"] is not None else default

def set_setting(key: str, val: str) -> None:
    conn = get_conn()
    conn.execute(
        "INSERT INTO settings(key,val) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET val=excluded.val",
        (key, val),
    )
    conn.commit(); conn.close()

# ---------- night mode (global owner toggle) ----------
def get_global_night_mode() -> bool:
    return (get_setting("global_night_mode", "0") == "1")

def set_global_night_mode(on: bool) -> None:
    set_setting("global_night_mode", "1" if on else "0")

# ---------- users: interval/ad/stats ----------
def ensure_user(user_id: int) -> None:
    conn = get_conn()
    conn.execute("INSERT OR IGNORE INTO users(user_id) VALUES(?)", (user_id,))
    conn.commit(); conn.close()

def set_interval(user_id: int, minutes: int) -> None:
    ensure_user(user_id)
    if minutes not in (30, 45, 60):
        minutes = 30
    conn = get_conn()
    conn.execute("UPDATE users SET interval_min=? WHERE user_id=?", (minutes, user_id))
    conn.commit(); conn.close()

def get_interval(user_id: int) -> int | None:
    conn = get_conn()
    row = conn.execute("SELECT interval_min FROM users WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    return (row["interval_min"] if row else None)

def set_ad(user_id: int, text: str) -> None:
    ensure_user(user_id)
    conn = get_conn()
    conn.execute("UPDATE users SET ad_text=? WHERE user_id=?", (text, user_id))
    conn.commit(); conn.close()

def get_ad(user_id: int) -> str | None:
    conn = get_conn()
    row = conn.execute("SELECT ad_text FROM users WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    return (row["ad_text"] if row else None)

def add_sent_ok(user_id: int, inc: int = 1) -> None:
    ensure_user(user_id)
    conn = get_conn()
    conn.execute(
        "UPDATE users SET sent_ok=COALESCE(sent_ok,0)+?, last_sent_at=datetime('now') WHERE user_id=?",
        (inc, user_id),
    )
    conn.commit(); conn.close()

# ---------- user groups (cap enforced in UI/worker) ----------
def add_group(user_id: int, target: str) -> None:
    ensure_user(user_id)
    conn = get_conn()
    conn.execute("INSERT OR IGNORE INTO user_groups(user_id, target) VALUES(?,?)", (user_id, target.strip()))
    conn.commit(); conn.close()

def remove_group(user_id: int, target: str) -> None:
    conn = get_conn()
    conn.execute("DELETE FROM user_groups WHERE user_id=? AND target=?", (user_id, target.strip()))
    conn.commit(); conn.close()

def list_groups(user_id: int) -> list[str]:
    conn = get_conn()
    rows = conn.execute("SELECT target FROM user_groups WHERE user_id=? ORDER BY target", (user_id,)).fetchall()
    conn.close()
    return [r["target"] for r in rows]

# ---------- sessions (slots 1..3) ----------
def list_user_sessions(user_id: int) -> List[sqlite3.Row]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT slot, api_id, api_hash, length(session_string) as slen "
        "FROM user_sessions WHERE user_id=? ORDER BY slot", (user_id,)
    ).fetchall()
    conn.close()
    return rows

def first_free_slot(user_id: int) -> int | None:
    used = {r["slot"] for r in list_user_sessions(user_id)}
    for s in (1, 2, 3):
        if s not in used:
            return s
    return None

def count_user_sessions(user_id: int) -> int:
    conn = get_conn()
    row = conn.execute("SELECT COUNT(*) AS c FROM user_sessions WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    return int(row["c"] if row else 0)

def upsert_session_slot(user_id: int, api_id: int, api_hash: str, session_string: str, slot: int | None = None) -> int | None:
    """Save into provided slot; if None, pick first free slot. Returns slot or None if full."""
    ensure_user(user_id)
    if slot is None:
        slot = first_free_slot(user_id)
        if slot is None:
            return None
    if slot not in (1,2,3):
        slot = 1
    conn = get_conn()
    conn.execute(
        "INSERT INTO user_sessions(user_id, slot, api_id, api_hash, session_string) "
        "VALUES(?,?,?,?,?) "
        "ON CONFLICT(user_id, slot) DO UPDATE SET api_id=excluded.api_id, api_hash=excluded.api_hash, session_string=excluded.session_string",
        (user_id, slot, api_id, api_hash, session_string),
    )
    conn.commit(); conn.close()
    return slot

def delete_session_slot(user_id: int, slot: int) -> int:
    conn = get_conn()
    cur = conn.execute("DELETE FROM user_sessions WHERE user_id=? AND slot=?", (user_id, slot))
    conn.commit(); conn.close()
    return cur.rowcount
