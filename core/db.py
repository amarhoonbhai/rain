# core/db.py
# SQLite helpers & bot storage
# Tables:
#   users(user_id PK, username, created_at)
#   user_sessions(user_id, slot, api_id, api_hash, session_string)  PK(user_id,slot)
#   user_groups(id PK, user_id, value, UNIQUE(user_id,value))
#   settings(key PK, val)
#   user_stats(user_id PK, sent_ok INT)
#
# Exposed fns match all current callers in main_bot.py / worker_forward.py / login_bot.py.

from __future__ import annotations
import os, sqlite3, time
from datetime import datetime

_DB_PATH = os.getenv("RAIN_DB_PATH", os.path.join(os.getcwd(), "rain.db"))

# ---------------- connection ----------------
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(_DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def _exec(conn: sqlite3.Connection, sql: str, args: tuple = ()):
    cur = conn.execute(sql, args)
    conn.commit()
    return cur

# ---------------- init ----------------
def init_db():
    conn = get_conn()
    _exec(conn, "PRAGMA journal_mode=WAL;")
    _exec(conn, """
    CREATE TABLE IF NOT EXISTS users(
        user_id   INTEGER PRIMARY KEY,
        username  TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );""")
    _exec(conn, """
    CREATE TABLE IF NOT EXISTS user_sessions(
        user_id        INTEGER NOT NULL,
        slot           INTEGER NOT NULL,
        api_id         INTEGER NOT NULL,
        api_hash       TEXT    NOT NULL,
        session_string TEXT    NOT NULL,
        PRIMARY KEY(user_id, slot)
    );""")
    _exec(conn, """
    CREATE TABLE IF NOT EXISTS user_groups(
        id       INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id  INTEGER NOT NULL,
        value    TEXT    NOT NULL,
        UNIQUE(user_id, value)
    );""")
    _exec(conn, """
    CREATE TABLE IF NOT EXISTS settings(
        key TEXT PRIMARY KEY,
        val TEXT
    );""")
    _exec(conn, """
    CREATE TABLE IF NOT EXISTS user_stats(
        user_id INTEGER PRIMARY KEY,
        sent_ok INTEGER NOT NULL DEFAULT 0
    );""")
    conn.close()

# ---------------- users ----------------
def ensure_user(user_id: int, username: str | None):
    conn = get_conn()
    _exec(conn, """
    INSERT INTO users(user_id, username) VALUES(?, ?)
    ON CONFLICT(user_id) DO UPDATE SET username=excluded.username;""", (user_id, username))
    conn.close()

def users_count() -> int:
    conn = get_conn()
    c = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
    conn.close()
    return int(c or 0)

# ---------------- sessions ----------------
def sessions_upsert_slot(user_id: int, slot: int, api_id: int, api_hash: str, session_string: str):
    conn = get_conn()
    _exec(conn, """
    INSERT INTO user_sessions(user_id, slot, api_id, api_hash, session_string)
    VALUES(?,?,?,?,?)
    ON CONFLICT(user_id,slot) DO UPDATE SET
        api_id=excluded.api_id, api_hash=excluded.api_hash, session_string=excluded.session_string;""",
        (user_id, slot, api_id, api_hash, session_string))
    conn.close()

def sessions_delete(user_id: int, slot: int):
    conn = get_conn()
    _exec(conn, "DELETE FROM user_sessions WHERE user_id=? AND slot=?", (user_id, slot))
    conn.close()

def sessions_list(user_id: int) -> list[sqlite3.Row]:
    conn = get_conn()
    rows = conn.execute("SELECT user_id, slot, api_id FROM user_sessions WHERE user_id=? ORDER BY slot", (user_id,)).fetchall()
    conn.close()
    return rows

def sessions_strings(user_id: int) -> list[dict]:
    conn = get_conn()
    rows = conn.execute("""
        SELECT user_id, slot, api_id, api_hash, session_string
        FROM user_sessions WHERE user_id=? ORDER BY slot""", (user_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def sessions_count_user(user_id: int) -> int:
    conn = get_conn()
    c = conn.execute("SELECT COUNT(*) AS c FROM user_sessions WHERE user_id=?", (user_id,)).fetchone()["c"]
    conn.close()
    return int(c or 0)

def sessions_count() -> int:
    conn = get_conn()
    c = conn.execute("SELECT COUNT(DISTINCT user_id) AS c FROM user_sessions").fetchone()["c"]
    conn.close()
    return int(c or 0)

def first_free_slot(user_id: int, max_slots: int = 3) -> int | None:
    used = {r["slot"] for r in sessions_list(user_id)}
    for s in range(1, max_slots+1):
        if s not in used:
            return s
    return None

def users_with_sessions() -> list[int]:
    conn = get_conn()
    rows = conn.execute("SELECT DISTINCT user_id FROM user_sessions ORDER BY user_id").fetchall()
    conn.close()
    return [int(r["user_id"]) for r in rows]

# ---------------- groups ----------------
def add_group(user_id: int, value: str) -> int:
    value = (value or "").strip()
    if not value:
        return 0
    conn = get_conn()
    try:
        _exec(conn, "INSERT OR IGNORE INTO user_groups(user_id, value) VALUES(?,?)", (user_id, value))
        c = conn.execute("SELECT changes() AS c").fetchone()["c"]
        return int(c or 0)
    finally:
        conn.close()

def clear_groups(user_id: int):
    conn = get_conn()
    _exec(conn, "DELETE FROM user_groups WHERE user_id=?", (user_id,))
    conn.close()

def list_groups(user_id: int) -> list[str]:
    conn = get_conn()
    rows = conn.execute("SELECT value FROM user_groups WHERE user_id=? ORDER BY id", (user_id,)).fetchall()
    conn.close()
    return [r["value"] for r in rows]

# ---------------- settings (KV) ----------------
def set_setting(key: str, val):
    # store as plain text
    conn = get_conn()
    _exec(conn, "INSERT INTO settings(key,val) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET val=excluded.val",
          (key, str(val)))
    conn.close()

def get_setting(key: str, default=None):
    conn = get_conn()
    row = conn.execute("SELECT val FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    if row is None:
        return default
    return row["val"]

# ---------------- interval / pause / timing ----------------
def set_interval(user_id: int, minutes: int):
    set_setting(f"user:{user_id}:interval", int(minutes))

def get_interval(user_id: int) -> int | None:
    v = get_setting(f"user:{user_id}:interval", None)
    try:
        return int(v) if v is not None else None
    except Exception:
        return None

def mark_sent_now(user_id: int):
    set_setting(f"user:{user_id}:last_sent_at", int(time.time()))

def get_last_sent_at(user_id: int) -> int | None:
    v = get_setting(f"user:{user_id}:last_sent_at", None)
    try:
        return int(v) if v is not None else None
    except Exception:
        return None

# ---------------- night mode (global) ----------------
def night_enabled() -> bool:
    v = str(get_setting("global:night", "0")).lower()
    return v in ("1","true","yes","on")

def set_night_enabled(on: bool):
    set_setting("global:night", "1" if on else "0")

# ---------------- stats ----------------
def inc_sent_ok(user_id: int, n: int = 1):
    conn = get_conn()
    _exec(conn, "INSERT INTO user_stats(user_id, sent_ok) VALUES(?,0) ON CONFLICT(user_id) DO NOTHING", (user_id,))
    _exec(conn, "UPDATE user_stats SET sent_ok = sent_ok + ? WHERE user_id=?", (int(n), user_id))
    conn.close()

def get_total_sent_ok() -> int:
    conn = get_conn()
    row = conn.execute("SELECT SUM(sent_ok) AS s FROM user_stats").fetchone()
    conn.close()
    return int(row["s"] or 0)

def top_users(n: int = 10) -> list[sqlite3.Row]:
    n = max(1, min(100, int(n)))
    conn = get_conn()
    rows = conn.execute("""
        SELECT us.user_id, us.sent_ok
        FROM user_stats us
        ORDER BY us.sent_ok DESC, us.user_id ASC
        LIMIT ?;
    """, (n,)).fetchall()
    conn.close()
    return rows
