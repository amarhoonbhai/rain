# core/db.py — Spinify stack DB helpers (CLEAN, FINAL)
from __future__ import annotations
import os, json, sqlite3
from typing import Any, List, Dict, Tuple, Iterator

DB_PATH = os.getenv("DB_PATH", "./data.db")
os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)

def _row_factory(cursor, row):
    return {d[0]: row[i] for i, d in enumerate(cursor.description)}

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
    conn.row_factory = _row_factory
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

# ---------- schema ----------
def _ensure_tables(conn: sqlite3.Connection):
    c = conn.cursor()

    # Users
    c.execute("""
    CREATE TABLE IF NOT EXISTS users(
        user_id     INTEGER PRIMARY KEY,
        username    TEXT,
        created_at  INTEGER DEFAULT (strftime('%s','now')),
        sent_ok     INTEGER DEFAULT 0
    )""")

    # User sessions (up to 3 slots per user)
    c.execute("""
    CREATE TABLE IF NOT EXISTS user_sessions(
        user_id        INTEGER,
        slot           INTEGER,
        api_id         INTEGER,
        api_hash       TEXT,
        session_string TEXT,
        updated_at     INTEGER DEFAULT (strftime('%s','now')),
        PRIMARY KEY(user_id, slot)
    )""")

    # Groups per user
    c.execute("""
    CREATE TABLE IF NOT EXISTS user_groups(
        user_id  INTEGER,
        group_id TEXT,
        added_at INTEGER DEFAULT (strftime('%s','now')),
        UNIQUE(user_id, group_id)
    )""")

    # Per-user interval (minutes)
    c.execute("""
    CREATE TABLE IF NOT EXISTS intervals(
        user_id INTEGER PRIMARY KEY,
        minutes INTEGER
    )""")

    # Global key/value settings
    c.execute("""
    CREATE TABLE IF NOT EXISTS settings(
        key TEXT PRIMARY KEY,
        val TEXT
    )""")

    # Ads per user (text + parse_mode)
    c.execute("""
    CREATE TABLE IF NOT EXISTS ads(
        user_id    INTEGER PRIMARY KEY,
        text       TEXT,
        parse_mode TEXT,
        updated_at INTEGER DEFAULT (strftime('%s','now'))
    )""")

    # Worker last-send timestamps
    c.execute("""
    CREATE TABLE IF NOT EXISTS worker_state(
        user_id      INTEGER PRIMARY KEY,
        last_sent_at INTEGER
    )""")

    conn.commit()

def _ensure_indexes(conn: sqlite3.Connection):
    c = conn.cursor()
    c.execute("CREATE INDEX IF NOT EXISTS idx_user_groups_uid ON user_groups(user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sessions_uid ON user_sessions(user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_intervals_uid ON intervals(user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_users_sentok ON users(sent_ok DESC)")
    conn.commit()

def init_db():
    conn = get_conn()
    _ensure_tables(conn); _ensure_indexes(conn)
    conn.close()

# ---------- users & stats ----------
def ensure_user(user_id: int, username: str | None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO users(user_id, username) VALUES(?,?)", (user_id, username))
    if username is not None:
        cur.execute("UPDATE users SET username=? WHERE user_id=?", (username, user_id))
    conn.commit(); conn.close()

def users_count() -> int:
    conn = get_conn()
    n = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
    conn.close(); return int(n or 0)

def all_user_ids() -> List[int]:
    conn = get_conn()
    rows = conn.execute("SELECT user_id FROM users").fetchall()
    conn.close(); return [r["user_id"] for r in rows]

def inc_sent_ok(user_id: int, add: int = 1):
    conn = get_conn()
    conn.execute("UPDATE users SET sent_ok=COALESCE(sent_ok,0)+? WHERE user_id=?", (add, user_id))
    conn.commit(); conn.close()

def get_total_sent_ok() -> int:
    conn = get_conn()
    s = conn.execute("SELECT SUM(sent_ok) AS s FROM users").fetchone()["s"]
    conn.close(); return int(s or 0)

def top_users(n: int = 10) -> List[Dict]:
    n = max(1, min(100, int(n)))
    conn = get_conn()
    rows = conn.execute(
        "SELECT user_id, sent_ok FROM users ORDER BY sent_ok DESC, user_id ASC LIMIT ?",
        (n,)
    ).fetchall()
    conn.close(); return rows

# ---------- sessions (3 slots) ----------
def sessions_list(user_id: int) -> List[Dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT slot, api_id, api_hash FROM user_sessions WHERE user_id=? ORDER BY slot",
        (user_id,)
    ).fetchall()
    conn.close(); return rows

def get_user_sessions_full(user_id: int) -> List[Dict]:
    conn = get_conn()
    rows = conn.execute("""
        SELECT slot, api_id, api_hash, session_string
        FROM user_sessions
        WHERE user_id=?
        ORDER BY slot
    """, (user_id,)).fetchall()
    conn.close(); return rows

def sessions_iter_full() -> Iterator[Dict]:
    conn = get_conn()
    rows = conn.execute("""
        SELECT user_id, slot, api_id, api_hash, session_string
        FROM user_sessions
        ORDER BY user_id, slot
    """).fetchall()
    conn.close()
    for r in rows:
        yield r

def sessions_delete(user_id: int, slot: int):
    conn = get_conn()
    conn.execute("DELETE FROM user_sessions WHERE user_id=? AND slot=?", (user_id, slot))
    conn.commit(); conn.close()

def sessions_count() -> int:
    conn = get_conn()
    n = conn.execute("SELECT COUNT(DISTINCT user_id) AS c FROM user_sessions").fetchone()["c"]
    conn.close(); return int(n or 0)

def sessions_count_user(user_id: int) -> int:
    conn = get_conn()
    n = conn.execute("SELECT COUNT(*) AS c FROM user_sessions WHERE user_id=?", (user_id,)).fetchone()["c"]
    conn.close(); return int(n or 0)

def first_free_slot(user_id: int) -> int:
    used = {r["slot"] for r in sessions_list(user_id)}
    for s in (1, 2, 3):
        if s not in used:
            return s
    return 0

def sessions_upsert_slot(user_id: int, slot: int, api_id: int, api_hash: str, session_string: str):
    conn = get_conn()
    conn.execute
