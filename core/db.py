# core/db.py — schema & helpers (counters + stats + sessions)
import sqlite3
from pathlib import Path
from typing import Iterable, Optional

_DB_PATH = Path(__file__).resolve().parent.parent / "spinify.db"

def get_conn() -> sqlite3.Connection:
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def init_db() -> None:
    conn = get_conn()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id          INTEGER PRIMARY KEY,
            username         TEXT,
            ad_message       TEXT,
            interval_minutes INTEGER DEFAULT 30,
            last_sent_at     TEXT,
            sent_ok          INTEGER DEFAULT 0,
            sent_fail        INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS user_groups (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id    INTEGER NOT NULL,
            group_link TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS user_sessions (
            user_id        INTEGER PRIMARY KEY,
            api_id         INTEGER NOT NULL,
            api_hash       TEXT NOT NULL,
            session_string TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            val TEXT
        );
        """
    )
    # ensure global counter key exists
    conn.execute("INSERT INTO settings(key, val) VALUES('total_sent_ok','0') ON CONFLICT(key) DO NOTHING")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_user_groups_user_id ON user_groups(user_id);")
    conn.commit(); conn.close()

# -------- users ----------
def upsert_user(user_id: int, username: Optional[str] = None) -> None:
    conn = get_conn()
    cur = conn.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
    if cur.fetchone():
        if username is not None:
            conn.execute("UPDATE users SET username = ? WHERE user_id = ?", (username, user_id))
    else:
        conn.execute("INSERT INTO users (user_id, username) VALUES (?, ?)", (user_id, username))
    conn.commit(); conn.close()

def users_count() -> int:
    conn = get_conn(); row = conn.execute("SELECT COUNT(*) c FROM users").fetchone()
    conn.close(); return row["c"]

# -------- ad & interval ----------
def set_ad(user_id: int, text: str) -> None:
    conn = get_conn()
    conn.execute("UPDATE users SET ad_message = ? WHERE user_id = ?", (text, user_id))
    conn.commit(); conn.close()

def get_ad(user_id: int) -> str:
    conn = get_conn()
    row = conn.execute("SELECT ad_message FROM users WHERE user_id = ?", (user_id,)).fetchone()
    conn.close(); return row["ad_message"] if row and row["ad_message"] else ""

def set_interval(user_id: int, minutes: int) -> None:
    conn = get_conn()
    conn.execute("UPDATE users SET interval_minutes = ? WHERE user_id = ?", (minutes, user_id))
    conn.commit(); conn.close()

def get_interval(user_id: int) -> int:
    conn = get_conn()
    row = conn.execute("SELECT interval_minutes FROM users WHERE user_id = ?", (user_id,)).fetchone()
    conn.close(); return int(row["interval_minutes"]) if row and row["interval_minutes"] else 30

# -------- groups ----------
def add_groups(user_id: int, groups: Iterable[str]) -> int:
    conn = get_conn(); n = 0
    for g in groups:
        g = (g or "").strip()
        if not g: continue
        conn.execute("INSERT INTO user_groups (user_id, group_link) VALUES (?, ?)", (user_id, g))
        n += 1
    conn.commit(); conn.close(); return n

def list_groups(user_id: int):
    conn = get_conn()
    rows = conn.execute("SELECT group_link FROM user_groups WHERE user_id = ?", (user_id,)).fetchall()
    conn.close(); return [r["group_link"] for r in rows]

def clear_groups(user_id: int) -> None:
    conn = get_conn()
    conn.execute("DELETE FROM user_groups WHERE user_id = ?", (user_id,))
    conn.commit(); conn.close()

# -------- sessions ----------
def list_active_sessions():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM user_sessions").fetchall()
    conn.close(); return rows

def sessions_count() -> int:
    conn = get_conn()
    row = conn.execute("SELECT COUNT(*) c FROM user_sessions").fetchone()
    conn.close(); return row["c"]

# -------- counters & stats ----------
def inc_sent(user_id: int, ok: int, fail: int) -> None:
    if ok == 0 and fail == 0: return
    conn = get_conn()
    conn.execute(
        "UPDATE users SET sent_ok = COALESCE(sent_ok,0) + ?, sent_fail = COALESCE(sent_fail,0) + ? WHERE user_id = ?",
        (ok, fail, user_id),
    )
    conn.execute("UPDATE settings SET val = CAST(COALESCE(val,'0') AS INTEGER) + ? WHERE key = 'total_sent_ok'", (ok,))
    conn.commit(); conn.close()

def get_total_sent_ok() -> int:
    conn = get_conn()
    row = conn.execute("SELECT val FROM settings WHERE key = 'total_sent_ok'").fetchone()
    conn.close()
    try: return int(row["val"]) if row and row["val"] is not None else 0
    except: return 0

def top_users(limit: int = 10):
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT user_id, username, sent_ok, sent_fail, last_sent_at
        FROM users
        ORDER BY COALESCE(sent_ok,0) DESC, COALESCE(last_sent_at,'') DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close(); return rows

def update_last_sent(user_id: int, iso: str) -> None:
    conn = get_conn()
    conn.execute("UPDATE users SET last_sent_at = ? WHERE user_id = ?", (iso, user_id))
    conn.commit(); conn.close()
