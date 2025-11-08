# core/db.py
# SQLite store for Spinify stack (users, sessions, groups, ads, settings, stats)
import os
import sqlite3
import time
from typing import Optional, List, Tuple, Dict, Any

# -------- DB path --------
DB_PATH = os.getenv("DB_PATH", os.path.join(os.getcwd(), "data.db"))

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn

# -------- bootstrap --------
def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # users
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id     INTEGER PRIMARY KEY,
        username    TEXT,
        created_at  INTEGER,
        last_seen   INTEGER
    )""")

    # per-user multi sessions (slots 1..3)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS user_sessions (
        user_id        INTEGER,
        slot           INTEGER,
        api_id         INTEGER,
        api_hash       TEXT,
        session_string TEXT,
        updated_at     INTEGER,
        PRIMARY KEY (user_id, slot)
    )""")

    # groups list (cap enforced in code)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS groups (
        user_id    INTEGER,
        g          TEXT,
        created_at INTEGER,
        UNIQUE(user_id, g)
    )""")

    # key-value settings (string values)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        val TEXT
    )""")

    # ads per user (text + parse mode)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ads (
        user_id INTEGER PRIMARY KEY,
        text    TEXT,
        mode    TEXT
    )""")

    # per-user stats
    cur.execute("""
    CREATE TABLE IF NOT EXISTS stats_user (
        user_id      INTEGER PRIMARY KEY,
        sent_ok      INTEGER DEFAULT 0,
        last_sent_at INTEGER
    )""")

    # helpful indices
    cur.execute("CREATE INDEX IF NOT EXISTS idx_groups_uid ON groups(user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sessions_uid ON user_sessions(user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_settings_key ON settings(key)")

    conn.commit()
    conn.close()

# -------- users --------
def ensure_user(user_id: int, username: Optional[str]):
    now = int(time.time())
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM users WHERE user_id=?", (user_id,))
    if cur.fetchone() is None:
        cur.execute("INSERT INTO users(user_id, username, created_at, last_seen) VALUES (?,?,?,?)",
                    (user_id, username, now, now))
    else:
        cur.execute("UPDATE users SET username=?, last_seen=? WHERE user_id=?",
                    (username, now, user_id))
    conn.commit()
    conn.close()

def users_count() -> int:
    conn = get_conn()
    r = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()
    conn.close()
    return int(r["c"] if r else 0)

# -------- sessions (multi-slot up to 3) --------
def first_free_slot(user_id: int, max_slots: int = 3) -> Optional[int]:
    conn = get_conn()
    rows = conn.execute("SELECT slot FROM user_sessions WHERE user_id=? ORDER BY slot", (user_id,)).fetchall()
    conn.close()
    used = {int(r["slot"]) for r in rows}
    for s in range(1, max_slots + 1):
        if s not in used:
            return s
    return None

def upsert_session_slot(user_id: int, slot: Optional[int], api_id: int, api_hash: str, session_string: str) -> int:
    """Insert/update a session at given slot; if slot is None, pick first free (default 1)."""
    if slot is None:
        slot = first_free_slot(user_id) or 1
    now = int(time.time())
    conn = get_conn()
    conn.execute("""
        INSERT INTO user_sessions(user_id, slot, api_id, api_hash, session_string, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id, slot) DO UPDATE SET
            api_id=excluded.api_id,
            api_hash=excluded.api_hash,
            session_string=excluded.session_string,
            updated_at=excluded.updated_at
    """, (user_id, slot, int(api_id), str(api_hash), str(session_string), now))
    conn.commit()
    conn.close()
    return slot

# single-slot helper (slot=1) for legacy flows
def upsert_user_session(user_id: int, api_id: int, api_hash: str, session_string: str) -> int:
    return upsert_session_slot(user_id, 1, api_id, api_hash, session_string)

def sessions_delete(user_id: int, slot: int) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM user_sessions WHERE user_id=? AND slot=?", (user_id, slot))
    conn.commit()
    n = cur.rowcount
    conn.close()
    return n

def sessions_list(user_id: int) -> List[sqlite3.Row]:
    conn = get_conn()
    rows = conn.execute("""
        SELECT user_id, slot, api_id, api_hash, session_string
        FROM user_sessions WHERE user_id=? ORDER BY slot
    """, (user_id,)).fetchall()
    conn.close()
    return rows

def sessions_strings(user_id: int) -> List[Dict[str, Any]]:
    conn = get_conn()
    rows = conn.execute("""
        SELECT user_id, slot, api_id, api_hash, session_string
        FROM user_sessions WHERE user_id=? ORDER BY slot
    """, (user_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def sessions_count_user(user_id: int) -> int:
    conn = get_conn()
    r = conn.execute("SELECT COUNT(*) AS c FROM user_sessions WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    return int(r["c"] if r else 0)

def sessions_count() -> int:
    """# users with ≥1 session."""
    conn = get_conn()
    r = conn.execute("SELECT COUNT(DISTINCT user_id) AS c FROM user_sessions").fetchone()
    conn.close()
    return int(r["c"] if r else 0)

def users_with_sessions() -> List[int]:
    conn = get_conn()
    rows = conn.execute("SELECT DISTINCT user_id FROM user_sessions ORDER BY user_id").fetchall()
    conn.close()
    return [int(r["user_id"]) for r in rows]

# -------- groups (cap 5) --------
def groups_cap() -> int:
    return 5

def list_groups(user_id: int) -> List[str]:
    conn = get_conn()
    rows = conn.execute("SELECT g FROM groups WHERE user_id=? ORDER BY created_at", (user_id,)).fetchall()
    conn.close()
    return [r["g"] for r in rows]

def add_group(user_id: int, g: str) -> int:
    g = (g or "").strip()
    if not g:
        return 0
    # enforce cap
    if len(list_groups(user_id)) >= groups_cap():
        return 0
    conn = get_conn()
    try:
        conn.execute("INSERT OR IGNORE INTO groups(user_id, g, created_at) VALUES (?,?,?)",
                     (user_id, g, int(time.time())))
        conn.commit()
        # detect actual insert
        r = conn.execute("SELECT changes() AS c").fetchone()
        return 1 if r and int(r["c"]) > 0 else 0
    finally:
        conn.close()

def clear_groups(user_id: int):
    conn = get_conn()
    conn.execute("DELETE FROM groups WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()

# -------- per-user interval --------
def set_interval(user_id: int, mins: int):
    set_setting(f"user:{user_id}:interval", int(mins))

def get_interval(user_id: int) -> Optional[int]:
    v = get_setting(f"user:{user_id}:interval", None)
    try:
        return int(v) if v is not None else None
    except Exception:
        return None

# -------- ads --------
def set_ad(user_id: int, text: str, mode: Optional[str]):
    conn = get_conn()
    conn.execute("""
        INSERT INTO ads(user_id, text, mode) VALUES (?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET text=excluded.text, mode=excluded.mode
    """, (user_id, text, mode))
    conn.commit()
    conn.close()

def get_ad(user_id: int) -> Tuple[Optional[str], Optional[str]]:
    conn = get_conn()
    r = conn.execute("SELECT text, mode FROM ads WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    if r:
        return (r["text"], r["mode"])
    return (None, None)

# -------- settings (KV) --------
def set_setting(key: str, val: Any):
    sval = str(val) if not isinstance(val, str) else val
    conn = get_conn()
    conn.execute("""
        INSERT INTO settings(key, val) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET val=excluded.val
    """, (key, sval))
    conn.commit()
    conn.close()

def get_setting(key: str, default=None):
    conn = get_conn()
    r = conn.execute("SELECT val FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    if r is None:
        return default
    return r["val"]

# -------- night mode (global) --------
def night_enabled() -> bool:
    v = get_setting("global:night_enabled", "0")
    return str(v).lower() in ("1", "true", "yes", "on")

def set_night_enabled(flag: bool):
    set_setting("global:night_enabled", 1 if flag else 0)

# -------- gate channels (two slots; caller may apply defaults) --------
def get_gate_channels_effective() -> Tuple[Optional[str], Optional[str]]:
    ch1 = get_setting("gate:ch1", None)
    ch2 = get_setting("gate:ch2", None)
    return (ch1, ch2)

# -------- premium name-lock --------
def set_name_lock(user_id: int, enabled: bool, name: Optional[str] = None):
    set_setting(f"name_lock:{user_id}:enabled", 1 if enabled else 0)
    if name is not None:
        set_setting(f"name_lock:{user_id}:name", name)

def get_name_lock(user_id: int) -> Tuple[bool, Optional[str]]:
    en = str(get_setting(f"name_lock:{user_id}:enabled", "0")).lower() in ("1","true","yes","on")
    nm = get_setting(f"name_lock:{user_id}:name", None)
    return en, nm

# -------- stats (forwarding) --------
def get_last_sent_at(user_id: int) -> Optional[int]:
    conn = get_conn()
    r = conn.execute("SELECT last_sent_at FROM stats_user WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    return int(r["last_sent_at"]) if r and r["last_sent_at"] is not None else None

def mark_sent_now(user_id: int):
    now = int(time.time())
    conn = get_conn()
    conn.execute("""
        INSERT INTO stats_user(user_id, last_sent_at) VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET last_sent_at=excluded.last_sent_at
    """, (user_id, now))
    conn.commit()
    conn.close()

def inc_sent_ok(user_id: int, n: int = 1):
    n = int(n)
    conn = get_conn()
    conn.execute("INSERT OR IGNORE INTO stats_user(user_id, sent_ok) VALUES (?, 0)", (user_id,))
    conn.execute("UPDATE stats_user SET sent_ok = COALESCE(sent_ok,0) + ? WHERE user_id=?", (n, user_id))
    conn.commit()
    conn.close()

def get_total_sent_ok() -> int:
    conn = get_conn()
    r = conn.execute("SELECT COALESCE(SUM(sent_ok),0) AS s FROM stats_user").fetchone()
    conn.close()
    return int(r["s"] if r and r["s"] is not None else 0)

def top_users(n: int = 10) -> List[sqlite3.Row]:
    n = max(1, min(100, int(n)))
    conn = get_conn()
    rows = conn.execute("""
        SELECT user_id, COALESCE(sent_ok,0) AS sent_ok
        FROM stats_user
        ORDER BY sent_ok DESC, user_id ASC
        LIMIT ?
    """, (n,)).fetchall()
    conn.close()
    return rows

# -------- Back-compat aliases (older module imports) --------
def delete_session_slot(user_id: int, slot: int) -> int:
    return sessions_delete(user_id, slot)

def count_user_sessions(user_id: int) -> int:
    return sessions_count_user(user_id)

def sessions_upsert_slot(user_id: int, slot: Optional[int], api_id: int, api_hash: str, session_string: str) -> int:
    """Alias used by some login_bot versions."""
    return upsert_session_slot(user_id, slot, api_id, api_hash, session_string)

def delete_session(user_id: int, slot: int) -> int:
    return sessions_delete(user_id, slot)
