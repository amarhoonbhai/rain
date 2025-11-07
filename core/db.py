# core/db.py — SQLite helpers for Spinify stack
# Complete: users/sessions/groups/intervals/settings + ads + worker state
from __future__ import annotations
import os, json, sqlite3
from typing import Any, List, Dict, Tuple, Iterator

# ---------------- Path & connection ----------------
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

# --------------- schema & indexes ------------------
def _ensure_tables(conn: sqlite3.Connection):
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS users(
        user_id     INTEGER PRIMARY KEY,
        username    TEXT,
        created_at  INTEGER DEFAULT (strftime('%s','now')),
        sent_ok     INTEGER DEFAULT 0
    )""")
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
    c.execute("""
    CREATE TABLE IF NOT EXISTS user_groups(
        user_id  INTEGER,
        group_id TEXT,
        added_at INTEGER DEFAULT (strftime('%s','now')),
        UNIQUE(user_id, group_id)
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS intervals(
        user_id INTEGER PRIMARY KEY,
        minutes INTEGER
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS settings(
        key TEXT PRIMARY KEY,
        val TEXT
    )""")
    -- Ads per user (text + parse_mode)
    c.execute("""
    CREATE TABLE IF NOT EXISTS ads(
        user_id    INTEGER PRIMARY KEY,
        text       TEXT,
        parse_mode TEXT,
        updated_at INTEGER DEFAULT (strftime('%s','now'))
    )""")
    -- Worker state (last sent)
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
    _ensure_tables(conn)
    _ensure_indexes(conn)
    conn.close()

# ---------------- users & stats --------------------
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

# ---------------- sessions (3 slots) --------------
def sessions_list(user_id: int) -> List[Dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT slot, api_id, api_hash FROM user_sessions WHERE user_id=? ORDER BY slot",
        (user_id,)
    ).fetchall()
    conn.close(); return rows

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
    conn.execute("""
        INSERT INTO user_sessions(user_id, slot, api_id, api_hash, session_string, updated_at)
        VALUES(?,?,?,?,?,strftime('%s','now'))
        ON CONFLICT(user_id, slot) DO UPDATE SET
            api_id=excluded.api_id,
            api_hash=excluded.api_hash,
            session_string=excluded.session_string,
            updated_at=excluded.updated_at
    """, (user_id, slot, api_id, api_hash, session_string))
    conn.commit(); conn.close()

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

def get_user_sessions_full(user_id: int) -> List[Dict]:
    conn = get_conn()
    rows = conn.execute("""
        SELECT slot, api_id, api_hash, session_string
        FROM user_sessions
        WHERE user_id=?
        ORDER BY slot
    """, (user_id,)).fetchall()
    conn.close(); return rows

# ---------------- groups (cap 5) ------------------
_GROUPS_CAP = 5
def groups_cap() -> int: return _GROUPS_CAP

def list_groups(user_id: int) -> List[str]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT group_id FROM user_groups WHERE user_id=? ORDER BY added_at",
        (user_id,)
    ).fetchall()
    conn.close(); return [r["group_id"] for r in rows]

def add_group(user_id: int, gid: str) -> int:
    gid = (gid or "").strip()
    if not gid: return 0
    if len(list_groups(user_id)) >= groups_cap(): return 0
    conn = get_conn()
    try:
        conn.execute("INSERT INTO user_groups(user_id, group_id) VALUES(?,?)", (user_id, gid))
        conn.commit(); return 1
    except sqlite3.IntegrityError:
        return 0
    finally:
        conn.close()

def clear_groups(user_id: int):
    conn = get_conn()
    conn.execute("DELETE FROM user_groups WHERE user_id=?", (user_id,))
    conn.commit(); conn.close()

def groups_iter_all() -> Iterator[Dict]:
    conn = get_conn()
    rows = conn.execute("SELECT user_id, group_id FROM user_groups ORDER BY user_id, added_at").fetchall()
    conn.close()
    for r in rows:
        yield r

# ---------------- intervals -----------------------
def set_interval(user_id: int, minutes: int):
    conn = get_conn()
    conn.execute("""
        INSERT INTO intervals(user_id, minutes) VALUES(?,?)
        ON CONFLICT(user_id) DO UPDATE SET minutes=excluded.minutes
    """, (user_id, minutes))
    conn.commit(); conn.close()

def get_interval(user_id: int) -> int | None:
    conn = get_conn()
    r = conn.execute("SELECT minutes FROM intervals WHERE user_id=?", (user_id,)).fetchone()
    conn.close(); return (r["minutes"] if r else None)

# ---------------- settings (KV) -------------------
def _encode_val(val: Any) -> str:
    if isinstance(val, (int, float)):
        return str(val)
    try:
        return json.dumps(val, ensure_ascii=False)
    except Exception:
        return str(val)

def _decode_val(s: str) -> Any:
    if s is None: return None
    s = str(s)
    if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
        try: return int(s)
        except: pass
    try:
        return json.loads(s)
    except Exception:
        return s

def set_setting(key: str, val: Any):
    conn = get_conn()
    conn.execute("""
        INSERT INTO settings(key,val) VALUES(?,?)
        ON CONFLICT(key) DO UPDATE SET val=excluded.val
    """, (key, _encode_val(val)))
    conn.commit(); conn.close()

def get_setting(key: str, default: Any=None) -> Any:
    conn = get_conn()
    r = conn.execute("SELECT val FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    return _decode_val(r["val"]) if r and r.get("val") is not None else default

# ---------------- night mode ----------------------
def night_enabled() -> bool:
    return bool(int(get_setting("night:enabled", 0) or 0))

def set_night_enabled(flag: bool):
    set_setting("night:enabled", 1 if flag else 0)

# ---------------- gate channels -------------------
def get_gate_channels() -> Tuple[str, str]:
    ch1 = get_setting("gate:ch1", "") or ""
    ch2 = get_setting("gate:ch2", "") or ""
    return (str(ch1), str(ch2))

def set_gate_channels(ch1: str | None, ch2: str | None):
    if ch1 is not None: set_setting("gate:ch1", ch1)
    if ch2 is not None: set_setting("gate:ch2", ch2)

def get_gate_channels_effective() -> Tuple[str, str]:
    env1 = (os.getenv("GATE_CHANNEL_1") or "").strip()
    env2 = (os.getenv("GATE_CHANNEL_2") or "").strip()
    if env1 or env2:
        return (env1, env2)
    return get_gate_channels()

# ---------------- premium name-lock ---------------
def set_name_lock(user_id: int, enabled: bool, name: str | None = None):
    set_setting(f"user:{user_id}:name_lock", 1 if enabled else 0)
    if name is not None:
        set_setting(f"user:{user_id}:locked_name", name)

def name_lock_enabled(user_id: int) -> bool:
    return bool(int(get_setting(f"user:{user_id}:name_lock", 0) or 0))

def locked_name(user_id: int) -> str | None:
    v = get_setting(f"user:{user_id}:locked_name", None)
    return v if v else None

# ---------------- ads (NEW) -----------------------
def set_ad(user_id: int, text: str, parse_mode: str | None = None):
    conn = get_conn()
    conn.execute("""
        INSERT INTO ads(user_id, text, parse_mode, updated_at)
        VALUES(?,?,?,strftime('%s','now'))
        ON CONFLICT(user_id) DO UPDATE SET
            text=excluded.text,
            parse_mode=excluded.parse_mode,
            updated_at=excluded.updated_at
    """, (user_id, text, (parse_mode or None)))
    conn.commit(); conn.close()

def get_ad(user_id: int) -> Tuple[str | None, str | None]:
    conn = get_conn()
    r = conn.execute("SELECT text, parse_mode FROM ads WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    if not r: return (None, None)
    return (r["text"], r["parse_mode"])

# --------------- worker state (NEW) --------------
def get_last_sent_at(user_id: int) -> int | None:
    conn = get_conn()
    r = conn.execute("SELECT last_sent_at FROM worker_state WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    return int(r["last_sent_at"]) if r and r["last_sent_at"] is not None else None

def mark_sent_now(user_id: int):
    conn = get_conn()
    conn.execute("""
        INSERT INTO worker_state(user_id, last_sent_at)
        VALUES(?, strftime('%s','now'))
        ON CONFLICT(user_id) DO UPDATE SET last_sent_at=excluded.last_sent_at
    """, (user_id,))
    conn.commit(); conn.close()

# --------------- worker helpers ------------------
def users_with_sessions() -> List[int]:
    conn = get_conn()
    rows = conn.execute("SELECT DISTINCT user_id FROM user_sessions").fetchall()
    conn.close(); return [r["user_id"] for r in rows]
