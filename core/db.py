# core/db.py — SQLite storage (users, sessions, groups, settings, stats)
import sqlite3, pathlib, time
from datetime import datetime

DB_PATH = pathlib.Path(__file__).resolve().parent.parent / "rain.sqlite3"

def get_conn():
    conn = sqlite3.connect(DB_PATH, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    conn.executescript(
        """
        PRAGMA journal_mode=WAL;

        CREATE TABLE IF NOT EXISTS users (
            user_id     INTEGER PRIMARY KEY,
            username    TEXT,
            joined_at   TEXT
        );

        -- up to 3 sessions per account (each account = different user_id)
        CREATE TABLE IF NOT EXISTS sessions (
            user_id         INTEGER NOT NULL,
            slot            INTEGER NOT NULL,
            api_id          INTEGER NOT NULL,
            api_hash        TEXT NOT NULL,
            session_string  TEXT NOT NULL,
            PRIMARY KEY (user_id, slot)
        );

        -- groups/targets saved raw (numeric id, @username, or t.me link)
        CREATE TABLE IF NOT EXISTS groups (
            user_id INTEGER NOT NULL,
            target  TEXT NOT NULL,
            UNIQUE (user_id, target)
        );

        -- generic KV
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            val TEXT
        );

        -- stats
        CREATE TABLE IF NOT EXISTS stats (
            user_id   INTEGER PRIMARY KEY,
            sent_ok   INTEGER DEFAULT 0,
            last_sent INTEGER
        );
        """
    )
    conn.close()

# ---------- KV helpers ----------
def set_setting(key: str, val):
    conn = get_conn()
    conn.execute(
        "INSERT INTO settings(key,val) VALUES (?,?) "
        "ON CONFLICT(key) DO UPDATE SET val=excluded.val",
        (key, str(val)),
    )
    conn.close()

def get_setting(key: str, default=None):
    conn = get_conn()
    r = conn.execute("SELECT val FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    if not r: return default
    v = r["val"]
    # try int
    try: return int(v)
    except Exception: pass
    # try float
    try: return float(v)
    except Exception: pass
    return v

# ---------- users ----------
def ensure_user(user_id: int, username: str | None):
    conn = get_conn()
    conn.execute(
        "INSERT INTO users(user_id, username, joined_at) VALUES (?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET username=excluded.username",
        (user_id, username, datetime.utcnow().isoformat()),
    )
    conn.close()

def users_count() -> int:
    conn = get_conn()
    n = conn.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"]
    conn.close()
    return int(n)

# ---------- sessions ----------
def sessions_upsert_slot(user_id: int, slot: int, api_id: int, api_hash: str, session_string: str):
    conn = get_conn()
    conn.execute(
        "INSERT INTO sessions(user_id,slot,api_id,api_hash,session_string) VALUES (?,?,?,?,?) "
        "ON CONFLICT(user_id,slot) DO UPDATE SET api_id=excluded.api_id, api_hash=excluded.api_hash, session_string=excluded.session_string",
        (user_id, slot, api_id, api_hash, session_string),
    )
    conn.close()

def sessions_list(user_id: int):
    conn = get_conn()
    rows = conn.execute(
        "SELECT user_id, slot, api_id FROM sessions WHERE user_id=? ORDER BY slot",
        (user_id,),
    ).fetchall()
    conn.close()
    return rows

def sessions_delete(user_id: int, slot: int):
    conn = get_conn()
    conn.execute("DELETE FROM sessions WHERE user_id=? AND slot=?", (user_id, slot))
    conn.close()

def sessions_count() -> int:
    conn = get_conn()
    n = conn.execute("SELECT COUNT(DISTINCT user_id) AS n FROM sessions").fetchone()["n"]
    conn.close()
    return int(n)

def sessions_count_user(user_id: int) -> int:
    conn = get_conn()
    n = conn.execute("SELECT COUNT(*) AS n FROM sessions WHERE user_id=?", (user_id,)).fetchone()["n"]
    conn.close()
    return int(n)

def sessions_strings(user_id: int):
    conn = get_conn()
    rows = conn.execute(
        "SELECT user_id, slot, api_id, api_hash, session_string FROM sessions WHERE user_id=? ORDER BY slot",
        (user_id,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def users_with_sessions():
    conn = get_conn()
    rows = conn.execute("SELECT DISTINCT user_id FROM sessions").fetchall()
    conn.close()
    return [r["user_id"] for r in rows]

# ---------- groups ----------
def list_groups(user_id: int):
    conn = get_conn()
    rows = conn.execute("SELECT target FROM groups WHERE user_id=? ORDER BY rowid", (user_id,)).fetchall()
    conn.close()
    return [r["target"] for r in rows]

def clear_groups(user_id: int):
    conn = get_conn()
    conn.execute("DELETE FROM groups WHERE user_id=?", (user_id,))
    conn.close()

def _groups_count(user_id: int) -> int:
    conn = get_conn()
    n = conn.execute("SELECT COUNT(*) AS n FROM groups WHERE user_id=?", (user_id,)).fetchone()["n"]
    conn.close()
    return int(n)

def add_group(user_id: int, target: str) -> int:
    if not target: return 0
    cap = groups_cap(user_id)
    if _groups_count(user_id) >= cap:
        return 0
    conn = get_conn()
    try:
        conn.execute("INSERT INTO groups(user_id,target) VALUES (?,?)", (user_id, target))
        return 1
    except Exception:
        return 0
    finally:
        conn.close()

# ---------- interval & schedule ----------
def set_interval(user_id: int, minutes: int):
    set_setting(f"interval:{user_id}", int(minutes))

def get_interval(user_id: int):
    v = get_setting(f"interval:{user_id}", None)
    return int(v) if v not in (None, "") else None

def mark_sent_now(user_id: int):
    ts = int(time.time())
    conn = get_conn()
    conn.execute(
        "INSERT INTO stats(user_id, sent_ok, last_sent) VALUES (?, 0, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET last_sent=excluded.last_sent",
        (user_id, ts),
    )
    conn.close()

def last_sent_at_for(user_id: int):
    conn = get_conn()
    r = conn.execute("SELECT last_sent FROM stats WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    return int(r["last_sent"]) if r and r["last_sent"] is not None else None

def inc_sent_ok(user_id: int, n: int):
    conn = get_conn()
    conn.execute(
        "INSERT INTO stats(user_id, sent_ok, last_sent) VALUES (?, ?, NULL) "
        "ON CONFLICT(user_id) DO UPDATE SET sent_ok=COALESCE(stats.sent_ok,0)+excluded.sent_ok",
        (user_id, n),
    )
    conn.close()

def get_total_sent_ok() -> int:
    conn = get_conn()
    r = conn.execute("SELECT SUM(sent_ok) AS s FROM stats").fetchone()
    conn.close()
    return int(r["s"] or 0)

def top_users(n: int = 10):
    conn = get_conn()
    rows = conn.execute(
        "SELECT user_id, COALESCE(sent_ok,0) AS sent_ok FROM stats ORDER BY sent_ok DESC LIMIT ?",
        (n,),
    ).fetchall()
    conn.close()
    return rows

# ---------- features / flags ----------
def is_premium(user_id: int) -> bool:
    return bool(int(get_setting(f"premium:{user_id}", 0) or 0))

def set_premium(user_id: int, val: bool):
    set_setting(f"premium:{user_id}", 1 if val else 0)

def list_premium_users():
    conn = get_conn()
    rows = conn.execute("SELECT key,val FROM settings WHERE key LIKE 'premium:%' AND CAST(val AS INTEGER)=1").fetchall()
    conn.close()
    out = []
    for r in rows:
        try:
            uid = int(r["key"].split(":")[1])
            out.append(uid)
        except Exception:
            continue
    return sorted(out)

def is_gc_unlocked(user_id: int) -> bool:
    return bool(int(get_setting(f"gc_unlock:{user_id}", 0) or 0))

def set_gc_unlock(user_id: int, val: bool):
    set_setting(f"gc_unlock:{user_id}", 1 if val else 0)

def groups_cap(user_id: int) -> int:
    if is_premium(user_id): return 50
    if is_gc_unlocked(user_id): return 10
    return 5

# ---------- night mode ----------
def night_enabled() -> bool:
    return bool(int(get_setting("night:enabled", 0) or 0))

def set_night_enabled(val: bool):
    set_setting("night:enabled", 1 if val else 0)

# ---------- gate channels (read only defaults) ----------
def get_gate_channels_effective():
    ch1 = get_setting("gate:ch1", None)
    ch2 = get_setting("gate:ch2", None)
    return ch1, ch2
