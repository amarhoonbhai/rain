# core/db.py — SQLite helpers and bot storage
# Tables:
#   users(user_id PK, username, created_at)
#   settings(key PK, val JSON-as-text)
#   user_sessions(user_id, slot, api_id, api_hash, session_string, PK(user_id,slot))
#   groups(user_id, target, UNIQUE(user_id,target))
#   stats(user_id PK, sent_ok INT, last_sent_at INT)
#   ads(user_id PK, text, parse_mode)

import os
import json
import time
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Optional

# ---- DB path ----
_BASE = Path(__file__).resolve().parent.parent  # repo root (…/rain)
_DB_PATH = os.getenv("DB_PATH") or str(_BASE / "data.sqlite")

# ---- basic open ----
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ---- init ----
def init_db() -> None:
    conn = get_conn()
    cur = conn.cursor()

    # users
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id   INTEGER PRIMARY KEY,
            username  TEXT,
            created_at INTEGER
        );
    """)

    # kv settings
    cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            val TEXT
        );
    """)

    # sessions
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_sessions (
            user_id        INTEGER NOT NULL,
            slot           INTEGER NOT NULL,
            api_id         INTEGER NOT NULL,
            api_hash       TEXT    NOT NULL,
            session_string TEXT    NOT NULL,
            PRIMARY KEY (user_id, slot)
        );
    """)

    # groups
    cur.execute("""
        CREATE TABLE IF NOT EXISTS groups (
            user_id INTEGER NOT NULL,
            target  TEXT    NOT NULL,
            UNIQUE(user_id, target)
        );
    """)

    # stats
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stats (
            user_id     INTEGER PRIMARY KEY,
            sent_ok     INTEGER DEFAULT 0,
            last_sent_at INTEGER
        );
    """)

    # ads (kept for compatibility; worker may not use it if pulling from pinned)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ads (
            user_id    INTEGER PRIMARY KEY,
            text       TEXT,
            parse_mode TEXT
        );
    """)

    conn.commit()
    conn.close()

# ---- helpers: settings as JSON text ----
def set_setting(key: str, val: Any) -> None:
    conn = get_conn()
    conn.execute(
        "INSERT INTO settings(key, val) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET val=excluded.val",
        (key, json.dumps(val))
    )
    conn.commit()
    conn.close()

def get_setting(key: str, default: Any = None) -> Any:
    conn = get_conn()
    row = conn.execute("SELECT val FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    if row is None:
        return default
    try:
        return json.loads(row["val"])
    except Exception:
        return default

# ---- users ----
def ensure_user(user_id: int, username: Optional[str] = None) -> None:
    conn = get_conn()
    now = int(time.time())
    row = conn.execute("SELECT 1 FROM users WHERE user_id=?", (user_id,)).fetchone()
    if row is None:
        conn.execute("INSERT INTO users(user_id, username, created_at) VALUES(?,?,?)",
                     (user_id, username, now))
    else:
        if username is not None:
            conn.execute("UPDATE users SET username=? WHERE user_id=?", (username, user_id))
    conn.commit()
    conn.close()

def users_count() -> int:
    conn = get_conn()
    n = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
    conn.close()
    return int(n)

# ---- sessions API ----
def sessions_list(user_id: int) -> list[sqlite3.Row]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT user_id, slot, api_id, api_hash, session_string "
        "FROM user_sessions WHERE user_id=? ORDER BY slot", (user_id,)
    ).fetchall()
    conn.close()
    return rows

def sessions_strings(user_id: int) -> list[sqlite3.Row]:
    # kept identical to sessions_list for worker usage
    return sessions_list(user_id)

def sessions_count_user(user_id: int) -> int:
    conn = get_conn()
    n = conn.execute("SELECT COUNT(*) AS c FROM user_sessions WHERE user_id=?", (user_id,)).fetchone()["c"]
    conn.close()
    return int(n)

def sessions_count() -> int:
    conn = get_conn()
    n = conn.execute("SELECT COUNT(DISTINCT user_id) AS c FROM user_sessions").fetchone()["c"]
    conn.close()
    return int(n)

def sessions_delete(user_id: int, slot: int) -> int:
    conn = get_conn()
    cur = conn.execute("DELETE FROM user_sessions WHERE user_id=? AND slot=?", (user_id, slot))
    conn.commit()
    conn.close()
    return int(cur.rowcount)

def _session_slots_cap(user_id: int) -> int:
    # default 3, overridable by env
    try:
        return max(1, int(os.getenv("SESSION_SLOTS_CAP", "3")))
    except Exception:
        return 3

def first_free_slot(user_id: int, cap: Optional[int] = None) -> Optional[int]:
    cap = cap or _session_slots_cap(user_id)
    used = {int(r["slot"]) for r in sessions_list(user_id)}
    for s in range(1, cap + 1):
        if s not in used:
            return s
    return None

def sessions_upsert_slot(user_id: int, slot: int, api_id: int, api_hash: str, session_string: str) -> None:
    conn = get_conn()
    conn.execute(
        "INSERT INTO user_sessions(user_id, slot, api_id, api_hash, session_string) "
        "VALUES (?,?,?,?,?) "
        "ON CONFLICT(user_id,slot) DO UPDATE SET "
        " api_id=excluded.api_id, api_hash=excluded.api_hash, session_string=excluded.session_string",
        (user_id, slot, int(api_id), str(api_hash), str(session_string))
    )
    conn.commit()
    conn.close()

# Back-compat aliases used by older code paths / logs
upsert_session_slot   = sessions_upsert_slot
sessions_upsert       = sessions_upsert_slot
sessions_upsert_slot  = sessions_upsert_slot
count_user_sessions   = sessions_count_user
delete_session_slot   = sessions_delete

# ---- groups ----
def _premium_flag_key(uid: int) -> str: return f"premium:{uid}"
def _unlock_flag_key(uid: int) -> str:  return f"gc_unlock:{uid}"

def is_premium(user_id: int) -> bool:
    return bool(int(get_setting(_premium_flag_key(user_id), 0) or 0))

def set_premium(user_id: int, enabled: bool) -> None:
    set_setting(_premium_flag_key(user_id), 1 if enabled else 0)

def list_premium_users() -> list[int]:
    conn = get_conn()
    rows = conn.execute("SELECT key, val FROM settings WHERE key LIKE 'premium:%' AND val='1'").fetchall()
    conn.close()
    out = []
    for r in rows:
        try:
            out.append(int(r["key"].split(":")[1]))
        except Exception:
            pass
    return out

def is_gc_unlocked(user_id: int) -> bool:
    return bool(int(get_setting(_unlock_flag_key(user_id), 0) or 0))

def set_gc_unlock(user_id: int, enabled: bool) -> None:
    set_setting(_unlock_flag_key(user_id), 1 if enabled else 0)

def groups_cap(user_id: Optional[int] = None) -> int:
    # default 5; 10 if unlocked; 50 if premium
    if user_id is None:
        return 5
    if is_premium(user_id):
        return 50
    if is_gc_unlocked(user_id):
        return 10
    return 5

def list_groups(user_id: int) -> list[str]:
    conn = get_conn()
    rows = conn.execute("SELECT target FROM groups WHERE user_id=? ORDER BY rowid", (user_id,)).fetchall()
    conn.close()
    return [r["target"] for r in rows]

def add_group(user_id: int, target: str) -> int:
    target = (target or "").strip()
    if not target:
        return 0
    # cap check
    if len(list_groups(user_id)) >= groups_cap(user_id):
        return 0
    conn = get_conn()
    try:
        conn.execute("INSERT OR IGNORE INTO groups(user_id, target) VALUES(?,?)", (user_id, target))
        conn.commit()
        # rowcount not reliable with OR IGNORE; compute diff
        added = 1 if target in list_groups(user_id) else 0
        return added
    finally:
        conn.close()

def clear_groups(user_id: int) -> None:
    conn = get_conn()
    conn.execute("DELETE FROM groups WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()

# ---- intervals (minutes) ----
def _interval_key(uid: int) -> str: return f"interval:{uid}"

def set_interval(user_id: int, minutes: int) -> None:
    set_setting(_interval_key(user_id), int(minutes))

def get_interval(user_id: int) -> int:
    val = get_setting(_interval_key(user_id), None)
    try:
        return int(val)
    except Exception:
        return 30  # default

# ---- stats (sent_ok / last_sent_at) ----
def _ensure_stats_row(conn: sqlite3.Connection, user_id: int) -> None:
    conn.execute(
        "INSERT INTO stats(user_id, sent_ok, last_sent_at) VALUES(?,?,?) "
        "ON CONFLICT(user_id) DO NOTHING",
        (user_id, 0, None)
    )

def get_last_sent_at(user_id: int) -> Optional[int]:
    conn = get_conn()
    row = conn.execute("SELECT last_sent_at FROM stats WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    return int(row["last_sent_at"]) if (row and row["last_sent_at"] is not None) else None

def mark_sent_now(user_id: int) -> None:
    now = int(time.time())
    conn = get_conn()
    _ensure_stats_row(conn, user_id)
    conn.execute("UPDATE stats SET last_sent_at=? WHERE user_id=?", (now, user_id))
    conn.commit()
    conn.close()

def inc_sent_ok(user_id: int, delta: int = 1) -> None:
    conn = get_conn()
    _ensure_stats_row(conn, user_id)
    conn.execute("UPDATE stats SET sent_ok = COALESCE(sent_ok,0) + ? WHERE user_id=?", (int(delta), user_id))
    conn.commit()
    conn.close()

def get_total_sent_ok() -> int:
    conn = get_conn()
    row = conn.execute("SELECT SUM(sent_ok) AS s FROM stats").fetchone()
    conn.close()
    return int(row["s"] or 0)

def top_users(limit: int = 10) -> list[sqlite3.Row]:
    limit = max(1, min(int(limit), 100))
    conn = get_conn()
    rows = conn.execute(
        "SELECT user_id, sent_ok FROM stats ORDER BY sent_ok DESC, user_id ASC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return rows

# back-compat name (some old code called this)
def last_sent_at_for(user_id: int) -> Optional[int]:
    return get_last_sent_at(user_id)

# ---- ads (compat) ----
def set_ad(user_id: int, text: str, parse_mode: Optional[str]) -> None:
    conn = get_conn()
    conn.execute(
        "INSERT INTO ads(user_id, text, parse_mode) VALUES(?,?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET text=excluded.text, parse_mode=excluded.parse_mode",
        (user_id, text, (parse_mode or None))
    )
    conn.commit()
    conn.close()

def get_ad(user_id: int) -> tuple[Optional[str], Optional[str]]:
    conn = get_conn()
    row = conn.execute("SELECT text, parse_mode FROM ads WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    if not row:
        return None, None
    return row["text"], row["parse_mode"]

# ---- gate channels + night mode ----
def get_gate_channels_effective() -> tuple[Optional[str], Optional[str]]:
    """Return (ch1, ch2) or (None,None) if unset; main_bot will apply defaults."""
    ch1 = get_setting("gate:ch1", None)
    ch2 = get_setting("gate:ch2", None)
    return (ch1, ch2)

def night_enabled() -> bool:
    return bool(int(get_setting("night:enabled", 0) or 0))

def set_night_enabled(enabled: bool) -> None:
    set_setting("night:enabled", 1 if enabled else 0)

# old alias used in some branches
def set_global_night_mode(enabled: bool) -> None:
    set_night_enabled(enabled)

# ---- premium name lock (used by enforcer / owner panel) ----
def set_name_lock(user_id: int, enabled: bool, name: Optional[str] = None) -> None:
    set_setting(f"premium:lock:enabled:{user_id}", 1 if enabled else 0)
    if name is not None:
        set_setting(f"premium:lock:name:{user_id}", name)

# ---- queries used by worker loops ----
def users_with_sessions() -> list[int]:
    conn = get_conn()
    rows = conn.execute("SELECT DISTINCT user_id FROM user_sessions ORDER BY user_id").fetchall()
    conn.close()
    return [int(r["user_id"]) for r in rows]
