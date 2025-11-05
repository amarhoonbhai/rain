# core/db.py — SQLite helpers for Spinify/Ads bot
# - users, settings, stats_user
# - user_sessions: multi-slot (up to 3 accounts per user)
# - groups stored in settings (JSON), capped to 5 per user
# - per-user ad + interval
# - global night mode toggle
# - stats helpers + compatibility aliases for older code

from __future__ import annotations
import sqlite3, json, os
from pathlib import Path
from typing import List, Any

DB_PATH = os.getenv("DB_PATH") or str(Path(__file__).resolve().parent.parent / "data.db")

# ---------- connection ----------
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

# ---------- init / migrations ----------
def init_db() -> None:
    conn = get_conn()
    # users
    conn.execute("""
    CREATE TABLE IF NOT EXISTS users (
      user_id    INTEGER PRIMARY KEY,
      username   TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    # kv settings
    conn.execute("""
    CREATE TABLE IF NOT EXISTS settings (
      key TEXT PRIMARY KEY,
      val TEXT
    )""")
    # per-user stats
    conn.execute("""
    CREATE TABLE IF NOT EXISTS stats_user (
      user_id INTEGER PRIMARY KEY,
      sent_ok INTEGER DEFAULT 0,
      last_sent_at TEXT
    )""")
    # multi-slot sessions
    conn.execute("""
    CREATE TABLE IF NOT EXISTS user_sessions (
      user_id INTEGER,
      slot INTEGER,
      api_id INTEGER,
      api_hash TEXT,
      session_string TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (user_id, slot)
    )""")
    # migrate from any old single-slot table variant
    try:
        cols = [r["name"] for r in conn.execute("PRAGMA table_info(user_sessions)")]
        if "slot" not in cols:
            conn.execute("ALTER TABLE user_sessions RENAME TO user_sessions_one")
            conn.execute("""
            CREATE TABLE user_sessions (
              user_id INTEGER,
              slot INTEGER,
              api_id INTEGER,
              api_hash TEXT,
              session_string TEXT,
              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (user_id, slot)
            )""")
            for row in conn.execute("SELECT user_id, api_id, api_hash, session_string FROM user_sessions_one"):
                conn.execute(
                    "INSERT OR REPLACE INTO user_sessions(user_id, slot, api_id, api_hash, session_string) VALUES(?,?,?,?,?)",
                    (row["user_id"], 1, row["api_id"], row["api_hash"], row["session_string"])
                )
            conn.execute("DROP TABLE user_sessions_one")
    except Exception:
        pass
    conn.commit()
    conn.close()

# ---------- users ----------
def ensure_user(user_id: int, username: str | None = None) -> None:
    conn = get_conn()
    conn.execute("INSERT OR IGNORE INTO users(user_id, username) VALUES(?, ?)", (user_id, username))
    conn.execute("UPDATE users SET username = ? WHERE user_id = ?", (username, user_id))
    conn.execute("INSERT OR IGNORE INTO stats_user(user_id, sent_ok) VALUES(?, 0)", (user_id,))
    conn.commit(); conn.close()

# alias for older callers
def upsert_user(user_id: int, username: str | None = None) -> None:
    return ensure_user(user_id, username)

def users_count() -> int:
    conn = get_conn()
    n = conn.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"]
    conn.close()
    return int(n or 0)

# ---------- generic settings ----------
def _set_setting(key: str, val: Any) -> None:
    conn = get_conn()
    conn.execute(
        "INSERT INTO settings(key, val) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET val=excluded.val",
        (key, json.dumps(val) if not isinstance(val, str) else val),
    )
    conn.commit(); conn.close()

def _get_setting(key: str, default: Any = None) -> Any:
    conn = get_conn()
    row = conn.execute("SELECT val FROM settings WHERE key = ?", (key,)).fetchone()
    conn.close()
    if not row or row["val"] is None:
        return default
    val = row["val"]
    try:
        return json.loads(val)
    except Exception:
        return val

def _del_setting(key: str) -> None:
    conn = get_conn()
    conn.execute("DELETE FROM settings WHERE key = ?", (key,))
    conn.commit(); conn.close()

# ---------- ad ----------
def set_ad(user_id: int, ad_text: str) -> None:
    _set_setting(f"ad:{user_id}", ad_text)

def get_ad(user_id: int) -> str | None:
    v = _get_setting(f"ad:{user_id}", None)
    return v

# ---------- interval ----------
def set_interval(user_id: int, minutes: int) -> None:
    _set_setting(f"interval:{user_id}", int(minutes))

def get_interval(user_id: int) -> int | None:
    v = _get_setting(f"interval:{user_id}", None)
    try:
        return int(v) if v is not None else None
    except Exception:
        return None

# ---------- groups (cap 5) ----------
def _norm_group(g: str) -> str:
    g = (g or "").strip()
    if not g:
        return ""
    if g.startswith("https://t.me/"):
        g = "@" + g.rsplit("/", 1)[-1]
    if not g.startswith("@"):
        g = "@" + g
    return g

def add_groups(user_id: int, group_ids: List[str]) -> int:
    key = f"groups:{user_id}"
    cur = _get_setting(key, [])
    try:
        current = set(cur if isinstance(cur, list) else [])
    except Exception:
        current = set()
    allowed = 5 - len(current)
    if allowed <= 0:
        return 0
    normalized = []
    for g in group_ids:
        ng = _norm_group(g)
        if ng and ng not in current:
            normalized.append(ng)
    to_add = normalized[:allowed]
    if not to_add:
        return 0
    current.update(to_add)
    _set_setting(key, sorted(list(current)))
    return len(to_add)

def list_groups(user_id: int) -> List[str]:
    v = _get_setting(f"groups:{user_id}", [])
    return list(v if isinstance(v, list) else [])

def clear_groups(user_id: int) -> None:
    _set_setting(f"groups:{user_id}", [])

# ---------- sessions (multi-slot up to 3) ----------
def sessions_list(user_id: int) -> List[sqlite3.Row]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT slot, api_id, api_hash, created_at FROM user_sessions WHERE user_id=? ORDER BY slot",
        (user_id,)
    ).fetchall()
    conn.close()
    return rows

def sessions_count_user(user_id: int) -> int:
    conn = get_conn()
    n = conn.execute("SELECT COUNT(*) AS n FROM user_sessions WHERE user_id=?", (user_id,)).fetchone()["n"]
    conn.close()
    return int(n or 0)

def sessions_count() -> int:
    """Active users that have >=1 session."""
    conn = get_conn()
    n = conn.execute("SELECT COUNT(DISTINCT user_id) AS n FROM user_sessions").fetchone()["n"]
    conn.close()
    return int(n or 0)

def sessions_add(user_id: int, api_id: int, api_hash: str, session_string: str) -> int:
    used = {r["slot"] for r in sessions_list(user_id)}
    for slot in (1, 2, 3):
        if slot not in used:
            conn = get_conn()
            conn.execute(
                "INSERT OR REPLACE INTO user_sessions(user_id, slot, api_id, api_hash, session_string) VALUES(?,?,?,?,?)",
                (user_id, slot, api_id, api_hash, session_string)
            )
            conn.commit(); conn.close()
            return slot
    return 0

def sessions_delete(user_id: int, slot: int) -> int:
    conn = get_conn()
    cur = conn.execute("DELETE FROM user_sessions WHERE user_id=? AND slot=?", (user_id, slot))
    conn.commit(); conn.close()
    return cur.rowcount

def sessions_strings(user_id: int) -> List[sqlite3.Row]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT slot, api_id, api_hash, session_string FROM user_sessions WHERE user_id=? ORDER BY slot",
        (user_id,)
    ).fetchall()
    conn.close()
    return rows

# ---------- stats ----------
def bump_sent(user_id: int, inc: int = 1, last_at_iso: str | None = None) -> None:
    conn = get_conn()
    conn.execute("INSERT OR IGNORE INTO stats_user(user_id, sent_ok) VALUES(?, 0)", (user_id,))
    conn.execute(
        "UPDATE stats_user SET sent_ok = sent_ok + ?, "
        "last_sent_at = CASE WHEN ? IS NOT NULL THEN ? ELSE last_sent_at END "
        "WHERE user_id = ?",
        (inc, last_at_iso, last_at_iso, user_id)
    )
    conn.commit(); conn.close()

def get_total_sent_ok() -> int:
    conn = get_conn()
    row = conn.execute("SELECT SUM(sent_ok) AS s FROM stats_user").fetchone()
    conn.close()
    return int(row["s"] or 0)

def top_users(limit: int = 10) -> List[sqlite3.Row]:
    limit = max(1, min(50, int(limit)))
    conn = get_conn()
    # SQLite doesn't support "NULLS LAST" — emulate with (last_sent_at IS NULL)
    rows = conn.execute("""
        SELECT u.user_id, u.username, s.sent_ok, s.last_sent_at
        FROM stats_user s
        JOIN users u ON u.user_id = s.user_id
        ORDER BY s.sent_ok DESC, (s.last_sent_at IS NULL), s.last_sent_at DESC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return rows

# ---------- night mode (global) ----------
def night_enabled() -> bool:
    v = _get_setting("global:night", "0")
    return str(v) == "1"

def set_night_enabled(on: bool) -> None:
    _set_setting("global:night", "1" if on else "0")

# ---------- compatibility aliases (old names -> new API) ----------
def count_user_sessions(user_id: int) -> int:
    return sessions_count_user(user_id)

def active_users_count() -> int:
    return sessions_count()

def get_total_messages_forwarded() -> int:
    return get_total_sent_ok()

def top_users_by_sent(limit: int = 10):
    return top_users(limit)

def list_user_sessions(user_id: int):
    return sessions_list(user_id)

def add_user_session(user_id: int, api_id: int, api_hash: str, session_string: str) -> int:
    return sessions_add(user_id, api_id, api_hash, session_string)

def delete_user_session(user_id: int, slot: int) -> int:
    return sessions_delete(user_id, slot)

def get_user_sessions_strings(user_id: int):
    return sessions_strings(user_id)

def is_night_enabled() -> bool:
    return night_enabled()

def set_night(on: bool) -> None:
    return set_night_enabled(on)
    
