# core/db.py — SQLite helpers for Spinify/Ads bot (Python 3.12+)
# - users, settings (KV), stats_user
# - user_sessions: multi-slot (default up to 3 accounts/user)
# - groups: stored in settings as JSON, capped to 5 per user
# - per-user ad + interval (30/45/60)
# - global night mode
# - premium name-lock (for profile_enforcer)
# - "next run" scheduler keys for worker
# - gate channels helpers
# - wide compatibility aliases (old names -> new)

from __future__ import annotations
import os, json, sqlite3
from pathlib import Path
from typing import Any, List, Tuple, Optional
from datetime import datetime

DB_PATH = os.getenv("DB_PATH") or str(Path(__file__).resolve().parent.parent / "data.db")

# Configurable session slots (default 3)
SESSIONS_MAX_SLOTS = int(os.getenv("SESSIONS_MAX_SLOTS", "3"))
MAX_SLOTS = SESSIONS_MAX_SLOTS  # legacy constant some code may import

# -------------------- connection --------------------
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

# -------------------- init / migrations --------------------
def init_db() -> None:
    conn = get_conn()
    # users
    conn.execute("""
    CREATE TABLE IF NOT EXISTS users(
      user_id    INTEGER PRIMARY KEY,
      username   TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    # settings (KV)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS settings(
      key TEXT PRIMARY KEY,
      val TEXT
    )""")
    # per-user stats
    conn.execute("""
    CREATE TABLE IF NOT EXISTS stats_user(
      user_id INTEGER PRIMARY KEY,
      sent_ok INTEGER DEFAULT 0,
      last_sent_at TEXT
    )""")
    # multi-slot sessions
    conn.execute("""
    CREATE TABLE IF NOT EXISTS user_sessions(
      user_id INTEGER,
      slot INTEGER,
      api_id INTEGER,
      api_hash TEXT,
      session_string TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY(user_id, slot)
    )""")
    # migrate legacy single-slot table if any
    try:
        cols = [r["name"] for r in conn.execute("PRAGMA table_info(user_sessions)")]
        if "slot" not in cols:
            conn.execute("ALTER TABLE user_sessions RENAME TO user_sessions_one")
            conn.execute("""
            CREATE TABLE user_sessions(
              user_id INTEGER,
              slot INTEGER,
              api_id INTEGER,
              api_hash TEXT,
              session_string TEXT,
              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY(user_id, slot)
            )""")
            for row in conn.execute("SELECT user_id, api_id, api_hash, session_string FROM user_sessions_one"):
                conn.execute(
                    "INSERT OR REPLACE INTO user_sessions(user_id, slot, api_id, api_hash, session_string) VALUES(?,?,?,?,?)",
                    (row["user_id"], 1, row["api_id"], row["api_hash"], row["session_string"])
                )
            conn.execute("DROP TABLE user_sessions_one")
    except Exception:
        pass
    conn.commit(); conn.close()

# -------------------- settings (generic) --------------------
def _set_setting(key: str, val: Any) -> None:
    conn = get_conn()
    conn.execute(
        "INSERT INTO settings(key,val) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET val=excluded.val",
        (key, json.dumps(val) if not isinstance(val, str) else val),
    )
    conn.commit(); conn.close()

def _get_setting(key: str, default: Any = None) -> Any:
    conn = get_conn()
    row = conn.execute("SELECT val FROM settings WHERE key=?", (key,)).fetchone()
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
    conn.execute("DELETE FROM settings WHERE key=?", (key,))
    conn.commit(); conn.close()

# Public wrappers (some code imports these names)
def set_setting(key: str, val: Any) -> None: _set_setting(key, val)
def get_setting(key: str, default: Any = None) -> Any: return _get_setting(key, default)
def del_setting(key: str) -> None: _del_setting(key)
# extra synonyms
def set_config(key: str, val: Any) -> None: _set_setting(key, val)
def get_config(key: str, default: Any = None) -> Any: return _get_setting(key, default)
def del_config(key: str) -> None: _del_setting(key)

# -------------------- users --------------------
def ensure_user(user_id: int, username: str | None = None) -> None:
    conn = get_conn()
    conn.execute("INSERT OR IGNORE INTO users(user_id, username) VALUES(?,?)", (user_id, username))
    conn.execute("UPDATE users SET username=? WHERE user_id=?", (username, user_id))
    conn.execute("INSERT OR IGNORE INTO stats_user(user_id, sent_ok) VALUES(?,0)", (user_id,))
    conn.commit(); conn.close()

# alias for older callers
def upsert_user(user_id: int, username: str | None = None) -> None:
    return ensure_user(user_id, username)

def users_count() -> int:
    conn = get_conn()
    n = conn.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"]
    conn.close()
    return int(n or 0)

# -------------------- ad --------------------
def set_ad(user_id: int, ad_text: str) -> None:
    _set_setting(f"ad:{user_id}", ad_text)

def get_ad(user_id: int) -> Optional[str]:
    v = _get_setting(f"ad:{user_id}", None)
    return v if (v is None or isinstance(v, str)) else str(v)

# -------------------- interval --------------------
def set_interval(user_id: int, minutes: int) -> None:
    _set_setting(f"interval:{user_id}", int(minutes))

def get_interval(user_id: int) -> Optional[int]:
    v = _get_setting(f"interval:{user_id}", None)
    try:
        return int(v) if v is not None else None
    except Exception:
        return None

# -------------------- groups (cap = 5) --------------------
def groups_cap() -> int: return 5

def _norm_group(g: str) -> str:
    g = (g or "").strip()
    if not g: return ""
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
    allowed = groups_cap() - len(current)
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

def add_group(user_id: int, group: str) -> int:
    return add_groups(user_id, [group])

def list_groups(user_id: int) -> List[str]:
    v = _get_setting(f"groups:{user_id}", [])
    return list(v if isinstance(v, list) else [])

def clear_groups(user_id: int) -> None:
    _set_setting(f"groups:{user_id}", [])

def get_groups(user_id: int) -> List[str]:  # legacy name
    return list_groups(user_id)

# -------------------- sessions (multi-slot) --------------------
def sessions_max_slots() -> int: return SESSIONS_MAX_SLOTS

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
    """Active users that have >= 1 session."""
    conn = get_conn()
    n = conn.execute("SELECT COUNT(DISTINCT user_id) AS n FROM user_sessions").fetchone()["n"]
    conn.close()
    return int(n or 0)

def first_free_slot(user_id: int) -> int:
    """Return the first available slot in 1..SESSIONS_MAX_SLOTS, or 0 if full."""
    used = {int(r["slot"]) for r in sessions_list(user_id)}
    for slot in range(1, sessions_max_slots() + 1):
        if slot not in used:
            return slot
    return 0

def sessions_add(user_id: int, api_id: int, api_hash: str, session_string: str) -> int:
    slot = first_free_slot(user_id)
    if slot == 0:
        return 0
    conn = get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO user_sessions(user_id, slot, api_id, api_hash, session_string) VALUES(?,?,?,?,?)",
        (user_id, slot, api_id, api_hash, session_string)
    )
    conn.commit(); conn.close()
    return slot

def sessions_upsert_slot(user_id: int, slot: int, api_id: int, api_hash: str, session_string: str) -> int:
    """
    Create/replace a session in a specific slot (1..SESSIONS_MAX_SLOTS) for this user.
    Returns the slot number on success.
    """
    slot = int(slot)
    if slot < 1 or slot > sessions_max_slots():
        raise ValueError(f"slot must be 1..{sessions_max_slots()}")
    conn = get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO user_sessions(user_id, slot, api_id, api_hash, session_string) VALUES(?,?,?,?,?)",
        (user_id, slot, api_id, api_hash, session_string),
    )
    conn.commit(); conn.close()
    return slot

def sessions_delete(user_id: int, slot: int) -> int:
    conn = get_conn()
    cur = conn.execute("DELETE FROM user_sessions WHERE user_id=? AND slot=?", (user_id, slot))
    conn.commit(); conn.close()
    return cur.rowcount

def sessions_delete_all(user_id: int) -> int:
    conn = get_conn()
    cur = conn.execute("DELETE FROM user_sessions WHERE user_id=?", (user_id,))
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

# -------------------- stats --------------------
def bump_sent(user_id: int, inc: int = 1, last_at_iso: str | None = None) -> None:
    conn = get_conn()
    conn.execute("INSERT OR IGNORE INTO stats_user(user_id, sent_ok) VALUES(?,0)", (user_id,))
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
    rows = conn.execute("""
        SELECT u.user_id, u.username, s.sent_ok, s.last_sent_at
        FROM stats_user s
        JOIN users u ON u.user_id = s.user_id
        ORDER BY s.sent_ok DESC, (s.last_sent_at IS NULL), s.last_sent_at DESC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return rows

# -------------------- night mode (global) --------------------
def night_enabled() -> bool:
    v = _get_setting("global:night", "0")
    return str(v) == "1"

def set_night_enabled(on: bool) -> None:
    _set_setting("global:night", "1" if on else "0")

# -------------------- premium / name-lock helpers --------------------
def set_name_lock(user_id: int, enabled: bool, name: str | None = None, expires_at: str | None = None) -> None:
    key = f"name_lock:{user_id}"
    cfg = _get_setting(key, {}) or {}
    cfg["enabled"] = bool(enabled)
    if name is not None:
        cfg["name"] = name
    cfg["expires_at"] = expires_at
    _set_setting(key, cfg)

def get_name_lock(user_id: int):
    v = _get_setting(f"name_lock:{user_id}", None)
    return v if isinstance(v, dict) else None

def name_lock_targets():
    conn = get_conn()
    rows = conn.execute("SELECT key, val FROM settings WHERE key LIKE 'name_lock:%'").fetchall()
    conn.close()
    out = []
    for r in rows:
        try:
            cfg = json.loads(r["val"]) if isinstance(r["val"], str) else r["val"]
        except Exception:
            cfg = None
        if not cfg or not isinstance(cfg, dict) or not cfg.get("enabled"):
            continue
        try:
            uid = int(r["key"].split(":", 1)[1])
        except Exception:
            continue
        out.append({"user_id": uid, "cfg": cfg})
    return out

# -------------------- gate channels helpers --------------------
def set_gate_channels(ch1: Optional[str], ch2: Optional[str]) -> None:
    _set_setting("gate:ch1", ch1 or "")
    _set_setting("gate:ch2", ch2 or "")

def get_gate_channels() -> Tuple[str, str]:
    ch1 = _get_setting("gate:ch1", "") or ""
    ch2 = _get_setting("gate:ch2", "") or ""
    return str(ch1), str(ch2)

# -------------------- per-user next-run scheduler --------------------
def set_next_time(user_id: int, when_iso: str) -> None:
    _set_setting(f"next:{user_id}", when_iso)

def get_next_time(user_id: int) -> Optional[datetime]:
    val = _get_setting(f"next:{user_id}", None)
    if not val:
        return None
    try:
        return datetime.fromisoformat(val)
    except Exception:
        return None

# -------------------- compatibility aliases (old names -> new) --------------------
# users
def upsertUser(user_id: int, username: str | None = None):
    return ensure_user(user_id, username)

# sessions counters
def count_user_sessions(user_id: int) -> int:
    return sessions_count_user(user_id)

def session_count(user_id: int) -> int:
    return sessions_count_user(user_id)

def active_users_count() -> int:
    return sessions_count()

# sessions list/strings
def list_user_sessions(user_id: int):
    return sessions_list(user_id)

def list_sessions(user_id: int):
    return sessions_list(user_id)

def get_user_sessions_strings(user_id: int):
    return sessions_strings(user_id)

def get_sessions_strings(user_id: int):
    return sessions_strings(user_id)

# sessions add/delete/upsert-slot/misc
def add_user_session(user_id: int, api_id: int, api_hash: str, session_string: str) -> int:
    return sessions_add(user_id, api_id, api_hash, session_string)

def upsert_session_slot(user_id: int, slot: int, api_id: int, api_hash: str, session_string: str) -> int:
    return sessions_upsert_slot(user_id, slot, api_id, api_hash, session_string)

def delete_user_session(user_id: int, slot: int) -> int:
    return sessions_delete(user_id, slot)

def delete_session_slot(user_id: int, slot: int) -> int:
    return sessions_delete(user_id, slot)

def delete_session(user_id: int, slot: int) -> int:
    return sessions_delete(user_id, slot)

def delete_all_sessions(user_id: int) -> int:
    return sessions_delete_all(user_id)

# slot helpers (legacy names)
def get_first_free_slot(user_id: int) -> int:
    return first_free_slot(user_id)

def next_free_slot(user_id: int) -> int:
    return first_free_slot(user_id)

# stats aliases
def get_total_messages_forwarded() -> int:
    return get_total_sent_ok()

def top_users_by_sent(limit: int = 10):
    return top_users(limit)

def bump_user_sent(user_id: int, inc: int = 1, last_at_iso: str | None = None) -> None:
    return bump_sent(user_id, inc, last_at_iso)

# group aliases
def add_user_group(user_id: int, group: str) -> int:
    return add_group(user_id, group)

def clear_user_groups(user_id: int) -> None:
    return clear_groups(user_id)

# setting aliases
def get_kv(key: str, default: Any = None) -> Any: return _get_setting(key, default)
def set_kv(key: str, val: Any) -> None: _set_setting(key, val)
def del_kv(key: str) -> None: _del_setting(key)

# ad/interval aliases
def set_user_ad(user_id: int, ad_text: str) -> None: set_ad(user_id, ad_text)
def get_user_ad(user_id: int) -> Optional[str]: return get_ad(user_id)
def set_interval_minutes(user_id: int, minutes: int) -> None: set_interval(user_id, minutes)
def get_user_interval(user_id: int) -> Optional[int]: return get_interval(user_id)

# night mode aliases
def is_night_enabled() -> bool:
    return night_enabled()

def set_night(on: bool) -> None:
    return set_night_enabled(on)

def set_global_night_mode(on: bool) -> None:
    return set_night_enabled(on)

def get_global_night_mode() -> bool:
    return night_enabled()

def global_night_enabled() -> bool:
    return night_enabled()

def is_global_night_mode() -> bool:
    return night_enabled()

def enable_night_mode() -> None:
    return set_night_enabled(True)

def disable_night_mode() -> None:
    return set_night_enabled(False)
  
