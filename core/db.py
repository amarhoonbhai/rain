# core/db.py — SQLite helpers for Spinify stack
import os, sqlite3, time
from typing import Any, Iterable, Optional, List, Dict

DB_PATH = os.getenv("RAIN_DB_PATH", os.path.join(os.getcwd(), "rain.db"))

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _exec(conn: sqlite3.Connection, sql: str, args: Iterable[Any] = ()):
    cur = conn.cursor()
    cur.execute(sql, args)
    conn.commit()
    return cur

def init_db():
    conn = get_conn()
    _exec(conn, """
    CREATE TABLE IF NOT EXISTS users(
      user_id   INTEGER PRIMARY KEY,
      username  TEXT,
      created_at INTEGER DEFAULT (strftime('%s','now'))
    )""")

    _exec(conn, """
    CREATE TABLE IF NOT EXISTS user_sessions(
      user_id        INTEGER,
      slot           INTEGER,
      api_id         INTEGER,
      api_hash       TEXT,
      session_string TEXT,
      created_at     INTEGER DEFAULT (strftime('%s','now')),
      PRIMARY KEY(user_id, slot)
    )""")

    _exec(conn, """
    CREATE TABLE IF NOT EXISTS user_groups(
      user_id   INTEGER,
      value     TEXT,
      created_at INTEGER DEFAULT (strftime('%s','now')),
      PRIMARY KEY(user_id, value)
    )""")

    _exec(conn, """
    CREATE TABLE IF NOT EXISTS user_intervals(
      user_id INTEGER PRIMARY KEY,
      minutes INTEGER
    )""")

    _exec(conn, """
    CREATE TABLE IF NOT EXISTS stats(
      user_id  INTEGER PRIMARY KEY,
      sent_ok  INTEGER DEFAULT 0
    )""")

    _exec(conn, """
    CREATE TABLE IF NOT EXISTS settings(
      key TEXT PRIMARY KEY,
      val TEXT
    )""")

    _exec(conn, "CREATE INDEX IF NOT EXISTS idx_groups_user ON user_groups(user_id)")
    _exec(conn, "CREATE INDEX IF NOT EXISTS idx_sessions_user ON user_sessions(user_id)")
    _exec(conn, "CREATE INDEX IF NOT EXISTS idx_settings_prefix ON settings(key)")

    conn.close()

# ---------------- generic KV ----------------
def set_setting(key: str, val: Any):
    conn = get_conn()
    _exec(conn, "INSERT INTO settings(key,val) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET val=excluded.val",
          (key, str(val)))
    conn.close()

def get_setting(key: str, default: Any = None) -> Any:
    conn = get_conn()
    row = conn.execute("SELECT val FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    return row["val"] if row else default

# ---------------- users ----------------
def ensure_user(user_id: int, username: Optional[str]):
    conn = get_conn()
    _exec(conn, "INSERT INTO users(user_id, username) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET username=excluded.username",
          (user_id, username))
    conn.close()

def users_count() -> int:
    conn = get_conn()
    n = conn.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"]
    conn.close()
    return int(n or 0)

# ---------------- sessions ----------------
def sessions_list(user_id: int) -> List[sqlite3.Row]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT user_id, slot, api_id, api_hash FROM user_sessions WHERE user_id=? ORDER BY slot ASC",
        (user_id,)).fetchall()
    conn.close()
    return rows

def sessions_strings(user_id: int) -> List[Dict[str, Any]]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT user_id, slot, api_id, api_hash, session_string FROM user_sessions WHERE user_id=? ORDER BY slot ASC",
        (user_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def sessions_delete(user_id: int, slot: int):
    conn = get_conn()
    _exec(conn, "DELETE FROM user_sessions WHERE user_id=? AND slot=?", (user_id, slot))
    conn.close()

def sessions_count_user(user_id: int) -> int:
    conn = get_conn()
    n = conn.execute("SELECT COUNT(*) AS n FROM user_sessions WHERE user_id=?", (user_id,)).fetchone()["n"]
    conn.close()
    return int(n or 0)

def sessions_count() -> int:
    conn = get_conn()
    n = conn.execute("SELECT COUNT(DISTINCT user_id) AS n FROM user_sessions").fetchone()["n"]
    conn.close()
    return int(n or 0)

# ---------------- groups ----------------
def _groups_cap_from_setting(user_id: int) -> int:
    v = get_setting(f"user:{user_id}:groups_cap", None)
    try:
        return int(v)
    except Exception:
        return 5

def set_user_groups_cap(user_id: int, cap: int):
    set_setting(f"user:{user_id}:groups_cap", int(cap))

def groups_cap(user_id: int) -> int:
    cap = _groups_cap_from_setting(user_id)
    if cap not in (5, 10): cap = 5
    return cap

def list_groups(user_id: int) -> List[str]:
    conn = get_conn()
    rows = conn.execute("SELECT value FROM user_groups WHERE user_id=? ORDER BY created_at ASC", (user_id,)).fetchall()
    conn.close()
    return [r["value"] for r in rows]

def add_group(user_id: int, value: str) -> int:
    value = str(value).strip()
    if not value:
        return 0
    # enforce cap
    if len(list_groups(user_id)) >= groups_cap(user_id):
        return 0
    conn = get_conn()
    try:
        _exec(conn, "INSERT INTO user_groups(user_id, value) VALUES(?,?)", (user_id, value))
        return 1
    except Exception:
        return 0
    finally:
        conn.close()

def clear_groups(user_id: int):
    conn = get_conn()
    _exec(conn, "DELETE FROM user_groups WHERE user_id=?", (user_id,))
    conn.close()

def replace_group_value(user_id: int, old: str, new: str):
    conn = get_conn()
    _exec(conn, "UPDATE user_groups SET value=? WHERE user_id=? AND value=?", (str(new), user_id, str(old)))
    conn.close()

# ---------------- intervals / scheduling ----------------
def set_interval(user_id: int, minutes: int):
    conn = get_conn()
    _exec(conn, "INSERT INTO user_intervals(user_id, minutes) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET minutes=excluded.minutes",
          (user_id, minutes))
    conn.close()

def get_interval(user_id: int) -> Optional[int]:
    conn = get_conn()
    row = conn.execute("SELECT minutes FROM user_intervals WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    return int(row["minutes"]) if row else None

def get_last_sent_at(user_id: int) -> Optional[int]:
    v = get_setting(f"user:{user_id}:last_sent_at", None)
    try:
        return int(v) if v is not None else None
    except Exception:
        return None

def mark_sent_now(user_id: int):
    set_setting(f"user:{user_id}:last_sent_at", int(time.time()))

def reset_last_sent(user_id: int):
    set_setting(f"user:{user_id}:last_sent_at", 0)

# ---------------- stats ----------------
def inc_sent_ok(user_id: int, delta: int):
    conn = get_conn()
    _exec(conn, "INSERT INTO stats(user_id, sent_ok) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET sent_ok = stats.sent_ok + ?",
          (user_id, delta, delta))
    conn.close()

def get_total_sent_ok() -> int:
    conn = get_conn()
    row = conn.execute("SELECT SUM(sent_ok) AS s FROM stats").fetchone()
    conn.close()
    return int(row["s"] or 0)

def top_users(n: int = 10) -> List[sqlite3.Row]:
    conn = get_conn()
    rows = conn.execute("SELECT user_id, sent_ok FROM stats ORDER BY sent_ok DESC LIMIT ?", (int(n),)).fetchall()
    conn.close()
    return rows

# ---------------- global/night mode & gate ----------------
def night_enabled() -> bool:
    v = str(get_setting("global:night_enabled", "0")).lower()
    return v in ("1","true","yes","on")

def set_night_enabled(on: bool):
    set_setting("global:night_enabled", "1" if on else "0")

def get_gate_channels_effective():
    # returns (ch1, ch2) from REQUIRED_CHANNELS env (csv) or blanks
    csv = os.getenv("REQUIRED_CHANNELS", "").strip()
    if not csv:
        return ("@PhiloBots", "@TheTrafficZone")
    items = [x.strip() for x in csv.split(",") if x.strip()]
    ch1 = items[0] if len(items) >= 1 else ""
    ch2 = items[1] if len(items) >= 2 else ""
    return (ch1, ch2)

# ---------------- premium name-lock flags (optional) ----------------
def set_name_lock(user_id: int, enabled: bool, name: Optional[str] = None):
    set_setting(f"name_lock:enabled:{user_id}", "1" if enabled else "0")
    if name is not None:
        set_setting(f"name_lock:name:{user_id}", name)

# ---------------- worker helpers ----------------
def users_with_sessions() -> List[int]:
    conn = get_conn()
    rows = conn.execute("SELECT DISTINCT user_id FROM user_sessions ORDER BY user_id ASC").fetchall()
    conn.close()
    return [r["user_id"] for r in rows]
