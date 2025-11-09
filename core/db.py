# core/db.py — SQLite helpers & bot storage
import os, sqlite3, time, json
from typing import Any, Iterable

DB_PATH = os.getenv("DB_PATH", os.path.join(os.path.dirname(__file__), "..", "rain.db"))
DB_PATH = os.path.abspath(DB_PATH)

# ---------------- Connection ----------------
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

# ---------------- Init ----------------
def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")

    # users
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id     INTEGER PRIMARY KEY,
        username    TEXT,
        created_at  INTEGER DEFAULT (strftime('%s','now'))
    );
    """)

    # sessions (up to 3 slots per user)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS user_sessions (
        user_id        INTEGER,
        slot           INTEGER,
        api_id         INTEGER,
        api_hash       TEXT,
        session_string TEXT,
        PRIMARY KEY (user_id, slot)
    );
    """)

    # groups (stored as raw tokens/usernames/links/ids)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS groups (
        user_id    INTEGER,
        group_text TEXT,
        UNIQUE(user_id, group_text)
    );
    """)

    # ads (text + parse mode)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS user_ads (
        user_id INTEGER PRIMARY KEY,
        text    TEXT,
        mode    TEXT
    );
    """)

    # stats (forwards counter)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS user_stats (
        user_id  INTEGER PRIMARY KEY,
        sent_ok  INTEGER DEFAULT 0
    );
    """)

    # KV settings (global or per-user keys)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        val TEXT
    );
    """)

    conn.commit()
    conn.close()

# ---------------- Utilities ----------------
def _to_dict_rows(rows: Iterable[sqlite3.Row]) -> list[dict]:
    return [dict(r) for r in rows]

def _put_setting_raw(key: str, val: Any):
    conn = get_conn()
    conn.execute("INSERT INTO settings(key,val) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET val=excluded.val", (key, val))
    conn.commit(); conn.close()

# ---------------- Settings (typed) ----------------
def set_setting(key: str, val: Any) -> None:
    try:
        s = json.dumps(val)
    except Exception:
        s = str(val)
    _put_setting_raw(key, s)

def get_setting(key: str, default: Any = None) -> Any:
    conn = get_conn()
    row = conn.execute("SELECT val FROM settings WHERE key = ?", (key,)).fetchone()
    conn.close()
    if not row:
        return default
    val = row["val"]
    try:
        return json.loads(val)
    except Exception:
        return val

# ---------------- Users ----------------
def ensure_user(user_id: int, username: str | None):
    conn = get_conn()
    conn.execute(
        "INSERT INTO users(user_id, username) VALUES(?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET username=excluded.username",
        (int(user_id), username)
    )
    conn.commit(); conn.close()

def users_count() -> int:
    conn = get_conn()
    n = conn.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"]
    conn.close()
    return int(n or 0)

# ---------------- Sessions ----------------
def sessions_list(user_id: int) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT user_id, slot, api_id, api_hash FROM user_sessions WHERE user_id=? ORDER BY slot",
        (int(user_id),)
    ).fetchall()
    conn.close()
    return _to_dict_rows(rows)

def sessions_delete(user_id: int, slot: int) -> None:
    conn = get_conn()
    conn.execute("DELETE FROM user_sessions WHERE user_id=? AND slot=?", (int(user_id), int(slot)))
    conn.commit(); conn.close()

def sessions_count() -> int:
    conn = get_conn()
    n = conn.execute("SELECT COUNT(DISTINCT user_id) AS n FROM user_sessions").fetchone()["n"]
    conn.close()
    return int(n or 0)

def sessions_count_user(user_id: int) -> int:
    conn = get_conn()
    n = conn.execute("SELECT COUNT(*) AS n FROM user_sessions WHERE user_id=?", (int(user_id),)).fetchone()["n"]
    conn.close()
    return int(n or 0)

def sessions_strings(user_id: int) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT user_id, slot, api_id, api_hash, session_string FROM user_sessions WHERE user_id=? ORDER BY slot",
        (int(user_id),)
    ).fetchall()
    conn.close()
    return _to_dict_rows(rows)

def users_with_sessions() -> list[int]:
    conn = get_conn()
    rows = conn.execute("SELECT DISTINCT user_id FROM user_sessions").fetchall()
    conn.close()
    return [int(r["user_id"]) for r in rows]

# slot helpers (needed by login_bot.py)
def first_free_slot(user_id: int, max_slots: int = 3) -> int | None:
    conn = get_conn()
    rows = conn.execute("SELECT slot FROM user_sessions WHERE user_id=? ORDER BY slot", (int(user_id),)).fetchall()
    conn.close()
    used = {int(r["slot"]) for r in rows}
    for s in range(1, int(max_slots) + 1):
        if s not in used:
            return s
    return None

def sessions_upsert_slot(user_id: int, slot: int, api_id: int, api_hash: str, session_string: str) -> None:
    conn = get_conn()
    conn.execute("DELETE FROM user_sessions WHERE user_id=? AND slot=?", (int(user_id), int(slot)))
    conn.execute(
        "INSERT INTO user_sessions(user_id, slot, api_id, api_hash, session_string) VALUES(?,?,?,?,?)",
        (int(user_id), int(slot), int(api_id), str(api_hash), str(session_string))
    )
    conn.commit(); conn.close()

# ---------------- Groups ----------------
def groups_cap(user_id: int | None = None) -> int:
    if user_id is None:
        return 5
    unlock = int(get_setting(f"gc_unlock:{int(user_id)}", 0) or 0)
    return 10 if unlock else 5

def list_groups(user_id: int) -> list[str]:
    conn = get_conn()
    rows = conn.execute("SELECT group_text FROM groups WHERE user_id=? ORDER BY rowid", (int(user_id),)).fetchall()
    conn.close()
    return [r["group_text"] for r in rows]

def add_group(user_id: int, group_text: str) -> int:
    group_text = (group_text or "").strip()
    if not group_text:
        return 0
    # capacity check
    cur_list = list_groups(user_id)
    if len(cur_list) >= groups_cap(user_id):
        return 0
    conn = get_conn()
    try:
        conn.execute("INSERT INTO groups(user_id, group_text) VALUES(?, ?)", (int(user_id), group_text))
        conn.commit(); added = 1
    except sqlite3.IntegrityError:
        added = 0
    finally:
        conn.close()
    return added

def clear_groups(user_id: int) -> None:
    conn = get_conn()
    conn.execute("DELETE FROM groups WHERE user_id=?", (int(user_id),))
    conn.commit(); conn.close()

# ---------------- Intervals & last-send ----------------
def set_interval(user_id: int, minutes: int) -> None:
    set_setting(f"interval:{int(user_id)}", int(minutes))

def get_interval(user_id: int) -> int | None:
    v = get_setting(f"interval:{int(user_id)}", None)
    return int(v) if v is not None else None

def mark_sent_now(user_id: int) -> None:
    set_setting(f"user:last_sent_at:{int(user_id)}", int(time.time()))

def get_last_sent_at(user_id: int) -> int | None:
    v = get_setting(f"user:last_sent_at:{int(user_id)}", None)
    return int(v) if v is not None else None

# ---------------- Ads (DB-stored text) ----------------
def set_ad(user_id: int, text: str, mode: str | None) -> None:
    mode = None if (mode is None or str(mode).strip().lower() in ("", "none", "plain")) else str(mode)
    conn = get_conn()
    conn.execute(
        "INSERT INTO user_ads(user_id, text, mode) VALUES(?,?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET text=excluded.text, mode=excluded.mode",
        (int(user_id), text, mode)
    )
    conn.commit(); conn.close()

def get_ad(user_id: int) -> tuple[str | None, str | None]:
    conn = get_conn()
    row = conn.execute("SELECT text, mode FROM user_ads WHERE user_id=?", (int(user_id),)).fetchone()
    conn.close()
    if not row:
        return None, None
    return row["text"], row["mode"]

# ---------------- Night mode ----------------
def set_night_enabled(enabled: bool) -> None:
    set_setting("night_mode", 1 if enabled else 0)

def night_enabled() -> bool:
    return bool(int(get_setting("night_mode", 0) or 0))

# ---------------- Gate channels ----------------
def get_gate_channels_effective() -> tuple[str | None, str | None]:
    ch1 = get_setting("gate:ch1", None)
    ch2 = get_setting("gate:ch2", None)
    return (ch1, ch2)

# ---------------- Premium (name lock) ----------------
def set_name_lock(user_id: int, enabled: bool, name: str | None = None) -> None:
    set_setting(f"name_lock:{int(user_id)}", 1 if enabled else 0)
    if name is not None:
        set_setting(f"name_lock:{int(user_id)}:name", name)

# ---------------- Stats ----------------
def inc_sent_ok(user_id: int, n: int = 1) -> None:
    conn = get_conn()
    # upsert
    conn.execute(
        "INSERT INTO user_stats(user_id, sent_ok) VALUES(?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET sent_ok = user_stats.sent_ok + ?",
        (int(user_id), int(n), int(n))
    )
    conn.commit(); conn.close()

def get_total_sent_ok() -> int:
    conn = get_conn()
    row = conn.execute("SELECT SUM(sent_ok) AS s FROM user_stats").fetchone()
    conn.close()
    return int(row["s"] or 0)

def top_users(n: int = 10) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT user_id, sent_ok FROM user_stats ORDER BY sent_ok DESC LIMIT ?",
        (int(n),)
    ).fetchall()
    conn.close()
    return _to_dict_rows(rows)

# ---------------- Optional: pause/resume (handy for future) ----------------
def set_paused(user_id: int, paused: bool) -> None:
    set_setting(f"pause:{int(user_id)}", 1 if paused else 0)

def is_paused(user_id: int) -> bool:
    return bool(int(get_setting(f"pause:{int(user_id)}", 0) or 0))
