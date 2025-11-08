# core/db.py — SQLite helpers for users/sessions/groups/stats/settings/ads
import sqlite3, time
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data.sqlite3"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.executescript("""
    PRAGMA journal_mode=WAL;
    PRAGMA synchronous=NORMAL;

    CREATE TABLE IF NOT EXISTS users (
      user_id     INTEGER PRIMARY KEY,
      username    TEXT,
      created_at  INTEGER
    );

    CREATE TABLE IF NOT EXISTS settings (
      key   TEXT PRIMARY KEY,
      val   TEXT
    );

    CREATE TABLE IF NOT EXISTS sessions (
      user_id         INTEGER NOT NULL,
      slot            INTEGER NOT NULL,
      api_id          INTEGER NOT NULL,
      api_hash        TEXT    NOT NULL,
      session_string  TEXT    NOT NULL,
      PRIMARY KEY (user_id, slot)
    );

    CREATE TABLE IF NOT EXISTS groups (
      user_id INTEGER NOT NULL,
      value   TEXT    NOT NULL,
      PRIMARY KEY (user_id, value)
    );

    CREATE TABLE IF NOT EXISTS ads (
      user_id    INTEGER PRIMARY KEY,
      text       TEXT,
      mode       TEXT,    -- Plain(None) | Markdown | HTML
      updated_at INTEGER
    );

    CREATE TABLE IF NOT EXISTS stats (
      user_id      INTEGER PRIMARY KEY,
      sent_ok      INTEGER DEFAULT 0,
      last_sent_at INTEGER
    );
    """)
    conn.commit()
    conn.close()

# ---------------- users ----------------
def ensure_user(user_id: int, username: str | None):
    conn = get_conn()
    conn.execute(
        "INSERT INTO users(user_id, username, created_at) VALUES (?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET username=excluded.username",
        (user_id, username, int(time.time()))
    )
    conn.commit(); conn.close()

def users_count() -> int:
    conn = get_conn()
    n = conn.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"]
    conn.close()
    return int(n)

# ---------------- settings (KV) ----------------
def set_setting(key: str, val):
    conn = get_conn()
    conn.execute(
        "INSERT INTO settings(key, val) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET val=excluded.val",
        (key, str(val))
    )
    conn.commit(); conn.close()

def get_setting(key: str, default=None):
    conn = get_conn()
    r = conn.execute("SELECT val FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    return r["val"] if r else default

# ---------------- sessions ----------------
MAX_SLOTS = 3

def sessions_list(user_id: int) -> list:
    conn = get_conn()
    rows = conn.execute(
        "SELECT user_id, slot, api_id, api_hash FROM sessions WHERE user_id=? ORDER BY slot ASC",
        (user_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def sessions_strings(user_id: int) -> list:
    conn = get_conn()
    rows = conn.execute(
        "SELECT user_id, slot, api_id, api_hash, session_string FROM sessions WHERE user_id=? ORDER BY slot ASC",
        (user_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def sessions_count_user(user_id: int) -> int:
    conn = get_conn()
    n = conn.execute("SELECT COUNT(*) AS n FROM sessions WHERE user_id=?", (user_id,)).fetchone()["n"]
    conn.close()
    return int(n)

def sessions_count() -> int:
    conn = get_conn()
    n = conn.execute("""
      SELECT COUNT(DISTINCT user_id) AS n FROM sessions
    """).fetchone()["n"]
    conn.close()
    return int(n)

def first_free_slot(user_id: int) -> int | None:
    have = {r["slot"] for r in sessions_list(user_id)}
    for s in range(1, MAX_SLOTS+1):
        if s not in have:
            return s
    return None

def sessions_upsert_auto(user_id: int, api_id: int, api_hash: str, session_string: str) -> int:
    slot = first_free_slot(user_id)
    if slot is None:
        # Overwrite slot 1 by default if full
        slot = 1
    conn = get_conn()
    conn.execute(
        "INSERT INTO sessions(user_id, slot, api_id, api_hash, session_string) VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT(user_id,slot) DO UPDATE SET api_id=excluded.api_id, api_hash=excluded.api_hash, session_string=excluded.session_string",
        (user_id, slot, api_id, api_hash, session_string)
    )
    conn.commit(); conn.close()
    return slot

def sessions_delete(user_id: int, slot: int):
    conn = get_conn()
    conn.execute("DELETE FROM sessions WHERE user_id=? AND slot=?", (user_id, slot))
    conn.commit(); conn.close()

def users_with_sessions() -> list[int]:
    conn = get_conn()
    rows = conn.execute("SELECT DISTINCT user_id FROM sessions ORDER BY user_id ASC").fetchall()
    conn.close()
    return [r["user_id"] for r in rows]

# ---------------- groups ----------------
def list_groups(user_id: int) -> list[str]:
    conn = get_conn()
    rows = conn.execute("SELECT value FROM groups WHERE user_id=? ORDER BY value ASC", (user_id,)).fetchall()
    conn.close()
    return [r["value"] for r in rows]

def add_group(user_id: int, value: str) -> int:
    value = (value or "").strip()
    if not value:
        return 0
    conn = get_conn()
    try:
        conn.execute("INSERT INTO groups(user_id, value) VALUES (?, ?)", (user_id, value))
        conn.commit()
        return 1
    except sqlite3.IntegrityError:
        return 0
    finally:
        conn.close()

def clear_groups(user_id: int):
    conn = get_conn()
    conn.execute("DELETE FROM groups WHERE user_id=?", (user_id,))
    conn.commit(); conn.close()

# ---------------- interval ----------------
def set_interval(user_id: int, minutes: int):
    set_setting(f"interval:{user_id}", int(minutes))

def get_interval(user_id: int) -> int | None:
    v = get_setting(f"interval:{user_id}", None)
    try:
        return int(v) if v is not None else None
    except Exception:
        return None

# ---------------- ads (fallback text) ----------------
def set_ad(user_id: int, text: str, mode: str | None):
    conn = get_conn()
    conn.execute(
        "INSERT INTO ads(user_id, text, mode, updated_at) VALUES(?, ?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET text=excluded.text, mode=excluded.mode, updated_at=excluded.updated_at",
        (user_id, text, mode, int(time.time()))
    )
    conn.commit(); conn.close()

def get_ad(user_id: int):
    conn = get_conn()
    r = conn.execute("SELECT text, mode FROM ads WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    return (r["text"], r["mode"]) if r else (None, None)

# ---------------- stats ----------------
def mark_sent_now(user_id: int):
    conn = get_conn()
    conn.execute(
        "INSERT INTO stats(user_id, sent_ok, last_sent_at) VALUES(?, 0, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET last_sent_at=excluded.last_sent_at",
        (user_id, int(time.time()))
    )
    conn.commit(); conn.close()

def get_last_sent_at(user_id: int) -> int | None:
    conn = get_conn()
    r = conn.execute("SELECT last_sent_at FROM stats WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    return int(r["last_sent_at"]) if (r and r["last_sent_at"] is not None) else None

def inc_sent_ok(user_id: int, n: int):
    conn = get_conn()
    conn.execute(
        "INSERT INTO stats(user_id, sent_ok, last_sent_at) VALUES(?, ?, NULL) "
        "ON CONFLICT(user_id) DO UPDATE SET sent_ok=sent_ok + ?",
        (user_id, int(n), int(n))
    )
    conn.commit(); conn.close()

def get_total_sent_ok() -> int:
    conn = get_conn()
    r = conn.execute("SELECT COALESCE(SUM(sent_ok),0) AS s FROM stats").fetchone()
    conn.close()
    return int(r["s"] or 0)

def top_users(n: int) -> list:
    n = max(1, min(50, int(n)))
    conn = get_conn()
    rows = conn.execute(
        "SELECT user_id, sent_ok FROM stats ORDER BY sent_ok DESC LIMIT ?",
        (n,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

# ---------------- night mode ----------------
def night_enabled() -> bool:
    v = str(get_setting("global:night_enabled", "0")).lower()
    return v in ("1","true","yes","on")

def set_night_enabled(on: bool):
    set_setting("global:night_enabled", "1" if on else "0")
