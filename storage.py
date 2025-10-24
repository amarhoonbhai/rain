import aiosqlite
import os
from typing import Optional, List, Tuple, Any, Dict
from cryptography.fernet import Fernet
import base64, binascii, json, time

DEFAULT_DB_PATH = os.getenv("DB_PATH", "data/spinify.sqlite")

def _fernet_from_hex(hexkey: str) -> Fernet:
    """
    Accepts a 32-byte hex key (64 chars), derives a Fernet key.
    """
    if not hexkey:
        raise RuntimeError("ENCRYPTION_KEY missing")
    try:
        raw = binascii.unhexlify(hexkey.strip())
    except Exception:
        raise RuntimeError("ENCRYPTION_KEY must be 64 hex chars (32 bytes)")
    if len(raw) != 32:
        raise RuntimeError("ENCRYPTION_KEY must be 32 bytes (64 hex chars)")
    # Derive urlsafe base64 key for Fernet
    fkey = base64.urlsafe_b64encode(raw)
    return Fernet(fkey)

class Storage:
    def __init__(self, db_path: str = DEFAULT_DB_PATH, enc_hex: str = ""):
        self.db_path = db_path
        self.enc = _fernet_from_hex(enc_hex)

    async def init(self):
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS users(
                tg_id INTEGER PRIMARY KEY,
                created_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sessions(
                tg_id INTEGER PRIMARY KEY,
                api_id INTEGER NOT NULL,
                api_hash TEXT NOT NULL,
                session_enc BLOB NOT NULL,
                valid INTEGER NOT NULL DEFAULT 1,
                last_check_at INTEGER
            );
            CREATE TABLE IF NOT EXISTS settings(
                tg_id INTEGER PRIMARY KEY,
                posting_on INTEGER NOT NULL DEFAULT 0,
                last_global_send INTEGER,
                interval_s INTEGER NOT NULL DEFAULT 1800, -- 30min
                created_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS blueprint(
                tg_id INTEGER PRIMARY KEY,
                src_chat_id INTEGER NOT NULL,
                src_msg_id INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS targets(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                title TEXT,
                type TEXT, -- 'group'|'channel'
                enabled INTEGER NOT NULL DEFAULT 1,
                last_send_at INTEGER,
                fail_count INTEGER NOT NULL DEFAULT 0,
                cooldown_until INTEGER,
                last_verified_at INTEGER,
                UNIQUE(tg_id, chat_id)
            );
            CREATE TABLE IF NOT EXISTS audit(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                payload TEXT,
                ts INTEGER NOT NULL
            );
            """)
            await db.commit()

    # --- Users & settings ---
    async def ensure_user(self, tg_id: int):
        now = int(time.time())
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("INSERT OR IGNORE INTO users(tg_id, created_at) VALUES (?,?)", (tg_id, now))
            await db.execute("INSERT OR IGNORE INTO settings(tg_id, created_at) VALUES (?,?)", (tg_id, now))
            await db.commit()

    async def set_posting(self, tg_id: int, on: bool):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE settings SET posting_on=? WHERE tg_id=?", (1 if on else 0, tg_id))
            await db.commit()

    async def get_settings(self, tg_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT posting_on, last_global_send, interval_s FROM settings WHERE tg_id=?", (tg_id,))
            row = await cur.fetchone()
            return row

    async def touch_global_send(self, tg_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE settings SET last_global_send=? WHERE tg_id=?", (int(time.time()), tg_id))
            await db.commit()

    # --- Sessions ---
    async def save_session(self, tg_id: int, api_id: int, api_hash: str, session_str: str):
        token = self.enc.encrypt(session_str.encode("utf-8"))
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
            INSERT INTO sessions(tg_id, api_id, api_hash, session_enc, valid, last_check_at)
            VALUES (?,?,?,?,1,?)
            ON CONFLICT(tg_id) DO UPDATE SET api_id=excluded.api_id, api_hash=excluded.api_hash,
                session_enc=excluded.session_enc, valid=1, last_check_at=excluded.last_check_at
            """, (tg_id, api_id, api_hash, token, int(time.time())))
            await db.commit()

    async def get_session(self, tg_id: int) -> Optional[Tuple[int, str, str]]:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT api_id, api_hash, session_enc FROM sessions WHERE tg_id=? AND valid=1", (tg_id,))
            row = await cur.fetchone()
            if not row: return None
            api_id, api_hash, enc = row
            session_str = self.enc.decrypt(enc).decode("utf-8")
            return api_id, api_hash, session_str

    async def invalidate_session(self, tg_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE sessions SET valid=0 WHERE tg_id=?", (tg_id,))
            await db.commit()

    # --- Blueprint ---
    async def save_blueprint(self, tg_id: int, src_chat_id: int, src_msg_id: int):
        now = int(time.time())
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
            INSERT INTO blueprint(tg_id, src_chat_id, src_msg_id, updated_at)
            VALUES (?,?,?,?)
            ON CONFLICT(tg_id) DO UPDATE SET src_chat_id=excluded.src_chat_id,
                src_msg_id=excluded.src_msg_id, updated_at=excluded.updated_at
            """, (tg_id, src_chat_id, src_msg_id, now))
            await db.commit()

    async def get_blueprint(self, tg_id: int) -> Optional[Tuple[int, int]]:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT src_chat_id, src_msg_id FROM blueprint WHERE tg_id=?", (tg_id,))
            return await cur.fetchone()

    async def delete_blueprint(self, tg_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM blueprint WHERE tg_id=?", (tg_id,))
            await db.commit()

    # --- Targets ---
    async def add_target(self, tg_id: int, chat_id: int, title: str, type_: str):
        now = int(time.time())
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
            INSERT OR IGNORE INTO targets(tg_id, chat_id, title, type, last_verified_at)
            VALUES (?,?,?,?,?)
            """, (tg_id, chat_id, title, type_, now))
            await db.commit()

    async def list_targets(self, tg_id: int) -> List[Tuple[int, str, str, int]]:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT chat_id, title, type, enabled FROM targets WHERE tg_id=? ORDER BY title", (tg_id,))
            return await cur.fetchall()

    async def update_target_meta(self, tg_id: int, chat_id: int, title: str = None, enabled: Optional[bool] = None):
        async with aiosqlite.connect(self.db_path) as db:
            if title is not None:
                await db.execute("UPDATE targets SET title=?, last_verified_at=? WHERE tg_id=? AND chat_id=?", (title, int(time.time()), tg_id, chat_id))
            if enabled is not None:
                await db.execute("UPDATE targets SET enabled=? WHERE tg_id=? AND chat_id=?", (1 if enabled else 0, tg_id, chat_id))
            await db.commit()

    async def targets_due(self, tg_id: int, now_ts: int) -> List[int]:
        """Return chat_ids that are due to receive a post (>=30m since last_send and no cooldown)."""
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("""
                SELECT chat_id FROM targets
                WHERE tg_id=?
                AND enabled=1
                AND (cooldown_until IS NULL OR cooldown_until<=?)
                AND (last_send_at IS NULL OR last_send_at<=? - 1800)
                ORDER BY last_send_at NULLS FIRST
            """, (tg_id, now_ts, now_ts))
            rows = await cur.fetchall()
            return [r[0] for r in rows]

    async def mark_sent(self, tg_id: int, chat_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE targets SET last_send_at=? WHERE tg_id=? AND chat_id=?", (int(time.time()), tg_id, chat_id))
            await db.commit()

    async def set_cooldown(self, tg_id: int, chat_id: int, seconds: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE targets SET cooldown_until=? WHERE tg_id=? AND chat_id=?", (int(time.time())+seconds, tg_id, chat_id))
            await db.commit()

    async def inc_fail(self, tg_id: int, chat_id: int) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT fail_count FROM targets WHERE tg_id=? AND chat_id=?", (tg_id, chat_id))
            row = await cur.fetchone()
            fails = (row[0] if row else 0) + 1
            await db.execute("UPDATE targets SET fail_count=? WHERE tg_id=? AND chat_id=?", (fails, tg_id, chat_id))
            await db.commit()
            return fails

    async def reset_fail(self, tg_id: int, chat_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE targets SET fail_count=0 WHERE tg_id=? AND chat_id=?", (tg_id, chat_id))
            await db.commit()

    # --- Audit ---
    async def audit(self, tg_id: int, action: str, payload: Dict[str, Any] = None):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("INSERT INTO audit(tg_id, action, payload, ts) VALUES (?,?,?,?)",
                             (tg_id, action, json.dumps(payload or {}), int(time.time())))
            await db.commit()

    async def last_audit(self, tg_id: int, limit: int = 10):
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT action, payload, ts FROM audit WHERE tg_id=? ORDER BY id DESC LIMIT ?", (tg_id, limit))
            rows = await cur.fetchall()
            return rows
