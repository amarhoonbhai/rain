import os
import time
import json
import binascii
import base64
import aiosqlite
from typing import Optional, List, Tuple, Any, Dict
from cryptography.fernet import Fernet

DEFAULT_DB_PATH = os.getenv("DB_PATH", "data/spinify.sqlite")

def _build_fernet(key_str: str) -> Fernet:
    """
    Accepts either:
      - urlsafe base64 32-byte Fernet key (44 chars, usually ends with '='), OR
      - 64-char hex string (32 raw bytes) which we will convert to Fernet key.
    """
    if not key_str:
        raise RuntimeError("ENCRYPTION_KEY missing in environment")
    key_str = key_str.strip()

    # Hex format?
    if all(c in "0123456789abcdefABCDEF" for c in key_str) and len(key_str) == 64:
        raw = binascii.unhexlify(key_str)
        fkey = base64.urlsafe_b64encode(raw)
        return Fernet(fkey)

    # Otherwise treat as base64
    try:
        # Validate by constructing a Fernet
        return Fernet(key_str.encode())
    except Exception as e:
        raise RuntimeError("Invalid ENCRYPTION_KEY; must be 64-hex or Fernet base64") from e


class Storage:
    def __init__(self, db_path: str = None, encryption_key: str = None):
        self.db_path = db_path or DEFAULT_DB_PATH
        self._fernet = _build_fernet(encryption_key or os.getenv("ENCRYPTION_KEY", ""))

    # ---------------- Schema ----------------
    async def init(self):
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users(
                    tg_id INTEGER PRIMARY KEY,
                    api_id TEXT,
                    api_hash TEXT,
                    string_session TEXT,
                    phone TEXT,
                    is_active INTEGER DEFAULT 0,
                    created_at INTEGER
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS messages(
                    tg_id INTEGER PRIMARY KEY,
                    text TEXT,
                    updated_at INTEGER
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS targets(
                    tg_id INTEGER,
                    chat_id INTEGER,
                    title TEXT,
                    username TEXT,
                    added_at INTEGER,
                    last_send_at INTEGER,
                    cooldown_until INTEGER,
                    fail_count INTEGER DEFAULT 0,
                    PRIMARY KEY (tg_id, chat_id)
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS audit(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tg_id INTEGER,
                    action TEXT,
                    payload TEXT,
                    ts INTEGER
                )
            """)
            await db.execute("CREATE INDEX IF NOT EXISTS idx_targets_tg ON targets(tg_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_audit_tg ON audit(tg_id)")
            await db.commit()

    # -------------- Helpers --------------
    def _enc(self, s: str) -> str:
        if s is None:
            return None
        token = self._fernet.encrypt(s.encode())
        return token.decode()

    def _dec(self, s: Optional[str]) -> Optional[str]:
        if not s:
            return None
        return self._fernet.decrypt(s.encode()).decode()

    # -------------- Users / Sessions --------------
    async def set_user_session(self, tg_id: int, api_id: str, api_hash: str, string_session: str, phone: str = ""):
        now = int(time.time())
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO users(tg_id, api_id, api_hash, string_session, phone, created_at)
                VALUES (?,?,?,?,?,?)
                ON CONFLICT(tg_id) DO UPDATE SET
                    api_id=excluded.api_id,
                    api_hash=excluded.api_hash,
                    string_session=excluded.string_session,
                    phone=excluded.phone
            """, (tg_id, self._enc(str(api_id)), self._enc(api_hash), self._enc(string_session), self._enc(phone), now))
            await db.commit()

    async def get_user_session(self, tg_id: int) -> Optional[Dict[str, str]]:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT api_id, api_hash, string_session, phone FROM users WHERE tg_id=?", (tg_id,))
            row = await cur.fetchone()
            if not row:
                return None
            api_id, api_hash, string_session, phone = row
            return {
                "api_id": self._dec(api_id),
                "api_hash": self._dec(api_hash),
                "string_session": self._dec(string_session),
                "phone": self._dec(phone) or ""
            }

    async def set_active(self, tg_id: int, active: bool):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE users SET is_active=? WHERE tg_id=?", (1 if active else 0, tg_id))
            await db.commit()

    async def list_active_users(self) -> List[int]:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT tg_id FROM users WHERE is_active=1")
            return [r[0] for r in await cur.fetchall()]

    # -------------- Message text --------------
    async def set_message(self, tg_id: int, text: str):
        now = int(time.time())
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO messages(tg_id, text, updated_at)
                VALUES (?,?,?)
                ON CONFLICT(tg_id) DO UPDATE SET text=excluded.text, updated_at=excluded.updated_at
            """, (tg_id, text, now))
            await db.commit()

    async def get_message(self, tg_id: int) -> Optional[str]:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT text FROM messages WHERE tg_id=?", (tg_id,))
            row = await cur.fetchone()
            return row[0] if row else None

    # -------------- Targets --------------
    async def add_target(self, tg_id: int, chat_id: int, title: str, username: str = None):
        now = int(time.time())
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO targets(tg_id, chat_id, title, username, added_at)
                VALUES (?,?,?,?,?)
                ON CONFLICT(tg_id, chat_id) DO UPDATE SET title=excluded.title, username=excluded.username
            """, (tg_id, int(chat_id), title or "", (username or "")[:64], now))
            await db.commit()

    async def list_targets(self, tg_id: int) -> List[Tuple[int, str, str, int, int, int]]:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("""
                SELECT chat_id, title, username, added_at, last_send_at, COALESCE(cooldown_until,0)
                FROM targets WHERE tg_id=? ORDER BY (last_send_at IS NOT NULL), last_send_at
            """, (tg_id,))
            return await cur.fetchall()

    async def remove_target(self, tg_id: int, chat_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM targets WHERE tg_id=? AND chat_id=?", (tg_id, int(chat_id)))
            await db.commit()

    async def due_targets(self, tg_id: int, now_ts: int) -> List[int]:
        """
        Return chat_ids where:
          - cooldown_until is NULL or <= now
          - last_send_at is NULL or <= now - 1800 (30 min)
        """
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("""
                SELECT chat_id FROM targets
                WHERE tg_id=?
                  AND (cooldown_until IS NULL OR cooldown_until<=?)
                  AND (last_send_at IS NULL OR last_send_at<=?)
                ORDER BY (last_send_at IS NOT NULL), last_send_at
            """, (tg_id, now_ts, now_ts - 1800))
            rows = await cur.fetchall()
            return [r[0] for r in rows]

    async def mark_sent(self, tg_id: int, chat_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE targets SET last_send_at=? WHERE tg_id=? AND chat_id=?", (int(time.time()), tg_id, int(chat_id)))
            await db.commit()

    async def set_cooldown(self, tg_id: int, chat_id: int, seconds: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE targets SET cooldown_until=? WHERE tg_id=? AND chat_id=?", (int(time.time()) + int(seconds), tg_id, int(chat_id)))
            await db.commit()

    async def inc_fail(self, tg_id: int, chat_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE targets SET fail_count=COALESCE(fail_count,0)+1 WHERE tg_id=? AND chat_id=?", (tg_id, int(chat_id)))
            await db.commit()

    async def reset_fail(self, tg_id: int, chat_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE targets SET fail_count=0 WHERE tg_id=? AND chat_id=?", (tg_id, int(chat_id)))
            await db.commit()

    # -------------- Audit --------------
    async def audit(self, tg_id: int, action: str, payload: Dict[str, Any] = None):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("INSERT INTO audit(tg_id, action, payload, ts) VALUES (?,?,?,?)",
                             (tg_id, action, json.dumps(payload or {}), int(time.time())))
            await db.commit()

    async def last_audit(self, tg_id: int, limit: int = 10):
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT action, payload, ts FROM audit WHERE tg_id=? ORDER BY id DESC LIMIT ?", (tg_id, int(limit)))
            return await cur.fetchall()
            
