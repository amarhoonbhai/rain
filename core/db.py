# core/db.py — tiny sqlite helper
import sqlite3
import pathlib

DB_PATH = pathlib.Path(__file__).resolve().parent.parent / "spinify.db"

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS user_sessions (
        user_id        INTEGER PRIMARY KEY,
        api_id         INTEGER NOT NULL,
        api_hash       TEXT    NOT NULL,
        session_string TEXT    NOT NULL,
        created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    conn.commit()
    conn.close()

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn
