# core/db.py
import os
import sqlite3
from pathlib import Path

_DB_PATH = Path(__file__).resolve().parent.parent / "spinify.db"

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = get_conn()
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS user_sessions(
      user_id        INTEGER PRIMARY KEY,
      api_id         INTEGER NOT NULL,
      api_hash       TEXT    NOT NULL,
      session_string TEXT    NOT NULL
    )""")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS user_settings(
      user_id          INTEGER PRIMARY KEY,
      interval_minutes INTEGER DEFAULT 60,
      ad_text          TEXT    DEFAULT '',
      groups_text      TEXT    DEFAULT '',
      updated_at       TEXT
    )""")
    conn.commit()
    conn.close()
