import sqlite3
import os

DB_PATH = os.getenv("DB_PATH", "db.sqlite3")

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    with open(os.path.join(os.path.dirname(__file__), "models.sql"), "r") as f:
        sql = f.read()
    conn.executescript(sql)
    conn.commit()
    conn.close()
  
