-- users = settings in main bot
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    ad_message TEXT,
    interval_minutes INTEGER DEFAULT 60,
    joined_ok INTEGER DEFAULT 0,
    last_sent_at TEXT,
    plan TEXT DEFAULT 'free'
);

-- groups added by user (max 5, overwrite)
CREATE TABLE IF NOT EXISTS user_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    group_link TEXT
);

-- sessions created by login bot
CREATE TABLE IF NOT EXISTS user_sessions (
    user_id INTEGER PRIMARY KEY,
    api_id INTEGER,
    api_hash TEXT,
    session_string TEXT
);
