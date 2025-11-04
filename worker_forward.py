# worker_forward.py — forwards saved ad to saved groups on interval
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Tuple

from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError, ChatWriteForbidden, ChannelPrivate, UsernameInvalid

from core.db import get_conn, init_db

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("worker")

FORWARD_TASKS: Dict[int, asyncio.Task] = {}

def ensure_tables():
    init_db()
    conn = get_conn()
    conn.execute("""
    CREATE TABLE IF NOT EXISTS user_settings(
      user_id INTEGER PRIMARY KEY,
      interval_minutes INTEGER DEFAULT 60,
      ad_text TEXT DEFAULT '',
      groups_text TEXT DEFAULT '',
      updated_at TEXT
    )""")
    conn.commit(); conn.close()

def load_accounts() -> List[Tuple[int, int, str, str, int, str, str]]:
    conn = get_conn()
    cur = conn.execute("""
      SELECT s.user_id, s.api_id, s.api_hash, s.session_string,
             COALESCE(u.interval_minutes, 60) AS interval_minutes,
             COALESCE(u.ad_text, '') AS ad_text,
             COALESCE(u.groups_text, '') AS groups_text
      FROM user_sessions s
      LEFT JOIN user_settings u ON u.user_id = s.user_id
    """)
    rows = cur.fetchall()
    conn.close()
    return rows

def reload_settings(user_id: int):
    conn = get_conn()
    row = conn.execute("""
      SELECT COALESCE(interval_minutes, 60), COALESCE(ad_text, ''), COALESCE(groups_text, '')
      FROM user_settings WHERE user_id=?
    """, (user_id,)).fetchone()
    conn.close()
    if row is None:
        return 60, "", ""
    return int(row[0]), str(row[1]), str(row[2])

def parse_groups(groups_text: str) -> List[str]:
    out = []
    for raw in (groups_text or "").splitlines():
        g = raw.strip()
        if not g: continue
        if g.startswith("https://t.me/"):
            g = g.split("https://t.me/", 1)[1].strip("/")
        if g.startswith("@"): g = g[1:]
        if g and g not in out:
            out.append(g)
        if len(out) == 5: break
    return out

async def send_once(app: Client, groups: List[str], text: str):
    ok, fail = 0, 0
    for g in groups:
        try:
            await app.send_message(g, text)
            ok += 1
            await asyncio.sleep(0.5)
        except FloodWait as fw:
            log.warning(f"[{app.name}] FloodWait {fw.value}s on @{g}")
            await asyncio.sleep(fw.value + 1)
        except (ChatWriteForbidden, ChannelPrivate, UsernameInvalid) as e:
            log.warning(f"[{app.name}] Cannot write to @{g}: {e.__class__.__name__}")
            fail += 1
        except RPCError as e:
            log.error(f"[{app.name}] RPCError @{g}: {e}")
            fail += 1
        except Exception as e:
            log.error(f"[{app.name}] Error @{g}: {e}")
            fail += 1
    return ok, fail

async def forward_loop(user_id: int, api_id: int, api_hash: str, session_string: str):
    app = Client(name=f"user-{user_id}", api_id=api_id, api_hash=api_hash, session_string=session_string)
    try:
        await app.start()
        log.info(f"[u{user_id}] client started")
    except Exception as e:
        log.error(f"[u{user_id}] cannot start session: {e}")
        return

    try:
        while True:
            interval, ad_text, groups_text = reload_settings(user_id)
            groups = parse_groups(groups_text)
            if not ad_text:
                log.info(f"[u{user_id}] no ad set — sleeping 15s"); await asyncio.sleep(15); continue
            if not groups:
                log.info(f"[u{user_id}] no groups set — sleeping 15s"); await asyncio.sleep(15); continue

            ok, fail = await send_once(app, groups, ad_text)
            now = datetime.utcnow().isoformat()
            log.info(f"[u{user_id}] sent {ok} ok, {fail} fail at {now}; next in {interval} min")
            await asyncio.sleep(max(10, int(interval) * 60))
    except asyncio.CancelledError:
        pass
    except Exception as e:
        log.error(f"[u{user_id}] loop error: {e}")
    finally:
        try: await app.stop()
        except Exception: pass
        log.info(f"[u{user_id}] client stopped")

async def loop_worker():
    ensure_tables()
    log.info("[worker] started")
    try:
        while True:
            rows = load_accounts()
            active = set()
            for (user_id, api_id, api_hash, session_string, *_rest) in rows:
                active.add(user_id)
                if user_id not in FORWARD_TASKS or FORWARD_TASKS[user_id].done():
                    log.info(f"[worker] starting task for user {user_id}")
                    FORWARD_TASKS[user_id] = asyncio.create_task(
                        forward_loop(user_id, api_id, api_hash, session_string)
                    )
            for uid in list(FORWARD_TASKS.keys()):
                if uid not in active:
                    t = FORWARD_TASKS.pop(uid)
                    log.info(f"[worker] stopping task for user {uid} (session removed)")
                    t.cancel()
            await asyncio.sleep(10)
    except asyncio.CancelledError:
        pass
    finally:
        for t in FORWARD_TASKS.values(): t.cancel()
        await asyncio.gather(*FORWARD_TASKS.values(), return_exceptions=True)
        log.info("[worker] stopped")

if __name__ == "__main__":
    asyncio.run(loop_worker())
