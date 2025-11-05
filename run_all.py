# run_all.py — orchestrates main bot, worker, login bot (optional), profile enforcer
from __future__ import annotations
import asyncio, os
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(*a, **k): pass

# Load .env beside this file
load_dotenv(dotenv_path=Path(__file__).with_name(".env"), override=True)

async def serve_bot():
    import core.db as db
    db.init_db()
    import main_bot
    if hasattr(main_bot, "_preflight"):
        await main_bot._preflight()
    await main_bot.main()

async def serve_worker():
    import worker_forward
    await worker_forward.main()

async def serve_login_bot():
    token = (os.getenv("LOGIN_BOT_TOKEN") or "").strip()
    if not token:
        print("[login-bot] LOGIN_BOT_TOKEN not set — skipping.")
        return
    import login_bot
    entry = getattr(login_bot, "login_bot_main", None) or getattr(login_bot, "main", None)
    if entry is None:
        print("[login-bot] No entrypoint found — skipping.")
        return
    await entry()

async def serve_enforcer():
    import profile_enforcer
    await profile_enforcer.main()

async def main():
    tasks = [
        asyncio.create_task(serve_bot(), name="bot"),
        asyncio.create_task(serve_worker(), name="worker"),
        asyncio.create_task(serve_login_bot(), name="login"),
        asyncio.create_task(serve_enforcer(), name="enforcer"),
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
