# run_all.py — start main bot, worker, login bot (optional), enforcer
import asyncio, os
from dotenv import load_dotenv
load_dotenv()

async def serve_bot():
    import main_bot
    await main_bot.main()

async def serve_worker():
    import worker_forward
    await worker_forward.main()

async def serve_login_bot():
    token = (os.getenv("LOGIN_BOT_TOKEN") or "").strip()
    if not token or ":" not in token:
        print("[login-bot] LOGIN_BOT_TOKEN not set — skipping.")
        while True:
            await asyncio.sleep(3600)
    import login_bot
    await login_bot.login_bot_main()

async def serve_enforcer():
    import profile_enforcer
    await profile_enforcer.main()

async def main():
    tasks = [serve_bot(), serve_worker(), serve_login_bot(), serve_enforcer()]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
