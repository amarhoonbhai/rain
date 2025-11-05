# run_all.py — run Main Bot + Worker (+ optional Login Bot)
import asyncio, os, sys

from dotenv import load_dotenv
load_dotenv()

async def serve_bot():
    import main_bot
    await main_bot._preflight()
    print("[bot] polling…")
    await main_bot.dp.start_polling(main_bot.bot)

async def serve_worker():
    import worker_forward
    print("[worker] loop…")
    await worker_forward.loop_worker()

async def serve_login_bot():
    if not os.getenv("LOGIN_BOT_TOKEN"):
        print("[login-bot] LOGIN_BOT_TOKEN not set — skipping.")
        return
    import login_bot
    print("[login-bot] polling…")
    await login_bot.login_bot_main()

async def main():
    if not (os.getenv("MAIN_BOT_TOKEN") or os.getenv("ADS_BOT_TOKEN")):
        print("ERROR: set MAIN_BOT_TOKEN in .env"); sys.exit(1)
    await asyncio.gather(serve_bot(), serve_worker(), serve_login_bot())

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
