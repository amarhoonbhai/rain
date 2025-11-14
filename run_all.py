# run_all.py — resilient runner with auto-restart + heartbeat
import asyncio, logging, os, time

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"),
                    format="%(asctime)s [%(levelname)s] %(message)s")

async def _run_forever(name: str, starter):
    backoff = 5
    while True:
        logging.info(f"[{name}] starting…")
        try:
            await starter()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logging.error(f"[{name}] crashed: {e}", exc_info=False)
        logging.info(f"[{name}] restarting in {backoff}s…")
        await asyncio.sleep(backoff)
        backoff = min(backoff + 6, 60)

async def serve_bot():
    import main_bot
    await main_bot.main()

async def serve_worker():
    import worker_forward
    await worker_forward.main()

async def serve_login_bot():
    try:
        import login_bot
    except Exception as e:
        logging.error(f"[login-bot] import failed: {e}")
        return
    await login_bot.login_bot_main()

async def serve_enforcer():
    import profile_enforcer
    await profile_enforcer.main()

async def heartbeat():
    n = 0
    while True:
        n += 1
        logging.info(f"[hb] alive #{n} @ {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")
        await asyncio.sleep(30)

async def main():
    tasks = [
        _run_forever("main-bot", serve_bot),
        _run_forever("worker", serve_worker),
        _run_forever("login-bot", serve_login_bot),
        _run_forever("enforcer", serve_enforcer),
        heartbeat(),
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
