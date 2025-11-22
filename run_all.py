# run_all.py — FULL A-Version
import asyncio
import logging
import importlib

logging.basicConfig(level="INFO")
log = logging.getLogger("runner")


async def run_service_loop(name, start_fn):
    while True:
        try:
            log.info(f"[{name}] starting…")
            await start_fn()
        except Exception as e:
            log.error(f"[{name}] crashed: {e}; restarting in 12s")
            await asyncio.sleep(12)


async def _run_main_bot():
    m = importlib.import_module("main_bot")
    await m.main()


async def _run_login_bot():
    m = importlib.import_module("login_bot")
    await m.main()


async def _run_worker():
    w = importlib.import_module("worker_forward")
    await w.start()


async def _run_enforcer():
    e = importlib.import_module("profile_enforcer")
    await e.start()


async def main():
    await asyncio.gather(
        run_service_loop("main_bot", _run_main_bot),
        run_service_loop("login_bot", _run_login_bot),
        run_service_loop("worker", _run_worker),
        run_service_loop("enforcer", _run_enforcer),
    )


if __name__ == "__main__":
    asyncio.run(main())
