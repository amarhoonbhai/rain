# run_all.py — Spinify Supervisor (A+)

import asyncio
import logging
import os
import importlib

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("runner")


async def start_bot(mod_name):
    mod = importlib.import_module(mod_name)
    await mod.main()


async def run_service_loop(title, mod_name):
    delay = 12
    while True:
        log.info(f"[runner] [{title:<8}] starting…")
        try:
            await start_bot(mod_name)
        except Exception as e:
            log.error(f"[runner] [{title:<8}] crashed: {e}; restarting in {delay}s")
        await asyncio.sleep(delay)


async def main():
    await asyncio.gather(
        run_service_loop("panel", "main_bot"),
        run_service_loop("login", "login_bot"),
        run_service_loop("worker", "worker_forward"),
        run_service_loop("enforcer", "enforcer"),
    )


if __name__ == "__main__":
    asyncio.run(main())
