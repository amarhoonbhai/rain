#!/usr/bin/env python3
import asyncio
import logging
import importlib
import os
import sys

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [runner] %(message)s"
)
log = logging.getLogger("runner")


# ----------------------------------------------
# Auto-loader utility
# ----------------------------------------------

async def run_service_loop(name: str, start_fn):
    """Runs a service forever. If it crashes, restart after delay."""
    delay = 12
    while True:
        try:
            log.info(f"[{name:<10}] starting…")
            await start_fn()
        except Exception as e:
            log.error(f"[{name:<10}] crashed: {e}; restarting in {delay}s")
        await asyncio.sleep(delay)


# ----------------------------------------------
# Loader wrappers
# ----------------------------------------------

async def _run_main_bot():
    mod = importlib.import_module("main_bot")
    await mod.main()


async def _run_login_bot():
    mod = importlib.import_module("login_bot")
    await mod.main()


async def _run_worker():
    import worker_forward
    if hasattr(worker_forward, "start"):
        await worker_forward.start()
    else:
        raise AttributeError("worker_forward.start() missing")


async def _run_enforcer():
    import profile_enforcer
    if hasattr(profile_enforcer, "start"):
        await profile_enforcer.start()
    else:
        raise AttributeError("profile_enforcer.start() missing")


# ----------------------------------------------
# MAIN
# ----------------------------------------------

async def main():
    tasks = []

    tasks.append(asyncio.create_task(run_service_loop("main-bot", _run_main_bot)))
    tasks.append(asyncio.create_task(run_service_loop("login-bot", _run_login_bot)))
    tasks.append(asyncio.create_task(run_service_loop("worker", _run_worker)))
    tasks.append(asyncio.create_task(run_service_loop("enforcer", _run_enforcer)))

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
