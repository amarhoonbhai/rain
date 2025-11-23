#!/usr/bin/env python3
import os
import asyncio
import logging
import importlib

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="[%(asctime)s] [runner] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

log = logging.getLogger("runner")

SERVICES = {
    "login": "login_bot",
    "dashboard": "main_bot",
    "worker": "worker_forward",
    "enforcer": "profile_enforcer",
}

async def run_service_loop(name: str, module_name: str):
    while True:
        try:
            mod = importlib.import_module(module_name)
            start_fn = getattr(mod, "start", None)
            if start_fn is None:
                raise RuntimeError(f"{module_name}.start() missing")
            log.info(f"[{name:<10}] starting…")
            await start_fn()

        except Exception as e:
            log.error(f"[{name:<10}] crashed: {e}; restarting in 10s")
            await asyncio.sleep(10)

async def main():
    tasks = []
    for name, mod in SERVICES.items():
        tasks.append(asyncio.create_task(run_service_loop(name, mod)))
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
