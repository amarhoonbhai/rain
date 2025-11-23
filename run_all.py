#!/usr/bin/env python3
# run_all.py — Spinify Ads Supervisor (B-2 Version)
# Automatically runs:
#   ✔ login_bot.py
#   ✔ main_bot.py
#   ✔ worker_forward.py
#   ✔ profile_enforcer.py
#
# Restarts crashed services automatically.

import asyncio
import logging
import os
import importlib

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='[%(asctime)s] [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

log = logging.getLogger("runner")

# ============================================================
# Helper: Dynamic Import
# ============================================================
def load(module_name: str):
    """Safe import & reload to reflect live changes."""
    try:
        if module_name in globals():
            return importlib.reload(globals()[module_name])
        mod = importlib.import_module(module_name)
        globals()[module_name] = mod
        return mod
    except Exception as e:
        log.error(f"Failed loading module {module_name}: {e}")
        raise


# ============================================================
# Service Launcher
# ============================================================
async def run_service(name: str, module_name: str, entry: str):
    """Runs services in endless loop and restarts if crash."""
    while True:
        try:
            mod = load(module_name)
            start_fn = getattr(mod, entry)

            log.info(f"[{name}] starting…")
            await start_fn()

        except Exception as e:
            log.error(f"[{name}] crashed: {e}; restarting in 12s")
            await asyncio.sleep(12)


# ============================================================
# Entrypoint
# ============================================================
async def main():
    log.info("Spinify Supervisor starting…")

    tasks = [
        # login bot
        asyncio.create_task(run_service(
            "login-bot", "login_bot", "main"
        )),

        # main panel
        asyncio.create_task(run_service(
            "main-bot", "main_bot", "main"
        )),

        # worker — forwards ads
        asyncio.create_task(run_service(
            "worker", "worker_forward", "start"
        )),

        # enforcer — fixes name & bio
        asyncio.create_task(run_service(
            "enforcer", "profile_enforcer", "start"
        )),
    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutting down.")
