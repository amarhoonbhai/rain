#!/usr/bin/env python3
import asyncio
import logging
import importlib
import traceback
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [runner] %(message)s",
)
log = logging.getLogger("runner")


# ---------------------------------------------------------
# Utility loader
# ---------------------------------------------------------
def load_module(name):
    try:
        return importlib.import_module(name)
    except Exception:
        log.error(f"Failed importing {name}:\n{traceback.format_exc()}")
        return None


# ---------------------------------------------------------
# Run a service loop (auto-restart if crashed)
# ---------------------------------------------------------
async def run_service_loop(name, start_fn):
    """Keeps service alive forever."""
    while True:
        try:
            log.info(f"[{name:<10}] starting…")
            await start_fn()
        except Exception:
            log.error(
                f"[{name:<10}] crashed:\n{traceback.format_exc()}\n"
                "Restarting in 12s…"
            )
        await asyncio.sleep(12)


# ---------------------------------------------------------
# Worker launcher (Telethon forward engine)
# ---------------------------------------------------------
async def _run_worker():
    worker_mod = load_module("worker_forward")
    if not worker_mod or not hasattr(worker_mod, "start"):
        raise RuntimeError("worker_forward.start() missing")

    await worker_mod.start()


# ---------------------------------------------------------
# Enforcer launcher (Profile + Name/Bio locking)
# ---------------------------------------------------------
async def _run_enforcer():
    enf_mod = load_module("profile_enforcer")
    if not enf_mod or not hasattr(enf_mod, "start_enforcer"):
        raise RuntimeError("profile_enforcer.start_enforcer() missing")

    await enf_mod.start_enforcer()


# ---------------------------------------------------------
# Login bot launcher
# ---------------------------------------------------------
async def _run_login_bot():
    lb = load_module("login_bot")
    if not lb or not hasattr(lb, "main"):
        raise RuntimeError("login_bot.main() missing")
    await lb.main()


# ---------------------------------------------------------
# Panel bot launcher (main dashboard)
# ---------------------------------------------------------
async def _run_main_bot():
    mb = load_module("main_bot")
    if not mb or not hasattr(mb, "main"):
        raise RuntimeError("main_bot.main() missing")
    await mb.main()


# ---------------------------------------------------------
# Main entry — run all 4 services in parallel
# ---------------------------------------------------------
async def main():
    tasks = [
        asyncio.create_task(run_service_loop("worker", _run_worker)),
        asyncio.create_task(run_service_loop("enforcer", _run_enforcer)),
        asyncio.create_task(run_service_loop("login_bot", _run_login_bot)),
        asyncio.create_task(run_service_loop("main_bot", _run_main_bot)),
    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Exiting…")
