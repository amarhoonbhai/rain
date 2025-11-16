# run_all.py — simple async supervisor for:
#   - main_bot      (Aiogram main UI)
#   - login_bot     (account login & session saver)
#   - worker_forward (Saved-All forwarder)
#   - profile_enforcer (bio/name enforcer)
#
# Features:
#   • Per-service env validation (tokens + Mongo)
#   • Safe import inside the service task (so one bad import won’t crash others)
#   • Auto-restart with exponential-ish backoff
#   • Periodic heartbeat
#   • Clean SIGINT/SIGTERM shutdown
#
# Usage:  python3 run_all.py

import os
import sys
import asyncio
import signal
import logging
from typing import Callable, Awaitable, Optional

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [runner] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("runner")

# --------- Heartbeat ---------
async def heartbeat():
    i = 0
    while True:
        i += 1
        log.info("[hb] alive #%d", i)
        await asyncio.sleep(30)

# --------- Env validation ---------
def _has_mongo_env() -> bool:
    uri = (os.getenv("MONGO_URI") or os.getenv("MONGODB_URI") or "").strip()
    dbn = (os.getenv("MONGO_DB_NAME") or "rain").strip()
    if not uri:
        log.error("MONGO_URI missing in environment")
        return False
    # guard against accidental trailing/leading spaces
    if dbn.strip() == "" or dbn.strip() == '" "' or dbn == " ":
        log.error('Bad database name in MONGO_DB_NAME (got %r)', dbn)
        return False
    return True

def _has_token(env_key: str) -> bool:
    val = (os.getenv(env_key) or "").strip()
    if not val or ":" not in val:
        log.error("%s missing or malformed", env_key)
        return False
    return True

def _ok_main_bot() -> bool:
    return _has_mongo_env() and _has_token("MAIN_BOT_TOKEN")

def _ok_login_bot() -> bool:
    return _has_mongo_env() and _has_token("LOGIN_BOT_TOKEN")

def _ok_worker() -> bool:
    return _has_mongo_env()

def _ok_enforcer() -> bool:
    return _has_mongo_env()

# --------- Service wrappers ---------
async def _run_main_bot():
    import main_bot
    await main_bot.main()

async def _run_login_bot():
    import login_bot
    await login_bot.main()

async def _run_worker():
    import worker_forward
    await worker_forward.main_loop()

async def _run_enforcer():
    import profile_enforcer
    await profile_enforcer.main()

# --------- Supervisor loop ---------
async def run_service_loop(
    name: str,
    ok_fn: Callable[[], bool],
    start_fn: Callable[[], Awaitable[None]],
    backoff_seq=(6, 12, 24, 30),
):
    idx = 0
    while True:
        try:
            if not ok_fn():
                d = backoff_seq[min(idx, len(backoff_seq) - 1)]
                log.info("[%s] waiting for env… retrying in %ss", f"{name:<10}", d)
                await asyncio.sleep(d)
                idx = min(idx + 1, len(backoff_seq) - 1)
                continue

            log.info("[%s] starting…", f"{name:<10}")
            idx = 0  # reset backoff on start
            await start_fn()

            # If the function returns normally, we still restart (bots usually never return).
            d = backoff_seq[min(idx, len(backoff_seq) - 1)]
            log.info("[%s] exited; restarting in %ss…", f"{name:<10}", d)
            await asyncio.sleep(d)
            idx = min(idx + 1, len(backoff_seq) - 1)

        except ImportError as ie:
            d = backoff_seq[min(idx, len(backoff_seq) - 1)]
            log.error("[%s] import failed: %s", f"{name:<10}", ie)
            await asyncio.sleep(d)
            idx = min(idx + 1, len(backoff_seq) - 1)
        except Exception as e:
            d = backoff_seq[min(idx, len(backoff_seq) - 1)]
            log.error("[%s] crashed: %s", f"{name:<10}", e)
            await asyncio.sleep(d)
            idx = min(idx + 1, len(backoff_seq) - 1)

# --------- Main ---------
async def main():
    # One instance per bot token! If you run multiple copies, you’ll get TelegramConflictError.
    tasks = [
        asyncio.create_task(run_service_loop("login-bot ", _ok_login_bot, _run_login_bot)),
        asyncio.create_task(run_service_loop("main-bot  ", _ok_main_bot,  _run_main_bot)),
        asyncio.create_task(run_service_loop("worker    ", _ok_worker,    _run_worker)),
        asyncio.create_task(run_service_loop("enforcer  ", _ok_enforcer,  _run_enforcer)),
        asyncio.create_task(heartbeat()),
    ]

    # graceful shutdown
    stop_ev = asyncio.Event()

    def _stop(*_):
        log.info("received SIGINT/SIGTERM — shutting down…")
        stop_ev.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            # Windows fallback
            signal.signal(sig, lambda *_: _stop())

    await stop_ev.wait()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    log.info("shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
