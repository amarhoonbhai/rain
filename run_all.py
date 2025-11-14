# run_all.py — supervisor for Spinify stack
# Runs: main_bot, login_bot, worker_forward, profile_enforcer
# Features:
# • Concurrent tasks with auto-restart (exponential backoff up to 60s)
# • Heartbeat logs every 30s
# • Graceful SIGINT/SIGTERM shutdown
# • Component toggles via env:
#     ENABLE_MAIN=1 ENABLE_LOGIN=1 ENABLE_WORKER=1 ENABLE_ENFORCER=1
# • Handles Telegram polling "Conflict" by backing off longer

import asyncio
import importlib
import logging
import os
import signal
from datetime import datetime, timezone

try:
    import uvloop  # optional, speeds up asyncio
    uvloop.install()
except Exception:
    pass

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
log = logging.getLogger("runner")

# ------------ Env toggles ------------
EN_MAIN      = os.getenv("ENABLE_MAIN",      "1") not in ("0", "false", "False")
EN_LOGIN     = os.getenv("ENABLE_LOGIN",     "1") not in ("0", "false", "False")
EN_WORKER    = os.getenv("ENABLE_WORKER",    "1") not in ("0", "false", "False")
EN_ENFORCER  = os.getenv("ENABLE_ENFORCER",  "1") not in ("0", "false", "False")

# Optional sanity check for tokens
MAIN_BOT_TOKEN  = os.getenv("MAIN_BOT_TOKEN", "")
LOGIN_BOT_TOKEN = os.getenv("LOGIN_BOT_TOKEN", "")

if EN_MAIN and (not MAIN_BOT_TOKEN or ":" not in MAIN_BOT_TOKEN):
    log.warning("ENABLE_MAIN=1 but MAIN_BOT_TOKEN missing/malformed")
if EN_LOGIN and (not LOGIN_BOT_TOKEN or ":" not in LOGIN_BOT_TOKEN):
    log.warning("ENABLE_LOGIN=1 but LOGIN_BOT_TOKEN missing/malformed")
if MAIN_BOT_TOKEN and LOGIN_BOT_TOKEN and MAIN_BOT_TOKEN == LOGIN_BOT_TOKEN:
    log.warning("MAIN_BOT_TOKEN == LOGIN_BOT_TOKEN — both bots share token; expect polling conflicts.")

# ------------ Graceful shutdown ------------
_shutdown = asyncio.Event()

def _signal_handler(sig_name: str):
    log.info("received %s — shutting down…", sig_name)
    _shutdown.set()

def _install_signal_handlers():
    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, _signal_handler, s.name)
        except NotImplementedError:
            # Windows or environments w/o signals
            pass

# ------------ Heartbeat ------------
async def _heartbeat():
    i = 0
    while not _shutdown.is_set():
        i += 1
        now = datetime.now(timezone.utc).isoformat()
        log.info("[hb] alive #%s @ %s", i, now)
        try:
            await asyncio.wait_for(_shutdown.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            pass

# ------------ Restart wrapper ------------
async def _run_forever(starter_coro_factory, name: str):
    """
    Runs starter coroutines repeatedly with backoff on failure.
    `starter_coro_factory` should return an *awaitable* that completes when the
    underlying service exits (normally or due to crash).
    """
    backoff = 3
    MAX_BACKOFF = 60

    while not _shutdown.is_set():
        try:
            log.info("[%s] starting…", name)
            await starter_coro_factory()
            # If returned normally, reset backoff a bit to avoid tight loops
            backoff = 3
            if not _shutdown.is_set():
                log.info("[%s] exited (no error). Restarting in %ss…", name, backoff)
                await asyncio.wait_for(_shutdown.wait(), timeout=backoff)
        except asyncio.CancelledError:
            log.info("[%s] cancelled.", name)
            break
        except Exception as e:
            msg = str(e)
            # Longer backoff on polling conflict
            if "Conflict: terminated by other getUpdates request" in msg:
                backoff = min(MAX_BACKOFF, max(15, backoff * 2))
            else:
                backoff = min(MAX_BACKOFF, backoff * 2)
            log.error("[%s] crashed: %s", name, msg, exc_info=LOG_LEVEL == "DEBUG")
            if _shutdown.is_set():
                break
            log.info("[%s] restarting in %ss…", name, backoff)
            try:
                await asyncio.wait_for(_shutdown.wait(), timeout=backoff)
            except asyncio.TimeoutError:
                pass

# ------------ Starters (import lazily so hot code reloads on crash) ------------
async def serve_main_bot():
    mod = importlib.import_module("main_bot")
    # main_bot exposes async def main()
    await mod.main()

async def serve_login_bot():
    mod = importlib.import_module("login_bot")
    # login_bot exposes async def login_bot_main() (or main())
    starter = getattr(mod, "login_bot_main", None) or getattr(mod, "main")
    await starter()

async def serve_worker():
    mod = importlib.import_module("worker_forward")
    # worker_forward exposes async def main()
    await mod.main()

async def serve_enforcer():
    mod = importlib.import_module("profile_enforcer")
    # profile_enforcer exposes async def main()
    await mod.main()

# ------------ Main ------------
async def main():
    _install_signal_handlers()

    tasks = [asyncio.create_task(_heartbeat(), name="heartbeat")]

    if EN_MAIN:
        tasks.append(asyncio.create_task(_run_forever(serve_main_bot, "main-bot"), name="main-bot"))
    if EN_LOGIN:
        tasks.append(asyncio.create_task(_run_forever(serve_login_bot, "login-bot"), name="login-bot"))
    if EN_WORKER:
        tasks.append(asyncio.create_task(_run_forever(serve_worker, "worker"), name="worker"))
    if EN_ENFORCER:
        tasks.append(asyncio.create_task(_run_forever(serve_enforcer, "enforcer"), name="enforcer"))

    # Wait for shutdown signal
    await _shutdown.wait()

    # Cancel all tasks except current
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    log.info("shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
