# run_all.py — async supervisor for all Telegram services
#   - main_bot         (Aiogram main UI)
#   - login_bot        (Aiogram + Telethon login & session saver)
#   - worker_forward   (Telethon Saved Messages forwarder)
#   - profile_enforcer (Telethon bio/name enforcer)
#
# Features:
#   • Shared .env loading via core.mongo._load_dotenv_best_effort()
#   • Per-service env validation (Mongo + bot tokens)
#   • Safe import inside the service task (one bad import won’t kill others)
#   • Auto-restart with simple exponential backoff
#   • Periodic heartbeat
#   • Clean SIGINT/SIGTERM shutdown
#
# Usage:
#   python3 run_all.py

import os
import sys
import asyncio
import signal
import logging
from typing import Callable, Awaitable

# --- Load environment (.env) before reading settings / Mongo URI ---
try:
    # Preferred: shared loader from core.mongo (handles repo root + .env.local)
    from core.mongo import _load_dotenv_best_effort  # type: ignore
except Exception:  # pragma: no cover
    _load_dotenv_best_effort = None  # type: ignore

if _load_dotenv_best_effort:
    try:
        _load_dotenv_best_effort()
    except Exception:
        # Don't crash supervisor if .env loading fails;
        # services will log detailed errors themselves.
        pass
else:
    # Fallback: plain python-dotenv from current working dir
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        load_dotenv = None  # type: ignore

    if load_dotenv:
        try:
            load_dotenv()
        except Exception:
            pass

# ---------- Logging ----------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [runner] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("runner")


# ---------- Heartbeat ----------
async def heartbeat() -> None:
    i = 0
    while True:
        i += 1
        log.info("[hb] alive #%d", i)
        await asyncio.sleep(30)


# ---------- Env validation ----------
def _has_mongo_env() -> bool:
    uri = (os.getenv("MONGO_URI") or os.getenv("MONGODB_URI") or "").strip()
    dbn = (os.getenv("MONGO_DB_NAME") or "rain").strip()
    if not uri:
        log.error("MONGO_URI missing in environment")
        return False
    if not dbn or dbn in ('""', "''", " "):
        log.error("Bad database name in MONGO_DB_NAME (got %r)", dbn)
        return False
    return True


def _has_token(env_key: str) -> bool:
    val = (os.getenv(env_key) or "").strip()
    # Aiogram-style tokens normally contain a colon
    if not val or ":" not in val:
        log.error("%s missing or malformed", env_key)
        return False
    return True


def _ok_main_bot() -> bool:
    return _has_mongo_env() and _has_token("MAIN_BOT_TOKEN")


def _ok_login_bot() -> bool:
    return _has_mongo_env() and _has_token("LOGIN_BOT_TOKEN")


def _ok_worker() -> bool:
    # Telethon worker uses Mongo sessions + settings, no bot token
    return _has_mongo_env()


def _ok_enforcer() -> bool:
    # Telethon profile_enforcer also uses Mongo + user sessions
    return _has_mongo_env()


# ---------- Service wrappers ----------
async def _run_main_bot() -> None:
    import main_bot
    await main_bot.main()


async def _run_login_bot() -> None:
    import login_bot
    await login_bot.main()


async def _run_worker() -> None:
    # Telethon-based Saved Messages forwarder
    import worker_forward
    await worker_forward.main_loop()


async def _run_enforcer() -> None:
    # Telethon-based bio/name enforcer
    import profile_enforcer
    await profile_enforcer.main()


# ---------- Supervisor loop ----------
async def run_service_loop(
    name: str,
    ok_fn: Callable[[], bool],
    start_fn: Callable[[], Awaitable[None]],
    backoff_seq=(6, 12, 24, 30),
) -> None:
    """
    Run a single service with:
      • pre-flight env check (ok_fn)
      • restart on crash with exponential-style backoff
    """
    idx = 0
    padded_name = f"{name:<10}"

    while True:
        try:
            if not ok_fn():
                d = backoff_seq[min(idx, len(backoff_seq) - 1)]
                log.info("[%s] waiting for env… retrying in %ss", padded_name, d)
                await asyncio.sleep(d)
                idx = min(idx + 1, len(backoff_seq) - 1)
                continue

            log.info("[%s] starting…", padded_name)
            await start_fn()
            # If the service returns instead of running forever, we still restart it.
            log.warning("[%s] exited normally; restarting after short pause", padded_name)
            idx = 0
            await asyncio.sleep(1)

        except asyncio.CancelledError:
            log.info("[%s] cancelled — stopping loop", padded_name)
            raise
        except Exception as e:
            d = backoff_seq[min(idx, len(backoff_seq) - 1)]
            log.exception("[%s] crashed: %s; restarting in %ss", padded_name, e, d)
            await asyncio.sleep(d)
            idx = min(idx + 1, len(backoff_seq) - 1)


# ---------- Main ----------
async def main() -> None:
    # One instance per bot token! If you run multiple copies,
    # Telegram will throw a conflict error.
    tasks = [
        asyncio.create_task(run_service_loop("login-bot", _ok_login_bot, _run_login_bot)),
        asyncio.create_task(run_service_loop("main-bot",  _ok_main_bot,  _run_main_bot)),
        asyncio.create_task(run_service_loop("worker",    _ok_worker,    _run_worker)),
        asyncio.create_task(run_service_loop("enforcer",  _ok_enforcer,  _run_enforcer)),
        asyncio.create_task(heartbeat()),
    ]

    stop_ev: asyncio.Event = asyncio.Event()

    def _stop(*_: object) -> None:
        log.info("received SIGINT/SIGTERM — shutting down…")
        stop_ev.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            # e.g. Windows or limited event loop
            signal.signal(sig, lambda *_: _stop())

    await stop_ev.wait()

    # graceful cancellation
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    log.info("shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Extra safety for Ctrl+C in some environments
        pass
