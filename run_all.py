#!/usr/bin/env python3
# run_all.py — unified runner for:
#   • main-bot       (SpinifyAdsBot main UI)
#   • login-bot      (SpinifyLoginBot — session creator)
#   • worker_forward (Saved-All forward worker)
#   • profile_enforcer (bio/name enforcer)
#
# Features:
#   • Per-component restart loop with exponential backoff
#   • Clear logs: [runner] [component ] message
#   • Graceful shutdown on Ctrl+C / SIGTERM
#   • Heartbeat log every 30s

import asyncio
import importlib
import logging
import signal
from datetime import datetime

LOG_LEVEL = "INFO"

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
log = logging.getLogger("runner")


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _l(label: str, msg: str):
    """Compact aligned label logger."""
    print(f"{_ts()} [runner] [{label:<10}] {msg}", flush=True)


# component_name -> (module, coroutine_attr)
COMPONENTS = {
    "main-bot": ("main_bot", "main"),              # aiogram UI bot
    "login-bot": ("login_bot", "login_bot_main"),  # login bot wrapper
    "worker": ("worker_forward", "main"),          # forwarder
    "enforcer": ("profile_enforcer", "main"),      # profile/bio enforcer
}


async def _run_component(name: str, stop_event: asyncio.Event):
    """
    Generic supervisor loop for a single component.
    Imports the module, gets the entry coroutine, runs it,
    restarts on crash with exponential backoff.
    """
    module_name, attr_name = COMPONENTS[name]
    backoff = 6  # seconds, will grow up to 60

    while not stop_event.is_set():
        _l(name, "starting…")

        # Import step (can fail e.g. MONGO_URI / core.db issues)
        try:
            mod = importlib.import_module(module_name)
            fn = getattr(mod, attr_name, None)
            if fn is None:
                # fallback: some modules only define main()
                fn = getattr(mod, "main")
        except Exception as e:
            _l(name, f"import failed: {e}")
            if stop_event.is_set():
                break
            _l(name, f"restarting in {backoff}s…")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue

        # Run the component
        try:
            await fn()
            # If it returns cleanly, treat as crash (bots normally run forever)
            if stop_event.is_set():
                break
            _l(name, "exited unexpectedly; treating as crash")
        except asyncio.CancelledError:
            _l(name, "cancelled — stopping")
            break
        except Exception as e:
            _l(name, f"crashed: {e}")

        if stop_event.is_set():
            break

        _l(name, f"restarting in {backoff}s…")
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)

    _l(name, "stopped.")


async def _heartbeat(stop_event: asyncio.Event, interval: int = 30):
    """Simple alive ticker."""
    n = 0
    while not stop_event.is_set():
        n += 1
        _l("hb", f"alive #{n}")
        await asyncio.sleep(interval)
    _l("hb", "stopped.")


async def main():
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    # graceful shutdown
    def _on_signal(sig):
        _l("runner", f"received {sig.name} — shutting down…")
        stop_event.set()

    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, _on_signal, s)
        except NotImplementedError:
            # On some platforms (e.g. Windows) signals may not be available
            pass

    # spawn tasks
    tasks = []
    for comp in ("main-bot", "login-bot", "worker", "enforcer"):
        tasks.append(asyncio.create_task(_run_component(comp, stop_event)))
    tasks.append(asyncio.create_task(_heartbeat(stop_event, 30)))

    # wait until stop_event is set
    await stop_event.wait()

    # cancel all tasks
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    _l("runner", "shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())
