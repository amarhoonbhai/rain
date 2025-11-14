#!/usr/bin/env python3
# run_all.py — unified process runner (async, restart-safe)
# Starts: main-bot, login-bot, worker, enforcer
# Flags:
#   --only main,login,worker,enforcer   # run subset
#   --skip login                        # skip some
#   --hb 30                             # heartbeat seconds (default 30)
#   --no-env                            # don’t load .env
#
# Console banner (quick help) shows all user-commands exposed by the bots.

import asyncio, argparse, importlib, os, signal, sys, time
from contextlib import suppress

BANNER = """\
────────────────────────────────────────────────────────────────
Spinify Runner — services: main-bot, login-bot, worker, enforcer

User commands (from your logged-in account via userbot):
  .help          — show all cmds
  .addgc         — send up to 5 links/IDs/usernames (one per line)
  .gc            — list targets
  .cleargc       — clear targets
  .time 30m|45m|60m — set interval (default 30m)
  .adreset       — restart Saved-All sequence

Main bot commands (everyone):
  /start  /fstats
Owner commands:
  /stats  /top N  /broadcast <text>
Owner panel (inline):
  🌙 Night toggle, 📊 Stats, 🏆 Top, 📣 Broadcast, 💎 Premium
Unlock:
  “Unlock GC” button → raises cap from 5 → 10
Premium (owner upgrade):
  cap 50 + name-lock

Tips:
  • Ensure MONGO_URI + tokens in .env
  • One instance per bot token (avoid TelegramConflictError)
────────────────────────────────────────────────────────────────
"""

SERVICES = {
    "main":     ("main_bot",          "main"),
    "login":    ("login_bot",         "login_bot_main"),  # defined in your file
    "worker":   ("worker_forward",    "main"),
    "enforcer": ("profile_enforcer",  "main"),
}

def _log(msg, *a):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} [runner] {msg % a if a else msg}", flush=True)

def _env_or(name, default=None):
    v = os.getenv(name)
    return v if v is not None else default

async def _sleep_n(n):
    try:
        await asyncio.sleep(n)
    except asyncio.CancelledError:
        pass

async def _run_forever(tag: str, module_name: str, coro_name: str):
    """
    Import module lazily, run its async entrypoint forever (with backoff on crash).
    """
    delay = 2
    while True:
        _log("[%-10s] starting…", tag)
        try:
            mod = importlib.import_module(module_name)
        except Exception as e:
            _log("[%-10s] import failed: %s", tag, e)
            await _sleep_n(delay)
            delay = min(60, delay * 2)
            continue

        try:
            coro = getattr(mod, coro_name, None)
            if coro is None or not callable(coro):
                raise RuntimeError(f"{module_name}.{coro_name} not found")
        except Exception as e:
            _log("[%-10s] bad entrypoint: %s", tag, e)
            await _sleep_n(delay)
            delay = min(60, delay * 2)
            continue

        try:
            await coro()  # block until stopped/crashed
            _log("[%-10s] exited cleanly — restarting in %ss…", tag, delay)
        except Exception as e:
            _log("[%-10s] crashed: %s", tag, e)

        await _sleep_n(delay)
        delay = min(60, delay * 2)

async def _heartbeat(period: int):
    i = 0
    while True:
        i += 1
        _log("[hb] alive #%d", i)
        await _sleep_n(period)

def _want(tag: str, only_set, skip_set):
    if only_set and tag not in only_set: return False
    if tag in skip_set: return False
    return True

def _env_check():
    miss = []
    for k in ("MONGO_URI", "MAIN_BOT_TOKEN", "LOGIN_BOT_TOKEN", "OWNER_ID"):
        if not os.getenv(k):
            miss.append(k)
    if miss:
        _log("WARN env missing: %s", ", ".join(miss))
        _log("Hint: create .env with MONGO_URI, MAIN_BOT_TOKEN, LOGIN_BOT_TOKEN, OWNER_ID")

async def main():
    ap = argparse.ArgumentParser(description="Spinify async runner")
    ap.add_argument("--only", help="comma list: main,login,worker,enforcer", default="")
    ap.add_argument("--skip", help="comma list to skip", default="")
    ap.add_argument("--hb",   help="heartbeat seconds", type=int, default=int(_env_or("RUN_HB_SEC", "30")))
    ap.add_argument("--no-env", action="store_true", help="don’t load .env")
    args = ap.parse_args()

    # Load .env early unless skipped
    if not args.no_env:
        with suppress(Exception):
            from dotenv import load_dotenv
            load_dotenv()

    print(BANNER, flush=True)
    _env_check()

    only_set = {p.strip() for p in args.only.split(",") if p.strip()} if args.only else set()
    skip_set = {p.strip() for p in args.skip.split(",") if p.strip()} if args.skip else set()

    tasks = []
    # Start with a tiny staggering to reduce initial Bot API conflicts
    if _want("main", only_set, skip_set):
        tasks.append(asyncio.create_task(_run_forever("main-bot", *SERVICES["main"])))
        await _sleep_n(1.0)
    if _want("login", only_set, skip_set):
        tasks.append(asyncio.create_task(_run_forever("login-bot", *SERVICES["login"])))
        await _sleep_n(1.0)
    if _want("worker", only_set, skip_set):
        tasks.append(asyncio.create_task(_run_forever("worker", *SERVICES["worker"])))
        await _sleep_n(1.0)
    if _want("enforcer", only_set, skip_set):
        tasks.append(asyncio.create_task(_run_forever("enforcer", *SERVICES["enforcer"])))

    # Heartbeat
    if args.hb > 0:
        tasks.append(asyncio.create_task(_heartbeat(args.hb)))

    # Graceful shutdown on SIGINT/SIGTERM
    stop = asyncio.Event()

    def _sig(*_):
        _log("received SIGINT/SIGTERM — shutting down…")
        stop.set()

    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(s, _sig)

    await stop.wait()
    for t in tasks:
        t.cancel()
    with suppress(Exception):
        await asyncio.gather(*tasks, return_exceptions=True)
    _log("shutdown complete.")

if __name__ == "__main__":
    # Optional uvloop for speed if installed
    with suppress(Exception):
        import uvloop
        uvloop.install()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        _log("KeyboardInterrupt — bye.")
