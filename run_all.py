# run_all.py — resilient supervisor for main_bot, worker_forward, login_bot, profile_enforcer
import asyncio, logging, os, signal, contextlib, traceback
from datetime import datetime
from dotenv import load_dotenv

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("supervisor")

load_dotenv()
MAIN_BOT_TOKEN  = (os.getenv("MAIN_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
LOGIN_BOT_TOKEN = (os.getenv("LOGIN_BOT_TOKEN") or "").strip()
ENFORCER_ENABLED = os.getenv("ENFORCER_ENABLED", "1").strip() not in ("0","false","False","")

async def _run_forever(name: str, starter):
    backoff = 3
    while True:
        try:
            log.info(f"[{name}] starting…")
            await starter()
            log.warning(f"[{name}] returned; restarting in {backoff}s…")
        except asyncio.CancelledError:
            log.info(f"[{name}] cancelled — shutting down.")
            raise
        except Exception as e:
            log.error(f"[{name}] crashed: {e}\n{traceback.format_exc()}")
            log.info(f"[{name}] restarting in {backoff}s…")
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 30)

async def serve_bot():
    import main_bot
    await main_bot.main()

async def serve_worker():
    import worker_forward
    entry = getattr(worker_forward, "main", None) or getattr(worker_forward, "main_loop", None)
    if entry is None:
        raise RuntimeError("worker_forward has no main()/main_loop()")
    await entry()

async def serve_login_bot():
    if not (LOGIN_BOT_TOKEN and ":" in LOGIN_BOT_TOKEN):
        log.info("[login-bot] LOGIN_BOT_TOKEN not set — skipping.")
        while True:  # placeholder idle task
            await asyncio.sleep(3600)
    import login_bot
    entry = getattr(login_bot, "login_bot_main", None) or getattr(login_bot, "main", None)
    if entry is None:
        raise RuntimeError("login_bot has no login_bot_main()/main()")
    await entry()

async def serve_enforcer():
    if not ENFORCER_ENABLED:
        log.info("[enforcer] disabled — skipping.")
        while True:
            await asyncio.sleep(3600)
    import profile_enforcer
    await profile_enforcer.main()

async def heartbeat(stop_event: asyncio.Event):
    n = 0
    while not stop_event.is_set():
        n += 1
        log.info(f"[hb] alive #{n} @ {datetime.utcnow().isoformat()}Z")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            pass

async def main():
    if not (MAIN_BOT_TOKEN and ":" in MAIN_BOT_TOKEN):
        raise RuntimeError("MAIN_BOT_TOKEN missing/malformed in .env")

    stop_event = asyncio.Event()

    def _sig_handler(sig, frame):
        log.info(f"[svc] got signal {sig}, shutting down…")
        stop_event.set()

    for s in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(Exception):
            signal.signal(s, _sig_handler)

    tasks = [
        asyncio.create_task(_run_forever("main-bot",  lambda: serve_bot())),
        asyncio.create_task(_run_forever("worker",    lambda: serve_worker())),
        asyncio.create_task(_run_forever("login-bot", lambda: serve_login_bot())),
        asyncio.create_task(_run_forever("enforcer",  lambda: serve_enforcer())),
        asyncio.create_task(heartbeat(stop_event)),
    ]

    await stop_event.wait()
    for t in tasks: t.cancel()
    for t in tasks:
        with contextlib.suppress(asyncio.CancelledError):
            await t

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
