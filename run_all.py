# run_all.py — supervise all services with singleton lock & restarts

import os, sys, asyncio, traceback, time

# -------- Singleton lock (avoid "Conflict: terminated by other getUpdates") ----
LOCK_PATH = "/tmp/rain_run_all.lock"
try:
    import fcntl
    _lockf = open(LOCK_PATH, "w")
    fcntl.flock(_lockf, fcntl.LOCK_EX | fcntl.LOCK_NB)
    _lockf.write(str(os.getpid()))
    _lockf.flush()
except Exception:
    print("Another run_all.py is already running. Exiting.")
    sys.exit(0)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

async def _run_forever(name: str, starter, min_backoff=3, max_backoff=30):
    """Restart a component on crash with capped backoff."""
    backoff = min_backoff
    while True:
        try:
            print(f"[{name}] starting…")
            await starter()
        except asyncio.CancelledError:
            print(f"[{name}] cancelled, stopping.")
            return
        except Exception as e:
            print(f"[{name}] crashed: {e}")
            traceback.print_exc()
        print(f"[{name}] restarting in {backoff}s…")
        await asyncio.sleep(backoff)
        backoff = min(max_backoff, backoff * 2)

# ----- Starters ---------------------------------------------------------------
async def serve_bot():
    import main_bot
    await main_bot.main()

async def serve_worker():
    import worker_forward
    await worker_forward.main()

async def serve_login_bot():
    # LOGIN_BOT_TOKEN may be optional; skip if not present
    token = (os.getenv("LOGIN_BOT_TOKEN") or "").strip()
    if not token or ":" not in token:
        print("[login-bot] LOGIN_BOT_TOKEN not set — skipping.")
        # idle loop so the supervisor keeps running without restarting this
        while True: await asyncio.sleep(3600)
    import login_bot
    await login_bot.login_bot_main()

async def serve_enforcer():
    # profile_enforcer is optional; if missing, skip quietly
    try:
        import profile_enforcer
    except Exception:
        print("[enforcer] module missing — skipping.")
        while True: await asyncio.sleep(3600)
    await profile_enforcer.main()

# ----- Orchestrator -----------------------------------------------------------
async def main():
    tasks = [
        asyncio.create_task(_run_forever("main-bot",   serve_bot)),
        asyncio.create_task(_run_forever("worker",     serve_worker)),
        asyncio.create_task(_run_forever("login-bot",  serve_login_bot)),
        asyncio.create_task(_run_forever("enforcer",   serve_enforcer)),
    ]
    try:
        while True:
            await asyncio.sleep(300)  # heartbeat
            print("[hb] alive @", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    except KeyboardInterrupt:
        print("KeyboardInterrupt — shutting down…")
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
