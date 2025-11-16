import os, asyncio, logging, importlib
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"), format="%(asctime)s [runner] %(message)s")
log=logging.getLogger("runner")

async def start_login():
    log.info("[login-bot ] starting…")
    try:
        mod=importlib.import_module("login_bot"); await mod.main()
    except Exception as e:
        log.info("[login-bot ] import failed: %s", e); await asyncio.sleep(12)

async def start_main():
    log.info("[main-bot  ] starting…")
    try:
        mod=importlib.import_module("main_bot"); await mod.main()
    except Exception as e:
        log.info("[main-bot  ] import failed: %s", e); await asyncio.sleep(24)

async def start_worker():
    log.info("[worker    ] starting…")
    try:
        mod=importlib.import_module("worker_forward"); await mod.main()
    except Exception as e:
        log.info("[worker    ] crashed: %s", e); await asyncio.sleep(12)

async def start_enforcer():
    log.info("[enforcer  ] starting…")
    try:
        mod=importlib.import_module("profile_enforcer"); await mod.main()
    except Exception as e:
        log.info("[enforcer  ] crashed: %s", e); await asyncio.sleep(12)

async def heartbeat():
    i=0
    while True:
        i+=1
        log.info("[hb] alive #%s", i)
        await asyncio.sleep(30)

async def main():
    tasks=[]
    tasks.append(asyncio.create_task(start_login()))
    await asyncio.sleep(1.0)
    tasks.append(asyncio.create_task(start_main()))
    await asyncio.sleep(1.0)
    tasks.append(asyncio.create_task(start_worker()))
    await asyncio.sleep(1.0)
    tasks.append(asyncio.create_task(start_enforcer()))
    tasks.append(asyncio.create_task(heartbeat()))
    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("received SIGINT/SIGTERM — shutting down…")

if __name__=="__main__":
    asyncio.run(main())
