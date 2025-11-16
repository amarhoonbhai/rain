import os, asyncio, logging
from pyrogram import Client
from core.db import init_db, users_with_sessions, sessions_list, get_setting_name_lock

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"))
log=logging.getLogger("enforcer")

DEF_BIO=os.getenv("ENFORCE_BIO","#1 Free Ads Bot — Join @PhiloBots")
DEF_SUFFIX=os.getenv("ENFORCE_NAME_SUFFIX"," — via @SpinifyAdsBot")
SLEEP_SEC=300
MIN_GAP=600  # per user gap
_CONC=3

_last_run={}

async def enforce(uid:int, slot:int, api_id:int, api_hash:str, session_string:str):
    app=Client(name=f"enf-{uid}-s{slot}", api_id=api_id, api_hash=api_hash, session_string=session_string)
    try:
        await app.start()
        try: await app.update_profile(bio=DEF_BIO)
        except Exception: pass
        me=await app.get_me()
        lock_on, lock_name = get_setting_name_lock(uid)
        desired = lock_name if (lock_on and lock_name) else ((me.first_name or "User").split(" — ")[0]+DEF_SUFFIX)
        try:
            if (me.first_name or "") != desired:
                await app.update_profile(first_name=desired)
        except Exception: pass
        log.info("enforced u%s s%s", uid, slot)
    except Exception as e:
        log.info("enforce fail u%s s%s: %s", uid, slot, e)
    finally:
        try: await app.stop()
        except Exception: pass

async def main():
    init_db()
    sem=asyncio.Semaphore(_CONC)
    while True:
        for uid in users_with_sessions():
            if (uid in _last_run) and (asyncio.get_event_loop().time()-_last_run[uid] < MIN_GAP): continue
            _last_run[uid]=asyncio.get_event_loop().time()
            for s in sessions_list(uid):
                async with sem:
                    asyncio.create_task(enforce(uid, s["slot"], int(s["api_id"]), str(s["api_hash"]), str(s["session_string"])))
                await asyncio.sleep(0.2)
        await asyncio.sleep(SLEEP_SEC)

if __name__=="__main__":
    asyncio.run(main())
