# run_all.py — run 4 services together
# Prefers importing async mains; falls back to subprocess if missing.
# Python 3.12 / asyncio

import asyncio
import importlib
import os
import sys
from types import ModuleType
from typing import Callable, Optional, Tuple

# (module_name, preferred_func_name, fallback_script_path, pretty_name)
TARGETS = [
    ("login_bot",          "login_bot_main",     "login_bot.py",          "@SpinifyLoginBot"),
    ("ads_bot",            "main_bot_main",      "ads_bot.py",            "@SpinifyAdsBot"),
    ("worker",             "worker_main",        "worker.py",             "forwarder"),
    ("profile_enforcer",   "enforcer_main",      "profile_enforcer.py",   "enforcer"),
]

# Each module can also expose a generic async `main()`. We'll try preferred name first, then `main`.
FALLBACK_FUNC = "main"

# ----- subprocess log readers -----
async def _read_stream(stream: asyncio.StreamReader, prefix: str):
    try:
        while True:
            line = await stream.readline()
            if not line:
                break
            try:
                text = line.decode(errors="replace").rstrip()
            except Exception:
                text = str(line).rstrip()
            print(f"[{prefix}] {text}")
    except Exception as e:
        print(f"[run_all] reader error ({prefix}): {e}")

# ----- try to import a module & function -----
def _try_import(module_name: str, func_name: str) -> Tuple[Optional[Callable], Optional[str]]:
    try:
        mod: ModuleType = importlib.import_module(module_name)
    except Exception as e:
        return None, f"import {module_name} failed: {e}"

    fn: Optional[Callable] = getattr(mod, func_name, None)
    if fn is None:
        # try fallback 'main'
        fn = getattr(mod, FALLBACK_FUNC, None)
        if fn is None:
            return None, f"{module_name} has no '{func_name}' or '{FALLBACK_FUNC}'"
        return fn, None
    return fn, None

# ----- run an async function as a task -----
async def _run_coro(fn: Callable, name: str):
    try:
        await fn()
    except Exception as e:
        print(f"[{name}] crashed: {e}")

# ----- spawn a script as a subprocess (fallback) -----
async def _spawn_script(script_path: str, name: str) -> Optional[asyncio.subprocess.Process]:
    if not os.path.exists(script_path):
        print(f"[run_all] skip: {script_path} not found for {name}")
        return None
    print(f"[run_all] starting: {name} via {script_path}")
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-u", script_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    # stream logs
    asyncio.create_task(_read_stream(proc.stdout, f"{name}"))
    asyncio.create_task(_read_stream(proc.stderr, f"{name}"))
    return proc

async def main():
    tasks = []
    procs = []

    for module_name, preferred_func, script, pretty in TARGETS:
        fn, err = _try_import(module_name, preferred_func)
        if fn is not None:
            print(f"[run_all] launching {pretty} via import: {module_name}.{getattr(fn, '__name__', preferred_func)}")
            tasks.append(asyncio.create_task(_run_coro(fn, pretty)))
        else:
            print(f"[run_all] {pretty}: {err} — falling back to subprocess")
            p = await _spawn_script(script, pretty)
            if p:
                procs.append(p)

    if not tasks and not procs:
        print("[run_all] nothing to run")
        return

    try:
        await asyncio.gather(
            *(tasks + [p.wait() for p in procs]),
            return_exceptions=False
        )
    except KeyboardInterrupt:
        print("\n[run_all] ^C received, terminating children…")
    finally:
        # cancel all coroutine tasks
        for t in tasks:
            if not t.done():
                t.cancel()
        # terminate subprocesses
        for p in procs:
            if p.returncode is None:
                try:
                    p.terminate()
                except ProcessLookupError:
                    pass
        await asyncio.sleep(1.0)
        for p in procs:
            if p.returncode is None:
                try:
                    p.kill()
                except ProcessLookupError:
                    pass
        print("[run_all] all done.")

if __name__ == "__main__":
    asyncio.run(main())
