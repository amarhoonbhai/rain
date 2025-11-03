# run_all.py — run 4 services together; prefers import, falls back to subprocess
# Python 3.12 / asyncio

import asyncio
import importlib
import inspect
import os
import sys
from types import ModuleType
from typing import Callable, Optional, Tuple

# (module_name, preferred_func_name, fallback_script_path, pretty_name)
TARGETS = [
    ("login_bot",        "login_bot_main",   "login_bot.py",        "@SpinifyLoginBot"),
    ("ads_bot",          "main_bot_main",    "ads_bot.py",          "@SpinifyAdsBot"),
    ("worker",           "worker_main",      "worker.py",           "forwarder"),
    ("profile_enforcer", "enforcer_main",    "profile_enforcer.py", "enforcer"),
]

FALLBACK_FUNC = "main"


async def _read_stream(stream: asyncio.StreamReader, prefix: str):
    try:
        while True:
            line = await stream.readline()
            if not line:
                break
            print(f"[{prefix}] {line.decode(errors='replace').rstrip()}")
    except Exception as e:
        print(f"[run_all] reader error ({prefix}): {e}")


def _try_import(module_name: str, func_name: str) -> Tuple[Optional[Callable], Optional[str]]:
    try:
        mod: ModuleType = importlib.import_module(module_name)
    except Exception as e:
        return None, f"import {module_name} failed: {e}"

    fn: Optional[Callable] = getattr(mod, func_name, None)
    if fn is None:
        fn = getattr(mod, FALLBACK_FUNC, None)
        if fn is None:
            return None, f"{module_name} has no '{func_name}' or '{FALLBACK_FUNC}'"
    return fn, None


async def _run_entry(fn: Callable, name: str):
    try:
        res = fn()
        if inspect.isawaitable(res):
            await res
    except Exception as e:
        print(f"[{name}] crashed: {e}")


async def _spawn_script(script_path: str, name: str) -> Optional[asyncio.subprocess.Process]:
    if not os.path.exists(script_path):
        print(f"[run_all] skip: {script_path} not found for {name}")
        return None
    print(f"[run_all] starting: {name} via {script_path}")
    p = await asyncio.create_subprocess_exec(
        sys.executable, "-u", script_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    asyncio.create_task(_read_stream(p.stdout, name))
    asyncio.create_task(_read_stream(p.stderr, name))
    return p


async def main():
    tasks: list[asyncio.Task] = []
    procs: list[asyncio.subprocess.Process] = []

    for module_name, preferred_func, script, pretty in TARGETS:
        fn, err = _try_import(module_name, preferred_func)
        if fn:
            print(f"[run_all] launching {pretty} via import: {module_name}.{getattr(fn,'__name__', preferred_func)}")
            tasks.append(asyncio.create_task(_run_entry(fn, pretty)))
        else:
            print(f"[run_all] {pretty}: {err} — falling back to subprocess")
            p = await _spawn_script(script, pretty)
            if p:
                procs.append(p)

    if not tasks and not procs:
        print("[run_all] nothing to run")
        return

    try:
        await asyncio.gather(*(tasks + [p.wait() for p in procs]))
    except KeyboardInterrupt:
        print("\n[run_all] ^C received, terminating children…")
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
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
