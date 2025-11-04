import os, sys, subprocess, time, shutil

APPS = [
    ("@SpinifyLoginBot", "login_bot.py"),
    ("@SpinifyAdsBot",   "main_bot.py"),          # <-- use main_bot.py
    ("forwarder",        "worker_forward.py"),    # <-- use worker_forward.py
    ("enforcer",         "profile_enforcer.py"),
]

def exists(fp): return os.path.isfile(fp)

def start_tag(tag, file):
    if not exists(file):
        print(f"[run_all] skip: {file} not found for {tag}")
        return None
    print(f"[run_all] starting: {tag} via {file}")
    return subprocess.Popen([sys.executable, file])

def main():
    procs = []
    for tag, file in APPS:
        procs.append(start_tag(tag, file))
        time.sleep(0.5)

    print("[run_all] all launched. Ctrl+C to stop.")
    try:
        while True:
            time.sleep(2)
            for i,p in enumerate(procs):
                if p and p.poll() is not None:
                    print(f"[run_all] {APPS[i][0]} exited with {p.returncode}. Restarting…")
                    procs[i] = start_tag(*APPS[i])
    except KeyboardInterrupt:
        print("\n[run_all] stopping…")
        for p in procs:
            if p and p.poll() is None:
                p.terminate()
        for p in procs:
            if p and p.poll() is None:
                p.kill()

if __name__ == "__main__":
    main()
