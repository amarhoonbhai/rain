# api_server.py — FastAPI backend for Spinify WebApp (verify Telegram, serve stats & actions)
import os, json, hmac, hashlib
from urllib.parse import parse_qsl
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List

from core.db import (
    init_db, set_ad, get_ad, set_interval, get_interval,
    add_groups, list_groups, clear_groups,
    users_count, sessions_count, get_total_sent_ok, top_users
)

BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN") or os.getenv("ADS_BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Set MAIN_BOT_TOKEN or ADS_BOT_TOKEN in env")

app = FastAPI(title="Spinify API", version="1.0")

# CORS (relax for dev; restrict in prod)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def verify_init_data(init_data: str):
    if not init_data:
        raise HTTPException(status_code=401, detail="Missing init_data")
    data = dict(parse_qsl(init_data, strict_parsing=False))
    their_hash = data.pop("hash", None)
    if not their_hash:
        raise HTTPException(status_code=401, detail="Missing hash")

    data_check_string = "\n".join(f"{k}={data[k]}" for k in sorted(data.keys()))
    secret_key = hashlib.sha256(BOT_TOKEN.encode()).digest()
    computed = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(computed, their_hash):
        raise HTTPException(status_code=401, detail="Bad signature")

    user_raw = data.get("user", "{}")
    try:
        user = json.loads(user_raw)
    except Exception:
        user = {}
    uid = int(user.get("id") or 0)
    uname = user.get("username")
    if not uid:
        raise HTTPException(status_code=401, detail="No user in init_data")
    return uid, uname

class SaveAd(BaseModel):
    text: str

class SetInterval(BaseModel):
    minutes: int

class AddGroup(BaseModel):
    group: str

class AddGroupsBulk(BaseModel):
    groups: List[str]

class RemoveGroup(BaseModel):
    group: str

@app.on_event("startup")
def _boot():
    init_db()

@app.get("/api/sync")
def sync(x_telegram_init_data: str = Header(default="")):
    uid, _ = verify_init_data(x_telegram_init_data)
    return {
        "ok": True,
        "user": uid,
        "ad": get_ad(uid),
        "interval": (get_interval(uid) or 30),
        "groups": list_groups(uid),
        "stats": {
            "users": users_count(),
            "active": sessions_count(),
            "forwards": get_total_sent_ok(),
        },
    }

@app.post("/api/save_ad")
def api_save_ad(body: SaveAd, x_telegram_init_data: str = Header(default="")):
    uid, _ = verify_init_data(x_telegram_init_data)
    set_ad(uid, (body.text or "").strip()); return {"ok": True}

@app.post("/api/set_interval")
def api_set_interval(body: SetInterval, x_telegram_init_data: str = Header(default="")):
    uid, _ = verify_init_data(x_telegram_init_data)
    minutes = 30 if body.minutes not in {30, 45, 60} else body.minutes
    set_interval(uid, minutes); return {"ok": True, "minutes": minutes}

@app.post("/api/add_group")
def api_add_group(body: AddGroup, x_telegram_init_data: str = Header(default="")):
    uid, _ = verify_init_data(x_telegram_init_data)
    add_groups(uid, [body.group]); return {"ok": True}

@app.post("/api/add_groups_bulk")
def api_add_groups_bulk(body: AddGroupsBulk, x_telegram_init_data: str = Header(default="")):
    uid, _ = verify_init_data(x_telegram_init_data)
    n = add_groups(uid, body.groups or []); return {"ok": True, "added": n}

@app.post("/api/remove_group")
def api_remove_group(body: RemoveGroup, x_telegram_init_data: str = Header(default="")):
    uid, _ = verify_init_data(x_telegram_init_data)
    # simple rewrite without the given group
    current = [g for g in list_groups(uid) if g != body.group]
    clear_groups(uid)
    if current: add_groups(uid, current)
    return {"ok": True}

@app.post("/api/clear_groups")
def api_clear_groups(x_telegram_init_data: str = Header(default="")):
    uid, _ = verify_init_data(x_telegram_init_data)
    clear_groups(uid); return {"ok": True}

@app.get("/api/top")
def api_top(limit: int = 10, x_telegram_init_data: str = Header(default="")):
    # You can require verification here if you prefer:
    # verify_init_data(x_telegram_init_data)
    rows = top_users(limit=limit)
    return {"ok": True, "top": [
        {"user_id": r["user_id"], "username": r["username"], "sent_ok": r["sent_ok"], "last_sent_at": r["last_sent_at"]}
        for r in rows
    ]}
