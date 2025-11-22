# core/db.py — Stable A1 Version (Clean, No Premium Bugs, Fully Compatible)

import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .mongo import db, ensure_indexes


def init_db():
    ensure_indexes()


# ====================================================================
# Helpers
# ====================================================================

def _now() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def _as_int(v, default=None):
    try:
        return int(v)
    except Exception:
        return default


# ====================================================================
# Fake SQL shim (used for broadcast + worker loader)
# ====================================================================

class _FakeCursor:
    def __init__(self, ids: List[int]):
        self._ids = ids

    def fetchall(self):
        return [{"user_id": uid} for uid in self._ids]


class _FakeConn:
    def execute(self, query: str):
        q = (query or "").lower().strip()

        # The worker expects:
        # SELECT DISTINCT user_id FROM sessions
        if "from sessions" in q:
            ids = [
                r["_id"]
                for r in db().sessions.aggregate([
                    {"$group": {"_id": "$user_id"}}
                ])
            ]
            return _FakeCursor(ids)

        # main_bot broadcast uses:
        # SELECT user_id FROM users
        if "from users" in q:
            ids = [r["user_id"] for r in db().users.find({}, {"user_id": 1})]
            return _FakeCursor(ids)

        raise RuntimeError("Unsupported SQL-like query: " + query)


def get_conn():
    return _FakeConn()


# ====================================================================
# Key/Value settings
# ====================================================================

def set_setting(key: str, value: Any):
    db().settings.update_one(
        {"key": str(key)},
        {"$set": {"key": str(key), "val": value}},
        upsert=True
    )


def get_setting(key: str, default: Any = None):
    doc = db().settings.find_one({"key": str(key)})
    return default if not doc else doc.get("val", default)


# ====================================================================
# Users
# ====================================================================

def ensure_user(user_id: int, username: Optional[str]):
    db().users.update_one(
        {"user_id": int(user_id)},
        {
            "$setOnInsert": {"created_at": _now(), "user_id": int(user_id)},
            "$set": {"username": username}
        },
        upsert=True
    )


def users_count() -> int:
    return db().users.count_documents({})


# ====================================================================
# Sessions
# ====================================================================

def sessions_list(user_id: int) -> List[Dict[str, Any]]:
    return list(
        db().sessions.find({"user_id": int(user_id)}, {"_id": 0}).sort("slot", 1)
    )


def sessions_count_user(user_id: int) -> int:
    return db().sessions.count_documents({"user_id": int(user_id)})


def sessions_delete(user_id: int, slot: int) -> int:
    res = db().sessions.delete_one({"user_id": int(user_id), "slot": int(slot)})
    return res.deleted_count


def _session_slots_cap() -> int:
    try:
        return max(1, int(os.getenv("SESSION_SLOTS_CAP", "3")))
    except:
        return 3


def first_free_slot(user_id: int) -> int:
    used = {r["slot"] for r in db().sessions.find({"user_id": int(user_id)}, {"slot": 1})}
    cap = _session_slots_cap()

    for s in range(1, cap + 1):
        if s not in used:
            return s

    # fallback → reuse oldest
    doc = db().sessions.find_one(
        {"user_id": int(user_id)},
        sort=[("updated_at", 1)]
    )
    return int(doc["slot"]) if doc else 1


def sessions_upsert_slot(
    user_id: int, slot: int, api_id: int, api_hash: str, session_string: str
):
    db().sessions.update_one(
        {"user_id": int(user_id), "slot": int(slot)},
        {
            "$set": {
                "api_id": int(api_id),
                "api_hash": api_hash,
                "session_string": session_string,
                "updated_at": _now()
            }
        },
        upsert=True
    )


# ====================================================================
# Groups
# ====================================================================

def groups_cap(user_id: Optional[int] = None) -> int:
    if user_id is None:
        return 5

    v = get_setting(f"groups_cap:{int(user_id)}", None)
    if v is not None:
        return _as_int(v, 5)

    # Legacy unlock flag
    unlock = get_setting(f"gc_unlock:{int(user_id)}", 0)
    try:
        if int(unlock) != 0:
            return 20
    except:
        pass

    return 5


def list_groups(user_id: int) -> List[str]:
    doc = db().groups.find_one({"user_id": int(user_id)})
    return [] if not doc else list(doc.get("targets", []))


def add_group(user_id: int, target: str) -> int:
    target = target.strip()
    if not target:
        return 0

    items = list_groups(user_id)
    if target in items:
        return 0

    if len(items) >= groups_cap(user_id):
        return 0

    items.append(target)
    db().groups.update_one(
        {"user_id": int(user_id)},
        {"$set": {"targets": items, "updated_at": _now()}},
        upsert=True
    )
    return 1


def remove_group(user_id: int, target: str):
    items = list_groups(user_id)
    if target not in items:
        return
    items.remove(target)
    db().groups.update_one(
        {"user_id": int(user_id)},
        {"$set": {"targets": items, "updated_at": _now()}}
    )


def clear_groups(user_id: int):
    db().groups.update_one(
        {"user_id": int(user_id)},
        {"$set": {"targets": [], "updated_at": _now()}},
        upsert=True
    )


# ====================================================================
# Interval
# ====================================================================

def set_interval(user_id: int, minutes: int):
    set_setting(f"interval:{int(user_id)}", int(minutes))


def get_interval(user_id: int) -> int:
    v = get_setting(f"interval:{int(user_id)}", None)
    v = _as_int(v, None)
    return v if v is not None else 30


def get_last_sent_at(user_id: int) -> Optional[int]:
    v = get_setting(f"last_sent_at:{int(user_id)}", None)
    return _as_int(v, None)


def set_last_sent_at(user_id: int, ts: Optional[int] = None):
    set_setting(f"last_sent_at:{int(user_id)}", ts if ts else _now())


# ====================================================================
# Stats
# ====================================================================

def inc_sent_ok(user_id: int, delta: int = 1):
    db().stats.update_one(
        {"user_id": int(user_id)},
        {"$inc": {"sent_ok": delta}},
        upsert=True
    )
    db().settings.update_one(
        {"key": "global:sent_ok"},
        {"$inc": {"val": delta}},
        upsert=True
    )


def get_total_sent_ok() -> int:
    doc = db().settings.find_one({"key": "global:sent_ok"})
    return _as_int(doc.get("val", 0), 0) if doc else 0


def top_users(limit: int = 10) -> List[Dict[str, Any]]:
    items = list(
        db().stats.find({}, {"_id": 0})
        .sort("sent_ok", -1)
        .limit(min(100, max(1, limit)))
    )
    for x in items:
        x["sent_ok"] = _as_int(x.get("sent_ok", 0), 0)
    return items


# ====================================================================
# Gate Channels
# ====================================================================

def get_gate_channels_effective() -> tuple[str | None, str | None]:
    ch1 = get_setting("gate:ch1", None)
    ch2 = get_setting("gate:ch2", None)

    if ch1 or ch2:
        return ch1, ch2

    env = os.getenv("REQUIRED_CHANNELS", "")
    if env:
        parts = [p.strip() for p in env.split(",") if p.strip()]
        if len(parts) >= 2:
            return parts[0], parts[1]
        if len(parts) == 1:
            return parts[0], None

    return "@PhiloBots", "@TheTrafficZone"
