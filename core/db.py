# core/db.py — clean stable B-2 version
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .mongo import db, ensure_indexes


def init_db():
    ensure_indexes()


# ========= Fake SQL layer for broadcast (main bot) =========
class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return [{"user_id": r} for r in self._rows]


class _FakeConn:
    def execute(self, q: str):
        if q.lower().startswith("select user_id from users"):
            rows = [r["user_id"] for r in db().users.find({}, {"user_id": 1})]
            return _FakeCursor(rows)
        raise RuntimeError("Unsupported query")

    def close(self):
        ...


def get_conn():
    return _FakeConn()


# ========= Helpers =========
def _now():
    return int(datetime.now(timezone.utc).timestamp())


def _as_int(v, d=None):
    try:
        return int(v)
    except:
        return d


# ========= Users =========
def ensure_user(uid: int, username: Optional[str] = None):
    db().users.update_one(
        {"user_id": uid},
        {
            "$setOnInsert": {"created_at": _now(), "user_id": uid},
            "$set": {"username": username},
        },
        upsert=True,
    )


def users_count():
    return db().users.count_documents({})


# ========= Sessions =========
def sessions_list(uid: int) -> List[Dict[str, Any]]:
    return list(
        db().sessions.find({"user_id": uid}, {"_id": 0}).sort("slot", 1)
    )


def sessions_upsert_slot(uid: int, slot: int, api_id: int, api_hash: str, ss: str):
    db().sessions.update_one(
        {"user_id": uid, "slot": slot},
        {
            "$set": {
                "api_id": api_id,
                "api_hash": api_hash,
                "session_string": ss,
                "updated_at": _now(),
            }
        },
        upsert=True,
    )


def sessions_delete(uid: int, slot: int):
    return db().sessions.delete_one({"user_id": uid, "slot": slot}).deleted_count


def first_free_slot(uid: int):
    used = {r["slot"] for r in db().sessions.find({"user_id": uid})}
    for s in range(1, 4):
        if s not in used:
            return s
    doc = db().sessions.find_one({"user_id": uid}, sort=[("updated_at", 1)])
    return doc["slot"] if doc else 1


def users_with_sessions() -> List[int]:
    out = db().sessions.aggregate([{"$group": {"_id": "$user_id"}}])
    return [r["_id"] for r in out]


# ========= Groups =========
def groups_cap(uid: Optional[int] = None) -> int:
    if uid is None:
        return 5

    cap = get_setting(f"groups_cap:{uid}", None)
    if cap is not None:
        return _as_int(cap, 5)

    legacy = get_setting(f"gc_unlock:{uid}", 0)
    if legacy not in (0, "0", None, False):
        return 20

    return 5


def list_groups(uid: int) -> List[str]:
    doc = db().groups.find_one({"user_id": uid})
    return list(doc.get("targets", [])) if doc else []


def add_group(uid: int, g: str) -> int:
    if not g:
        return 0
    g = g.strip()

    lst = list_groups(uid)
    if g in lst:
        return 0
    if len(lst) >= groups_cap(uid):
        return 0

    lst.append(g)
    db().groups.update_one(
        {"user_id": uid},
        {"$set": {"targets": lst, "updated_at": _now()}},
        upsert=True,
    )
    return 1


def clear_groups(uid: int):
    db().groups.update_one(
        {"user_id": uid},
        {"$set": {"targets": [], "updated_at": _now()}},
        upsert=True,
    )


# ========= KV Settings =========
def set_setting(key: str, val: Any):
    db().settings.update_one(
        {"key": key},
        {"$set": {"key": key, "val": val}},
        upsert=True,
    )


def get_setting(key: str, default=None):
    doc = db().settings.find_one({"key": key})
    return doc.get("val", default) if doc else default


# ========= Intervals =========
def set_interval(uid: int, m: int):
    set_setting(f"interval:{uid}", m)


def get_interval(uid: int) -> int:
    v = get_setting(f"interval:{uid}", None)
    return _as_int(v, 30)


def get_last_sent_at(uid: int):
    return _as_int(get_setting(f"last_sent_at:{uid}", None))


def set_last_sent_at(uid: int, ts: Optional[int] = None):
    set_setting(f"last_sent_at:{uid}", ts or _now())


# ========= Stats =========
def inc_sent_ok(uid: int, delta: int = 1):
    db().stats.update_one(
        {"user_id": uid},
        {"$inc": {"sent_ok": delta}},
        upsert=True,
    )

    db().settings.update_one(
        {"key": "global:sent_ok"},
        {"$inc": {"val": delta}},
        upsert=True,
    )


def get_total_sent_ok():
    doc = db().settings.find_one({"key": "global:sent_ok"})
    return _as_int(doc.get("val", 0), 0) if doc else 0


def top_users(limit=10):
    rows = list(
        db().stats.find({}, {"_id": 0})
        .sort("sent_ok", -1)
        .limit(limit)
    )
    for r in rows:
        r["sent_ok"] = _as_int(r.get("sent_ok", 0), 0)
    return rows


# ========= Gate Channels =========
def get_gate_channels_effective():
    c1 = get_setting("gate:ch1", None)
    c2 = get_setting("gate:ch2", None)

    if c1 or c2:
        return c1, c2

    env = os.getenv("REQUIRED_CHANNELS", "")
    parts = [p.strip() for p in env.split(",") if p.strip()]

    if len(parts) >= 2:
        return parts[0], parts[1]
    if len(parts) == 1:
        return parts[0], None

    return "@PhiloBots", "@TheTrafficZone"
