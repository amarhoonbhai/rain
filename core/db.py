# core/db.py — Mongo-backed DB facade (clean + no-premium version)
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .mongo import db, ensure_indexes


def init_db():
    ensure_indexes()


# ====================================
# Fake SQL shim (used in broadcast)
# ====================================
class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return [{"user_id": r} for r in self._rows]


class _FakeConn:
    def execute(self, query: str):
        q = (query or "").lower().strip()
        if q.startswith("select user_id from users"):
            ids = [r["user_id"] for r in db().users.find({}, {"user_id": 1})]
            return _FakeCursor(ids)
        raise RuntimeError("unsupported query")

    def close(self):
        ...


def get_conn():
    return _FakeConn()


# ====================================
# Helpers
# ====================================
def _now() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def _as_int(v, default=None):
    try:
        return int(v)
    except Exception:
        return default


# ====================================
# SETTINGS (Key/Value Store)
# ====================================
def set_setting(key: str, val: Any):
    db().settings.update_one(
        {"key": str(key)},
        {"$set": {"key": str(key), "val": val}},
        upsert=True,
    )


def get_setting(key: str, default: Any = None) -> Any:
    doc = db().settings.find_one({"key": str(key)})
    return default if not doc else doc.get("val", default)


# ====================================
# USERS
# ====================================
def ensure_user(user_id: int, username: Optional[str] = None):
    db().users.update_one(
        {"user_id": int(user_id)},
        {
            "$setOnInsert": {"created_at": _now(), "user_id": int(user_id)},
            "$set": {"username": username},
        },
        upsert=True,
    )


def users_count() -> int:
    return db().users.count_documents({})


# ====================================
# SESSIONS
# ====================================
def sessions_list(user_id: int) -> List[Dict[str, Any]]:
    return list(
        db().sessions.find({"user_id": int(user_id)}, {"_id": 0}).sort("slot", 1)
    )


def sessions_count_user(user_id: int) -> int:
    return db().sessions.count_documents({"user_id": int(user_id)})


def sessions_delete(user_id: int, slot: int) -> int:
    res = db().sessions.delete_one({"user_id": int(user_id), "slot": int(slot)})
    return int(res.deleted_count or 0)


def _session_slots_cap(user_id: int) -> int:
    try:
        return max(1, int(os.getenv("SESSION_SLOTS_CAP", "3")))
    except Exception:
        return 3


def first_free_slot(user_id: int, cap: Optional[int] = None) -> Optional[int]:
    cap = cap or _session_slots_cap(user_id)
    used = {r["slot"] for r in db().sessions.find(
        {"user_id": int(user_id)}, {"slot": 1}
    )}

    for s in range(1, cap + 1):
        if s not in used:
            return s

    doc = db().sessions.find_one(
        {"user_id": int(user_id)}, sort=[("updated_at", 1)]
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
                "updated_at": _now(),
            }
        },
        upsert=True,
    )


# compatibility
def store_api_credentials(user_id: int, slot: int, api_id: int, api_hash: str, ss: str):
    sessions_upsert_slot(user_id, slot, api_id, api_hash, ss)


def delete_api_credentials(user_id: int, slot: int):
    return sessions_delete(user_id, slot)


# ====================================
# REQUIRED FOR ENFORCER
# ====================================
def users_with_sessions() -> List[int]:
    return [
        r["_id"]
        for r in db().sessions.aggregate([{"$group": {"_id": "$user_id"}}])
    ]


# ====================================
# GROUPS
# ====================================
def groups_cap(user_id: Optional[int] = None) -> int:
    if user_id is None:
        return 5

    uid = int(user_id)

    v = get_setting(f"groups_cap:{uid}", None)
    if v is not None:
        return _as_int(v, 5)

    if get_setting(f"gc_unlock:{uid}", 0) not in (0, "0", None, False):
        return 20

    return 5


def list_groups(user_id: int) -> List[str]:
    doc = db().groups.find_one({"user_id": int(user_id)})
    if not doc:
        return []
    return list(doc.get("targets", []))


def add_group(user_id: int, target: str) -> int:
    if not target:
        return 0
    target = target.strip()
    items = list_groups(user_id)
    if target in items:
        return 0
    if len(items) >= groups_cap(user_id):
        return 0
    items.append(target)
    db().groups.update_one(
        {"user_id": int(user_id)},
        {"$set": {"targets": items, "updated_at": _now()}},
        upsert=True,
    )
    return 1


def clear_groups(user_id: int):
    db().groups.update_one(
        {"user_id": int(user_id)},
        {"$set": {"targets": [], "updated_at": _now()}},
        upsert=True,
    )


# REQUIRED — you forgot this in your version
def remove_group(user_id: int, g: str):
    items = list_groups(user_id)
    if g in items:
        items.remove(g)
        db().groups.update_one(
            {"user_id": int(user_id)},
            {"$set": {"targets": items, "updated_at": _now()}},
        )
        return 1
    return 0


# ====================================
# INTERVALS / SCHEDULING
# ====================================
def set_interval(user_id: int, minutes: int):
    set_setting(f"interval:{int(user_id)}", int(minutes))


def get_interval(user_id: int) -> int:
    v = get_setting(f"interval:{int(user_id)}", None)
    iv = _as_int(v, None)
    return iv if iv is not None else int(os.getenv("DEFAULT_INTERVAL_MIN", "30"))


def get_last_sent_at(user_id: int) -> Optional[int]:
    v = get_setting(f"last_sent_at:{int(user_id)}", None)
    return _as_int(v, None)


def set_last_sent_at(user_id: int, ts: int | None = None):
    if ts is None:
        ts = _now()
    set_setting(f"last_sent_at:{int(user_id)}", ts)


# ====================================
# STATS
# ====================================
def inc_sent_ok(user_id: int, delta: int = 1):
    db().stats.update_one(
        {"user_id": int(user_id)},
        {"$inc": {"sent_ok": int(delta)}},
        upsert=True,
    )
    db().settings.update_one(
        {"key": "global:sent_ok"},
        {"$inc": {"val": int(delta)}},
        upsert=True,
    )


def get_total_sent_ok() -> int:
    doc = db().settings.find_one({"key": "global:sent_ok"})
    return _as_int(doc.get("val", 0), 0) if doc else 0


def top_users(limit: int = 10) -> List[Dict[str, Any]]:
    rows = list(
        db().stats.find({}, {"_id": 0})
        .sort("sent_ok", -1)
        .limit(int(max(1, min(100, limit))))
    )
    for r in rows:
        r["sent_ok"] = _as_int(r.get("sent_ok", 0), 0)
    return rows


# ====================================
# GATE CHANNELS
# ====================================
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
