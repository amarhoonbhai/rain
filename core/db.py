# core/db.py — Mongo-backed DB facade (keeps old function names)
# Covers:
#   users, sessions, groups (caps/unlock/premium), intervals, stats,
#   settings/KV, gate channels, night mode, name-lock, ads (compat),
#   plus a tiny get_conn() shim for broadcast code.
#
# NOTE:
#   Requires core/mongo.py with MONGO_URI and optional MONGO_DB_NAME.

import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .mongo import db, ensure_indexes

# ---------------- Boot ----------------
def init_db():
    ensure_indexes()

# ---------------- Small SQL-compat shim (for main_bot broadcast) ----------------
class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        # emulate sqlite3.Row list with 'user_id'
        return [{"user_id": r} for r in self._rows]


class _FakeConn:
    def execute(self, query: str):
        q = (query or "").strip().lower()
        # main_bot uses: "SELECT user_id FROM users"
        if q.startswith("select user_id from users"):
            ids = [doc["user_id"] for doc in db().users.find({}, {"user_id": 1})]
            return _FakeCursor(ids)
        raise RuntimeError(f"get_conn().execute(): unsupported query: {query!r}")

    def close(self):
        ...


def get_conn():
    """Tiny shim so old broadcast code using sqlite-style API keeps working."""
    return _FakeConn()

# ---------------- Helpers ----------------
def _now_epoch() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def _as_int(x, default=None):
    try:
        return int(x)
    except Exception:
        return default

# ---------------- Settings (KV) ----------------
def set_setting(key: str, val: Any):
    db().settings.update_one(
        {"key": str(key)},
        {"$set": {"key": str(key), "val": val}},
        upsert=True,
    )


def get_setting(key: str, default: Any = None) -> Any:
    doc = db().settings.find_one({"key": str(key)})
    return default if not doc else doc.get("val", default)

# ---------------- Users ----------------
def ensure_user(user_id: int, username: Optional[str] = None):
    uid = int(user_id)
    db().users.update_one(
        {"user_id": uid},
        {
            "$setOnInsert": {"user_id": uid, "created_at": _now_epoch()},
            "$set": {"username": username},
        },
        upsert=True,
    )


def users_count() -> int:
    # exact count (collection is small)
    return db().users.count_documents({})


def list_user_ids() -> List[int]:
    """Compatibility helper. Used by some old tools/broadcast logic."""
    return [doc["user_id"] for doc in db().users.find({}, {"user_id": 1})]

# ---------------- Sessions ----------------
def sessions_list(user_id: int) -> List[Dict[str, Any]]:
    cur = (
        db()
        .sessions.find({"user_id": int(user_id)}, {"_id": 0})
        .sort("slot", 1)
    )
    return list(cur)


def sessions_strings(user_id: int) -> List[Dict[str, Any]]:
    # kept same shape for worker usage
    return sessions_list(user_id)


def sessions_count_user(user_id: int) -> int:
    return db().sessions.count_documents({"user_id": int(user_id)})


def sessions_delete(user_id: int, slot: int) -> int:
    res = db().sessions.delete_one(
        {"user_id": int(user_id), "slot": int(slot)}
    )
    return int(res.deleted_count or 0)


def _session_slots_cap(user_id: int) -> int:
    # default 3; overridable via env
    try:
        return max(1, int(os.getenv("SESSION_SLOTS_CAP", "3")))
    except Exception:
        return 3


def first_free_slot(user_id: int, cap: Optional[int] = None) -> Optional[int]:
    """
    1-based slot numbering (1..cap).
    If all taken, reuse the oldest slot by updated_at (for convenience).
    """
    cap = cap or _session_slots_cap(user_id)
    used = {
        doc["slot"]
        for doc in db().sessions.find(
            {"user_id": int(user_id)}, {"slot": 1}
        )
    }
    for s in range(1, cap + 1):
        if s not in used:
            return s
    # All taken → reuse the oldest slot
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
                "api_hash": str(api_hash),
                "session_string": str(session_string),
                "updated_at": _now_epoch(),
            }
        },
        upsert=True,
    )


# Aliases kept for legacy imports
upsert_session_slot = sessions_upsert_slot
sessions_upsert = sessions_upsert_slot
sessions_upsert_slot = sessions_upsert_slot
count_user_sessions = sessions_count_user
delete_session_slot = sessions_delete


def users_with_sessions() -> List[int]:
    return [
        doc["_id"]
        for doc in db().sessions.aggregate(
            [{"$group": {"_id": "$user_id"}}]
        )
    ]


def sessions_count() -> int:
    return len(users_with_sessions())

# ---------------- Groups & Caps ----------------
def _groups_doc(user_id: int) -> Dict[str, Any]:
    doc = db().groups.find_one({"user_id": int(user_id)})
    if not doc:
        doc = {
            "user_id": int(user_id),
            "targets": [],
            "updated_at": _now_epoch(),
        }
        db().groups.insert_one(doc)
    return doc


def list_groups(user_id: int) -> List[str]:
    doc = _groups_doc(int(user_id))
    return list(doc.get("targets", []))


def is_premium(user_id: int) -> bool:
    v = get_setting(f"premium:{int(user_id)}", 0)
    try:
        return bool(int(v))
    except Exception:
        return bool(v)


def set_premium(user_id: int, enabled: bool):
    set_setting(f"premium:{int(user_id)}", 1 if enabled else 0)


def list_premium_users() -> List[int]:
    rows = db().settings.find(
        {
            "key": {"$regex": r"^premium:\d+$"},
            "val": {"$in": [1, "1", True]},
        }
    )
    out: List[int] = []
    for r in rows:
        try:
            out.append(int(r["key"].split(":")[1]))
        except Exception:
            pass
    return out


def is_gc_unlocked(user_id: int) -> bool:
    v = get_setting(f"gc_unlock:{int(user_id)}", 0)
    try:
        return bool(int(v))
    except Exception:
        return bool(v)


def set_gc_unlock(user_id: int, enabled: bool):
    set_setting(f"gc_unlock:{int(user_id)}", 1 if enabled else 0)


def groups_cap(user_id: Optional[int] = None) -> int:
    """
    Default: 5 free
    Unlock GC: 10
    Premium: 50
    Per-user overrides via key: groups_cap:{uid}
    """
    if user_id is None:
        return 5
    uid = int(user_id)

    # Premium always wins
    if is_premium(uid):
        return 50

    # Explicit override
    v = get_setting(f"groups_cap:{uid}", None)
    if v is not None:
        vi = _as_int(v, None)
        if vi is not None:
            return vi

    # Unlock flag → 10, else 5
    return 10 if is_gc_unlocked(uid) else 5


def set_groups_cap_for(user_id: int, cap: int):
    """Alias used by some older code to override per-user group cap."""
    set_setting(f"groups_cap:{int(user_id)}", int(cap))


def groups_cap_for(user_id: int) -> int:
    """Alias wrapper for groups_cap(user_id)."""
    return groups_cap(user_id)


def add_group(user_id: int, target: str) -> int:
    target = (target or "").strip()
    if not target:
        return 0
    doc = _groups_doc(int(user_id))
    items = list(doc.get("targets", []))
    if target in items:
        return 0
    if len(items) >= groups_cap(user_id):
        return 0
    items.append(target)
    db().groups.update_one(
        {"user_id": int(user_id)},
        {"$set": {"targets": items, "updated_at": _now_epoch()}},
        upsert=True,
    )
    return 1


def clear_groups(user_id: int):
    db().groups.update_one(
        {"user_id": int(user_id)},
        {"$set": {"targets": [], "updated_at": _now_epoch()}},
        upsert=True,
    )

# ---------------- Intervals & Schedule ----------------
def set_interval(user_id: int, minutes: int):
    set_setting(f"interval:{int(user_id)}", int(minutes))


def get_interval(user_id: int) -> int:
    v = get_setting(f"interval:{int(user_id)}", None)
    iv = _as_int(v, None)
    return iv if iv is not None else 30


def get_last_sent_at(user_id: int) -> Optional[int]:
    v = get_setting(f"last_sent_at:{int(user_id)}", None)
    return _as_int(v, None) if v is not None else None


def mark_sent_now(user_id: int):
    set_setting(f"last_sent_at:{int(user_id)}", _now_epoch())

# ---------------- Stats ----------------
def inc_sent_ok(user_id: int, delta: int = 1):
    db().stats.update_one(
        {"user_id": int(user_id)},
        {"$inc": {"sent_ok": int(delta)}},
        upsert=True,
    )
    # also keep global aggregate
    db().settings.update_one(
        {"key": "global:sent_ok"},
        {"$inc": {"val": int(delta)}},
        upsert=True,
    )


def get_total_sent_ok() -> int:
    doc = db().settings.find_one({"key": "global:sent_ok"})
    return _as_int(doc.get("val", 0), 0) if doc else 0


def top_users(limit: int = 10) -> List[Dict[str, Any]]:
    cur = (
        db()
        .stats.find({}, {"_id": 0})
        .sort("sent_ok", -1)
        .limit(int(max(1, min(100, limit))))
    )
    rows = list(cur)
    for r in rows:
        r.setdefault("user_id", 0)
        r["sent_ok"] = _as_int(r.get("sent_ok", 0), 0)
    return rows


def last_sent_at_for(user_id: int) -> Optional[int]:
    """Back-compat alias for get_last_sent_at()."""
    return get_last_sent_at(user_id)

# ---------------- Ads (compat; worker may ignore when using Saved-All) ----------------
def set_ad(user_id: int, text: str, parse_mode: Optional[str]):
    db().settings.update_one(
        {"key": f"ad:{int(user_id)}"},
        {
            "$set": {
                "key": f"ad:{int(user_id)}",
                "text": text,
                "mode": parse_mode,
            }
        },
        upsert=True,
    )


def get_ad(user_id: int):
    doc = db().settings.find_one({"key": f"ad:{int(user_id)}"})
    if not doc:
        return None, None
    return doc.get("text"), doc.get("mode")

# ---------------- Gate channels + Night mode ----------------
def get_gate_channels_effective() -> tuple[Optional[str], Optional[str]]:
    ch1 = get_setting("gate:ch1", None)
    ch2 = get_setting("gate:ch2", None)
    if ch1 or ch2:
        return ch1, ch2

    # fallback: env REQUIRED_CHANNELS
    env_csv = os.getenv("REQUIRED_CHANNELS", "").strip()
    if env_csv:
        parts = [p.strip() for p in env_csv.split(",") if p.strip()]
        if len(parts) >= 2:
            return parts[0], parts[1]
        if len(parts) == 1:
            return parts[0], None

    # final defaults (your pair)
    return "@PhiloBots", "@TheTrafficZone"


def night_enabled() -> bool:
    v = get_setting("night:enabled", 0)
    try:
        return bool(int(v))
    except Exception:
        return bool(v)


def set_night_enabled(enabled: bool):
    set_setting("night:enabled", 1 if enabled else 0)


def set_global_night_mode(enabled: bool):
    """Old alias name used in some code paths."""
    set_night_enabled(enabled)

# ---------------- Premium name-lock (used by enforcer/owner menu) ----------------
def set_name_lock(user_id: int, enabled: bool, name: Optional[str] = None):
    set_setting(f"premium:lock:enabled:{int(user_id)}", 1 if enabled else 0)
    if name is not None:
        set_setting(f"premium:lock:name:{int(user_id)}", name)
