# core/db.py — Mongo-backed DB facade (compat API)
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .mongo import db, ensure_indexes


def init_db():
    ensure_indexes()


# --- tiny SQL-ish shim for main_bot broadcast ---
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


def _now() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def _as_int(v, default=None):
    try:
        return int(v)
    except Exception:
        return default


# ---- settings KV ----
def set_setting(key: str, val: Any):
    db().settings.update_one(
        {"key": str(key)},
        {"$set": {"key": str(key), "val": val}},
        upsert=True,
    )


def get_setting(key: str, default: Any = None) -> Any:
    doc = db().settings.find_one({"key": str(key)})
    return default if not doc else doc.get("val", default)


# ---- users ----
def ensure_user(user_id: int, username: Optional[str] = None):
    uid = int(user_id)
    db().users.update_one(
        {"user_id": uid},
        {
            "$setOnInsert": {"user_id": uid, "created_at": _now()},
            "$set": {"username": username},
        },
        upsert=True,
    )


def users_count() -> int:
    return db().users.count_documents({})


# ---- sessions ----
def sessions_list(user_id: int) -> List[Dict[str, Any]]:
    return list(
        db()
        .sessions.find({"user_id": int(user_id)}, {"_id": 0})
        .sort("slot", 1)
    )


def sessions_strings(user_id: int) -> List[Dict[str, Any]]:
    return sessions_list(user_id)


def sessions_count_user(user_id: int) -> int:
    return db().sessions.count_documents({"user_id": int(user_id)})


def sessions_delete(user_id: int, slot: int) -> int:
    return int(
        db()
        .sessions.delete_one({"user_id": int(user_id), "slot": int(slot)})
        .deleted_count
        or 0
    )


def _session_slots_cap(user_id: int) -> int:
    try:
        return max(1, int(os.getenv("SESSION_SLOTS_CAP", "3")))
    except Exception:
        return 3


def first_free_slot(user_id: int, cap: Optional[int] = None) -> Optional[int]:
    cap = cap or _session_slots_cap(user_id)
    used = {r["slot"] for r in db().sessions.find({"user_id": int(user_id)}, {"slot": 1})}
    for s in range(1, cap + 1):
        if s not in used:
            return s
    doc = db().sessions.find_one(
        {"user_id": int(user_id)}, sort=[("updated_at", 1)]
    )
    return int(doc["slot"]) if doc else 1


def sessions_upsert_slot(
    user_id: int,
    slot: int,
    api_id: int,
    api_hash: str,
    session_string: str,
):
    db().sessions.update_one(
        {"user_id": int(user_id), "slot": int(slot)},
        {
            "$set": {
                "api_id": int(api_id),
                "api_hash": str(api_hash),
                "session_string": str(session_string),
                "updated_at": _now(),
            }
        },
        upsert=True,
    )


# legacy aliases
upsert_session_slot = sessions_upsert_slot
sessions_upsert = sessions_upsert_slot
sessions_upsert_slot = sessions_upsert_slot
count_user_sessions = sessions_count_user
delete_session_slot = sessions_delete


def users_with_sessions() -> List[int]:
    return [
        r["_id"]
        for r in db().sessions.aggregate([{"$group": {"_id": "$user_id"}}])
    ]


def sessions_count() -> int:
    return len(users_with_sessions())


# ---- premium flags ----
def is_premium(user_id: int) -> bool:
    v = get_setting(f"premium:{int(user_id)}", 0)
    try:
        return bool(int(v))
    except Exception:
        return bool(v)


def set_premium(user_id: int, enabled: bool):
    set_setting(f"premium:{int(user_id)}", 1 if enabled else 0)


def list_premium_users() -> List[int]:
    out = []
    for r in db().settings.find(
        {"key": {"$regex": r"^premium:\d+$"}, "val": {"$in": [1, "1", True]}}
    ):
        try:
            out.append(int(r["key"].split(":")[1]))
        except Exception:
            pass
    return out


# ---- premium vouchers (redeem codes) ----
def create_voucher(code: str, user_id: int | None = None):
    """
    Create / overwrite a premium voucher code.
    user_id=None => code can be redeemed by ANY user.
    user_id=int  => code locked to that Telegram user_id.
    """
    db().settings.update_one(
        {"key": f"voucher:{code}"},
        {
            "$set": {
                "key": f"voucher:{code}",
                "val": {
                    "user_id": int(user_id) if user_id is not None else None,
                    "used": False,
                },
            }
        },
        upsert=True,
    )


def get_voucher(code: str):
    doc = db().settings.find_one({"key": f"voucher:{code}"})
    return doc.get("val") if doc else None


def mark_voucher_used(code: str, user_id: int):
    db().settings.update_one(
        {"key": f"voucher:{code}"},
        {
            "$set": {
                "val.used": True,
                "val.used_by": int(user_id),
                "val.used_at": _now(),
            }
        },
    )


# ---- groups / caps ----
def is_gc_unlocked(user_id: int) -> bool:
    v = get_setting(f"gc_unlock:{int(user_id)}", 0)
    try:
        return bool(int(v))
    except Exception:
        return bool(v)


def set_gc_unlock(user_id: int, enabled: bool):
    set_setting(f"gc_unlock:{int(user_id)}", 1 if enabled else 0)


def groups_cap(user_id: Optional[int] = None) -> int:
    if user_id is None:
        return 5
    uid = int(user_id)
    if is_premium(uid):
        return 50
    v = get_setting(f"groups_cap:{uid}", None)
    if v is not None:
        vi = _as_int(v, None)
        if vi is not None:
            return vi
    return 10 if is_gc_unlocked(uid) else 5


def _groups_doc(user_id: int):
    doc = db().groups.find_one({"user_id": int(user_id)})
    if not doc:
        doc = {"user_id": int(user_id), "targets": [], "updated_at": _now()}
        db().groups.insert_one(doc)
    return doc


def list_groups(user_id: int) -> List[str]:
    return list(_groups_doc(int(user_id)).get("targets", []))


def add_group(user_id: int, target: str) -> int:
    target = (target or "").strip()
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
        upsert=True,
    )
    return 1


def clear_groups(user_id: int):
    db().groups.update_one(
        {"user_id": int(user_id)},
        {"$set": {"targets": [], "updated_at": _now()}},
        upsert=True,
    )


# ---- intervals / schedule ----
def set_interval(user_id: int, minutes: int):
    set_setting(f"interval:{int(user_id)}", int(minutes))


def get_interval(user_id: int) -> int:
    v = get_setting(f"interval:{int(user_id)}", None)
    iv = _as_int(v, None)
    return iv if iv is not None else int(
        os.getenv("DEFAULT_INTERVAL_MIN", "30")
    )


def get_last_sent_at(user_id: int) -> Optional[int]:
    v = get_setting(f"last_sent_at:{int(user_id)}", None)
    return _as_int(v, None) if v is not None else None


def mark_sent_now(user_id: int):
    set_setting(f"last_sent_at:{int(user_id)}", _now())


# ---- stats ----
def inc_sent_ok(user_id: int, delta: int = 1):
    db().stats.update_one(
        {"user_id": int(user_id)}, {"$inc": {"sent_ok": int(delta)}}, upsert=True
    )
    db().settings.update_one(
        {"key": "global:sent_ok"}, {"$inc": {"val": int(delta)}}, upsert=True
    )


def get_total_sent_ok() -> int:
    doc = db().settings.find_one({"key": "global:sent_ok"})
    return _as_int(doc.get("val", 0), 0) if doc else 0


def top_users(limit: int = 10) -> List[Dict[str, Any]]:
    rows = list(
        db()
        .stats.find({}, {"_id": 0})
        .sort("sent_ok", -1)
        .limit(int(max(1, min(100, limit))))
    )
    for r in rows:
        r.setdefault("user_id", 0)
        r["sent_ok"] = _as_int(r.get("sent_ok", 0), 0)
    return rows


def last_sent_at_for(user_id: int):
    return get_last_sent_at(user_id)  # alias


# ---- ads (compat) ----
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
    return (doc.get("text"), doc.get("mode")) if doc else (None, None)


# ---- gate + night ----
def get_gate_channels_effective() -> tuple[Optional[str], Optional[str]]:
    ch1 = get_setting("gate:ch1", None)
    ch2 = get_setting("gate:ch2", None)
    if ch1 or ch2:
        return ch1, ch2
    env = os.getenv("REQUIRED_CHANNELS", "").strip()
    if env:
        parts = [p.strip() for p in env.split(",") if p.strip()]
        if len(parts) >= 2:
            return parts[0], parts[1]
        if len(parts) == 1:
            return parts[0], None
    return "@PhiloBots", "@TheTrafficZone"


def night_enabled() -> bool:
    v = get_setting("night:enabled", 0)
    try:
        return bool(int(v))
    except Exception:
        return bool(v)


def set_night_enabled(enabled: bool):
    set_setting("night:enabled", 1 if enabled else 0)


# ---- premium name-lock (enforcer uses) ----
def set_name_lock(user_id: int, enabled: bool, name: Optional[str] = None):
    set_setting(f"premium:lock:enabled:{int(user_id)}", 1 if enabled else 0)
    if name is not None:
        set_setting(f"premium:lock:name:{int(user_id)}", name)


def get_setting_name_lock(user_id: int):
    en = get_setting(f"premium:lock:enabled:{int(user_id)}", 0)
    try:
        en = bool(int(en))
    except Exception:
        en = bool(en)
    name = get_setting(f"premium:lock:name:{int(user_id)}", None)
    return en, name
