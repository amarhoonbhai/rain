"""
core/db.py — Mongo-backed DB facade (compat API)

This module provides a small, stable API that the bots and services
(main_bot, login_bot, profile_enforcer, etc.) use.

Everything is backed by MongoDB via core.mongo.db(), but the rest of the
codebase should only talk to these helpers, NOT to pymongo directly.
"""

import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .mongo import db, ensure_indexes


# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------

def init_db() -> None:
    """Ensure Mongo indexes exist (idempotent, safe to call multiple times)."""
    ensure_indexes()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> int:
    """Return current UTC timestamp as int."""
    return int(datetime.now(timezone.utc).timestamp())


def _as_int(v: Any, default: Optional[int] = None) -> Optional[int]:
    """Best-effort cast to int, with default on failure."""
    try:
        return int(v)
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Tiny SQL-ish shim for main_bot broadcast
# ---------------------------------------------------------------------------

class _FakeCursor:
    def __init__(self, rows: List[int]) -> None:
        self._rows = rows

    def fetchall(self) -> List[Dict[str, Any]]:
        return [{"user_id": r} for r in self._rows]


class _FakeConn:
    """
    Very small shim to support:
      SELECT user_id FROM users
    used by broadcast in main_bot.
    """

    def execute(self, query: str) -> _FakeCursor:
        q = (query or "").lower().strip()
        if q.startswith("select user_id from users"):
            ids = [r["user_id"] for r in db().users.find({}, {"user_id": 1})]
            return _FakeCursor(ids)
        raise RuntimeError(f"unsupported query: {query!r}")

    def close(self) -> None:
        ...


def get_conn() -> _FakeConn:
    """Return a fake connection object for broadcast code."""
    return _FakeConn()


# ---------------------------------------------------------------------------
# Settings KV
# ---------------------------------------------------------------------------

def set_setting(key: str, val: Any) -> None:
    db().settings.update_one(
        {"key": str(key)},
        {"$set": {"key": str(key), "val": val}},
        upsert=True,
    )


def get_setting(key: str, default: Any = None) -> Any:
    doc = db().settings.find_one({"key": str(key)})
    return default if not doc else doc.get("val", default)


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

def ensure_user(user_id: int, username: Optional[str] = None) -> None:
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


# ---------------------------------------------------------------------------
# Sessions (multi-slot per panel user)
# ---------------------------------------------------------------------------

def sessions_list(user_id: int) -> List[Dict[str, Any]]:
    return list(
        db().sessions
        .find({"user_id": int(user_id)}, {"_id": 0})
        .sort("slot", 1)
    )


def sessions_strings(user_id: int) -> List[Dict[str, Any]]:
    # Legacy alias
    return sessions_list(user_id)


def sessions_count_user(user_id: int) -> int:
    return db().sessions.count_documents({"user_id": int(user_id)})


def sessions_delete(user_id: int, slot: int) -> int:
    return int(
        db().sessions.delete_one(
            {"user_id": int(user_id), "slot": int(slot)}
        ).deleted_count
        or 0
    )


def _session_slots_cap(user_id: int) -> int:
    """Max slots per panel user (default 3, overridable with env)."""
    try:
        return max(1, int(os.getenv("SESSION_SLOTS_CAP", "3")))
    except Exception:
        return 3


def first_free_slot(user_id: int, cap: Optional[int] = None) -> Optional[int]:
    """
    Find first free slot 1..cap.
    If all are full, reuse the oldest updated slot.
    """
    cap = cap or _session_slots_cap(user_id)
    used = {
        r["slot"]
        for r in db().sessions.find({"user_id": int(user_id)}, {"slot": 1})
    }
    for s in range(1, cap + 1):
        if s not in used:
            return s
    doc = db().sessions.find_one(
        {"user_id": int(user_id)},
        sort=[("updated_at", 1)],
    )
    return int(doc["slot"]) if doc else 1


def sessions_upsert_slot(
    user_id: int,
    slot: int,
    api_id: int,
    api_hash: str,
    session_string: str,
) -> None:
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


# Legacy aliases
upsert_session_slot = sessions_upsert_slot
sessions_upsert = sessions_upsert_slot

def users_with_sessions() -> List[int]:
    """List panel user_ids that have at least 1 session."""
    return [
        r["_id"]
        for r in db().sessions.aggregate(
            [{"$group": {"_id": "$user_id"}}]
        )
    ]


def sessions_count() -> int:
    """Number of panel users that have ≥1 session."""
    return len(users_with_sessions())


# ---------------------------------------------------------------------------
# Groups / caps (per panel user)
# ---------------------------------------------------------------------------

def is_premium(user_id: int) -> bool:
    """
    Panel-level premium flag (used mainly for caps).
    Stored as settings key 'premium:<user_id>'.
    """
    v = get_setting(f"premium:{int(user_id)}", 0)
    try:
        return bool(int(v))
    except Exception:
        return bool(v)


def set_premium(user_id: int, enabled: bool) -> None:
    set_setting(f"premium:{int(user_id)}", 1 if enabled else 0)


def list_premium_users() -> List[int]:
    out: List[int] = []
    for r in db().settings.find(
        {
            "key": {"$regex": r"^premium:\d+$"},
            "val": {"$in": [1, "1", True]},
        }
    ):
        try:
            out.append(int(r["key"].split(":")[1]))
        except Exception:
            pass
    return out


def is_gc_unlocked(user_id: int) -> bool:
    """
    Legacy "unlock GC" flag; kept for compatibility.
    In practice you mostly override via groups_cap:<uid>.
    """
    v = get_setting(f"gc_unlock:{int(user_id)}", 0)
    try:
        return bool(int(v))
    except Exception:
        return bool(v)


def set_gc_unlock(user_id: int, enabled: bool) -> None:
    set_setting(f"gc_unlock:{int(user_id)}", 1 if enabled else 0)


def groups_cap(user_id: Optional[int] = None) -> int:
    """
    Max groups for a panel user:
      • None → 5 (generic default)
      • premium → 50
      • explicit groups_cap:<uid> setting → that value
      • else → 10 if gc_unlocked, else 5
    """
    if user_id is None:
        return 5
    uid = int(user_id)

    # Hard premium flag wins
    if is_premium(uid):
        return 50

    # Per-user override
    v = get_setting(f"groups_cap:{uid}", None)
    if v is not None:
        vi = _as_int(v, None)
        if vi is not None:
            return vi

    # Legacy unlock
    return 10 if is_gc_unlocked(uid) else 5


def _groups_doc(user_id: int) -> Dict[str, Any]:
    """Internal: fetch or create group doc for a panel user."""
    doc = db().groups.find_one({"user_id": int(user_id)})
    if not doc:
        doc = {"user_id": int(user_id), "targets": [], "updated_at": _now()}
        db().groups.insert_one(doc)
    return doc


def list_groups(user_id: int) -> List[str]:
    """
    Panel-level group targets (used by main_bot stats / some flows).
    Note: your Telethon worker stores its own per-account groups in JSON,
    this is independent.
    """
    return list(_groups_doc(int(user_id)).get("targets", []))


def add_group(user_id: int, target: str) -> int:
    """
    Add a single target to panel user's list, respecting groups_cap().
    Returns 1 if added, 0 if duplicate/empty/cap reached.
    """
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


def clear_groups(user_id: int) -> None:
    db().groups.update_one(
        {"user_id": int(user_id)},
        {"$set": {"targets": [], "updated_at": _now()}},
        upsert=True,
    )


# ---------------------------------------------------------------------------
# Intervals / schedule (panel-level)
# ---------------------------------------------------------------------------

def set_interval(user_id: int, minutes: int) -> None:
    set_setting(f"interval:{int(user_id)}", int(minutes))


def get_interval(user_id: int) -> int:
    v = get_setting(f"interval:{int(user_id)}", None)
    iv = _as_int(v, None)
    return iv if iv is not None else int(os.getenv("DEFAULT_INTERVAL_MIN", "30"))


def get_last_sent_at(user_id: int) -> Optional[int]:
    v = get_setting(f"last_sent_at:{int(user_id)}", None)
    return _as_int(v, None) if v is not None else None


def mark_sent_now(user_id: int) -> None:
    set_setting(f"last_sent_at:{int(user_id)}", _now())


# ---------------------------------------------------------------------------
# Stats (only used if some worker increments them)
# ---------------------------------------------------------------------------

def inc_sent_ok(user_id: int, delta: int = 1) -> None:
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
        r.setdefault("user_id", 0)
        r["sent_ok"] = _as_int(r.get("sent_ok", 0), 0) or 0
    return rows


def last_sent_at_for(user_id: int) -> Optional[int]:
    """Legacy alias."""
    return get_last_sent_at(user_id)


# ---------------------------------------------------------------------------
# Ads (legacy, kept for compat)
# ---------------------------------------------------------------------------

def set_ad(user_id: int, text: str, parse_mode: Optional[str]) -> None:
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


def get_ad(user_id: int) -> Tuple[Optional[str], Optional[str]]:
    doc = db().settings.find_one({"key": f"ad:{int(user_id)}"})
    if not doc:
        return (None, None)
    return (doc.get("text"), doc.get("mode"))


# ---------------------------------------------------------------------------
# Gate + global night toggle (panel-level)
# ---------------------------------------------------------------------------

def get_gate_channels_effective() -> Tuple[Optional[str], Optional[str]]:
    """
    Gate channels are either stored in settings (gate:ch1, gate:ch2) or,
    if absent, picked from REQUIRED_CHANNELS env or fallback default.
    """
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

    # Final fallback
    return "@PhiloBots", "@TheTrafficZone"


def night_enabled() -> bool:
    v = get_setting("night:enabled", 0)
    try:
        return bool(int(v))
    except Exception:
        return bool(v)


def set_night_enabled(enabled: bool) -> None:
    set_setting("night:enabled", 1 if enabled else 0)


# ---------------------------------------------------------------------------
# Premium name-lock (used by profile_enforcer)
# ---------------------------------------------------------------------------

def set_name_lock(user_id: int, enabled: bool, name: Optional[str] = None) -> None:
    set_setting(f"premium:lock:enabled:{int(user_id)}", 1 if enabled else 0)
    if name is not None:
        set_setting(f"premium:lock:name:{int(user_id)}", name)


def get_setting_name_lock(user_id: int) -> Tuple[bool, Optional[str]]:
    en = get_setting(f"premium:lock:enabled:{int(user_id)}", 0)
    try:
        en_bool = bool(int(en))
    except Exception:
        en_bool = bool(en)
    name = get_setting(f"premium:lock:name:{int(user_id)}", None)
    return en_bool, name
