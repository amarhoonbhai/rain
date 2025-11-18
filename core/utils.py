from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional, Union

# Default timezone: IST (UTC+5:30)
IST = timezone(timedelta(hours=5, minutes=30))


def now_ist_iso() -> str:
    """
    Return current time in IST as ISO-8601 string with offset.
    Example: '2025-11-18T09:45:12+05:30'
    """
    return datetime.now(IST).isoformat(timespec="seconds")


def minutes_ago_iso(minutes: int, *, tz: timezone = IST) -> str:
    """
    Return ISO-8601 string for <minutes> minutes ago in the given timezone.
    Default timezone = IST.
    """
    return (datetime.now(tz) - timedelta(minutes=int(minutes))).isoformat(timespec="seconds")


def _parse_ts(value: Union[str, int, float, None], tz: timezone = IST) -> Optional[datetime]:
    """
    Best-effort parse of a timestamp:
      • ISO string (with or without offset) → datetime
      • int/float or numeric string → treated as UNIX seconds
      • None / invalid → returns None
    """
    if value is None:
        return None

    # Already datetime? (just in case you pass it directly)
    if isinstance(value, datetime):
        return value

    # Numeric → unix seconds
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value, tz=tz)
        except Exception:
            return None

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None

        # Try ISO-8601 first
        try:
            dt = datetime.fromisoformat(s)
            # If naive, assume tz
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=tz)
            return dt
        except Exception:
            pass

        # Try numeric string as unix timestamp
        try:
            num = float(s)
            return datetime.fromtimestamp(num, tz=tz)
        except Exception:
            return None

    return None


def is_due(
    last_sent_at: Union[str, int, float, None],
    interval_minutes: int,
    *,
    tz: timezone = IST,
) -> bool:
    """
    Return True if the next run is due.

    last_sent_at:
      • ISO string (what you’re storing now)
      • or UNIX timestamp (int/float)
      • or None → always due

    interval_minutes:
      • minimum minutes between runs
    """
    if interval_minutes <= 0:
        # If someone misconfigures interval, treat as always due.
        return True

    last_dt = _parse_ts(last_sent_at, tz=tz)
    if last_dt is None:
        # No valid history → run immediately
        return True

    diff = datetime.now(tz) - last_dt
    return diff.total_seconds() >= interval_minutes * 60
