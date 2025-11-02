from datetime import datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))

def now_ist_iso():
    return datetime.now(IST).isoformat()

def minutes_ago_iso(minutes: int):
    return (datetime.now(IST) - timedelta(minutes=minutes)).isoformat()

def is_due(last_sent_at: str | None, interval_minutes: int) -> bool:
    if not last_sent_at:
        return True
    last_dt = datetime.fromisoformat(last_sent_at)
    diff = datetime.now(IST) - last_dt
    return diff.total_seconds() >= interval_minutes * 60
  
