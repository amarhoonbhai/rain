import secrets
from redis import Redis
from .settings import settings

r = Redis.from_url(settings.redis_url, decode_responses=True)

NONCE_TTL = 600  # 10 minutes

def new_nonce(bot_chat_id: int) -> str:
    n = secrets.token_urlsafe(16)
    r.setex(f"nonce:{n}", NONCE_TTL, str(bot_chat_id))
    return n

def resolve_nonce(nonce: str) -> int | None:
    val = r.get(f"nonce:{nonce}")
    return int(val) if val else None

def clear_nonce(nonce: str):
    r.delete(f"nonce:{nonce}")
