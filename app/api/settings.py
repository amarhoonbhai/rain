import os
from pydantic import BaseModel

class Settings(BaseModel):
    # Bot tokens (used by bots; API reads for completeness)
    adsbot_token: str | None = os.getenv("ADSBOT_TOKEN")
    loginbot_token: str | None = os.getenv("LOGINBOT_TOKEN")

    # Join-gate targets
    join_channel_username: str = os.getenv("JOIN_CHANNEL_USERNAME", "@PhiloBots")
    join_group_id: int = int(os.getenv("JOIN_GROUP_ID", "-1002424072993"))

    # App / crypto / URLs
    app_secret: str = os.getenv("APP_SECRET", "dev-secret")
    encryption_key: str = os.getenv("ENCRYPTION_KEY", "")  # REQUIRED by security.py
    base_url: str = os.getenv("BASE_URL", "http://localhost:8000")
    env: str = os.getenv("ENV", "dev")

    # DB / Redis
    database_url: str = os.getenv("DATABASE_URL", "postgresql+psycopg://spinify:spinify@db:5432/spinify")
    redis_url: str = os.getenv("REDIS_URL", "redis://redis:6379/0")

settings = Settings()
