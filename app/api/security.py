from cryptography.fernet import Fernet
from .settings import settings

if not settings.encryption_key:
    raise RuntimeError("ENCRYPTION_KEY missing. Generate a 32-byte urlsafe base64 key.")

fernet = Fernet(settings.encryption_key.encode())

def enc(data: str) -> str:
    return fernet.encrypt(data.encode()).decode()

def dec(token: str) -> str:
    return fernet.decrypt(token.encode()).decode()
