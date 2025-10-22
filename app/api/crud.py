from sqlalchemy.orm import Session
from sqlalchemy import select, func
from . import models

# Users
def get_or_create_user(db: Session, bot_chat_id: int) -> models.User:
    u = db.execute(select(models.User).where(models.User.bot_chat_id == bot_chat_id)).scalar_one_or_none()
    if u: return u
    u = models.User(bot_chat_id=bot_chat_id)
    db.add(u); db.flush()
    return u

# Sessions
def create_or_update_session(db: Session, user_id: int, session_blob_enc: str) -> models.Session:
    s = models.Session(user_id=user_id, session_blob_enc=session_blob_enc, is_active=True)
    db.add(s); db.flush()
    return s

def get_active_session(db: Session, user_id: int):
    return db.execute(
        select(models.Session).where(
            models.Session.user_id == user_id, models.Session.is_active == True
        ).order_by(models.Session.id.desc())
    ).scalar_one_or_none()

# Groups
def count_groups(db: Session, user_id: int) -> int:
    return db.execute(select(func.count(models.Group.id)).where(models.Group.user_id == user_id)).scalar() or 0

def add_group(db: Session, user_id: int, chat_id: int, title: str, can_post: bool) -> models.Group:
    g = models.Group(user_id=user_id, chat_id=chat_id, title=title, can_post=can_post)
    db.add(g); db.flush()
    return g

def list_groups(db: Session, user_id: int):
    return db.execute(select(models.Group).where(models.Group.user_id == user_id)).scalars().all()
