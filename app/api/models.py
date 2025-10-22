from datetime import datetime
from sqlalchemy import Column, Integer, String, BigInteger, DateTime, Boolean, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from .db import Base

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    bot_chat_id = Column(BigInteger, unique=True, index=True)   # AdsBot chat id
    tg_user_id = Column(BigInteger, index=True, nullable=True)  # Telethon side
    phone_e164 = Column(String, nullable=True)
    display_name = Column(String, nullable=True)
    tier = Column(String, default="free")
    created_at = Column(DateTime, default=datetime.utcnow)
    bio_consent_at = Column(DateTime, nullable=True)

    sessions = relationship("Session", back_populates="user", cascade="all, delete-orphan")
    groups = relationship("Group", back_populates="user", cascade="all, delete-orphan")

class Session(Base):
    __tablename__ = "sessions"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"))
    session_blob_enc = Column(String)  # Fernet-encrypted Telethon StringSession
    created_at = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)

    user = relationship("User", back_populates="sessions")

class Group(Base):
    __tablename__ = "groups"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"))
    chat_id = Column(BigInteger)
    title = Column(String)
    can_post = Column(Boolean, default=False)
    added_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("user_id", "chat_id", name="uq_user_group"),
    )

    user = relationship("User", back_populates="groups")
