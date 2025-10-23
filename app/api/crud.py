"""CRUD (Create, Read, Update, Delete) helpers for the Spinify service.

This module encapsulates simple database operations on the ``User``, ``Session``
and ``Group`` models.  These functions provide a clean interface for the API
layer to retrieve and manipulate persistent data without dealing with the
underlying SQLAlchemy session details each time.  Where appropriate, type
annotations are used to clarify the expected input and output types.

Most functions accept a SQLAlchemy ``Session`` instance and return ORM
instances or primitive values.  They do *not* commit the session; this is
handled by the context manager in ``db.py``.  Adding new helpers here keeps
the business logic out of your route handlers and makes it easier to test
individual pieces in isolation.
"""

from sqlalchemy.orm import Session
from sqlalchemy import select, func
from . import models

# Users
def get_or_create_user(db: Session, bot_chat_id: int) -> models.User:
    """Return the user matching the given AdsBot chat ID, creating it if necessary.

    Args:
        db: An active SQLAlchemy session.
        bot_chat_id: The chat ID used by the AdsBot to identify the user.

    Returns:
        The ``User`` ORM instance corresponding to the provided chat ID.  If no
        such user exists, a new one is created and flushed to the session.
    """
    u = db.execute(
        select(models.User).where(models.User.bot_chat_id == bot_chat_id)
    ).scalar_one_or_none()
    if u:
        return u
    u = models.User(bot_chat_id=bot_chat_id)
    db.add(u)
    db.flush()
    return u

# Sessions
def create_or_update_session(db: Session, user_id: int, session_blob_enc: str) -> models.Session:
    """Create a new active session row for the given user.

    Any existing sessions for the user are left untouched; callers must
    deactivate old sessions if desired.  The encrypted Telethon session string
    should be passed in via ``session_blob_enc``.

    Args:
        db: An active SQLAlchemy session.
        user_id: The primary key of the ``User`` this session belongs to.
        session_blob_enc: The Fernet‑encrypted Telethon ``StringSession``.

    Returns:
        The newly created ``Session`` ORM instance.
    """
    s = models.Session(
        user_id=user_id,
        session_blob_enc=session_blob_enc,
        is_active=True,
    )
    db.add(s)
    db.flush()
    return s

def get_active_session(db: Session, user_id: int) -> models.Session | None:
    """Retrieve the most recent active session for a user.

    Args:
        db: An active SQLAlchemy session.
        user_id: The primary key of the ``User`` to look up.

    Returns:
        The ``Session`` ORM instance if one exists and is marked active,
        otherwise ``None``.
    """
    return db.execute(
        select(models.Session)
        .where(
            models.Session.user_id == user_id,
            models.Session.is_active == True,
        )
        .order_by(models.Session.id.desc())
    ).scalar_one_or_none()

# Groups
def count_groups(db: Session, user_id: int) -> int:
    """Return the number of groups associated with a given user."""
    return (
        db.execute(
            select(func.count(models.Group.id)).where(
                models.Group.user_id == user_id
            )
        ).scalar()
        or 0
    )

def add_group(
    db: Session, user_id: int, chat_id: int, title: str, can_post: bool
) -> models.Group:
    """Create and persist a new group for the given user.

    Args:
        db: An active SQLAlchemy session.
        user_id: The ID of the ``User`` who is adding the group.
        chat_id: The Telegram chat ID of the group being added.
        title: A human‑readable title for the group.
        can_post: Whether the user has permission to post in the group.

    Returns:
        The newly created ``Group`` ORM instance.
    """
    g = models.Group(
        user_id=user_id,
        chat_id=chat_id,
        title=title,
        can_post=can_post,
    )
    db.add(g)
    db.flush()
    return g

def list_groups(db: Session, user_id: int) -> list[models.Group]:
    """Return all groups belonging to the given user."""
    return (
        db.execute(
            select(models.Group).where(models.Group.user_id == user_id)
        )
        .scalars()
        .all()
    )

# Additional helpers for session and group management

def has_active_session(db: Session, user_id: int) -> bool:
    """Return ``True`` if the user has at least one active session."""
    cnt = db.execute(
        select(func.count(models.Session.id)).where(
            models.Session.user_id == user_id,
            models.Session.is_active == True,
        )
    ).scalar()
    return bool(cnt)


def delete_group(db: Session, user_id: int, chat_id: int) -> bool:
    """Remove a group from a user's list.

    Args:
        db: An active SQLAlchemy session.
        user_id: The ID of the user whose group list should be modified.
        chat_id: The Telegram chat ID of the group to remove.

    Returns:
        ``True`` if a group was found and deleted, otherwise ``False``.
    """
    g = db.execute(
        select(models.Group).where(
            models.Group.user_id == user_id,
            models.Group.chat_id == chat_id,
        )
    ).scalar_one_or_none()
    if not g:
        return False
    db.delete(g)
    db.flush()
    return True
