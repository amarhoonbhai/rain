from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from .db import Base, engine, session_scope
from . import crud, models
from .security import enc, dec
from telethon import TelegramClient
from telethon.sessions import StringSession

app = FastAPI(title="Spinify Ads API")

# Auto-create tables for MVP (use Alembic in prod)
Base.metadata.create_all(bind=engine)

@app.get("/healthz")
async def healthz():
    return {"ok": True}

# --- Nonce (AdsBot -> LoginBot deep-link) ---
class NonceOut(BaseModel):
    nonce: str

@app.get("/nonce/{bot_chat_id}", response_model=NonceOut)
async def api_nonce(bot_chat_id: int):
    from .utils import new_nonce
    n = new_nonce(bot_chat_id)
    return {"nonce": n}

# --- Session bind (LoginBot -> API) ---
class SessionBindIn(BaseModel):
    nonce: str
    string_session: str
    phone_e164: str
    tg_user_id: int
    display_name: str | None = None
    api_id: int | None = None
    api_hash: str | None = None

@app.post("/session/bind")
async def api_session_bind(payload: SessionBindIn):
    from .utils import resolve_nonce, clear_nonce
    bot_chat_id = resolve_nonce(payload.nonce)
    if not bot_chat_id:
        raise HTTPException(400, "Invalid or expired nonce")
    with session_scope() as db:
        user = crud.get_or_create_user(db, bot_chat_id)
        user.tg_user_id = payload.tg_user_id
        user.phone_e164 = payload.phone_e164
        if payload.display_name:
            user.display_name = payload.display_name
        s = models.Session(
            user_id=user.id,
            session_blob_enc=enc(payload.string_session),
            is_active=True,
            api_id=payload.api_id,
            api_hash_enc=(enc(payload.api_hash) if payload.api_hash else None),
        )
        db.add(s)
    clear_nonce(payload.nonce)
    return {"ok": True}

# --- Groups (AdsBot uses these) ---
class GroupAddCheckIn(BaseModel):
    bot_chat_id: int
    chat_id: int
    title: str
    can_post: bool = Field(default=False)

@app.post("/groups/add_checked")
async def api_group_add_checked(payload: GroupAddCheckIn):
    with session_scope() as db:
        user = crud.get_or_create_user(db, payload.bot_chat_id)
        if crud.count_groups(db, user.id) >= 5:
            raise HTTPException(400, "Group limit reached (5)")
        g = crud.add_group(db, user.id, payload.chat_id, payload.title, payload.can_post)
        return {"ok": True, "id": g.id}

@app.get("/groups/list/{bot_chat_id}")
async def api_group_list(bot_chat_id: int):
    with session_scope() as db:
        user = crud.get_or_create_user(db, bot_chat_id)
        groups = crud.list_groups(db, user.id)
        return [{"chat_id": g.chat_id, "title": g.title, "can_post": g.can_post} for g in groups]

class MeOut(BaseModel):
    has_session: bool
    groups_count: int

@app.get("/me/{bot_chat_id}", response_model=MeOut)
async def api_me(bot_chat_id: int):
    with session_scope() as db:
        user = crud.get_or_create_user(db, bot_chat_id)
        has_sess = crud.has_active_session(db, user.id)
        groups = crud.list_groups(db, user.id)
        return {"has_session": has_sess, "groups_count": len(groups)}

class GroupDeleteIn(BaseModel):
    bot_chat_id: int
    chat_id: int

@app.post("/groups/delete")
async def api_group_delete(payload: GroupDeleteIn):
    with session_scope() as db:
        user = crud.get_or_create_user(db, payload.bot_chat_id)
        ok = crud.delete_group(db, user.id, payload.chat_id)
        if not ok:
            raise HTTPException(404, "Group not found for this user")
        return {"ok": True}

# --- S7: verify via user's session, then add ---
class VerifyAddIn(BaseModel):
    bot_chat_id: int
    link_or_id: str

class VerifyAddOut(BaseModel):
    ok: bool
    chat_id: int | None = None
    title: str | None = None
    can_post: bool | None = None
    reason: str | None = None

@app.post("/groups/verify_add", response_model=VerifyAddOut)
async def api_groups_verify_add(payload: VerifyAddIn):
    # 1) Load user + session
    with session_scope() as db:
        user = crud.get_or_create_user(db, payload.bot_chat_id)
        sess = crud.get_active_session(db, user.id)
        if not sess:
            return VerifyAddOut(ok=False, reason="No active session. Connect an account first.")
        if not sess.api_id or not sess.api_hash_enc:
            return VerifyAddOut(ok=False, reason="Login again so we can save your API ID/Hash (S7 update).")
        try:
            string_session = dec(sess.session_blob_enc)
            api_hash = dec(sess.api_hash_enc)
        except Exception:
            return VerifyAddOut(ok=False, reason="Failed to read session. Please relogin.")
        api_id = int(sess.api_id)
    # 2) Normalize input
    link = payload.link_or_id.strip()
    if link.startswith("http"):
        if "t.me/+" in link or "joinchat" in link:
            return VerifyAddOut(ok=False, reason="Invite links not supported. Join with your account first, then send @username or ID.")
        if "t.me/" in link:
            link = link.split("t.me/")[1].lstrip("@").split("?")[0]
    # 3) Use Telethon to verify membership
    client = TelegramClient(StringSession(string_session), api_id=api_id, api_hash=api_hash)
    await client.connect()
    try:
        entity = await client.get_entity(link if not link.lstrip("-").isdigit() else int(link))
        in_group = True
        can_post = True
        try:
            perms = await client.get_permissions(entity, "me")
            sm = getattr(perms, "send_messages", None)
            if sm is not None:
                can_post = bool(sm)
        except Exception:
            in_group = False
        if not in_group:
            return VerifyAddOut(ok=False, reason="Your account is not a member of this group. Join it first.")
        title = getattr(entity, "title", None) or getattr(entity, "username", None) or str(entity.id)
        chat_id = int(entity.id)
    finally:
        await client.disconnect()
    # 4) Store
    with session_scope() as db:
        user = crud.get_or_create_user(db, payload.bot_chat_id)
        if crud.count_groups(db, user.id) >= 5:
            raise HTTPException(400, "Group limit reached (5)")
        crud.add_group(db, user.id, chat_id=chat_id, title=title, can_post=bool(can_post))
    return VerifyAddOut(ok=True, chat_id=chat_id, title=title, can_post=bool(can_post))
