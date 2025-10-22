from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from .db import Base, engine, session_scope
from . import crud

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

@app.post("/session/bind")
async def api_session_bind(payload: SessionBindIn):
    from .utils import resolve_nonce, clear_nonce
    from .security import enc

    bot_chat_id = resolve_nonce(payload.nonce)
    if not bot_chat_id:
        raise HTTPException(400, "Invalid or expired nonce")

    with session_scope() as db:
        user = crud.get_or_create_user(db, bot_chat_id)
        user.tg_user_id = payload.tg_user_id
        user.phone_e164 = payload.phone_e164
        if payload.display_name:
            user.display_name = payload.display_name
        enc_blob = enc(payload.string_session)
        crud.create_or_update_session(db, user.id, enc_blob)

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
