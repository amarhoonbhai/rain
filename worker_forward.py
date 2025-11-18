import os
import asyncio
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone

from pyrogram import Client, filters
from pyrogram.errors import FloodWait, Unauthorized, RPCError
from pyrogram.types import Message

# Ensure .env is loaded even if worker_forward is run standalone
try:
    from core.mongo import _load_dotenv_best_effort  # type: ignore
except Exception:
    _load_dotenv_best_effort = None  # type: ignore

if _load_dotenv_best_effort:
    try:
        _load_dotenv_best_effort()
    except Exception:
        pass

from core.db import (
    init_db, users_with_sessions, sessions_list,
    list_groups, add_group, clear_groups, groups_cap,
    get_interval, set_interval, get_last_sent_at, mark_sent_now,
    inc_sent_ok, set_setting, get_setting,
)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [worker] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("worker")

PARALLEL_USERS = int(os.getenv("PARALLEL_USERS", "3"))
PER_GROUP_DELAY = float(os.getenv("PER_GROUP_DELAY", "30"))
SEND_TIMEOUT = int(os.getenv("SEND_TIMEOUT", "60"))
TICK_INTERVAL = int(os.getenv("TICK_INTERVAL", "15"))

# Per-user runtime state (cached Saved-All list + current index)
STATE: Dict[int, Dict[str, Any]] = {}  # {user_id: {"apps":[Client...], "saved_ids":[int], "idx":int}}


# ---------- Saved-All helpers ----------
async def fetch_saved_ids(app: Client) -> List[int]:
    """Return Saved Messages ids, oldest → newest."""
    ids: List[int] = []
    async for m in app.get_chat_history("me", limit=1000):  # newest → oldest
        if m.text or m.caption or m.media:
            ids.append(m.id)
    ids.reverse()  # oldest → newest
    return ids


def _next_idx(user_id: int, n: int) -> int:
    st = STATE.setdefault(user_id, {})
    i = int(st.get("idx", 0))
    if n <= 0:
        return 0
    i = (i + 1) % n
    st["idx"] = i
    return i


def _cur_idx(user_id: int) -> int:
    return int(STATE.get(user_id, {}).get("idx", 0))


# ---------- command parsing (from any chat, only for .me sender) ----------
HELP_TEXT = (
    "📜 Commands\n"
    ".help — this help\n"
    ".status — show current worker status\n"
    ".addgc <one-or-many> — add targets (@handle, id, or any t.me link). "
    "You can also send a list in the next lines or reply to a message.\n"
    ".gc — list saved targets | .cleargc — clear all\n"
    ".time 30m|45m|60m|120 — set interval (minutes). Send just .time to see current.\n"
    ".adreset — restart Saved-All cycle from first message"
)


def _parse_time(s: str) -> Optional[int]:
    s = s.strip().lower()
    if not s:
        return None
    if s.endswith("m"):
        try:
            return max(1, int(s[:-1]))
        except Exception:
            return None
    try:
        return max(1, int(s))
    except Exception:
        return None


def _split_targets_from_text(body: str) -> List[str]:
    items: List[str] = []
    for raw in (body or "").splitlines():
        raw = raw.strip()
        if not raw:
            continue
        # allow space-separated @handles on same line
        for token in raw.split():
            token = token.strip()
            if not token:
                continue
            items.append(token)
    return items


def _fmt_ts(ts: Optional[int]) -> str:
    if not ts:
        return "never"
    try:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        # you can tweak format if you want local time
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return str(ts)


def register_session_handlers(app: Client, user_id: int) -> None:
    """Register .help/.status/.addgc/.gc/.cleargc/.time/.adreset on this Client."""

    @app.on_message(filters.me & filters.text)
    async def my_text(_, msg: Message) -> None:  # type: ignore[override]
        text = (msg.text or "").strip()
        if not text.startswith("."):
            return

        log.info("[cmd] u%s chat=%s text=%r", user_id, msg.chat.id, text)

        try:
            if text.startswith(".help"):
                await msg.reply_text(HELP_TEXT)
                return

            if text.startswith(".status"):
                targets = list_groups(user_id)
                cap = groups_cap(user_id)
                interval = get_interval(user_id)
                last = get_last_sent_at(user_id)
                st = STATE.get(user_id) or {}
                saved_ids = st.get("saved_ids") or []
                idx = int(st.get("idx", 0))
                status_lines = [
                    f"👤 User: <code>{user_id}</code>",
                    f"⏱ Interval: <b>{interval}</b> minute(s)",
                    f"🎯 Targets: <b>{len(targets)}</b> / {cap}",
                    f"🕒 Last sent: <b>{_fmt_ts(last)}</b>",
                    f"💾 Saved messages cached: <b>{len(saved_ids)}</b>",
                ]
                if saved_ids:
                    status_lines.append(f"➡️ Current index: <b>{idx + 1}</b> / {len(saved_ids)}")
                await msg.reply_text("\n".join(status_lines))
                return

            if text.startswith(".gc"):
                targets = list_groups(user_id)
                cap = groups_cap(user_id)
                if not targets:
                    await msg.reply_text(f"GC list empty (cap {cap}).")
                    return
                out = "\n".join(f"• {x}" for x in targets[:100])
                more = "" if len(targets) <= 100 else f"\n… +{len(targets) - 100} more"
                await msg.reply_text(f"GC ({len(targets)}/{cap})\n{out}{more}")
                return

            if text.startswith(".cleargc"):
                clear_groups(user_id)
                await msg.reply_text("✅ Cleared groups.")
                return

            if text.startswith(".addgc"):
                # Accept:
                #   .addgc @gc1 @gc2
                #   .addgc\n@gc1\n@gc2
                #   .addgc  (and reply to a message with list)
                body_after_cmd = text[len(".addgc"):].strip()
                lines: List[str] = []

                if body_after_cmd:
                    lines = _split_targets_from_text(body_after_cmd)
                else:
                    # Use remaining lines in same message (if multiline)
                    tails = text.splitlines()[1:]
                    if tails:
                        lines = _split_targets_from_text("\n".join(tails))

                if not lines and msg.reply_to_message and (
                    msg.reply_to_message.text or msg.reply_to_message.caption
                ):
                    body = msg.reply_to_message.text or msg.reply_to_message.caption or ""
                    lines = _split_targets_from_text(body)

                if not lines:
                    await msg.reply_text(
                        "❌ Send `.addgc @target1 @target2` or `.addgc` and then a list "
                        "(or reply to a message containing the list)."
                    )
                    return

                added = 0
                cap = groups_cap(user_id)
                for token in lines:
                    added += add_group(user_id, token)
                    if len(list_groups(user_id)) >= cap:
                        break

                await msg.reply_text(
                    f"✅ Added {added}. Now {len(list_groups(user_id))}/{cap} targets."
                )
                return

            if text.startswith(".time"):
                parts = text.split(maxsplit=1)
                if len(parts) == 1:
                    cur = get_interval(user_id)
                    await msg.reply_text(f"⏱ Current interval: {cur} minute(s).")
                    return

                mins = _parse_time(parts[1])
                if not mins:
                    await msg.reply_text("❌ Usage: .time 30m|45m|60m|120")
                    return
                set_interval(user_id, mins)
                await msg.reply_text(f"✅ Interval set to {mins}m.")
                return

            if text.startswith(".adreset"):
                st = STATE.setdefault(user_id, {})
                st["idx"] = 0
                await msg.reply_text("✅ Saved-All cycle reset to first message.")
                return

        except Exception as e:
            log.exception("cmd error u%s: %s", user_id, e)


# ---------- per-user lifecycle ----------
async def build_clients_for_user(uid: int) -> List[Client]:
    apps: List[Client] = []
    for s in sessions_list(uid):
        try:
            c = Client(
                name=f"u{uid}s{s['slot']}",
                api_id=int(s["api_id"]),
                api_hash=str(s["api_hash"]),
                session_string=str(s["session_string"]),
            )
            await c.start()
            register_session_handlers(c, uid)
            apps.append(c)
            log.info("[u%s] started session slot=%s", uid, s["slot"])
        except Unauthorized:
            # ping via setting for main bot to DM later (auto-rehydrate)
            set_setting(
                f"rehydrate:{uid}", int(datetime.now(timezone.utc).timestamp())
            )
            log.warning("[u%s] session unauthorized, flagged for rehydrate", uid)
        except Exception as e:
            log.error("[u%s] start session failed: %s", uid, e)
    return apps


async def ensure_state(uid: int) -> None:
    st = STATE.get(uid)
    if st and st.get("apps"):
        return
    apps = await build_clients_for_user(uid)
    STATE[uid] = {"apps": apps, "idx": 0, "saved_ids": []}


async def refresh_saved(uid: int) -> None:
    st = STATE.get(uid)
    if not st or not st.get("apps"):
        return
    try:
        saved = await fetch_saved_ids(st["apps"][0])
        st["saved_ids"] = saved
        log.info("[u%s] refreshed saved messages: %s items", uid, len(saved))
    except Exception as e:
        log.error("[u%s] fetch_saved_ids error: %s", uid, e)


async def send_copy(app: Client, from_chat: str | int, msg_id: int, to_target: str) -> bool:
    try:
        await app.copy_message(
            chat_id=to_target,
            from_chat_id=from_chat,
            message_id=msg_id,
        )
        return True
    except FloodWait as fw:
        log.warning("FloodWait for %ss when sending to %s", fw.value, to_target)
        await asyncio.sleep(fw.value)
        return False
    except RPCError as e:
        log.warning("RPCError when sending to %s: %s", to_target, e)
        return False
    except Exception as e:
        log.warning("copy fail → %s", e)
        return False


async def run_cycle(uid: int) -> None:
    st = STATE.get(uid)
    if not st or not st.get("apps"):
        return

    apps = st["apps"]
    targets = list_groups(uid)
    if not targets:
        return

    if not st.get("saved_ids"):
        await refresh_saved(uid)
    if not st.get("saved_ids"):
        log.info("[u%s] no saved messages to send", uid)
        return

    idx = _cur_idx(uid)
    msg_id = st["saved_ids"][idx]

    app = apps[0]
    ok_any = False
    for tg in targets:
        try:
            good = await asyncio.wait_for(
                send_copy(app, "me", msg_id, tg), timeout=SEND_TIMEOUT
            )
            if good:
                ok_any = True
                inc_sent_ok(uid, 1)
        except asyncio.TimeoutError:
            log.warning("[u%s] send timeout to %s", uid, tg)
        await asyncio.sleep(PER_GROUP_DELAY)

    if ok_any:
        mark_sent_now(uid)
        _next_idx(uid, len(st["saved_ids"]))


async def user_loop(uid: int) -> None:
    await ensure_state(uid)
    interval = get_interval(uid)
    last = get_last_sent_at(uid)
    now = int(datetime.now(timezone.utc).timestamp())
    if last is None or (now - last) >= interval * 60:
        await run_cycle(uid)


# ---------- main loop ----------
async def main_loop() -> None:
    init_db()
    log.info("worker started")
    while True:
        uids = users_with_sessions()
        if not uids:
            log.debug("no users_with_sessions yet")
        sem = asyncio.Semaphore(PARALLEL_USERS)

        async def run(uid: int) -> None:
            async with sem:
                try:
                    await user_loop(uid)
                except Unauthorized:
                    set_setting(
                        f"rehydrate:{uid}",
                        int(datetime.now(timezone.utc).timestamp(),
                        ),
                    )
                    log.warning("[u%s] Unauthorized in user_loop; flagged for rehydrate", uid)
                except Exception as e:
                    log.error("loop error u%s: %s", uid, e)

        tasks = [asyncio.create_task(run(uid)) for uid in uids]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(TICK_INTERVAL)


async def main() -> None:
    try:
        await main_loop()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    asyncio.run(main())
