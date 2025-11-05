# run_all.py — one-file launcher: API (FastAPI) + WebApp static + Main Bot + Worker + Login Bot
import asyncio
import os
import sys
import threading
from functools import partial
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler

from dotenv import load_dotenv
from uvicorn import Config, Server

load_dotenv()

API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))
WEBAPP_HOST = os.getenv("WEBAPP_HOST", "0.0.0.0")
WEBAPP_PORT = int(os.getenv("WEBAPP_PORT", "8080"))

def serve_webapp():
    """Serve /webapp as static files (for the Telegram WebApp dashboard)."""
    web_dir = os.path.join(os.path.dirname(__file__), "webapp")
    handler = partial(SimpleHTTPRequestHandler, directory=web_dir)
    srv = ThreadingHTTPServer((WEBAPP_HOST, WEBAPP_PORT), handler)
    print(f"[webapp] Serving {web_dir} at http://{WEBAPP_HOST}:{WEBAPP_PORT}/  (try /index.html)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass

async def serve_api():
    """Run FastAPI (api_server.app) via uvicorn inside this event loop."""
    import api_server  # uses your existing FastAPI app
    cfg = Config(
        app=api_server.app,
        host=API_HOST,
        port=API_PORT,
        loop="asyncio",
        log_level="info",
        reload=False,
    )
    server = Server(cfg)
    print(f"[api] FastAPI on http://{API_HOST}:{API_PORT}/")
    await server.serve()

async def serve_bot():
    """Run main aiogram bot (inline UI + /stats + /top + WebApp button)."""
    import main_bot
    await main_bot._preflight()
    print("[bot] Starting aiogram polling…")
    await main_bot.dp.start_polling(main_bot.bot)

async def serve_worker():
    """Run the forwarder worker (30/45/60 min intervals to added groups)."""
    import worker_forward
    print("[worker] Loop started…")
    await worker_forward.loop_worker()

async def serve_login_bot():
    """
    Run the OTP login wizard bot (if LOGIN_BOT_TOKEN is set).
    This is optional; skip gracefully if token missing.
    """
    if not os.getenv("LOGIN_BOT_TOKEN"):
        print("[login-bot] LOGIN_BOT_TOKEN not set — skipping login bot.")
        return
    try:
        import login_bot
        print("[login-bot] Starting aiogram polling…")
        await login_bot.login_bot_main()
    except Exception as e:
        print(f"[login-bot] Failed to start: {e}")

async def main():
    if not (os.getenv("MAIN_BOT_TOKEN") or os.getenv("ADS_BOT_TOKEN")):
        print("ERROR: set MAIN_BOT_T
