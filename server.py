"""FastAPI WebSocket server for the Avaya RCMS ↔ OpenAI Realtime bridge.

Avaya Infinity connects to this server over a secure WebSocket (WSS).
Each incoming connection is handled by an AvayaHandler instance that
bridges audio to the OpenAI Realtime API via the openai-agents SDK.

Deployment (Render):
    Render terminates TLS at its load balancer, so the app runs plain WS
    internally.  Avaya connects to wss://<service>.onrender.com/media-stream.
    No SSL_CERT / SSL_KEY are needed on Render.

Local usage:
    python server.py              # plain WS on port 8000 (reads .env automatically)
    # For WSS locally, set SSL_CERT + SSL_KEY in .env

Environment variables (see .env.example):
    OPENAI_API_KEY          required
    OPENAI_REALTIME_MODEL   optional (default: gpt-4o-realtime-preview)
    PORT                    optional (default: 8000); set automatically by Render
    SSL_CERT                optional — TLS certificate path (local WSS only)
    SSL_KEY                 optional — TLS private key path  (local WSS only)
    ENABLE_AUTH             optional — "true" to require Avaya JWT token
    JWT_SECRET_KEY          required when ENABLE_AUTH=true
"""

import logging
import os
from typing import TYPE_CHECKING

# Load .env in local development; no-op when env vars are already set (e.g. on Render)
try:
    from dotenv import load_dotenv  # type: ignore[import]
    load_dotenv()
except ImportError:
    pass

import jwt
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

if TYPE_CHECKING:
    from .avaya_handler import AvayaHandler
else:
    try:
        from .avaya_handler import AvayaHandler
    except ImportError:
        from avaya_handler import AvayaHandler

log = logging.getLogger("server")

# ─────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────

ENABLE_AUTH = os.getenv("ENABLE_AUTH", "false").lower() == "true"
JWT_SECRET  = os.getenv("JWT_SECRET_KEY", "")

# ─────────────────────────────────────────────────────────────────
# Startup log
# ─────────────────────────────────────────────────────────────────

_model   = os.getenv("OPENAI_REALTIME_MODEL", "gpt-4o-realtime-preview")
_api_key = os.getenv("OPENAI_API_KEY", "")

log.info("═══════════════════════════════════════════")
log.info("  Avaya RCMS → OpenAI Realtime Bridge")
log.info("  model     : %s", _model)
log.info("  api_key   : %s", ("set ✓" if _api_key else "NOT SET ✗"))
log.info("  auth      : %s", ("enabled" if ENABLE_AUTH else "disabled"))
log.info("  endpoint  : ws://0.0.0.0:PORT/media-stream")
log.info("═══════════════════════════════════════════")

# ─────────────────────────────────────────────────────────────────
# Application
# ─────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Avaya RCMS → OpenAI Realtime Bridge",
    description="WebSocket server that bridges Avaya RCMS media streams to OpenAI Realtime API.",
)


@app.get("/")
async def health_check():
    return {"status": "ok", "service": "Avaya RCMS → OpenAI Realtime"}


@app.websocket("/media-stream")
async def media_stream(websocket: WebSocket):
    """Main WebSocket endpoint — Avaya Infinity connects here."""

    # Optional JWT authentication (Avaya sends Bearer token in Authorization header)
    if ENABLE_AUTH:
        auth_header = websocket.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            await websocket.close(code=4401, reason="Missing Bearer token")
            return
        token = auth_header[len("Bearer "):]
        if not _validate_jwt(token):
            await websocket.close(code=4403, reason="Invalid or expired JWT")
            return

    log.info("Incoming WebSocket connection — path=%s", websocket.url.path)
    handler = AvayaHandler(websocket)
    try:
        await handler.start()
        await handler.wait_until_done()
    except WebSocketDisconnect:
        log.info("WebSocket disconnected (clean)")
    except Exception as exc:
        log.error("Unhandled error in media_stream: %s", exc, exc_info=True)
    finally:
        await handler.cleanup()


# ─────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────

def _validate_jwt(token: str) -> bool:
    if not JWT_SECRET:
        return True  # No secret configured — accept all tokens
    try:
        jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        return True
    except jwt.PyJWTError:
        return False


# ─────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    port     = int(os.getenv("PORT", "8000"))
    ssl_cert = os.getenv("SSL_CERT")
    ssl_key  = os.getenv("SSL_KEY")

    ssl_kwargs: dict = {}
    if ssl_cert and ssl_key:
        ssl_kwargs["ssl_certfile"] = ssl_cert
        ssl_kwargs["ssl_keyfile"]  = ssl_key
        print(f"[Server] Starting with TLS (WSS) on port {port}")
    else:
        print(f"[Server] Starting without TLS (WS) on port {port}")
        print("[Server] NOTE: Avaya Infinity requires WSS (TLS). Use a reverse proxy or set SSL_CERT/SSL_KEY.")

    uvicorn.run(app, host="0.0.0.0", port=port, **ssl_kwargs)
