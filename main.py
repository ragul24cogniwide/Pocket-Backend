"""
Flick backend: the FastAPI application.

This file only puts the app together (startup / shutdown, middleware, background tasks) and
includes the routers, in this order (the order routes are matched in):

    groups, web_sessions, reactions, archive, calllog   feature modules
    admin_routes      health checks, admin hooks
    media_store       serving photos / voice notes (/uploads/...)
    upload_routes     uploading them
    call_routes       call signaling, ICE servers
    root_routes       the landing page
    auth_routes       sign-in, sign-out, my profile
    status_routes     statuses
    user_routes       profiles, chat list, delta sync, contacts
    message_routes    one-to-one message history / send / edit / delete
    meeting_routes    calendar
    agent_routes      the personal AI agent
    gateway           the WebSocket gateway (/ws/{user_id})

Shared helpers live in core.py; message delivery shared by REST and WebSocket in messaging.py.
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

logging.basicConfig(level=logging.INFO)

import httpx  # noqa: E402
from fastapi import FastAPI, HTTPException  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402

import admin_routes  # noqa: E402
import agent_routes  # noqa: E402
import archive  # noqa: E402
import auth_routes  # noqa: E402
import call_routes  # noqa: E402
import devices  # noqa: E402
import calllog  # noqa: E402
import gateway  # noqa: E402
import groups  # noqa: E402
import media_store  # noqa: E402
import meeting_routes  # noqa: E402
import message_routes  # noqa: E402
import reactions  # noqa: E402
import root_routes  # noqa: E402
import status_routes  # noqa: E402
import upload_routes  # noqa: E402
import user_routes  # noqa: E402
import web_sessions  # noqa: E402
from ai_agent import ensure_personal_agent_user  # noqa: E402
from call_routes import cleanup_stale_call_sessions_loop  # noqa: E402
from connection_manager import manager  # noqa: E402
from core import DOCUMENT_UPLOAD_PATH, MAX_DOCUMENT_REQUEST_BYTES, MAX_REQUEST_BODY_BYTES  # noqa: E402
from database import async_session_factory, init_db  # noqa: E402
from sync_service import prune_expired_change_logs  # noqa: E402

logger = logging.getLogger("Main")

RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "https://pocket-backend-7lmx.onrender.com").strip().rstrip("/")


async def server_keep_alive_loop():
    """Background task that periodically pings the server to prevent Render idle sleep."""
    await asyncio.sleep(5)  # Initial warm-up ping 5s after startup
    while True:
        try:
            target_url = f"{RENDER_EXTERNAL_URL}/api/health"
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(target_url)
                logger.info("[KeepAlive] Render self-ping (20s) status: %s", resp.status_code)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning("[KeepAlive] Notice: %s", e)
        await asyncio.sleep(20)  # Ping every 20 seconds


CHANGE_LOG_PRUNE_INTERVAL_SECONDS = 6 * 3600


async def prune_change_logs_loop() -> None:
    """
    Deletes delta-sync change log entries past their retention (sync_service.CHANGE_LOG_RETENTION_DAYS)
    every 6 hours; the table otherwise grows with every message forever. First run shortly
    after startup. Errors are logged and retried next round.
    """
    await asyncio.sleep(60)
    while True:
        try:
            async with async_session_factory() as db:
                removed = await prune_expired_change_logs(db)
            if removed:
                logger.info("[ChangeLog] Pruned %d expired change log entries", removed)
            async with async_session_factory() as db:
                pruned = await devices.prune_deliveries(db)
            if pruned:
                logger.info("[Devices] Pruned %d old delivery records", pruned)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("[ChangeLog] Pruning failed: %s", e)
        await asyncio.sleep(CHANGE_LOG_PRUNE_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown hooks."""
    logger.info("Initializing database tables...")
    try:
        await init_db()
    except Exception as e:
        logger.error("Error during init_db: %s", e)

    async with async_session_factory() as session:
        try:
            await ensure_personal_agent_user(session)
        except Exception as ag_err:
            logger.warning("Could not auto-provision AI agent: %s", ag_err)
    logger.info("Initializing connection manager & Redis Pub/Sub...")
    await manager.initialize()
    keep_alive_task = asyncio.create_task(server_keep_alive_loop())
    call_cleanup_task = asyncio.create_task(cleanup_stale_call_sessions_loop())
    prune_task = asyncio.create_task(prune_change_logs_loop())
    yield
    keep_alive_task.cancel()
    call_cleanup_task.cancel()
    prune_task.cancel()
    logger.info("Shutting down connection manager...")
    await manager.shutdown()


app = FastAPI(
    title="Flick Real-Time Messaging API",
    description="Production-grade messaging backend using FastAPI, Neon Postgres, Render Redis, and WebSockets.",
    version="1.0.0",
    lifespan=lifespan,
)

class BodySizeLimitMiddleware:
    """
    Rejects request bodies over MAX_REQUEST_BODY_BYTES with 413 before any of it is parsed.
    Checked from Content-Length up front; a body sent without one (chunked) is counted as it
    arrives and refused once it goes over. WebSockets have their own frame limit.
    """

    def __init__(self, app, max_bytes: int, per_path: Optional[dict] = None):
        self.app = app
        self.max_bytes = max_bytes
        self.per_path = per_path or {}  # path -> its own limit (document uploads: 20 MB files)

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        max_bytes = self.per_path.get(scope.get("path"), self.max_bytes)
        for name, value in scope.get("headers") or []:
            if name == b"content-length":
                try:
                    too_big = int(value) > max_bytes
                except ValueError:
                    too_big = False
                if too_big:
                    return await self._reject(send)
                break

        received = 0

        async def limited_receive():
            nonlocal received
            message = await receive()
            if message.get("type") == "http.request":
                received += len(message.get("body") or b"")
                if received > max_bytes:
                    # Raised while the route reads its body: FastAPI turns it into a 413 response
                    raise HTTPException(status_code=413, detail="Request is too large.")
            return message

        return await self.app(scope, limited_receive, send)

    @staticmethod
    async def _reject(send):
        body = json.dumps({"detail": "Request is too large."}).encode()
        await send({
            "type": "http.response.start",
            "status": 413,
            "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())],
        })
        await send({"type": "http.response.body", "body": body})


# Added before CORS so CORS stays the outermost layer and 413 answers still carry CORS headers
app.add_middleware(
    BodySizeLimitMiddleware,
    max_bytes=MAX_REQUEST_BODY_BYTES,
    per_path={DOCUMENT_UPLOAD_PATH: MAX_DOCUMENT_REQUEST_BYTES},
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(groups.router)
app.include_router(web_sessions.router)
app.include_router(reactions.router)
app.include_router(archive.router)
app.include_router(calllog.router)

app.include_router(admin_routes.router)
# Photos / voice notes: stored in the database, served from /uploads/... (see media_store.py)
app.include_router(media_store.router)
app.include_router(upload_routes.router)
app.include_router(call_routes.router)
app.include_router(root_routes.router)
app.include_router(auth_routes.router)
app.include_router(status_routes.router)
app.include_router(user_routes.router)
app.include_router(message_routes.router)
app.include_router(meeting_routes.router)
app.include_router(agent_routes.router)
app.include_router(gateway.router)
# Devices and per-device delivery tracking (watch mode, see devices.py)
app.include_router(devices.router)
