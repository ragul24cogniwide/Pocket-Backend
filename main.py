"""
Main FastAPI application providing REST API endpoints and WebSocket Real-Time Gateway.
Full support for Neon PostgreSQL, Render Redis, Phone Number OTP, and Real-Time Chat.
"""

import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Any, Dict

import hmac
import time
from collections import defaultdict, deque

from fastapi import Depends, FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from sqlalchemy import desc, func, or_, and_, select, update
from sqlalchemy.orm import joinedload
from sqlalchemy.ext.asyncio import AsyncSession

from auth import (
    create_access_token,
    generate_otp_code,
    get_current_user,
    get_current_user_optional,
    same_identity,
    user_from_token,
)
from connection_manager import manager
from database import async_session_factory, get_db, init_db
from models import Message, MessageStatus, OtpRecord, User, Status, Meeting
from notifications import send_push_notification
from schemas import (
    AuthResponse,
    CallAnswerRequest,
    CallEndRequest,
    CallOfferRequest,
    CallRejectRequest,
    ChatContactItem,
    CreateStatusRequest,
    EditMessageRequest,
    SendMessageRequest,
    SendOtpRequest,
    SendOtpResponse,
    SyncContactsRequest,
    SyncContactsResponse,
    UpdateFcmTokenRequest,
    UpdateProfileRequest,
    UserProfileResponse,
    VerifyOtpRequest,
    MeetingCreate,
    MeetingUpdate,
    MeetingResponse,
    CheckConflictRequest,
    AgentChatRequest,
    AgentChatResponse,
    SyncConversationsResponse,
)
from sync_service import (
    get_conversation_sync_page,
    record_conversation_change,
    record_profile_change_for_contacts,
)
from ai_agent import (
    process_personal_agent_message,
    ensure_personal_agent_user,
    check_meeting_conflicts,
    AGENT_USER_ID,
    AGENT_NAME,
    AGENT_AVATAR,
    AGENT_AVATAR_COLOR,
    AGENT_QUOTE,
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Main")

# Returning the OTP in the API response (and accepting a fixed master code) lets anyone log in
# as any phone number. Both are for local testing only and must stay off in production.
DEV_MODE_EXPOSE_OTP = os.getenv("DEV_MODE_EXPOSE_OTP", "false").lower() == "true"
ALLOW_MASTER_OTP = os.getenv("ALLOW_MASTER_OTP", "false").lower() == "true"
MASTER_OTP_CODE = "123456"
OTP_EXPIRY_MINUTES = int(os.getenv("OTP_EXPIRY_MINUTES", "5"))
OTP_RESEND_COOLDOWN_SECONDS = 30
OTP_MAX_SENDS_PER_HOUR = 5
OTP_MAX_VERIFY_ATTEMPTS = 5

# Admin endpoints are disabled unless ADMIN_API_KEY is set; callers send it as X-Admin-Key.
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "").strip()

MAX_UPLOAD_BYTES = 10 * 1024 * 1024
ALLOWED_IMAGE_EXTS = {"jpg", "jpeg", "png", "gif", "webp", "heic"}

if DEV_MODE_EXPOSE_OTP or ALLOW_MASTER_OTP:
    logging.getLogger("Main").warning(
        "INSECURE AUTH: DEV_MODE_EXPOSE_OTP=%s ALLOW_MASTER_OTP=%s. Anyone can log in as any number. "
        "Turn both off and configure SMS delivery before going live.",
        DEV_MODE_EXPOSE_OTP,
        ALLOW_MASTER_OTP,
    )

# Per-phone OTP throttling. In-memory, so limits are per server instance (fine for a single Render instance).
_otp_send_log: dict[str, deque] = defaultdict(deque)
_otp_failed_attempts: dict[str, int] = defaultdict(int)


def require_admin(x_admin_key: Optional[str] = Header(default=None)) -> None:
    if not ADMIN_API_KEY:
        raise HTTPException(status_code=404, detail="Not found")
    if not x_admin_key or not hmac.compare_digest(x_admin_key, ADMIN_API_KEY):
        raise HTTPException(status_code=403, detail="Forbidden")


def normalize_phone(raw: str) -> str:
    phone = "".join(ch for ch in (raw or "").strip() if ch.isdigit() or ch == "+")
    if not phone.startswith("+"):
        phone = "+91" + phone if len(phone) == 10 else "+" + phone
    return phone


async def deliver_otp_sms(phone: str, otp_code: str) -> bool:
    """
    Sends the OTP to the user's phone. Returns True when an SMS provider accepted it.

    TODO: wire up an SMS provider here (Twilio Verify, MSG91, AWS SNS, ...). Until then,
    the only way to read the code is DEV_MODE_EXPOSE_OTP, which is not safe in production.
    """
    return False


async def dispatch_push_to_user(
    sender_id: str,
    receiver_id: str,
    content: str,
    data_payload: Optional[dict] = None,
):
    """Dispatches high-priority push notification to offline receiver with phone tolerance."""
    try:
        async with async_session_factory() as push_db:
            # 1. Look up receiver with active token prioritization
            rec_digits = "".join(filter(str.isdigit, str(receiver_id)))
            rec_last10 = rec_digits[-10:] if len(rec_digits) >= 10 else rec_digits
            r_stmt = (
                select(User)
                .where(
                    or_(
                        User.id == receiver_id,
                        User.phone_number == receiver_id,
                        User.phone_number.endswith(rec_last10) if len(rec_last10) >= 7 else False,
                        User.id.endswith(rec_last10) if len(rec_last10) >= 7 else False,
                    )
                )
                .order_by(desc(User.fcm_token.isnot(None)), desc(User.last_seen))
            )
            r_res = await push_db.execute(r_stmt)
            rec_user = r_res.scalars().first()

            # 2. Look up sender name
            send_digits = "".join(filter(str.isdigit, str(sender_id)))
            send_last10 = send_digits[-10:] if len(send_digits) >= 10 else send_digits
            s_stmt = (
                select(User)
                .where(
                    or_(
                        User.id == sender_id,
                        User.phone_number == sender_id,
                        User.phone_number.endswith(send_last10) if len(send_last10) >= 7 else False,
                        User.id.endswith(send_last10) if len(send_last10) >= 7 else False,
                    )
                )
                .order_by(desc(User.last_seen))
            )
            s_res = await push_db.execute(s_stmt)
            sender_user = s_res.scalars().first()

            if rec_user and rec_user.fcm_token:
                sender_title = (
                    sender_user.username
                    if sender_user
                    else f"User {sender_id[-4:]}" if len(sender_id) >= 4 else "Flick"
                )
                payload_dict = {
                    "sender_id": sender_id,
                    "receiver_id": receiver_id,
                    **(data_payload or {}),
                }
                await send_push_notification(
                    fcm_token=rec_user.fcm_token,
                    sender_name=sender_title,
                    content=content,
                    data_payload=payload_dict,
                )
            else:
                logger.info("[Push] User %s has no active FCM token registered (rec_user=%s)", receiver_id, rec_user is not None)
    except Exception as err:
        logger.warning("Error dispatching offline push to %s: %s", receiver_id, err)


# In-memory + Redis active call sessions registry
# call_id -> { "status": "ringing" | "answered" | "ended" | "rejected", "caller_id": ..., "receiver_id": ..., "created_at": ..., "updated_at": ... }
active_call_sessions: dict[str, dict] = {}
CALL_SESSION_TTL_SECONDS = 3600  # 1 hour TTL


async def save_call_session(call_id: str, session: dict) -> None:
    """Persists call session state to memory cache and distributed Redis cluster."""
    now_ts = datetime.now(timezone.utc).timestamp()
    session["updated_at_ts"] = now_ts
    if "created_at_ts" not in session:
        session["created_at_ts"] = now_ts
    active_call_sessions[call_id] = session
    if manager.redis:
        try:
            await manager.redis.set(f"call_session:{call_id}", json.dumps(session), ex=CALL_SESSION_TTL_SECONDS)
        except Exception as e:
            logger.debug("[CallSession] Redis set failed for %s: %s", call_id, e)


async def get_call_session(call_id: str) -> dict:
    """Retrieves call session state from memory or Redis cache."""
    if call_id in active_call_sessions:
        return active_call_sessions[call_id]
    if manager.redis:
        try:
            raw = await manager.redis.get(f"call_session:{call_id}")
            if raw:
                session = json.loads(raw)
                active_call_sessions[call_id] = session
                return session
        except Exception as e:
            logger.debug("[CallSession] Redis get failed for %s: %s", call_id, e)
    return {}


async def cleanup_stale_call_sessions_loop() -> None:
    """Periodic background task running every 5 minutes to sweep stale in-memory call sessions."""
    while True:
        try:
            await asyncio.sleep(300)
            now_ts = datetime.now(timezone.utc).timestamp()
            stale = [
                cid for cid, s in list(active_call_sessions.items())
                if now_ts - s.get("created_at_ts", s.get("updated_at_ts", now_ts)) > 1800
            ]
            for cid in stale:
                active_call_sessions.pop(cid, None)
            if stale:
                logger.info("[CallSessions] Cleaned up %d stale in-memory call sessions", len(stale))
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning("[CallSessions] Sweeper error: %s", e)


async def broadcast_call_frame(target_id: str, frame_type: str, data: dict, sender_id: Optional[str] = None) -> bool:
    """Delivers call signaling frame across all known aliases of target_id with zero-latency Fast Path, excluding caller."""
    delivered = False
    call_frame = {"type": frame_type, "data": data}

    target_str = str(target_id).strip()
    targets = {target_str}
    digits = "".join(filter(str.isdigit, target_str))
    if len(digits) >= 8:
        last10 = digits[-10:]
        targets.add(last10)
        targets.add(f"+91{last10}")
        targets.add(digits)

    # Exclude sender's own identifiers from targets so caller never receives their own call_offer
    if sender_id:
        sender_str = str(sender_id).strip()
        sender_digits = "".join(filter(str.isdigit, sender_str))
        sender_last10 = sender_digits[-10:] if len(sender_digits) >= 8 else sender_digits
        targets = {
            t for t in targets
            if t != sender_str and (not sender_last10 or not "".join(filter(str.isdigit, t)).endswith(sender_last10))
        }

    # 1. FAST PATH: Deliver immediately to active socket connections (0ms overhead)
    for t in list(targets):
        if await manager.send_personal_message(t, call_frame):
            delivered = True

    # 2. If not yet delivered, check DB for mapped phone/id aliases as fallback
    if not delivered:
        try:
            async with async_session_factory() as call_db:
                stmt = (
                    select(User)
                    .where(
                        or_(
                            User.id == target_str,
                            User.phone_number == target_str,
                            User.phone_number.endswith(digits[-10:]) if len(digits) >= 10 else False,
                            User.id.endswith(digits[-10:]) if len(digits) >= 10 else False,
                        )
                    )
                )
                res = await call_db.execute(stmt)
                user_obj = res.scalars().first()
                if user_obj:
                    extra_targets = set()
                    if user_obj.id:
                        extra_targets.add(str(user_obj.id))
                    if user_obj.phone_number:
                        extra_targets.add(str(user_obj.phone_number))
                    for t in extra_targets:
                        if t not in targets:
                            if await manager.send_personal_message(t, call_frame):
                                delivered = True
        except Exception as e:
            logger.warning("[CallRouting] DB alias lookup fallback error: %s", e)

    return delivered


import httpx

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
    yield
    keep_alive_task.cancel()
    call_cleanup_task.cancel()
    logger.info("Shutting down connection manager...")
    await manager.shutdown()


app = FastAPI(
    title="Flick Real-Time Messaging API",
    description="Production-grade messaging backend using FastAPI, Neon Postgres, Render Redis, and WebSockets.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
@app.get("/api/ping")
async def health_check():
    """Lightweight 0ms health check for keep-alive pings and uptime monitors."""
    return {
        "status": "active",
        "service": "Flick Messaging Backend",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.api_route("/api/admin/init-db", methods=["GET", "POST"], dependencies=[Depends(require_admin)])
async def trigger_init_db():
    """Manual or automated hook to verify and recreate database tables and seeds."""
    logger.info("Manual init-db requested. Rebuilding database schema...")
    await init_db()
    async with async_session_factory() as session:
        try:
            await ensure_personal_agent_user(session)
        except Exception as e:
            logger.warning("Could not provision agent: %s", e)
    return {
        "status": "success",
        "message": "All database tables and seed records verified successfully.",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/agent/status")
async def get_agent_status():
    """Checks whether Google Gemini LLM is active and ready."""
    from ai_agent import get_gemini_api_key, HAS_GENAI
    key = get_gemini_api_key()
    has_key = bool(key and key != "your_gemini_api_key_here")
    masked_key = f"{key[:6]}...{key[-4:]}" if (has_key and len(key) >= 10) else None
    return {
        "gemini_sdk_installed": HAS_GENAI,
        "api_key_configured": has_key,
        "masked_key": masked_key,
        "status": "ready" if (HAS_GENAI and has_key) else "fallback_mode",
    }


@app.post("/api/admin/set-gemini-key", dependencies=[Depends(require_admin)])
async def set_gemini_key_endpoint(payload: dict):
    """Dynamically updates the Gemini API Key on the live running instance."""
    new_key = payload.get("key", "").strip()
    if new_key:
        os.environ["GEMINI_API_KEY"] = new_key
        return {"status": "success", "message": "Gemini API key updated successfully."}
    return {"status": "error", "message": "No key provided."}


from fastapi.staticfiles import StaticFiles
import base64

os.makedirs("uploads/audio", exist_ok=True)
os.makedirs("uploads/images", exist_ok=True)

app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")


# -----------------------------------------------------------------------------
# Media & Audio Upload Endpoint
# -----------------------------------------------------------------------------
@app.post("/api/upload/image")
@app.post("/api/upload/media")
async def upload_image(payload: dict, current_user: User = Depends(get_current_user)):
    """
    Saves image / photo (.jpg/.png) from base64 data to cloud server and returns streaming URL.
    """
    try:
        image_base64 = payload.get("image_base64") or payload.get("base64") or payload.get("media_base64")
        file_ext = str(payload.get("ext") or "jpg").replace(".", "").lower()

        if not image_base64:
            raise HTTPException(status_code=400, detail="Image base64 data required")

        if "," in image_base64:
            # Extract extension if in data URI (e.g. data:image/png;base64,...)
            if "data:image/" in image_base64:
                try:
                    header = image_base64.split(";")[0]
                    file_ext = header.split("/")[1].lower()
                except Exception:
                    pass
            image_base64 = image_base64.split(",", 1)[1]

        if file_ext not in ALLOWED_IMAGE_EXTS:
            raise HTTPException(status_code=400, detail="Unsupported image type")

        image_bytes = base64.b64decode(image_base64)
        if len(image_bytes) > MAX_UPLOAD_BYTES:
            raise HTTPException(status_code=413, detail="Image is too large (max 10 MB)")
        file_id = f"photo_{int(datetime.now(timezone.utc).timestamp())}_{uuid.uuid4().hex[:8]}.{file_ext}"
        file_path = os.path.join("uploads", "images", file_id)

        with open(file_path, "wb") as f:
            f.write(image_bytes)

        media_url = f"/uploads/images/{file_id}"
        logger.info("Saved photo (%d bytes) to %s", len(image_bytes), file_path)

        return {
            "status": "success",
            "media_url": media_url,
            "url": media_url,
            "file_name": file_id,
            "size": len(image_bytes),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Image upload failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Image upload failed: {str(e)}")


@app.post("/api/upload/audio")
async def upload_audio(payload: dict, current_user: User = Depends(get_current_user)):
    """
    Saves recorded voice audio (.m4a AAC) to cloud server and returns streaming URL.
    """
    try:
        audio_base64 = payload.get("audio_base64") or payload.get("base64")
        duration = payload.get("duration", 0)

        if not audio_base64:
            raise HTTPException(status_code=400, detail="Audio base64 data required")

        if "," in audio_base64:
            audio_base64 = audio_base64.split(",", 1)[1]

        audio_bytes = base64.b64decode(audio_base64)
        if len(audio_bytes) > MAX_UPLOAD_BYTES:
            raise HTTPException(status_code=413, detail="Voice note is too large (max 10 MB)")
        file_id = f"voice_{int(datetime.now(timezone.utc).timestamp())}_{uuid.uuid4().hex[:8]}.m4a"
        file_path = os.path.join("uploads", "audio", file_id)

        with open(file_path, "wb") as f:
            f.write(audio_bytes)

        # Public relative or absolute path
        media_url = f"/uploads/audio/{file_id}"
        logger.info("Saved voice note (%d bytes, %ds) to %s", len(audio_bytes), duration, file_path)

        return {
            "status": "success",
            "media_url": media_url,
            "url": media_url,
            "duration": duration,
            "file_name": file_id,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Audio upload failed: %s", e)
        raise HTTPException(status_code=500, detail="Audio upload failed")


# -----------------------------------------------------------------------------
# Voice Call Signaling Endpoints (Dual-Channel Handshake & REST Fallback)
# -----------------------------------------------------------------------------
@app.post("/api/call/offer")
async def call_offer_endpoint(payload: CallOfferRequest, current_user: User = Depends(get_current_user)):
    """Initiates an outgoing call offer, updates active session, sends push notification, and broadcasts over WS."""
    now_iso = datetime.now(timezone.utc).isoformat()
    # The caller is always the authenticated user, whatever the body claims
    payload.caller_id = current_user.id

    caller_name = payload.caller_name
    caller_avatar = payload.caller_avatar
    caller_avatar_url = payload.caller_avatar_url
    caller_color = payload.caller_color

    # Enrich caller profile details from DB if missing or default
    if not caller_avatar_url or not caller_name or caller_name in ("Pocket User", "Flick User"):
        try:
            async with async_session_factory() as db:
                c_stmt = select(User).where(or_(User.id == str(payload.caller_id), User.phone_number == str(payload.caller_id)))
                c_res = await db.execute(c_stmt)
                caller_user = c_res.scalars().first()
                if caller_user:
                    if caller_user.username:
                        caller_name = caller_user.username
                    if caller_user.avatar_url:
                        caller_avatar_url = caller_user.avatar_url
                    if caller_user.avatar_color:
                        caller_color = caller_user.avatar_color
                    if caller_user.username and len(caller_user.username) >= 2:
                        caller_avatar = caller_user.username[:2].upper()
        except Exception as e:
            logger.warning("[CallOffer] Caller enrichment error: %s", e)

    await save_call_session(payload.call_id, {
        "call_id": payload.call_id,
        "caller_id": payload.caller_id,
        "receiver_id": payload.receiver_id,
        "caller_name": caller_name or "Flick User",
        "caller_avatar": caller_avatar or "FK",
        "caller_color": caller_color or "#FFB800",
        "caller_avatar_url": caller_avatar_url,
        "status": "ringing",
        "created_at": now_iso,
        "updated_at": now_iso,
    })

    call_data = {
        "call_id": payload.call_id,
        "caller_id": payload.caller_id,
        "receiver_id": payload.receiver_id,
        "sender_id": payload.caller_id,
        "caller_name": caller_name or "Flick User",
        "caller_avatar": caller_avatar or "FK",
        "caller_avatar_url": caller_avatar_url,
        "caller_color": caller_color or "#FFB800",
    }
    delivered = await broadcast_call_frame(payload.receiver_id, "call_offer", call_data)

    # Dispatch high-priority FCM wake-up call push
    caller_title = caller_name or f"User {str(payload.caller_id)[-4:]}"
    asyncio.create_task(
        dispatch_push_to_user(
            sender_id=payload.caller_id,
            receiver_id=payload.receiver_id,
            content=f"📞 Incoming voice call from {caller_title}...",
            data_payload={
                "type": "incoming_call",
                "caller_id": str(payload.caller_id),
                "caller_name": caller_title,
                "caller_avatar": payload.caller_avatar or "PK",
                "caller_color": payload.caller_color or "#FFB800",
                "caller_avatar_url": payload.caller_avatar_url or "",
                "call_id": payload.call_id,
            },
        )
    )

    return {"status": "ringing", "call_id": payload.call_id, "delivered": delivered}


@app.post("/api/call/answer")
async def call_answer_endpoint(payload: CallAnswerRequest, current_user: User = Depends(get_current_user)):
    """Answers an incoming call, sets session to 'answered', notifies caller immediately over WS and push."""
    now_iso = datetime.now(timezone.utc).isoformat()
    payload.receiver_id = current_user.id
    session = await get_call_session(payload.call_id)
    session.update({
        "call_id": payload.call_id,
        "caller_id": payload.caller_id,
        "receiver_id": payload.receiver_id,
        "status": "answered",
        "answered_at": now_iso,
        "updated_at": now_iso,
    })
    await save_call_session(payload.call_id, session)

    call_data = {
        "call_id": payload.call_id,
        "caller_id": payload.caller_id,
        "receiver_id": payload.caller_id,
        "sender_id": payload.receiver_id,
        "sdp": payload.sdp,
    }
    delivered = await broadcast_call_frame(payload.caller_id, "call_answer", call_data)
    logger.info("[VoiceCall REST] Call answered: %s (caller: %s, delivered: %s)", payload.call_id, payload.caller_id, delivered)

    # Wake-up / answer push notification to caller
    asyncio.create_task(
        dispatch_push_to_user(
            sender_id=payload.receiver_id,
            receiver_id=payload.caller_id,
            content="Call Answered",
            data_payload={
                "type": "call_answered",
                "call_id": payload.call_id,
                "receiver_id": str(payload.receiver_id),
            },
        )
    )

    return {"status": "answered", "call_id": payload.call_id, "delivered": delivered}


def ensure_call_participant(user: User, *ids: Optional[str]) -> None:
    if not any(same_identity(user.id, i) or same_identity(user.phone_number, i) for i in ids):
        raise HTTPException(status_code=403, detail="You are not part of this call")


@app.post("/api/call/reject")
async def call_reject_endpoint(payload: CallRejectRequest, current_user: User = Depends(get_current_user)):
    """Declines an incoming call and terminates for both sides."""
    now_iso = datetime.now(timezone.utc).isoformat()
    ensure_call_participant(current_user, payload.caller_id, payload.receiver_id)
    session = await get_call_session(payload.call_id)
    session.update({
        "call_id": payload.call_id,
        "status": "rejected",
        "reason": payload.reason or "declined",
        "updated_at": now_iso,
    })
    await save_call_session(payload.call_id, session)

    target = payload.caller_id or payload.receiver_id or payload.other_user_id
    call_data = {
        "call_id": payload.call_id,
        "caller_id": payload.caller_id,
        "receiver_id": payload.receiver_id,
        "reason": payload.reason or "declined",
    }
    if target:
        await broadcast_call_frame(target, "call_reject", call_data)
        asyncio.create_task(
            dispatch_push_to_user(
                sender_id=str(payload.receiver_id or "system"),
                receiver_id=str(target),
                content="Call Declined",
                data_payload={
                    "type": "call_rejected",
                    "call_id": payload.call_id,
                    "reason": payload.reason or "declined",
                },
            )
        )

    return {"status": "rejected", "call_id": payload.call_id}


@app.post("/api/call/end")
async def call_end_endpoint(payload: CallEndRequest, current_user: User = Depends(get_current_user)):
    """Ends an ongoing call and terminates for both sides immediately."""
    now_iso = datetime.now(timezone.utc).isoformat()
    ensure_call_participant(current_user, payload.caller_id, payload.receiver_id)
    session = await get_call_session(payload.call_id)
    session.update({
        "call_id": payload.call_id,
        "status": "ended",
        "duration": payload.duration or 0,
        "updated_at": now_iso,
    })
    await save_call_session(payload.call_id, session)

    targets = set()
    if payload.caller_id:
        targets.add(payload.caller_id)
    if payload.receiver_id:
        targets.add(payload.receiver_id)
    if payload.other_user_id:
        targets.add(payload.other_user_id)

    call_data = {
        "call_id": payload.call_id,
        "caller_id": payload.caller_id,
        "receiver_id": payload.receiver_id,
        "duration": payload.duration or 0,
    }
    for t in targets:
        await broadcast_call_frame(t, "call_end", call_data)
        asyncio.create_task(
            dispatch_push_to_user(
                sender_id="system",
                receiver_id=str(t),
                content="Call Ended",
                data_payload={
                    "type": "call_ended",
                    "call_id": payload.call_id,
                },
            )
        )

    return {"status": "ended", "call_id": payload.call_id}


@app.get("/api/call/status/{call_id}")
async def get_call_status(call_id: str, current_user: User = Depends(get_current_user)):
    """Returns active call status ('ringing', 'answered', 'ended', 'rejected', 'unknown')."""
    session = await get_call_session(call_id)
    if not session:
        return {"call_id": call_id, "status": "unknown"}
    return {
        "call_id": call_id,
        "status": session.get("status", "unknown"),
        "caller_id": session.get("caller_id"),
        "receiver_id": session.get("receiver_id"),
        "created_at": session.get("created_at"),
        "answered_at": session.get("answered_at"),
    }


# -----------------------------------------------------------------------------
# Web & Testbed Endpoint
# -----------------------------------------------------------------------------
@app.get("/")
async def root():
    """Serves the test interface for dual-user simulation."""
    if os.path.exists("test_client.html"):
        return FileResponse("test_client.html")
    elif os.path.exists("index.html"):
        return FileResponse("index.html")
    return {"message": "Flick Real-Time Messaging API is active"}


# -----------------------------------------------------------------------------
# Authentication & OTP Endpoints
# -----------------------------------------------------------------------------
@app.post("/api/auth/send-otp", response_model=SendOtpResponse)
async def send_otp(payload: SendOtpRequest, db: AsyncSession = Depends(get_db)):
    """
    Generates a 6-digit OTP for the provided phone number.
    In testing/dev mode, returns the OTP in the response for easy instant login.
    """
    phone = normalize_phone(payload.phone_number)
    digits = phone.lstrip("+")
    if not digits.isdigit() or not (8 <= len(digits) <= 15):
        raise HTTPException(status_code=400, detail="Enter a valid phone number.")

    # Throttle: one code per cooldown window and a cap per hour, per number
    now_ts = time.monotonic()
    sends = _otp_send_log[phone]
    while sends and now_ts - sends[0] > 3600:
        sends.popleft()
    if sends and now_ts - sends[-1] < OTP_RESEND_COOLDOWN_SECONDS:
        wait = int(OTP_RESEND_COOLDOWN_SECONDS - (now_ts - sends[-1])) + 1
        raise HTTPException(status_code=429, detail=f"Please wait {wait}s before requesting another code.")
    if len(sends) >= OTP_MAX_SENDS_PER_HOUR:
        raise HTTPException(status_code=429, detail="Too many codes requested. Try again in an hour.")

    otp_code = generate_otp_code(6)
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=OTP_EXPIRY_MINUTES)

    # Invalidate previous unused OTPs for this number
    await db.execute(
        update(OtpRecord)
        .where(OtpRecord.phone_number == phone, OtpRecord.is_used == False)
        .values(is_used=True)
    )

    otp_record = OtpRecord(
        phone_number=phone,
        otp_code=otp_code,
        expires_at=expires_at,
        is_used=False,
    )
    db.add(otp_record)
    await db.commit()

    sends.append(now_ts)
    _otp_failed_attempts.pop(phone, None)

    sms_sent = await deliver_otp_sms(phone, otp_code)
    if not sms_sent and not DEV_MODE_EXPOSE_OTP:
        logger.error("OTP for %s could not be delivered: no SMS provider configured.", phone)
        raise HTTPException(
            status_code=503,
            detail="We couldn't send a code right now. Please try again later.",
        )

    if DEV_MODE_EXPOSE_OTP:
        logger.info("Generated OTP for %s: %s (dev mode, expires in %d mins)", phone, otp_code, OTP_EXPIRY_MINUTES)
    else:
        logger.info("Sent OTP to %s (expires in %d mins)", phone, OTP_EXPIRY_MINUTES)

    return SendOtpResponse(
        message="OTP sent successfully.",
        phone_number=phone,
        dev_otp=otp_code if DEV_MODE_EXPOSE_OTP else None,
        expires_in_seconds=OTP_EXPIRY_MINUTES * 60,
    )


@app.post("/api/auth/verify-otp", response_model=AuthResponse)
async def verify_otp(payload: VerifyOtpRequest, db: AsyncSession = Depends(get_db)):
    """
    Verifies the OTP code for the phone number.
    If valid, logs in the existing user or creates a new user, returning a JWT token.
    """
    phone = normalize_phone(payload.phone_number)
    code = (payload.otp_code or "").strip()

    if _otp_failed_attempts[phone] >= OTP_MAX_VERIFY_ATTEMPTS:
        raise HTTPException(status_code=429, detail="Too many incorrect attempts. Request a new code.")

    now = datetime.now(timezone.utc)

    # Check OTP record
    stmt = (
        select(OtpRecord)
        .where(
            OtpRecord.phone_number == phone,
            OtpRecord.otp_code == code,
            OtpRecord.is_used == False,
            OtpRecord.expires_at > now,
        )
        .order_by(desc(OtpRecord.created_at))
    )
    result = await db.execute(stmt)
    otp_record = result.scalars().first()

    if not otp_record and not (ALLOW_MASTER_OTP and code == MASTER_OTP_CODE):
        _otp_failed_attempts[phone] += 1
        if _otp_failed_attempts[phone] >= OTP_MAX_VERIFY_ATTEMPTS:
            # Burn any outstanding code so it can't be brute-forced further
            await db.execute(
                update(OtpRecord)
                .where(OtpRecord.phone_number == phone, OtpRecord.is_used == False)
                .values(is_used=True)
            )
            await db.commit()
            raise HTTPException(status_code=429, detail="Too many incorrect attempts. Request a new code.")
        raise HTTPException(status_code=400, detail="Invalid or expired OTP code.")

    _otp_failed_attempts.pop(phone, None)
    if otp_record:
        otp_record.is_used = True
        await db.commit()

    # Find or Create User
    user_stmt = select(User).where(or_(User.id == phone, User.phone_number == phone))
    user_res = await db.execute(user_stmt)
    user = user_res.scalars().first()
    is_new = user is None

    if not user:
        colors = ["#FFB800", "#10B981", "#3B82F6", "#8B5CF6", "#EC4899", "#F97316"]
        avatar_color = colors[len(phone) % len(colors)]
        username = payload.username or f"User {phone[-4:]}"

        user = User(
            id=phone,  # Use phone as unique ID for straightforward routing
            phone_number=phone,
            username=username,
            avatar_color=avatar_color,
            quote="Hey there! I am using Flick.",
            is_online=True,
            last_seen=now,
        )
        db.add(user)
        await db.commit()
        await db.refresh(user)

    token = create_access_token(user_id=user.id, phone_number=user.phone_number)

    return AuthResponse(
        access_token=token,
        token_type="bearer",
        user=user.to_dict(),
        is_new_user=is_new,
    )


@app.post("/api/auth/logout")
async def logout(current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """Detaches this device's push token so a logged-out phone stops receiving the user's notifications."""
    current_user.fcm_token = None
    current_user.is_online = False
    current_user.last_seen = datetime.now(timezone.utc)
    await db.commit()
    return {"message": "Logged out"}


@app.delete("/api/auth/me")
async def delete_my_account(current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """Permanently deletes the authenticated account and everything it owns."""
    from sqlalchemy import delete as sql_delete
    from models import ConversationChangeLog, UserSyncSequence

    me = current_user.id
    peer_rows = await db.execute(
        select(Message.sender_id, Message.receiver_id).where(
            or_(Message.sender_id == me, Message.receiver_id == me)
        )
    )
    peers = {p for row in peer_rows.all() for p in row if p and p != me}

    await db.execute(sql_delete(Message).where(or_(Message.sender_id == me, Message.receiver_id == me)))
    await db.execute(sql_delete(Status).where(Status.user_id == me))
    await db.execute(sql_delete(Meeting).where(Meeting.user_id == me))
    await db.execute(sql_delete(OtpRecord).where(OtpRecord.phone_number == current_user.phone_number))
    await db.execute(sql_delete(ConversationChangeLog).where(ConversationChangeLog.user_id == me))
    await db.execute(sql_delete(UserSyncSequence).where(UserSyncSequence.user_id == me))

    # Tell everyone this user chatted with to drop the conversation on their next sync
    for peer in peers:
        try:
            await record_conversation_change(db, user_id=peer, peer_id=me, action="deleted")
        except Exception as sync_err:
            logger.warning("Failed to record account deletion for %s: %s", peer, sync_err)

    await db.delete(current_user)
    await db.commit()
    logger.info("Deleted account %s", me)
    return {"message": "Account deleted"}


@app.get("/api/auth/me", response_model=UserProfileResponse)
@app.get("/api/users/profile/me", response_model=UserProfileResponse)
@app.get("/api/users/me", response_model=UserProfileResponse)
async def get_my_profile(current_user: User = Depends(get_current_user)):
    """Returns the authenticated user's profile to maintain login state."""
    return UserProfileResponse(
        id=current_user.id,
        phone_number=current_user.phone_number,
        username=current_user.username,
        avatar_color=current_user.avatar_color,
        avatar_url=current_user.avatar_url,
        quote=current_user.quote or "Hey there! I am using Flick.",
        is_online=current_user.is_online,
        last_seen=current_user.last_seen.isoformat() if current_user.last_seen else None,
    )


@app.put("/api/users/profile", response_model=UserProfileResponse)
@app.patch("/api/users/profile", response_model=UserProfileResponse)
@app.post("/api/users/profile", response_model=UserProfileResponse)
@app.put("/api/profile", response_model=UserProfileResponse)
@app.patch("/api/profile", response_model=UserProfileResponse)
@app.post("/api/profile", response_model=UserProfileResponse)
async def update_user_profile(
    payload: UpdateProfileRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Updates the authenticated user's profile (username, avatar_url, avatar_color, quote)."""
    try:
        # user_id / phone_number in the body are ignored: you can only edit your own profile
        target_user = current_user

        if payload.username is not None and payload.username.strip():
            target_user.username = payload.username.strip()
        if payload.avatar_url is not None:
            target_user.avatar_url = payload.avatar_url.strip() if payload.avatar_url else None
        if payload.avatar_color is not None and payload.avatar_color.strip():
            target_user.avatar_color = payload.avatar_color.strip()
        if payload.quote is not None:
            target_user.quote = payload.quote.strip() if payload.quote else "Hey there! I am using Flick."

        # Propagate profile updates to conversation changelog for delta sync
        try:
            await record_profile_change_for_contacts(db, target_user)
        except Exception as sync_err:
            logger.warning("Failed to record profile change for sync: %s", sync_err)

        await db.commit()
        await db.refresh(target_user)

        return UserProfileResponse(
            id=target_user.id,
            phone_number=target_user.phone_number,
            username=target_user.username,
            avatar_color=target_user.avatar_color,
            avatar_url=target_user.avatar_url,
            quote=target_user.quote or "Hey there! I am using Flick.",
            is_online=target_user.is_online,
            last_seen=target_user.last_seen.isoformat() if target_user.last_seen else None,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error updating user profile: %s", e, exc_info=True)
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Database update failed: {str(e)}")


@app.post("/api/users/fcm-token")
async def update_fcm_token(
    payload: UpdateFcmTokenRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Registers this device's FCM push token for the authenticated user."""
    # A device token belongs to one account at a time: detach it from whoever had it before
    await db.execute(
        update(User)
        .where(User.fcm_token == payload.fcm_token, User.id != current_user.id)
        .values(fcm_token=None)
    )
    current_user.fcm_token = payload.fcm_token
    await db.commit()
    logger.info("Updated FCM token for user %s", current_user.id)
    return {"message": "FCM token updated successfully"}



# -----------------------------------------------------------------------------
# Status & Stories Endpoints (24-Hour Ephemeral Status)
# -----------------------------------------------------------------------------
@app.post("/api/statuses")
async def create_status(
    payload: CreateStatusRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Creates a new 24-hour status story in PostgreSQL and broadcasts to peers."""
    now = datetime.now(timezone.utc)
    expires = now + timedelta(hours=24)
    new_status = Status(
        id=str(uuid.uuid4()),
        user_id=current_user.id,
        type=payload.type,
        media_url=payload.media_url,
        caption=payload.caption,
        bg_gradient=payload.bg_gradient or "#1E293B",
        viewers="[]",
        created_at=now,
        expires_at=expires,
    )
    db.add(new_status)
    await db.commit()
    new_status.user = current_user
    st_dict = new_status.to_dict()

    # Real-time WebSocket broadcast to all connected contacts
    asyncio.create_task(
        manager.broadcast({
            "type": "status_update",
            "data": st_dict,
        })
    )
    return st_dict


@app.get("/api/statuses/feed")
async def get_status_feed(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Returns active 24h statuses with eager-loaded user profiles from PostgreSQL."""
    now = datetime.now(timezone.utc)
    stmt = (
        select(Status)
        .options(joinedload(Status.user))
        .where(Status.expires_at > now)
        .order_by(Status.created_at.desc())
    )
    result = await db.execute(stmt)
    all_statuses = result.scalars().all()

    my_statuses = []
    contact_dict = {}

    for s in all_statuses:
        st_dict = s.to_dict()
        if s.user_id == current_user.id or s.user_id == current_user.phone_number:
            my_statuses.append(st_dict)
        else:
            cid = s.user_id
            if cid not in contact_dict:
                user_obj = s.user
                contact_dict[cid] = {
                    "contactId": cid,
                    "phone_number": user_obj.phone_number if user_obj else cid,
                    "userName": st_dict.get("userName") or (user_obj.username if user_obj else "Flick User"),
                    "avatar": st_dict.get("avatar") or "FK",
                    "avatarColor": st_dict.get("avatarColor") or "#FFB800",
                    "avatarUrl": st_dict.get("avatarUrl"),
                    "latestTime": st_dict.get("time") or "Just now",
                    "stories": [],
                }
            contact_dict[cid]["stories"].append(st_dict)

    return {
        "my_statuses": my_statuses,
        "contact_statuses": list(contact_dict.values()),
    }


@app.post("/api/statuses/{status_id}/view")
async def mark_status_viewed(
    status_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Records that current user viewed the status."""
    stmt = select(Status).where(Status.id == status_id)
    result = await db.execute(stmt)
    st = result.scalar_one_or_none()
    if not st:
        raise HTTPException(status_code=404, detail="Status not found")

    viewers = []
    if st.viewers:
        try:
            viewers = json.loads(st.viewers)
        except Exception:
            viewers = []

    if current_user.id not in viewers:
        viewers.append(current_user.id)
        st.viewers = json.dumps(viewers)
        await db.commit()

    return {"message": "Status view recorded", "viewsCount": len(viewers)}


@app.delete("/api/statuses/{status_id}")
async def delete_status(
    status_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Deletes a status posted by current user."""
    stmt = select(Status).where(Status.id == status_id, Status.user_id == current_user.id)
    result = await db.execute(stmt)
    st = result.scalar_one_or_none()
    if not st:
        raise HTTPException(status_code=404, detail="Status not found or unauthorized")

    await db.delete(st)
    await db.commit()
    return {"message": "Status deleted successfully"}


# -----------------------------------------------------------------------------
# User Discovery & Contacts
# -----------------------------------------------------------------------------
@app.get("/api/users/profile/{user_id}")
@app.get("/api/users/{user_id}")
async def get_user_public_profile(
    user_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Returns a user's public profile information (username, avatar, avatar_color, online status)."""
    uid_str = str(user_id).strip()
    digits = "".join(filter(str.isdigit, uid_str))

    stmt = (
        select(User)
        .where(
            or_(
                User.id == uid_str,
                User.phone_number == uid_str,
                User.phone_number.endswith(digits[-10:]) if len(digits) >= 10 else False,
                User.id.endswith(digits[-10:]) if len(digits) >= 10 else False,
            )
        )
    )
    res = await db.execute(stmt)
    user = res.scalars().first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    is_online = await manager.is_user_online(user.id, user.phone_number)
    return {
        "id": user.id,
        "phone_number": user.phone_number,
        "username": user.username,
        "avatar_url": user.avatar_url,
        "avatar_color": user.avatar_color or "#FFB800",
        "avatar": user.username[:2].upper() if user.username and len(user.username) >= 2 else "FK",
        "quote": user.quote or "Hey there! I am using Flick.",
        "is_online": is_online,
        "last_seen": user.last_seen.isoformat() if user.last_seen else None,
    }


async def resolve_user_context(user_opt: Optional[User], db: AsyncSession) -> User:
    """Returns the authenticated user. Unauthenticated callers are rejected rather than mapped to a shared demo account."""
    if user_opt:
        return user_opt
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired session. Please log in again.",
        headers={"WWW-Authenticate": "Bearer"},
    )


@app.get("/api/users", response_model=List[ChatContactItem])
async def list_available_contacts(
    user_opt: Optional[User] = Depends(get_current_user_optional),
    db: AsyncSession = Depends(get_db),
):
    """Lists registered users excluding current user, with real-time online status."""
    current_user = await resolve_user_context(user_opt, db)
    await ensure_personal_agent_user(db)
    stmt = select(User).where(User.id != current_user.id).order_by(User.username.asc())
    result = await db.execute(stmt)
    users = result.scalars().all()

    contacts = []
    seen_identifiers = set()

    # Pre-calculate current_user identifiers
    cur_ids = {current_user.id, current_user.phone_number}
    cur_digits = "".join(filter(str.isdigit, str(current_user.id)))
    cur_last10 = cur_digits[-10:] if len(cur_digits) >= 7 else cur_digits
    if cur_last10:
        cur_ids.add(cur_last10)
        cur_ids.add(f"+91{cur_last10}")
    cur_ids = {str(x) for x in cur_ids if x}

    for u in users:
        # Avoid duplicate contacts pointing to same user
        norm_key = u.username.lower().strip()
        if norm_key in seen_identifiers:
            continue
        seen_identifiers.add(norm_key)

        is_online = await manager.is_user_online(u.id, u.phone_number)

        # Collect target user identifier aliases
        u_ids = {u.id, u.phone_number}
        u_digits = "".join(filter(str.isdigit, str(u.id)))
        u_last10 = u_digits[-10:] if len(u_digits) >= 7 else u_digits
        if u_last10:
            u_ids.add(u_last10)
            u_ids.add(f"+91{u_last10}")
        u_ids = {str(x) for x in u_ids if x}

        # Get latest message between current_user and u
        msg_stmt = (
            select(Message)
            .where(
                or_(
                    Message.sender_id.in_(cur_ids) & Message.receiver_id.in_(u_ids),
                    Message.sender_id.in_(u_ids) & Message.receiver_id.in_(cur_ids),
                    (
                        Message.sender_id.endswith(cur_last10) & Message.receiver_id.endswith(u_last10)
                        if cur_last10 and u_last10 else False
                    ),
                    (
                        Message.sender_id.endswith(u_last10) & Message.receiver_id.endswith(cur_last10)
                        if cur_last10 and u_last10 else False
                    ),
                )
            )
            .order_by(desc(Message.timestamp))
            .limit(1)
        )
        msg_res = await db.execute(msg_stmt)
        latest_msg = msg_res.scalars().first()

        # Count unread messages from u to current_user
        unread_stmt = select(func.count(Message.id)).where(
            Message.sender_id.in_(u_ids),
            Message.receiver_id.in_(cur_ids),
            Message.status != "READ",
        )
        unread_res = await db.execute(unread_stmt)
        unread_count = unread_res.scalar_one() or 0

        initials = (
            "".join([part[0] for part in u.username.split()[:2]]).upper()
            if u.username
            else "PK"
        )

        contacts.append(
            (
                latest_msg.timestamp.timestamp() if latest_msg and latest_msg.timestamp else 0.0,
                ChatContactItem(
                    id=u.id,
                    phone_number=u.phone_number,
                    name=u.username,
                    avatar=initials,
                    avatar_color=u.avatar_color,
                    avatar_url=u.avatar_url,
                    quote=u.quote or "Hey there! I am using Flick.",
                    online=is_online,
                    last_message=latest_msg.content if latest_msg else (u.quote or "Tap to start conversation"),
                    time=(
                        latest_msg.timestamp.strftime("%I:%M %p")
                        if latest_msg and latest_msg.timestamp
                        else ""
                    ),
                    unread=unread_count,
                    last_seen=u.last_seen.isoformat() if u.last_seen else None,
                )
            )
        )

    # Sort contacts: active conversations with recent messages first, then remaining contacts
    contacts.sort(key=lambda x: (x[0], x[1].name), reverse=True)
    normal_contacts = [c[1] for c in contacts if c[1].id != AGENT_USER_ID]

    # Prepend User's Personal AI Agent at the very top of conversations
    agent_msg_stmt = (
        select(Message)
        .where(
            or_(
                and_(Message.sender_id == current_user.id, Message.receiver_id == AGENT_USER_ID),
                and_(Message.sender_id == AGENT_USER_ID, Message.receiver_id == current_user.id),
            )
        )
        .order_by(desc(Message.timestamp))
        .limit(1)
    )
    agent_msg_res = await db.execute(agent_msg_stmt)
    latest_agent_msg = agent_msg_res.scalars().first()

    agent_item = ChatContactItem(
        id=AGENT_USER_ID,
        phone_number="+00000000000",
        name=AGENT_NAME,
        avatar=AGENT_AVATAR,
        avatar_color=AGENT_AVATAR_COLOR,
        avatar_url=None,
        quote=AGENT_QUOTE,
        online=True,
        last_message=latest_agent_msg.content if latest_agent_msg else "Your personal AI agent is ready. Ask me anything!",
        time=(
            latest_agent_msg.timestamp.strftime("%I:%M %p")
            if latest_agent_msg and latest_agent_msg.timestamp
            else ""
        ),
        unread=0,
        last_seen=datetime.now(timezone.utc).isoformat(),
    )

    return [agent_item] + normal_contacts


# -----------------------------------------------------------------------------
# Conversation Delta Sync
# -----------------------------------------------------------------------------
@app.get("/sync/conversations", response_model=SyncConversationsResponse)
@app.get("/api/sync/conversations", response_model=SyncConversationsResponse)
async def sync_conversations(
    cursor: Optional[str] = None,
    limit: int = 50,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Delta sync endpoint returning only created, updated, and deleted conversation
    records since the provided opaque cursor.
    """
    res = await get_conversation_sync_page(db, current_user, cursor_str=cursor, limit=limit)
    return SyncConversationsResponse(**res)


# -----------------------------------------------------------------------------
# Contact Sync (Matches Phone Contacts with Registered Users)
# -----------------------------------------------------------------------------
@app.post("/api/contacts/sync", response_model=SyncContactsResponse)
async def sync_device_contacts(
    payload: SyncContactsRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Matches device phone contacts against registered Flick users.
    Returns only users who have registered and installed the app.
    """
    raw_numbers = payload.phone_numbers
    cleaned_numbers = set()

    for num in raw_numbers:
        clean = "".join(filter(lambda c: c.isdigit() or c == '+', num.strip()))
        if clean:
            if not clean.startswith("+") and len(clean) == 10:
                cleaned_numbers.add("+91" + clean)
            cleaned_numbers.add(clean)

    if not cleaned_numbers:
        return SyncContactsResponse(registered_users=[], total_synced=0)

    stmt = (
        select(User)
        .where(
            User.id != current_user.id,
            or_(
                User.phone_number.in_(cleaned_numbers),
                User.id.in_(cleaned_numbers),
            )
        )
        .order_by(User.username.asc())
    )
    result = await db.execute(stmt)
    users = result.scalars().all()

    registered_contacts = []
    seen = set()
    for u in users:
        if u.id in seen:
            continue
        seen.add(u.id)

        is_online = await manager.is_user_online(u.id)
        initials = (
            "".join([part[0] for part in u.username.split()[:2]]).upper()
            if u.username
            else "FK"
        )
        registered_contacts.append(
            ChatContactItem(
                id=u.id,
                phone_number=u.phone_number,
                name=u.username,
                avatar=initials,
                avatar_color=u.avatar_color,
                avatar_url=u.avatar_url,
                quote=u.quote or "Hey there! I am using Flick.",
                online=is_online,
                last_message=u.quote or "Available on Flick",
                time="",
                unread=0,
                last_seen=u.last_seen.isoformat() if u.last_seen else None,
            )
        )

    return SyncContactsResponse(
        registered_users=registered_contacts,
        total_synced=len(registered_contacts),
    )


# -----------------------------------------------------------------------------
# Conversation History
# -----------------------------------------------------------------------------
@app.get("/api/messages/{other_user_id}")
async def get_messages_with_user(
    other_user_id: str,
    limit: int = 50,
    offset: int = 0,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Retrieves paginated message history (default: latest 50 messages) with phone tolerance."""
    other_user_id = other_user_id.strip()

    # 1. Collect all possible identifier aliases for current_user
    cur_ids = {current_user.id, current_user.phone_number}
    cur_digits = "".join(filter(str.isdigit, str(current_user.id)))
    cur_last10 = cur_digits[-10:] if len(cur_digits) >= 7 else cur_digits
    if cur_last10:
        cur_ids.add(cur_last10)
        cur_ids.add(f"+91{cur_last10}")
        if cur_digits:
            cur_ids.add(f"+{cur_digits}")

    # 2. Collect all possible identifier aliases for other_user
    other_ids = {other_user_id}
    other_digits = "".join(filter(str.isdigit, str(other_user_id)))
    other_last10 = other_digits[-10:] if len(other_digits) >= 7 else other_digits
    if other_last10:
        other_ids.add(other_last10)
        other_ids.add(f"+91{other_last10}")
        if other_digits:
            other_ids.add(f"+{other_digits}")

    # Look up other user in DB to retrieve their exact db id & phone_number
    o_stmt = (
        select(User)
        .where(
            or_(
                User.id == other_user_id,
                User.phone_number == other_user_id,
                User.phone_number.endswith(other_last10) if other_last10 else False,
                User.id.endswith(other_last10) if other_last10 else False,
            )
        )
        .order_by(desc(User.last_seen))
    )
    o_res = await db.execute(o_stmt)
    o_user = o_res.scalars().first()
    if o_user:
        other_ids.add(o_user.id)
        other_ids.add(o_user.phone_number)

    # Clean out None or empty values
    cur_ids = {str(i) for i in cur_ids if i}
    other_ids = {str(i) for i in other_ids if i}

    stmt = (
        select(Message)
        .where(
            or_(
                Message.sender_id.in_(cur_ids) & Message.receiver_id.in_(other_ids),
                Message.sender_id.in_(other_ids) & Message.receiver_id.in_(cur_ids),
                (
                    Message.sender_id.endswith(cur_last10) & Message.receiver_id.endswith(other_last10)
                    if cur_last10 and other_last10 else False
                ),
                (
                    Message.sender_id.endswith(other_last10) & Message.receiver_id.endswith(cur_last10)
                    if cur_last10 and other_last10 else False
                ),
            )
        )
        .order_by(desc(Message.timestamp))
        .offset(offset)
        .limit(limit)
    )
    result = await db.execute(stmt)
    messages = result.scalars().all()
    ordered_messages = list(reversed(messages))
    return [msg.to_dict() for msg in ordered_messages]


@app.post("/api/messages/send")
async def send_message_rest(
    payload: SendMessageRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Sends a message via REST with immediate DB persistence and WebSocket dispatch."""
    receiver_id = payload.receiver_id
    content = payload.content.strip()
    client_temp_id = payload.temp_msg_id or f"temp_{int(datetime.now().timestamp() * 1000)}"

    rec_stmt = select(User).where(User.id == receiver_id)
    rec_res = await db.execute(rec_stmt)
    if not rec_res.scalar_one_or_none():
        names = {"1": "Rahul Sharma", "2": "Priya Patel", "3": "React Native Devs", "4": "Alex Johnson"}
        rec_name = names.get(receiver_id, f"Contact {receiver_id[-4:]}" if len(receiver_id) >= 4 else f"Contact {receiver_id}")
        db.add(User(id=receiver_id, phone_number=receiver_id, username=rec_name, avatar_color="#3B82F6", is_online=False))

    is_rec_online = await manager.is_user_online(receiver_id)
    initial_status = MessageStatus.DELIVERED if is_rec_online else MessageStatus.SENT
    now = datetime.now(timezone.utc)

    combined_extra = {}
    if payload.reply_to:
        combined_extra["reply_to"] = payload.reply_to
    if payload.metadata:
        combined_extra["metadata"] = payload.metadata
    reply_to_str = json.dumps(combined_extra) if combined_extra else None

    new_msg = Message(
        sender_id=current_user.id,
        receiver_id=receiver_id,
        content=content,
        reply_to=reply_to_str,
        timestamp=now,
        status=initial_status,
        delivered_at=now if is_rec_online else None,
    )
    db.add(new_msg)
    await db.commit()
    await db.refresh(new_msg)

    # Record conversation changes for delta sync
    try:
        await record_conversation_change(db, user_id=current_user.id, peer_id=receiver_id, action="updated")
        await record_conversation_change(db, user_id=receiver_id, peer_id=current_user.id, action="updated")
        await db.commit()
    except Exception as sync_err:
        logger.warning("Failed to record conversation change for REST send: %s", sync_err)

    message_dict = new_msg.to_dict()

    # Forward to Receiver via WebSocket
    chat_frame = {
        "type": "chat_message",
        "data": message_dict,
    }
    await manager.send_personal_message(receiver_id, chat_frame)

    # Dispatch FCM push notification to receiver for background/closed device alerts
    asyncio.create_task(
        dispatch_push_to_user(
            sender_id=current_user.id,
            receiver_id=receiver_id,
            content=content,
            data_payload={
                "type": "chat_message",
                "message_id": message_dict["message_id"],
                "sender_id": current_user.id,
                "sender_name": current_user.username or "Flick User",
            },
        )
    )

    # Trigger personal AI agent if messaging agent, or bot responder if demo contact
    if receiver_id == AGENT_USER_ID or receiver_id.startswith("agent_"):
        asyncio.create_task(handle_personal_agent_reply(current_user.id, content, payload.metadata))
    else:
        asyncio.create_task(handle_bot_auto_reply(current_user.id, receiver_id, content))

    return {
        "status": "success",
        "temp_msg_id": client_temp_id,
        "message_id": message_dict["message_id"],
        **message_dict,
    }


@app.put("/api/messages/{message_id}")
async def edit_message(
    message_id: str,
    payload: EditMessageRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Edits a message sent by the current user and broadcasts the update to the recipient.
    """
    stmt = select(Message).where(Message.id == message_id)
    result = await db.execute(stmt)
    msg = result.scalar_one_or_none()

    if not msg:
        raise HTTPException(status_code=404, detail="Message not found")

    if msg.sender_id != current_user.id:
        raise HTTPException(status_code=403, detail="You can only edit your own messages")

    msg.content = payload.content.strip()
    msg.is_edited = True  # Persist edit flag to DB
    await db.commit()
    await db.refresh(msg)

    try:
        await record_conversation_change(db, user_id=msg.sender_id, peer_id=msg.receiver_id, action="updated")
        await record_conversation_change(db, user_id=msg.receiver_id, peer_id=msg.sender_id, action="updated")
        await db.commit()
    except Exception as sync_err:
        logger.warning("Failed to record conversation change for edit: %s", sync_err)

    # Broadcast edit frame to receiver over WebSocket
    edit_frame = {
        "type": "message_edited",
        "data": {
            "message_id": msg.id,
            "sender_id": msg.sender_id,
            "receiver_id": msg.receiver_id,
            "content": msg.content,
            "is_edited": True,
        },
    }
    await manager.send_personal_message(msg.receiver_id, edit_frame)

    return {"status": "success", "message": msg.to_dict(), "is_edited": True}


@app.delete("/api/messages/{message_id}")
async def delete_message(
    message_id: str,
    for_everyone: bool = True,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Deletes a message for the user or for everyone.
    """
    stmt = select(Message).where(Message.id == message_id)
    result = await db.execute(stmt)
    msg = result.scalar_one_or_none()

    if not msg:
        return {"status": "success", "message_id": message_id, "deleted": True}

    if for_everyone and msg.sender_id == current_user.id:
        receiver_id = msg.receiver_id
        sender_id = msg.sender_id
        await db.delete(msg)
        await db.commit()

        try:
            await record_conversation_change(db, user_id=sender_id, peer_id=receiver_id, action="updated")
            await record_conversation_change(db, user_id=receiver_id, peer_id=sender_id, action="updated")
            await db.commit()
        except Exception as sync_err:
            logger.warning("Failed to record conversation change for delete: %s", sync_err)

        # Broadcast delete frame to receiver
        delete_frame = {
            "type": "message_deleted",
            "data": {
                "message_id": message_id,
                "sender_id": current_user.id,
                "receiver_id": receiver_id,
            },
        }
        await manager.send_personal_message(receiver_id, delete_frame)
    else:
        # Delete for self only
        other_id = msg.receiver_id if msg.sender_id == current_user.id else msg.sender_id
        await db.delete(msg)
        await db.commit()
        try:
            await record_conversation_change(db, user_id=current_user.id, peer_id=other_id, action="updated")
            await db.commit()
        except Exception:
            pass

    return {"status": "success", "message_id": message_id, "deleted": True}


# -----------------------------------------------------------------------------
# Offline Queue Flush Helper
# -----------------------------------------------------------------------------
async def flush_offline_messages(user_id: str) -> None:
    """
    On user connection setup:
    1. Queries DB for all pending 'SENT' messages for receiver_id == user_id.
    2. Sends them in chronological order over socket.
    3. Bulk updates status to 'DELIVERED'.
    4. Pushes 'ack_delivered' receipt to original senders.
    """
    async with async_session_factory() as db:
        stmt = (
            select(Message)
            .where(Message.receiver_id == user_id, Message.status == "SENT")
            .order_by(Message.timestamp.asc())
        )
        result = await db.execute(stmt)
        pending_messages = result.scalars().all()

        if not pending_messages:
            return

        message_ids = [m.id for m in pending_messages]
        logger.info("Flushing %d offline messages for user %s", len(message_ids), user_id)

        # 1. Deliver pending frames to connected user
        for msg in pending_messages:
            msg_payload = {
                "type": "chat_message",
                "data": {
                    "message_id": msg.id,
                    "sender_id": msg.sender_id,
                    "receiver_id": msg.receiver_id,
                    "content": msg.content,
                    "timestamp": msg.timestamp.isoformat(),
                    "status": "DELIVERED",
                    "is_offline_replay": True,
                },
            }
            await manager.send_personal_message(user_id, msg_payload)

        # 2. Bulk update status in DB
        now = datetime.now(timezone.utc)
        await db.execute(
            update(Message)
            .where(Message.id.in_(message_ids))
            .values(status="DELIVERED", delivered_at=now)
        )
        await db.commit()

        # 3. Notify senders of delivery
        for msg in pending_messages:
            ack_payload = {
                "type": "ack_delivered",
                "data": {
                    "message_id": msg.id,
                    "receiver_id": user_id,
                    "delivered_at": now.isoformat(),
                },
            }
            await manager.send_personal_message(msg.sender_id, ack_payload)


# -----------------------------------------------------------------------------
# Personal AI Agent Background Handler
# -----------------------------------------------------------------------------
async def handle_personal_agent_reply(user_id: str, content: str, client_metadata: Optional[Any] = None) -> None:
    """Invokes the user-isolated Personal AI Agent, performs actions, and sends back the response."""
    try:
        # 1. Send typing indicator from agent to user
        typing_start = {
            "type": "typing_start",
            "data": {
                "sender_id": AGENT_USER_ID,
                "receiver_id": user_id,
            },
        }
        await manager.send_personal_message(user_id, typing_start)
        await asyncio.sleep(0.4)

        tz_offset = 0
        if isinstance(client_metadata, dict):
            tz_offset = client_metadata.get("tz_offset_minutes", 0)

        async with async_session_factory() as db:
            # Look up current user
            u_stmt = select(User).where(or_(User.id == user_id, User.phone_number == user_id))
            u_res = await db.execute(u_stmt)
            current_user = u_res.scalars().first()
            if not current_user:
                current_user = User(
                    id=user_id,
                    phone_number=user_id,
                    username=f"User {user_id[-4:]}" if len(user_id) >= 4 else "Flick User",
                    avatar_color="#FFB800",
                    is_online=True,
                )
                db.add(current_user)
                await db.commit()
                await db.refresh(current_user)

            # Ensure agent user exists
            await ensure_personal_agent_user(db)

            # Process AI request with user's calendar and message context
            agent_result = await process_personal_agent_message(current_user, content, db, tz_offset)

            # Save AI response message to DB
            now = datetime.now(timezone.utc)
            reply_metadata = {
                "is_agent": True,
                "action": agent_result.get("action"),
                "meeting": agent_result.get("meeting"),
                "meetings": agent_result.get("meetings"),
                "suggested_actions": agent_result.get("suggested_actions"),
            }
            reply_to_str = json.dumps({"metadata": reply_metadata})

            agent_msg = Message(
                sender_id=AGENT_USER_ID,
                receiver_id=user_id,
                content=agent_result.get("reply", "I am your personal AI assistant."),
                reply_to=reply_to_str,
                timestamp=now,
                status=MessageStatus.DELIVERED,
                delivered_at=now,
            )
            db.add(agent_msg)
            await db.commit()
            await db.refresh(agent_msg)

            try:
                await record_conversation_change(db, user_id=user_id, peer_id=AGENT_USER_ID, action="updated")
                await db.commit()
            except Exception:
                pass

            msg_dict = agent_msg.to_dict()

        # Stop typing
        typing_stop = {
            "type": "typing_stop",
            "data": {
                "sender_id": AGENT_USER_ID,
                "receiver_id": user_id,
            },
        }
        await manager.send_personal_message(user_id, typing_stop)

        # Dispatch AI message over WebSocket
        chat_frame = {
            "type": "chat_message",
            "data": msg_dict,
        }
        await manager.send_personal_message(user_id, chat_frame)

    except Exception as e:
        logger.error("Error in handle_personal_agent_reply: %s", e, exc_info=True)
        try:
            typing_stop = {
                "type": "typing_stop",
                "data": {"sender_id": AGENT_USER_ID, "receiver_id": user_id},
            }
            await manager.send_personal_message(user_id, typing_stop)
        except Exception:
            pass




# -----------------------------------------------------------------------------
# Calendar & Meeting Management APIs (Microsoft Teams / Arattai style)
# -----------------------------------------------------------------------------
@app.get("/api/meetings")
async def list_user_meetings(
    date_str: Optional[str] = None, # YYYY-MM-DD
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    status: Optional[str] = None,
    user_opt: Optional[User] = Depends(get_current_user_optional),
    db: AsyncSession = Depends(get_db),
):
    """Retrieves all meetings for the current user, optionally filtered by date or status."""
    current_user = await resolve_user_context(user_opt, db)
    stmt = select(Meeting).where(Meeting.user_id == current_user.id)
    if status:
        stmt = stmt.where(Meeting.status == status)

    if date_str:
        try:
            d = datetime.strptime(date_str, "%Y-%m-%d").date()
            d_start = datetime.combine(d, time.min).replace(tzinfo=timezone.utc)
            d_end = datetime.combine(d, time.max).replace(tzinfo=timezone.utc)
            stmt = stmt.where(Meeting.start_time >= d_start, Meeting.start_time <= d_end)
        except Exception:
            pass
    elif start_date and end_date:
        try:
            sd = datetime.fromisoformat(start_date)
            ed = datetime.fromisoformat(end_date)
            stmt = stmt.where(Meeting.start_time >= sd, Meeting.start_time <= ed)
        except Exception:
            pass

    stmt = stmt.order_by(Meeting.start_time.asc())
    result = await db.execute(stmt)
    meetings = result.scalars().all()
    return [m.to_dict() for m in meetings]


@app.post("/api/meetings", status_code=status.HTTP_201_CREATED)
async def create_user_meeting(
    payload: MeetingCreate,
    user_opt: Optional[User] = Depends(get_current_user_optional),
    db: AsyncSession = Depends(get_db),
):
    """Creates a new calendar event for current user with conflict check."""
    current_user = await resolve_user_context(user_opt, db)

    # Check conflicts
    conflicts = await check_meeting_conflicts(db, current_user.id, payload.start_time, payload.end_time)
    has_conflict = len(conflicts) > 0

    attendees_json = json.dumps(payload.attendees or [current_user.username or "Flick User"])

    meeting = Meeting(
        user_id=current_user.id,
        title=payload.title,
        description=payload.description or "",
        start_time=payload.start_time,
        end_time=payload.end_time,
        timezone=payload.timezone or "UTC",
        location=payload.location or "",
        meeting_link=payload.meeting_link or None,
        attendees=attendees_json,
        status=payload.status or "SCHEDULED",
    )
    db.add(meeting)
    await db.commit()
    await db.refresh(meeting)

    result_dict = meeting.to_dict()
    result_dict["has_conflict"] = has_conflict
    if has_conflict:
        result_dict["conflicting_meetings"] = [c.to_dict() for c in conflicts]

    return result_dict


@app.get("/api/meetings/{meeting_id}")
async def get_user_meeting(
    meeting_id: str,
    user_opt: Optional[User] = Depends(get_current_user_optional),
    db: AsyncSession = Depends(get_db),
):
    """Fetches a single meeting owned by current user."""
    current_user = await resolve_user_context(user_opt, db)
    stmt = select(Meeting).where(Meeting.id == meeting_id, Meeting.user_id == current_user.id)
    res = await db.execute(stmt)
    meeting = res.scalar_one_or_none()
    if not meeting:
        raise HTTPException(status_code=404, detail="Meeting not found or access denied")
    return meeting.to_dict()


@app.put("/api/meetings/{meeting_id}")
async def update_user_meeting(
    meeting_id: str,
    payload: MeetingUpdate,
    user_opt: Optional[User] = Depends(get_current_user_optional),
    db: AsyncSession = Depends(get_db),
):
    """Updates an authorized meeting."""
    current_user = await resolve_user_context(user_opt, db)
    stmt = select(Meeting).where(Meeting.id == meeting_id, Meeting.user_id == current_user.id)
    res = await db.execute(stmt)
    meeting = res.scalar_one_or_none()
    if not meeting:
        raise HTTPException(status_code=404, detail="Meeting not found or access denied")

    if payload.title is not None:
        meeting.title = payload.title
    if payload.description is not None:
        meeting.description = payload.description
    if payload.start_time is not None:
        meeting.start_time = payload.start_time
    if payload.end_time is not None:
        meeting.end_time = payload.end_time
    if payload.location is not None:
        meeting.location = payload.location
    if payload.meeting_link is not None:
        meeting.meeting_link = payload.meeting_link
    if payload.timezone is not None:
        meeting.timezone = payload.timezone
    if payload.attendees is not None:
        meeting.attendees = json.dumps(payload.attendees)
    if payload.status is not None:
        meeting.status = payload.status

    meeting.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(meeting)
    return meeting.to_dict()


@app.delete("/api/meetings/{meeting_id}")
async def delete_user_meeting(
    meeting_id: str,
    user_opt: Optional[User] = Depends(get_current_user_optional),
    db: AsyncSession = Depends(get_db),
):
    """Deletes or cancels a meeting owned by current user."""
    current_user = await resolve_user_context(user_opt, db)
    stmt = select(Meeting).where(Meeting.id == meeting_id, Meeting.user_id == current_user.id)
    res = await db.execute(stmt)
    meeting = res.scalar_one_or_none()
    if not meeting:
        raise HTTPException(status_code=404, detail="Meeting not found or access denied")

    await db.delete(meeting)
    await db.commit()
    return {"status": "success", "message": "Meeting deleted successfully", "id": meeting_id}


@app.post("/api/meetings/check-conflicts")
async def check_conflicts_endpoint(
    payload: CheckConflictRequest,
    user_opt: Optional[User] = Depends(get_current_user_optional),
    db: AsyncSession = Depends(get_db),
):
    """Checks whether the requested start_time and end_time conflict with existing meetings."""
    current_user = await resolve_user_context(user_opt, db)
    conflicts = await check_meeting_conflicts(
        db, current_user.id, payload.start_time, payload.end_time, payload.exclude_meeting_id
    )
    return {
        "has_conflict": len(conflicts) > 0,
        "conflicts": [c.to_dict() for c in conflicts],
    }


# -----------------------------------------------------------------------------
# Personal AI Agent Dedicated Endpoints
# -----------------------------------------------------------------------------
@app.get("/api/agent/profile")
async def get_agent_profile(
    user_opt: Optional[User] = Depends(get_current_user_optional),
    db: AsyncSession = Depends(get_db),
):
    """Returns the profile info for current user's personal AI agent."""
    await resolve_user_context(user_opt, db)
    await ensure_personal_agent_user(db)
    return {
        "id": AGENT_USER_ID,
        "name": AGENT_NAME,
        "avatar": AGENT_AVATAR,
        "avatar_color": AGENT_AVATAR_COLOR,
        "quote": AGENT_QUOTE,
        "online": True,
        "is_agent": True,
    }


@app.post("/api/agent/chat", response_model=AgentChatResponse)
async def chat_with_agent_endpoint(
    payload: AgentChatRequest,
    user_opt: Optional[User] = Depends(get_current_user_optional),
    db: AsyncSession = Depends(get_db),
):
    """Direct REST endpoint to chat with Personal AI Agent and execute calendar actions."""
    current_user = await resolve_user_context(user_opt, db)
    await ensure_personal_agent_user(db)

    # 1. Save user query message to persistent database
    now = datetime.now(timezone.utc)
    user_msg = Message(
        sender_id=current_user.id,
        receiver_id=AGENT_USER_ID,
        content=payload.content,
        timestamp=now,
        status=MessageStatus.READ,
        delivered_at=now,
        read_at=now,
    )
    db.add(user_msg)

    # 2. Process query via Personal AI Agent engine
    res = await process_personal_agent_message(
        current_user=current_user,
        content=payload.content,
        db=db,
        tz_offset_minutes=payload.client_tz_offset_minutes or 0,
    )

    # 3. Save AI response message with metadata to persistent database
    reply_metadata = {
        "is_agent": True,
        "action": res.get("action"),
        "meeting": res.get("meeting"),
        "meetings": res.get("meetings"),
        "suggested_actions": res.get("suggested_actions"),
    }
    agent_msg = Message(
        sender_id=AGENT_USER_ID,
        receiver_id=current_user.id,
        content=(res.get("reply") or "I am your personal AI assistant.").strip(),
        reply_to=json.dumps({"metadata": reply_metadata}),
        timestamp=datetime.now(timezone.utc),
        status=MessageStatus.READ,
        delivered_at=now,
        read_at=now,
    )
    db.add(agent_msg)
    await db.commit()
    await db.refresh(user_msg)
    await db.refresh(agent_msg)

    try:
        await record_conversation_change(db, user_id=current_user.id, peer_id=AGENT_USER_ID, action="updated")
        await db.commit()
    except Exception:
        pass

    res["user_message_id"] = user_msg.id
    res["message_id"] = agent_msg.id
    res["timestamp"] = agent_msg.timestamp.isoformat()

    return res


# -----------------------------------------------------------------------------
# Simulated AI / Bot Auto-Responder (Takes Flick to the Next Level 🚀)
# -----------------------------------------------------------------------------
async def handle_bot_auto_reply(sender_id: str, receiver_id: str, user_content: str) -> None:
    """Simulates smart interactive chat when messaging demo bot / contacts."""
    bot_names = {
        "1": "Rahul Sharma",
        "2": "Priya Patel",
        "3": "React Native Devs",
        "4": "Alex Johnson",
        "+919876543210": "Rahul Sharma",
        "+919123456789": "Priya Patel",
        "+919988776655": "React Native Devs",
        "+919811223344": "Alex Johnson",
    }

    if receiver_id not in bot_names:
        return

    bot_name = bot_names[receiver_id]

    # 1. Send typing indicator and mark incoming messages as READ
    await asyncio.sleep(0.5)
    now_read = datetime.now(timezone.utc)
    try:
        async with async_session_factory() as read_db:
            await read_db.execute(
                update(Message)
                .where(
                    Message.sender_id == sender_id,
                    Message.receiver_id == receiver_id,
                    Message.status != "READ",
                )
                .values(status="READ", read_at=now_read)
            )
            await read_db.commit()

        ack_read_frame = {
            "type": "ack_read",
            "data": {
                "reader_id": receiver_id,
                "sender_id": sender_id,
                "read_at": now_read.isoformat(),
            },
        }
        await manager.send_personal_message(sender_id, ack_read_frame)
    except Exception as read_err:
        logger.warning("Bot mark read error: %s", read_err)

    typing_start_frame = {
        "type": "typing_start",
        "data": {
            "sender_id": receiver_id,
            "receiver_id": sender_id,
        },
    }
    await manager.send_personal_message(sender_id, typing_start_frame)

    # 2. Formulate smart context-aware response
    await asyncio.sleep(1.0)
    lowered = user_content.lower()

    if "hello" in lowered or "hi" in lowered or "hey" in lowered:
        reply_text = f"Hey! Great to hear from you. How are you doing today? 😊"
    elif "bye" in lowered or "byee" in lowered or "cya" in lowered:
        reply_text = f"Goodbye! Have an awesome day ahead! 🚀"
    elif "thanks" in lowered or "thank" in lowered:
        reply_text = f"You're very welcome! Always happy to connect on Flick. ✨"
    elif "how are you" in lowered:
        reply_text = f"I'm doing fantastic! Flick's real-time messaging is super fast! ⚡"
    elif "react native" in lowered or "expo" in lowered or "flashlist" in lowered:
        reply_text = f"FlashList v2 and React 19 in Flick make 60fps scrolling effortless! 📱"
    else:
        reply_text = f"Got your message: '{user_content}'. Everything is synced real-time with Neon DB & Redis! 👍"

    # Stop typing
    typing_stop_frame = {
        "type": "typing_stop",
        "data": {
            "sender_id": receiver_id,
            "receiver_id": sender_id,
        },
    }
    await manager.send_personal_message(sender_id, typing_stop_frame)

    # Save bot message to DB
    async with async_session_factory() as db:
        now = datetime.now(timezone.utc)
        bot_msg = Message(
            sender_id=receiver_id,
            receiver_id=sender_id,
            content=reply_text,
            timestamp=now,
            status=MessageStatus.DELIVERED,
            delivered_at=now,
        )
        db.add(bot_msg)
        await db.commit()
        await db.refresh(bot_msg)

        # Send message frame to user
        msg_frame = {
            "type": "chat_message",
            "data": bot_msg.to_dict(),
        }
        delivered = await manager.send_personal_message(sender_id, msg_frame)

        if not delivered:
            # User is in background / app closed -> Send FCM Push Notification
            u_stmt = select(User).where(User.id == sender_id)
            u_res = await db.execute(u_stmt)
            target_user = u_res.scalar_one_or_none()

            b_stmt = select(User).where(User.id == bot_id)
            b_res = await db.execute(b_stmt)
            bot_user = b_res.scalar_one_or_none()

            if target_user and target_user.fcm_token:
                await send_push_notification(
                    fcm_token=target_user.fcm_token,
                    sender_name=bot_user.username if bot_user else "Rahul Sharma",
                    content=bot_reply,
                    data_payload={
                        "sender_id": bot_id,
                        "receiver_id": sender_id,
                        "message_id": bot_msg.id,
                    },
                )


# -----------------------------------------------------------------------------
# Real-Time WebSocket Gateway
# -----------------------------------------------------------------------------
WS_CLOSE_UNAUTHORIZED = 4401  # app-defined close code: the client should log the user out


@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    """
    Core bidirectional real-time communication endpoint.
    Handles:
    - User connection and Redis Pub/Sub subscription
    - Guaranteed message persistence and delivery
    - Offline queue replay
    - Typing indicators (typing_start / typing_stop)
    - Status receipts (SENT -> DELIVERED -> READ)
    - Heartbeat keep-alive
    """
    # Authenticate before registering the socket: the token (?token=<jwt>) must belong to the user in the path.
    # Without this, anyone could connect as any phone number and read or send that person's messages.
    token = websocket.query_params.get("token")
    async with async_session_factory() as db:
        user_obj = await user_from_token(token, db)
        if not user_obj or not (
            same_identity(user_obj.id, user_id) or same_identity(user_obj.phone_number, user_id)
        ):
            await websocket.accept()
            await websocket.close(code=WS_CLOSE_UNAUTHORIZED, reason="Invalid or expired session")
            return
        user_id = user_obj.id
        user_obj.is_online = True
        await db.commit()
        alias_ids = [user_obj.id, user_obj.phone_number]

    await manager.connect(user_id, websocket)
    manager.register_aliases(user_id, alias_ids)
    await manager.refresh_presence(user_id)

    # Flush any unread messages stored while this user was offline
    asyncio.create_task(flush_offline_messages(user_id))

    try:
        while True:
            raw_data = await websocket.receive_text()
            try:
                payload = json.loads(raw_data)
            except json.JSONDecodeError:
                logger.warning("Received invalid JSON from %s", user_id)
                continue

            frame_type = payload.get("type")
            data = payload.get("data", {})

            # -----------------------------------------------------------------
            # 1. SEND MESSAGE
            # -----------------------------------------------------------------
            if frame_type in ("send_message", "chat_message"):
                receiver_id = data.get("receiver_id")
                content = (data.get("content") or "").strip()
                client_temp_id = data.get("temp_msg_id") or data.get("temp_id") or f"temp_{int(datetime.now(timezone.utc).timestamp()*1000)}"

                if not receiver_id or not content:
                    continue

                async with async_session_factory() as db:
                    # Single round-trip batch check for sender & receiver
                    users_res = await db.execute(
                        select(User).where(User.id.in_([user_id, receiver_id]))
                    )
                    existing_users = {u.id: u for u in users_res.scalars().all()}

                    if user_id not in existing_users:
                        db.add(User(
                            id=user_id,
                            phone_number=user_id,
                            username=f"User {user_id[-4:]}" if len(user_id) >= 4 else "Flick User",
                            avatar_color="#FFB800",
                            is_online=True,
                        ))
                    if receiver_id not in existing_users:
                        # Auto-seed receiver to avoid Foreign Key violations
                        names = {"1": "Rahul Sharma", "2": "Priya Patel", "3": "React Native Devs", "4": "Alex Johnson"}
                        rec_name = names.get(receiver_id, f"Contact {receiver_id[-4:]}" if len(receiver_id) >= 4 else f"Contact {receiver_id}")
                        db.add(User(
                            id=receiver_id,
                            phone_number=receiver_id,
                            username=rec_name,
                            avatar_color="#3B82F6",
                            is_online=False,
                        ))

                    # Determine delivery state
                    is_rec_online = await manager.is_user_online(receiver_id)
                    initial_status = MessageStatus.DELIVERED if is_rec_online else MessageStatus.SENT
                    now = datetime.now(timezone.utc)
                    reply_to_raw = data.get("reply_to")
                    metadata_raw = data.get("metadata")
                    combined_extra = {}
                    if reply_to_raw:
                        combined_extra["reply_to"] = reply_to_raw
                    if metadata_raw:
                        combined_extra["metadata"] = metadata_raw
                    reply_to_str = json.dumps(combined_extra) if combined_extra else None

                    new_msg = Message(
                        sender_id=user_id,
                        receiver_id=receiver_id,
                        content=content,
                        reply_to=reply_to_str,
                        timestamp=now,
                        status=initial_status,
                        delivered_at=now if is_rec_online else None,
                    )
                    db.add(new_msg)
                    await db.commit()
                    await db.refresh(new_msg)

                    try:
                        await record_conversation_change(db, user_id=user_id, peer_id=receiver_id, action="updated")
                        await record_conversation_change(db, user_id=receiver_id, peer_id=user_id, action="updated")
                        await db.commit()
                    except Exception as sync_err:
                        logger.warning("Failed to record conversation change for WS send: %s", sync_err)

                    message_dict = new_msg.to_dict()

                # Acknowledge to Sender (SENT / DELIVERED)
                ack_sent_frame = {
                    "type": "ack_sent",
                    "data": {
                        "temp_id": client_temp_id,
                        "temp_msg_id": client_temp_id,
                        "message_id": message_dict["message_id"],
                        "timestamp": message_dict["timestamp"],
                        "status": message_dict["status"],
                    },
                }
                try:
                    await websocket.send_text(json.dumps(ack_sent_frame))
                except Exception:
                    await manager.send_personal_message(user_id, ack_sent_frame)

                # Forward to Receiver
                chat_frame = {
                    "type": "chat_message",
                    "data": message_dict,
                }
                delivered = await manager.send_personal_message(receiver_id, chat_frame)

                # Dispatch FCM Push Notification so background/closed devices receive tray alert
                asyncio.create_task(
                    dispatch_push_to_user(
                        sender_id=user_id,
                        receiver_id=receiver_id,
                        content=content,
                        data_payload={
                            "type": "chat_message",
                            "message_id": message_dict["message_id"],
                            "sender_id": user_id,
                        },
                    )
                )

                # Trigger personal AI agent if receiver is agent, or bot responder if demo contact
                if receiver_id == AGENT_USER_ID or receiver_id.startswith("agent_"):
                    asyncio.create_task(handle_personal_agent_reply(user_id, content, metadata_raw))
                else:
                    asyncio.create_task(handle_bot_auto_reply(user_id, receiver_id, content))

            # -----------------------------------------------------------------
            # 2. TYPING INDICATORS
            # -----------------------------------------------------------------
            elif frame_type in ("typing_start", "typing_stop"):
                receiver_id = data.get("receiver_id")
                if receiver_id:
                    typing_frame = {
                        "type": frame_type,
                        "data": {
                            "sender_id": user_id,
                            "receiver_id": receiver_id,
                        },
                    }
                    await manager.send_personal_message(receiver_id, typing_frame)

            # -----------------------------------------------------------------
            # 3. READ & DELIVERY RECEIPTS
            # -----------------------------------------------------------------
            elif frame_type == "ack_delivered":
                sender_id = data.get("sender_id")
                message_id = data.get("message_id")
                if sender_id and message_id:
                    now = datetime.now(timezone.utc)
                    async with async_session_factory() as db:
                        check = await db.execute(
                            select(Message.id, Message.status).where(Message.id == message_id)
                        )
                        row = check.fetchone()
                        if row and row.status != MessageStatus.READ:
                            stmt = (
                                update(Message)
                                .where(
                                    Message.id == message_id,
                                    Message.status == MessageStatus.SENT,
                                )
                                .values(status=MessageStatus.DELIVERED, delivered_at=now)
                            )
                            await db.execute(stmt)
                            await db.commit()

                    ack_del_frame = {
                        "type": "ack_delivered",
                        "data": {
                            "message_id": message_id,
                            "receiver_id": user_id,
                            "delivered_at": now.isoformat(),
                        },
                    }
                    await manager.send_personal_message(sender_id, ack_del_frame)

            elif frame_type == "mark_read":
                sender_id = data.get("sender_id")  # original sender of the messages being read
                if sender_id:
                    now = datetime.now(timezone.utc)
                    async with async_session_factory() as db:
                        stmt = (
                            update(Message)
                            .where(
                                Message.sender_id == sender_id,
                                Message.receiver_id == user_id,
                                Message.status != "READ",
                            )
                            .values(status="READ", read_at=now)
                        )
                        await db.execute(stmt)
                        await db.commit()

                        try:
                            await record_conversation_change(db, user_id=user_id, peer_id=sender_id, action="updated")
                            await db.commit()
                        except Exception as sync_err:
                            logger.warning("Failed to record conversation change for mark_read: %s", sync_err)

                    ack_read_frame = {
                        "type": "ack_read",
                        "data": {
                            "reader_id": user_id,
                            "sender_id": sender_id,
                            "read_at": now.isoformat(),
                        },
                    }
                    await manager.send_personal_message(sender_id, ack_read_frame)

            # -----------------------------------------------------------------
            # 4. REAL-TIME CALL AUDIO CHUNK FORWARDING (Zero-Latency Relay)
            # -----------------------------------------------------------------
            elif frame_type == "call_audio_chunk":
                data["sender_id"] = user_id
                target_id = data.get("receiver_id") or data.get("other_user_id") or data.get("caller_id")
                if target_id and str(target_id) != str(user_id):
                    # Zero-overhead streaming to target user's active socket
                    await manager.send_personal_message(str(target_id), payload)

            # -----------------------------------------------------------------
            # 5. VOICE CALL SIGNALING (Full-Duplex Audio Handshake)
            # -----------------------------------------------------------------
            elif frame_type in ("call_offer", "call_answer", "call_ice_candidate", "call_reject", "call_end"):
                # Frames always carry the socket's own identity, never one the client claims
                data["sender_id"] = user_id
                if frame_type == "call_offer":
                    data["caller_id"] = user_id

                # Determine target recipient for the call frame
                target_id = data.get("receiver_id")
                if (not target_id or str(target_id) == str(user_id)) and frame_type != "call_offer":
                    target_id = data.get("caller_id") or data.get("other_user_id")

                call_id = data.get("call_id") or f"call_{int(datetime.now(timezone.utc).timestamp())}"
                now_iso = datetime.now(timezone.utc).isoformat()

                if frame_type == "call_offer":
                    caller_id_val = str(user_id)
                    caller_name = data.get("caller_name")
                    caller_avatar_url = data.get("caller_avatar_url")
                    caller_color = data.get("caller_color", "#FFB800")
                    caller_avatar = data.get("caller_avatar", "FK")

                    if not caller_avatar_url or not caller_name or caller_name in ("Pocket User", "Flick User"):
                        try:
                            async with async_session_factory() as db:
                                c_stmt = select(User).where(or_(User.id == caller_id_val, User.phone_number == caller_id_val))
                                c_res = await db.execute(c_stmt)
                                caller_user = c_res.scalars().first()
                                if caller_user:
                                    if caller_user.username:
                                        caller_name = caller_user.username
                                    if caller_user.avatar_url:
                                        caller_avatar_url = caller_user.avatar_url
                                    if caller_user.avatar_color:
                                        caller_color = caller_user.avatar_color
                                    if caller_user.username and len(caller_user.username) >= 2:
                                        caller_avatar = caller_user.username[:2].upper()
                        except Exception:
                            pass

                    data["caller_name"] = caller_name or f"User {caller_id_val[-4:]}"
                    data["caller_avatar_url"] = caller_avatar_url
                    data["caller_color"] = caller_color
                    data["caller_avatar"] = caller_avatar

                    await save_call_session(call_id, {
                        "call_id": call_id,
                        "caller_id": user_id,
                        "receiver_id": target_id,
                        "caller_name": data["caller_name"],
                        "caller_avatar": data["caller_avatar"],
                        "caller_color": data["caller_color"],
                        "caller_avatar_url": data["caller_avatar_url"],
                        "status": "ringing",
                        "created_at": now_iso,
                        "updated_at": now_iso,
                    })
                elif frame_type == "call_answer":
                    session = await get_call_session(call_id)
                    session.update({
                        "call_id": call_id,
                        "status": "answered",
                        "answered_at": now_iso,
                        "updated_at": now_iso,
                    })
                    await save_call_session(call_id, session)
                elif frame_type == "call_reject":
                    session = await get_call_session(call_id)
                    session.update({
                        "call_id": call_id,
                        "status": "rejected",
                        "reason": data.get("reason", "declined"),
                        "updated_at": now_iso,
                    })
                    await save_call_session(call_id, session)
                elif frame_type == "call_end":
                    session = await get_call_session(call_id)
                    session.update({
                        "call_id": call_id,
                        "status": "ended",
                        "duration": data.get("duration", 0),
                        "updated_at": now_iso,
                    })
                    await save_call_session(call_id, session)

                def is_same_user_id(u1: str, u2: str) -> bool:
                    if not u1 or not u2:
                        return False
                    s1, s2 = str(u1).strip(), str(u2).strip()
                    if s1 == s2:
                        return True
                    d1 = "".join(filter(str.isdigit, s1))
                    d2 = "".join(filter(str.isdigit, s2))
                    if d1 and d2:
                        if d1 == d2:
                            return True
                        if len(d1) >= 7 and len(d2) >= 7:
                            return d1.endswith(d2) or d2.endswith(d1)
                    return False

                if target_id and not is_same_user_id(target_id, user_id):
                    call_data = {
                        **data,
                        "sender_id": user_id,
                        "caller_id": data.get("caller_id", user_id),
                        "receiver_id": target_id,
                        "call_id": call_id,
                    }
                    delivered = await broadcast_call_frame(target_id, frame_type, call_data, sender_id=user_id)
                    logger.info("[VoiceCall WS] Frame %s from %s to %s (delivered: %s)", frame_type, user_id, target_id, delivered)

                    # For voice call offers, ALWAYS dispatch high-priority FCM push notification
                    if frame_type == "call_offer":
                        caller_name = data.get("caller_name") or f"User {str(user_id)[-4:]}"
                        asyncio.create_task(
                            dispatch_push_to_user(
                                sender_id=user_id,
                                receiver_id=target_id,
                                content=f"📞 Incoming voice call from {caller_name}...",
                                data_payload={
                                    "type": "incoming_call",
                                    "caller_id": str(user_id),
                                    "caller_name": caller_name,
                                    "caller_avatar": data.get("caller_avatar", "PK"),
                                    "caller_color": data.get("caller_color", "#FFB800"),
                                    "caller_avatar_url": data.get("caller_avatar_url", ""),
                                    "call_id": call_id,
                                },
                            )
                        )
                    elif frame_type == "call_answer":
                        asyncio.create_task(
                            dispatch_push_to_user(
                                sender_id=user_id,
                                receiver_id=target_id,
                                content="Call Answered",
                                data_payload={
                                    "type": "call_answered",
                                    "call_id": call_id,
                                    "receiver_id": str(user_id),
                                },
                            )
                        )
                    elif frame_type in ("call_end", "call_reject"):
                        asyncio.create_task(
                            dispatch_push_to_user(
                                sender_id=user_id,
                                receiver_id=target_id,
                                content="Call Ended" if frame_type == "call_end" else "Call Declined",
                                data_payload={
                                    "type": "call_ended" if frame_type == "call_end" else "call_rejected",
                                    "call_id": call_id,
                                },
                            )
                        )

                    # Demo contact simulation ONLY for mock bots (1, 2, 3, 4) when offline
                    demo_mock_ids = ("1", "2", "3", "4")
                    if frame_type == "call_offer" and str(target_id) in demo_mock_ids:
                        is_target_online = await manager.is_user_online(target_id)
                        if not is_target_online:
                            async def simulate_demo_answer():
                                await asyncio.sleep(2.0)
                                ans_data = {
                                    "call_id": call_id,
                                    "caller_id": target_id,
                                    "receiver_id": user_id,
                                    "sender_id": target_id,
                                }
                                await broadcast_call_frame(user_id, "call_answer", ans_data)

                            asyncio.create_task(simulate_demo_answer())

            # -----------------------------------------------------------------
            # 5. HEARTBEAT / PING & PRESENCE CONTROL
            # -----------------------------------------------------------------
            elif frame_type == "ping":
                await manager.refresh_presence(user_id)
                await websocket.send_text(json.dumps({"type": "pong", "timestamp": datetime.now(timezone.utc).isoformat()}))

            elif frame_type == "presence_online":
                async with async_session_factory() as db:
                    await db.execute(
                        update(User)
                        .where(or_(User.id == user_id, User.phone_number == user_id))
                        .values(is_online=True)
                    )
                    await db.commit()
                await manager.refresh_presence(user_id)
                await manager.broadcast_presence(user_id, is_online=True)

            elif frame_type == "presence_offline":
                now_utc = datetime.now(timezone.utc)
                async with async_session_factory() as db:
                    await db.execute(
                        update(User)
                        .where(or_(User.id == user_id, User.phone_number == user_id))
                        .values(is_online=False, last_seen=now_utc)
                    )
                    await db.commit()
                await manager.broadcast_presence(user_id, is_online=False, last_seen=now_utc.isoformat())

            elif frame_type == "query_presence":
                target_id = data.get("user_id")
                if target_id:
                    target_last_seen = None
                    is_target_online = False
                    async with async_session_factory() as db:
                        digits = "".join(filter(str.isdigit, str(target_id)))
                        t_stmt = select(User).where(
                            or_(
                                User.id == str(target_id),
                                User.phone_number == str(target_id),
                                User.phone_number.endswith(digits[-10:]) if len(digits) >= 10 else False,
                                User.id.endswith(digits[-10:]) if len(digits) >= 10 else False,
                            )
                        )
                        t_res = await db.execute(t_stmt)
                        target_user = t_res.scalars().first()
                        if target_user:
                            is_target_online = await manager.is_user_online(target_user.id, target_user.phone_number)
                            if target_user.last_seen:
                                target_last_seen = target_user.last_seen.isoformat()
                        else:
                            is_target_online = await manager.is_user_online(target_id)
                    try:
                        aliases = [target_id]
                        if target_user:
                            aliases.extend([target_user.id, target_user.phone_number])
                        await websocket.send_text(json.dumps({
                            "type": "presence_update",
                            "data": {
                                "user_id": target_id,
                                "aliases": [a for a in aliases if a],
                                "is_online": is_target_online,
                                "last_seen": target_last_seen,
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                            }
                        }))
                    except Exception:
                        pass

    except WebSocketDisconnect:
        logger.info("WebSocket disconnected for user %s", user_id)
        await manager.disconnect(user_id, websocket)
        if not await manager.is_user_online(user_id):
            now_utc = datetime.now(timezone.utc)
            async with async_session_factory() as db:
                await db.execute(
                    update(User)
                    .where(or_(User.id == user_id, User.phone_number == user_id))
                    .values(is_online=False, last_seen=now_utc)
                )
                await db.commit()
    except Exception as e:
        logger.error("WebSocket runtime error for %s: %s", user_id, e)
        await manager.disconnect(user_id, websocket)
        if not await manager.is_user_online(user_id):
            now_utc = datetime.now(timezone.utc)
            async with async_session_factory() as db:
                await db.execute(
                    update(User)
                    .where(or_(User.id == user_id, User.phone_number == user_id))
                    .values(is_online=False, last_seen=now_utc)
                )
                await db.commit()
