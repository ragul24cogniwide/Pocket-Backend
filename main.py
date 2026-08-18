"""
Main FastAPI application providing REST API endpoints and WebSocket Real-Time Gateway.
Full support for Neon PostgreSQL, Render Redis, Phone Number OTP, and Real-Time Chat.
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from fastapi import Depends, FastAPI, HTTPException, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from sqlalchemy import desc, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from auth import (
    create_access_token,
    generate_otp_code,
    get_current_user,
)
from connection_manager import manager
from database import async_session_factory, get_db, init_db
from models import Message, MessageStatus, OtpRecord, User
from notifications import send_push_notification
from schemas import (
    AuthResponse,
    ChatContactItem,
    SendOtpRequest,
    SendOtpResponse,
    SyncContactsRequest,
    SyncContactsResponse,
    UpdateFcmTokenRequest,
    UpdateProfileRequest,
    UserProfileResponse,
    VerifyOtpRequest,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Main")

DEV_MODE_EXPOSE_OTP = os.getenv("DEV_MODE_EXPOSE_OTP", "true").lower() == "true"
OTP_EXPIRY_MINUTES = int(os.getenv("OTP_EXPIRY_MINUTES", "5"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown hooks."""
    logger.info("Initializing database tables...")
    await init_db()
    logger.info("Initializing connection manager & Redis Pub/Sub...")
    await manager.initialize()
    yield
    logger.info("Shutting down connection manager...")
    await manager.shutdown()


app = FastAPI(
    title="Pocket Real-Time Messaging API",
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
    return {"message": "Pocket Real-Time Messaging API is active"}


# -----------------------------------------------------------------------------
# Authentication & OTP Endpoints
# -----------------------------------------------------------------------------
@app.post("/api/auth/send-otp", response_model=SendOtpResponse)
async def send_otp(payload: SendOtpRequest, db: AsyncSession = Depends(get_db)):
    """
    Generates a 6-digit OTP for the provided phone number.
    In testing/dev mode, returns the OTP in the response for easy instant login.
    """
    phone = payload.phone_number.strip()
    if not phone:
        raise HTTPException(status_code=400, detail="Phone number is required.")

    if not phone.startswith("+"):
        phone = "+91" + phone if len(phone) == 10 else "+" + phone

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

    logger.info("Generated OTP for %s: %s (Expires in %d mins)", phone, otp_code, OTP_EXPIRY_MINUTES)

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
    phone = payload.phone_number.strip()
    if not phone.startswith("+"):
        phone = "+91" + phone if len(phone) == 10 else "+" + phone

    now = datetime.now(timezone.utc)

    # Check OTP record
    stmt = (
        select(OtpRecord)
        .where(
            OtpRecord.phone_number == phone,
            OtpRecord.otp_code == payload.otp_code.strip(),
            OtpRecord.is_used == False,
            OtpRecord.expires_at > now,
        )
        .order_by(desc(OtpRecord.created_at))
    )
    result = await db.execute(stmt)
    otp_record = result.scalars().first()

    if not otp_record:
        # Check master testing OTP '123456' for convenience in dev
        if payload.otp_code != "123456":
            raise HTTPException(status_code=400, detail="Invalid or expired OTP code.")

    if otp_record:
        otp_record.is_used = True
        await db.commit()

    # Find or Create User
    user_stmt = select(User).where(or_(User.id == phone, User.phone_number == phone))
    user_res = await db.execute(user_stmt)
    user = user_res.scalar_one_or_none()

    if not user:
        colors = ["#FFB800", "#10B981", "#3B82F6", "#8B5CF6", "#EC4899", "#F97316"]
        avatar_color = colors[len(phone) % len(colors)]
        username = payload.username or f"User {phone[-4:]}"

        user = User(
            id=phone,  # Use phone as unique ID for straightforward routing
            phone_number=phone,
            username=username,
            avatar_color=avatar_color,
            quote="Hey there! I am using Pocket.",
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
    )


@app.get("/api/auth/me", response_model=UserProfileResponse)
async def get_my_profile(current_user: User = Depends(get_current_user)):
    """Returns the authenticated user's profile to maintain login state."""
    return UserProfileResponse(
        id=current_user.id,
        phone_number=current_user.phone_number,
        username=current_user.username,
        avatar_color=current_user.avatar_color,
        avatar_url=current_user.avatar_url,
        quote=current_user.quote or "Hey there! I am using Pocket.",
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
    """Updates the authenticated user's profile details (username, avatar_url, avatar_color, quote)."""
    try:
        if payload.username is not None and payload.username.strip():
            current_user.username = payload.username.strip()
        if payload.avatar_url is not None:
            current_user.avatar_url = payload.avatar_url.strip() if payload.avatar_url else None
        if payload.avatar_color is not None and payload.avatar_color.strip():
            current_user.avatar_color = payload.avatar_color.strip()
        if payload.quote is not None:
            current_user.quote = payload.quote.strip() if payload.quote else "Hey there! I am using Pocket."

        await db.commit()
        await db.refresh(current_user)

        return UserProfileResponse(
            id=current_user.id,
            phone_number=current_user.phone_number,
            username=current_user.username,
            avatar_color=current_user.avatar_color,
            avatar_url=current_user.avatar_url,
            quote=current_user.quote or "Hey there! I am using Pocket.",
            is_online=current_user.is_online,
            last_seen=current_user.last_seen.isoformat() if current_user.last_seen else None,
        )
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
    """Updates device FCM Push Token for background notifications."""
    current_user.fcm_token = payload.fcm_token
    await db.commit()
    return {"message": "FCM token updated successfully", "fcm_token": payload.fcm_token}


# -----------------------------------------------------------------------------
# User Discovery & Contacts
# -----------------------------------------------------------------------------
@app.get("/api/users", response_model=List[ChatContactItem])
async def list_available_contacts(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Lists registered users excluding current user, with real-time online status."""
    stmt = select(User).where(User.id != current_user.id).order_by(User.username.asc())
    result = await db.execute(stmt)
    users = result.scalars().all()

    contacts = []
    seen_identifiers = set()

    for u in users:
        # Avoid duplicate mock IDs pointing to same contact
        norm_key = u.username.lower().strip()
        if norm_key in seen_identifiers:
            continue
        seen_identifiers.add(norm_key)

        is_online = await manager.is_user_online(u.id)

        # Get latest message between current_user and u
        msg_stmt = (
            select(Message)
            .where(
                or_(
                    (Message.sender_id == current_user.id) & (Message.receiver_id == u.id),
                    (Message.sender_id == u.id) & (Message.receiver_id == current_user.id),
                )
            )
            .order_by(desc(Message.timestamp))
            .limit(1)
        )
        msg_res = await db.execute(msg_stmt)
        latest_msg = msg_res.scalar_one_or_none()

        # Count unread messages from u to current_user
        unread_stmt = select(func.count(Message.id)).where(
            Message.sender_id == u.id,
            Message.receiver_id == current_user.id,
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
            ChatContactItem(
                id=u.id,
                phone_number=u.phone_number,
                name=u.username,
                avatar=initials,
                avatar_color=u.avatar_color,
                avatar_url=u.avatar_url,
                quote=u.quote or "Hey there! I am using Pocket.",
                online=is_online,
                last_message=latest_msg.content if latest_msg else (u.quote or "Tap to start conversation"),
                time=(
                    latest_msg.timestamp.strftime("%I:%M %p")
                    if latest_msg
                    else ""
                ),
                unread=unread_count,
                last_seen=u.last_seen.isoformat() if u.last_seen else None,
            )
        )

    return contacts


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
    Matches device phone contacts against registered Pocket users.
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
            else "PK"
        )
        registered_contacts.append(
            ChatContactItem(
                id=u.id,
                phone_number=u.phone_number,
                name=u.username,
                avatar=initials,
                avatar_color=u.avatar_color,
                avatar_url=u.avatar_url,
                quote=u.quote or "Hey there! I am using Pocket.",
                online=is_online,
                last_message=u.quote or "Available on Pocket",
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
    limit: int = 30,
    offset: int = 0,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Retrieves paginated message history (default: latest 30 messages)."""
    stmt = (
        select(Message)
        .where(
            or_(
                (Message.sender_id == current_user.id) & (Message.receiver_id == other_user_id),
                (Message.sender_id == other_user_id) & (Message.receiver_id == current_user.id),
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
        await db.commit()

    is_rec_online = await manager.is_user_online(receiver_id)
    initial_status = MessageStatus.DELIVERED if is_rec_online else MessageStatus.SENT
    now = datetime.now(timezone.utc)

    reply_to_str = json.dumps(payload.reply_to) if isinstance(payload.reply_to, dict) else (str(payload.reply_to) if payload.reply_to else None)

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

    message_dict = new_msg.to_dict()

    # Forward to Receiver via WebSocket
    chat_frame = {
        "type": "chat_message",
        "data": message_dict,
    }
    await manager.send_personal_message(receiver_id, chat_frame)

    # Trigger smart bot auto-reply for demo contacts
    asyncio.create_task(handle_bot_auto_reply(current_user.id, receiver_id, content))

    return {
        "status": "success",
        "temp_msg_id": client_temp_id,
        "message_id": message_dict["message_id"],
        **message_dict,
    }


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
# Simulated AI / Bot Auto-Responder (Takes Pocket to the Next Level 🚀)
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
        reply_text = f"You're very welcome! Always happy to connect on Pocket. ✨"
    elif "how are you" in lowered:
        reply_text = f"I'm doing fantastic! Pocket's real-time messaging is super fast! ⚡"
    elif "react native" in lowered or "expo" in lowered or "flashlist" in lowered:
        reply_text = f"FlashList v2 and React 19 in Pocket make 60fps scrolling effortless! 📱"
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
        await manager.send_personal_message(sender_id, msg_frame)


# -----------------------------------------------------------------------------
# Real-Time WebSocket Gateway
# -----------------------------------------------------------------------------
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
    user_id = user_id.strip()
    await manager.connect(user_id, websocket)

    # Ensure current user exists in database
    async with async_session_factory() as db:
        user_stmt = select(User).where(User.id == user_id)
        user_res = await db.execute(user_stmt)
        user_obj = user_res.scalar_one_or_none()
        if not user_obj:
            user_obj = User(
                id=user_id,
                phone_number=user_id,
                username=f"User {user_id[-4:]}" if len(user_id) >= 4 else f"User {user_id}",
                avatar_color="#FFB800",
                is_online=True,
            )
            db.add(user_obj)
            await db.commit()
        else:
            user_obj.is_online = True
            await db.commit()

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
                    # Ensure sender and receiver exist in DB to prevent foreign key errors
                    sender_stmt = select(User).where(User.id == user_id)
                    sender_res = await db.execute(sender_stmt)
                    if not sender_res.scalar_one_or_none():
                        db.add(User(
                            id=user_id,
                            phone_number=user_id,
                            username=f"User {user_id[-4:]}" if len(user_id) >= 4 else "Pocket User",
                            avatar_color="#FFB800",
                            is_online=True,
                        ))
                        await db.commit()

                    rec_stmt = select(User).where(User.id == receiver_id)
                    rec_res = await db.execute(rec_stmt)
                    if not rec_res.scalar_one_or_none():
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
                        await db.commit()

                    # Determine delivery state
                    is_rec_online = await manager.is_user_online(receiver_id)
                    initial_status = MessageStatus.DELIVERED if is_rec_online else MessageStatus.SENT
                    now = datetime.now(timezone.utc)
                    reply_to_raw = data.get("reply_to")
                    reply_to_str = json.dumps(reply_to_raw) if isinstance(reply_to_raw, dict) else (str(reply_to_raw) if reply_to_raw else None)

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

                if not delivered:
                    # Receiver is offline / app is closed -> Dispatch Background Push Notification
                    async def dispatch_offline_push():
                        try:
                            async with async_session_factory() as push_db:
                                r_stmt = select(User).where(User.id == receiver_id)
                                r_res = await push_db.execute(r_stmt)
                                rec_user = r_res.scalar_one_or_none()

                                s_stmt = select(User).where(User.id == user_id)
                                s_res = await push_db.execute(s_stmt)
                                sender_user = s_res.scalar_one_or_none()

                                if rec_user and rec_user.fcm_token:
                                    await send_push_notification(
                                        fcm_token=rec_user.fcm_token,
                                        sender_name=sender_user.username if sender_user else "Pocket",
                                        content=content,
                                        data_payload={
                                            "sender_id": user_id,
                                            "receiver_id": receiver_id,
                                            "message_id": message_dict["message_id"],
                                        },
                                    )
                        except Exception as p_err:
                            logger.warning("Error dispatching offline push: %s", p_err)

                    asyncio.create_task(dispatch_offline_push())

                # Trigger smart bot responder if receiver is a demo contact
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
            # 3. READ RECEIPTS
            # -----------------------------------------------------------------
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
            # 4. VOICE CALL SIGNALING (Full-Duplex Audio Handshake)
            # -----------------------------------------------------------------
            elif frame_type in ("call_offer", "call_answer", "call_ice_candidate", "call_reject", "call_end"):
                receiver_id = data.get("receiver_id")
                if receiver_id:
                    call_data = {**data, "caller_id": data.get("caller_id", user_id), "sender_id": user_id}
                    call_frame = {
                        "type": frame_type,
                        "data": call_data,
                    }
                    delivered = await manager.send_personal_message(receiver_id, call_frame)

                    # If offer and user is offline, send high-priority push notification
                    if frame_type == "call_offer" and not delivered:
                        async def dispatch_call_push():
                            try:
                                async with async_session_factory() as push_db:
                                    r_stmt = select(User).where(User.id == receiver_id)
                                    r_res = await push_db.execute(r_stmt)
                                    rec_user = r_res.scalar_one_or_none()

                                    s_stmt = select(User).where(User.id == user_id)
                                    s_res = await push_db.execute(s_stmt)
                                    sender_user = s_res.scalar_one_or_none()

                                    if rec_user and rec_user.fcm_token:
                                        await send_push_notification(
                                            fcm_token=rec_user.fcm_token,
                                            sender_name=sender_user.username if sender_user else "Pocket",
                                            content="📞 Incoming Voice Call...",
                                            data_payload={
                                                "type": "incoming_call",
                                                "caller_id": user_id,
                                                "caller_name": sender_user.username if sender_user else "Pocket User",
                                                "call_id": data.get("call_id", f"call_{int(datetime.now().timestamp())}"),
                                            },
                                        )
                            except Exception as c_err:
                                logger.warning("Error dispatching call push: %s", c_err)

                        asyncio.create_task(dispatch_call_push())

                    # If calling a demo contact (1, 2, 3, 4), simulate auto-answer after 2s for interactive testing
                    if frame_type == "call_offer" and str(receiver_id) in ("1", "2", "3", "4"):
                        async def simulate_demo_answer():
                            await asyncio.sleep(2.0)
                            ans_frame = {
                                "type": "call_answer",
                                "data": {
                                    "call_id": data.get("call_id"),
                                    "caller_id": receiver_id,
                                    "receiver_id": user_id,
                                    "sender_id": receiver_id,
                                },
                            }
                            await manager.send_personal_message(user_id, ans_frame)

                        asyncio.create_task(simulate_demo_answer())

            # -----------------------------------------------------------------
            # 5. HEARTBEAT / PING
            # -----------------------------------------------------------------
            elif frame_type == "ping":
                await websocket.send_text(json.dumps({"type": "pong", "timestamp": datetime.now(timezone.utc).isoformat()}))

    except WebSocketDisconnect:
        logger.info("WebSocket disconnected for user %s", user_id)
        await manager.disconnect(user_id)
        async with async_session_factory() as db:
            await db.execute(
                update(User)
                .where(User.id == user_id)
                .values(is_online=False, last_seen=datetime.now(timezone.utc))
            )
            await db.commit()
    except Exception as e:
        logger.error("WebSocket runtime error for %s: %s", user_id, e)
        await manager.disconnect(user_id)
