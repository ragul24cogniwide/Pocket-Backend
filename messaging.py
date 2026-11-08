"""Message delivery shared by the REST routes and the WebSocket gateway: sending a one-to-one
message, offline replay, the personal AI agent's replies and the demo bots."""

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional

from fastapi import HTTPException, WebSocket
from sqlalchemy import or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ai_agent import (
    AGENT_USER_ID,
    ensure_personal_agent_user,
    process_personal_agent_message,
)
from connection_manager import manager
from core import (
    AGENT_MESSAGES_PER_MINUTE,
    DEMO_BOTS_ENABLED,
    MAX_AGENT_MESSAGE_CHARS,
    MAX_METADATA_CHARS,
    dispatch_push_to_user,
    enforce_rate_limit,
    find_client_message,
    push_metadata_field,
    receipts_shared,
)
from database import async_session_factory
from identity import id_forms
from models import Message, MessageStatus, User
from notifications import chat_thread_key, send_clear_notification, send_push_notification
import groups
from schemas import MAX_MESSAGE_CHARS
from sync_service import record_conversation_change

logger = logging.getLogger("Main")

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
            # Same shape as a live chat_message frame (Message.to_dict(): metadata such as photos
            # and voice notes, reply_to quotes, ...); replayed frames used to drop both
            msg_payload = {
                "type": "chat_message",
                "data": {
                    **msg.to_dict(),
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

        tz_offset = None  # unknown unless the app sent it (the agent then assumes India time)
        if isinstance(client_metadata, dict):
            tz_offset = client_metadata.get("tz_offset_minutes")

        # Same limits as /api/agent/chat. The message itself is already stored, so over the
        # limit the agent answers with a short note instead of calling the AI.
        limit_note = None
        try:
            await check_agent_limits(user_id, content)
        except HTTPException as limit_err:
            limit_note = str(limit_err.detail)

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

            if limit_note:
                agent_result = {"reply": limit_note, "action": "general_chat"}
            else:
                # Process AI request with user's calendar and message context. The agent ends its
                # read transaction before calling Gemini, so no DB connection waits on the AI.
                agent_result = await process_personal_agent_message(current_user, content, db, tz_offset)
            await db.commit()  # meeting changes made by the agent, if any

        # Save the AI response in a new short session
        async with async_session_factory() as db:
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
# Simulated AI / Bot Auto-Responder (Takes Flick to the Next Level 🚀)
# -----------------------------------------------------------------------------
async def handle_bot_auto_reply(sender_id: str, receiver_id: str, user_content: str) -> None:
    """Demo-bot reply (SEED_DEMO_USERS only). Runs as a background task: never raises, logs instead."""
    try:
        await _handle_bot_auto_reply(sender_id, receiver_id, user_content)
    except Exception as e:
        logger.warning("Demo bot reply to %s failed: %s", sender_id, e)


async def _handle_bot_auto_reply(sender_id: str, receiver_id: str, user_content: str) -> None:
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
            sender_user = await read_db.get(User, sender_id)
            hidden_now = not receipts_shared(sender_user)  # same rule as real users
            await read_db.execute(
                update(Message)
                .where(
                    Message.sender_id == sender_id,
                    Message.receiver_id == receiver_id,
                    Message.status != "READ",
                )
                .values(status="READ", read_at=now_read, **({"receipt_hidden": True} if hidden_now else {}))
            )
            await read_db.commit()

        if not hidden_now:  # read either way; blue ticks only if the sender shares receipts
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

            # The bot is the receiver of the user's message (bot_id / bot_reply used to be
            # undefined names here, so this push always raised NameError)
            b_stmt = select(User).where(User.id == receiver_id)
            b_res = await db.execute(b_stmt)
            bot_user = b_res.scalar_one_or_none()

            if target_user and target_user.fcm_token:
                await send_push_notification(
                    fcm_token=target_user.fcm_token,
                    sender_name=bot_user.username if bot_user else bot_name,
                    content=reply_text,
                    data_payload={
                        "sender_id": receiver_id,
                        "receiver_id": sender_id,
                        "message_id": bot_msg.id,
                    },
                )


# The personal AI agent: idempotent replies and per-user limits (used by the REST route and the gateway)
def agent_reply_client_id(user_id: str, client_msg_id: str) -> str:
    """
    client_msg_id of the agent's answer to a user's question. The agent is the sender of every
    answer, so the key must include the user (two people can pick the same id), and it must fit
    the 64-character column: hence a hash.
    """
    digest = hashlib.sha256(f"{user_id}:{client_msg_id}".encode()).hexdigest()[:40]
    return f"reply:{digest}"


async def find_agent_reply(db: AsyncSession, user_id: str, client_msg_id: Optional[str]) -> Optional[Message]:
    """The agent's stored answer to this user's question, if any (also the older "reply:<id>" form)."""
    if not client_msg_id:
        return None
    res = await db.execute(
        select(Message).where(
            Message.sender_id == AGENT_USER_ID,
            Message.receiver_id == user_id,
            Message.client_msg_id.in_([agent_reply_client_id(user_id, client_msg_id), f"reply:{client_msg_id}"]),
        )
    )
    return res.scalars().first()


async def check_agent_limits(user_id: str, content: str) -> None:
    """Personal agent: message length cap (400) and per-user rate limit (429); each message is an LLM call."""
    if len(content or "") > MAX_AGENT_MESSAGE_CHARS:
        raise HTTPException(
            status_code=400,
            detail=f"Message is too long for the AI agent (max {MAX_AGENT_MESSAGE_CHARS} characters).",
        )
    await enforce_rate_limit(
        f"agent:{user_id}", AGENT_MESSAGES_PER_MINUTE, 60, "You're sending messages to the AI agent too quickly."
    )


# -----------------------------------------------------------------------------
# Sending a one-to-one message (REST POST /api/messages/send and the WebSocket "send_message")
# -----------------------------------------------------------------------------
NOT_ON_FLICK = "This number isn’t on Flick"
TOO_LONG = "Message is too long"


async def resolve_reply_ref(db: AsyncSession, reply_to: Any, sender_id: str, receiver_id: str) -> Any:
    """
    A reply quoting a message by the id its phone gave it while sending (the quote was made before
    the server's id arrived) is stored with the server's id, so tapping the quote finds the message
    on every device. Only messages of this chat (sent by either person) are looked at.
    """
    ref = reply_to.get("id") if isinstance(reply_to, dict) else None
    if not ref:
        return reply_to
    people = id_forms(sender_id, receiver_id)
    quoted = (
        await db.execute(
            select(Message.id).where(
                Message.client_msg_id == str(ref),
                Message.sender_id.in_(people),
                Message.receiver_id.in_(people),
            )
        )
    ).scalars().first()
    return {**reply_to, "id": quoted} if quoted is not None and quoted != ref else reply_to


class SendError(Exception):
    """The message can't be sent: REST answers `status_code`, the socket a message_error with `reason`."""

    def __init__(self, status_code: int, reason: str):
        super().__init__(reason)
        self.status_code = status_code
        self.reason = reason


@dataclass
class SendResult:
    message: dict          # Message.to_dict() of the stored message
    duplicate: bool        # True: this send was a retry of one already stored (nothing new was sent)


async def send_direct_message(
    sender_id: str,
    receiver_id: str,
    content: str,
    client_msg_id: Optional[str] = None,
    reply_to: Any = None,
    metadata: Any = None,
    *,
    sender_name: Optional[str] = None,
    exclude_ws: Optional[WebSocket] = None,
    on_stored: Optional[Callable[[dict], Awaitable[None]]] = None,
) -> SendResult:
    """
    Stores a one-to-one message once and delivers it everywhere it has to go:

      1. a retry of a message already stored (same sender + client_msg_id) returns that message
      2. too long / unknown receiver -> SendError (nothing stored)
      3. stored as SENT, or DELIVERED when the receiver is connected right now
      4. delta-sync change records for both people
      5. `on_stored(message)` (the gateway acknowledges the sender here, before anything else)
      6. the receiver's devices, the sender's other devices (not `exclude_ws`), a push notification,
         and the personal AI agent's reply / a demo bot reply

    Two copies of the same send arriving at once (socket + REST fallback) can't both be stored:
    the unique index on (sender_id, client_msg_id) keeps one and the other returns it.
    """
    content = (content or "").strip()
    if client_msg_id:
        async with async_session_factory() as db:
            existing = await find_client_message(db, sender_id, client_msg_id)
        if existing is not None:
            return SendResult(existing.to_dict(), duplicate=True)
    if len(content) > MAX_MESSAGE_CHARS or len(json.dumps(metadata or {})) > MAX_METADATA_CHARS:
        raise SendError(413, TOO_LONG)

    async with async_session_factory() as db:
        found = await db.execute(select(User).where(User.id.in_([sender_id, receiver_id])))
        users = {u.id: u for u in found.scalars().all()}
        if receiver_id not in users:
            # Only real accounts can receive messages (no placeholder users for unknown ids)
            alt = (await db.execute(select(User).where(User.phone_number == receiver_id))).scalars().first()
            if alt is None:
                raise SendError(404, NOT_ON_FLICK)
            receiver_id = alt.id
            users[receiver_id] = alt

        is_rec_online = await manager.is_user_online(receiver_id)
        now = datetime.now(timezone.utc)
        extra = {}
        if reply_to:
            extra["reply_to"] = await resolve_reply_ref(db, reply_to, sender_id, receiver_id)
        if metadata:
            extra["metadata"] = metadata
        new_msg = Message(
            sender_id=sender_id,
            receiver_id=receiver_id,
            content=content,
            reply_to=json.dumps(extra) if extra else None,
            timestamp=now,
            status=MessageStatus.DELIVERED if is_rec_online else MessageStatus.SENT,
            delivered_at=now if is_rec_online else None,
            client_msg_id=client_msg_id,
            receipt_hidden=not receipts_shared(users.get(sender_id), users.get(receiver_id)),
        )
        db.add(new_msg)
        try:
            await db.commit()
        except IntegrityError:
            await db.rollback()
            existing = await find_client_message(db, sender_id, client_msg_id)
            if existing is None:
                raise
            return SendResult(existing.to_dict(), duplicate=True)
        await db.refresh(new_msg)

        try:
            await record_conversation_change(db, user_id=sender_id, peer_id=receiver_id, action="updated")
            await record_conversation_change(db, user_id=receiver_id, peer_id=sender_id, action="updated")
            await db.commit()
        except Exception as sync_err:
            logger.warning("Failed to record conversation change for send: %s", sync_err)

        message_dict = new_msg.to_dict()

    if on_stored is not None:
        await on_stored(message_dict)

    await manager.send_personal_message(receiver_id, {"type": "chat_message", "data": message_dict})
    # The sender's other devices (phone / browsers) show it as sent too
    await manager.send_to_my_devices(sender_id, {"type": "message_sent_sync", "data": message_dict}, exclude=exclude_ws)

    # Push notification so background / closed devices get a tray alert
    push_data = {"type": "chat_message", "message_id": message_dict["message_id"], "sender_id": sender_id}
    if sender_name is not None:
        push_data["sender_name"] = sender_name
    push_data.update(push_metadata_field(message_dict.get("metadata")))
    asyncio.create_task(dispatch_push_to_user(
        sender_id=sender_id, receiver_id=receiver_id, content=content, data_payload=push_data,
    ))

    # The personal AI agent answers; demo contacts answer when demo bots are on
    if receiver_id == AGENT_USER_ID or receiver_id.startswith("agent_"):
        asyncio.create_task(handle_personal_agent_reply(sender_id, content, metadata))
    elif DEMO_BOTS_ENABLED:
        asyncio.create_task(handle_bot_auto_reply(sender_id, receiver_id, content))

    return SendResult(message_dict, duplicate=False)


async def clear_chat_notification(user_id: str, chat_id: str) -> None:
    """The chat was read on another device (Flick Web): remove its notification from the phone."""
    try:
        async with async_session_factory() as db:
            user = await db.get(User, user_id)
        if user is not None and user.fcm_token:
            await send_clear_notification(user.fcm_token, chat_thread_key(chat_id))
    except Exception as e:
        logger.warning("Could not clear the chat notification of %s: %s", user_id, e)


async def read_group_and_clear(user_id: str, group_id: str) -> None:
    """A group read on Flick Web: mark it read, then remove its notification from the phone."""
    await groups.mark_group_read(user_id, group_id)
    await clear_chat_notification(user_id, group_id)


async def mark_messages_delivered(user_id: str, message_ids: list) -> int:
    """
    These messages reached this person's phone (over the socket, or as a push while the app was
    closed): SENT becomes DELIVERED and each sender sees grey double ticks. Only messages sent TO
    this person count (no fake receipts); read or already delivered ones are left as they are.
    Returns how many changed.
    """
    ids = [str(i) for i in dict.fromkeys(message_ids or []) if i][:200]
    if not ids:
        return 0
    now = datetime.now(timezone.utc)
    async with async_session_factory() as db:
        me = await db.get(User, user_id)
        mine = id_forms(user_id, me.phone_number if me else None)
        rows = (
            await db.execute(
                select(Message.id, Message.sender_id).where(
                    Message.id.in_(ids),
                    Message.receiver_id.in_(mine),
                    Message.status == MessageStatus.SENT,
                )
            )
        ).all()
        if not rows:
            return 0
        await db.execute(
            update(Message)
            .where(Message.id.in_([r.id for r in rows]), Message.status == MessageStatus.SENT)
            .values(status=MessageStatus.DELIVERED, delivered_at=now)
        )
        await db.commit()
    for r in rows:
        await manager.send_personal_message(r.sender_id, {
            "type": "ack_delivered",
            "data": {"message_id": r.id, "receiver_id": user_id, "delivered_at": now.isoformat()},
        })
    return len(rows)
