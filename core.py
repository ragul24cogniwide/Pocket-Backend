"""Shared settings and helpers used by the API routes and the WebSocket gateway."""

import json
import logging
import os
from typing import Optional, Any

import hmac
from collections import defaultdict

from fastapi import Header, HTTPException, Request, status
from sqlalchemy import desc, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from identity import id_forms
from database import async_session_factory
from models import Message, SavedContact, User
from notification_stack import direct_chat_notification
from notifications import chat_thread_key, send_push_notification
import rate_limits
from ai_agent import (
    AGENT_USER_ID,
)


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

# OTP throttling. Counts live in Redis (shared by every server process, updated atomically) and
# fall back to this process's memory when Redis isn't available (see rate_limits.py).
# Per number: one code per OTP_RESEND_COOLDOWN_SECONDS and OTP_MAX_SENDS_PER_HOUR codes an hour
# (the existing limits, now shared). Per client IP: OTP_MAX_SENDS_PER_IP_PER_HOUR codes an hour,
# so one client can't request codes for thousands of different numbers.
OTP_MAX_SENDS_PER_IP_PER_HOUR = int(os.getenv("OTP_MAX_SENDS_PER_IP_PER_HOUR", "30"))

# Other per-user limits (429 when exceeded)
PROFILE_LOOKUPS_PER_HOUR = 120   # GET /api/users/{number}: enough for normal use, too few to scan numbers
UPLOADS_PER_HOUR = 60            # photos + voice notes
AGENT_MESSAGES_PER_MINUTE = 20   # personal AI agent (each one is a paid LLM call)
MAX_AGENT_MESSAGE_CHARS = 4000
# Largest request body accepted at all (a 10 MB upload is ~13.4 MB as base64 + JSON overhead)
MAX_REQUEST_BODY_BYTES = 15 * 1024 * 1024
# Longest base64 string that can decode to MAX_UPLOAD_BYTES (4 chars per 3 bytes, plus padding)
MAX_UPLOAD_BASE64_CHARS = ((MAX_UPLOAD_BYTES + 2) // 3) * 4

# Contact checks answer "is this number on Flick?", so each account may check a limited number of
# *different* numbers per day: enough for any address book, too few to scan number ranges.
# Re-checking numbers already checked today is free, so reopening the contact screen never hits it.
CONTACT_CHECK_MAX_NUMBERS_PER_DAY = 10_000
_contact_check_log: dict[str, dict[str, float]] = defaultdict(dict)  # user id -> {number: first checked}


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


def client_ip(request: Request) -> str:
    """
    The caller's IP address, for per-IP limits. Behind Render's proxy request.client is the proxy
    (uvicorn only trusts X-Forwarded-For from 127.0.0.1 by default), so the first X-Forwarded-For
    entry is used when present. A client can forge that header, which only lets it dodge the
    per-IP limit (the per-number limits still apply); trusting the proxy address instead would
    put every user behind one shared limit.
    """
    forwarded = request.headers.get("x-forwarded-for", "")
    first = forwarded.split(",")[0].strip() if forwarded else ""
    if first:
        return first[:64]
    return (request.client.host if request.client else "") or "unknown"


async def enforce_rate_limit(key: str, limit: int, window_seconds: int, message: str) -> None:
    """Counts one request; raises 429 (with Retry-After) when `key` is over `limit` per window."""
    retry_after = await rate_limits.hit(key, limit, window_seconds)
    if retry_after is not None:
        logger.warning("[RateLimit] %s over %d per %ds", key, limit, window_seconds)
        raise HTTPException(
            status_code=429,
            detail=f"{message} Try again in {rate_limits.wait_text(retry_after)}.",
            headers={"Retry-After": str(retry_after)},
        )


async def deliver_otp_sms(phone: str, otp_code: str) -> bool:
    """
    Sends the OTP to the user's phone. Returns True when an SMS provider accepted it.

    TODO: wire up an SMS provider here (Twilio Verify, MSG91, AWS SNS, ...). Until then,
    the only way to read the code is DEV_MODE_EXPOSE_OTP, which is not safe in production.
    """
    return False


from schemas import MAX_MESSAGE_CHARS  # noqa: E402 (shared with EditMessageRequest's limit)
MAX_METADATA_CHARS = 20_000
MAX_WS_FRAME_CHARS = 100_000
MAX_CONTACT_LOOKUP = 500
DEMO_BOTS_ENABLED = os.getenv("SEED_DEMO_USERS", "false").lower() == "true"


async def chat_peer_ids(db: AsyncSession, user: User) -> set:
    """Ids (and phone numbers) of everyone this user has exchanged messages with, plus the AI agent."""
    mine = {i for i in (user.id, user.phone_number) if i}
    # Each person once (DISTINCT), not one row per message: this runs every few seconds per user
    sent_to = await db.execute(select(Message.receiver_id).where(Message.sender_id.in_(mine)).distinct())
    got_from = await db.execute(select(Message.sender_id).where(Message.receiver_id.in_(mine)).distinct())
    ids = {p for (p,) in [*sent_to.all(), *got_from.all()] if p and p not in mine}
    if ids:
        res = await db.execute(select(User.id, User.phone_number).where(or_(User.id.in_(ids), User.phone_number.in_(ids))))
        for uid, phone in res.all():
            ids.update(x for x in (uid, phone) if x)
    ids.add(AGENT_USER_ID)
    return ids


async def _with_phone_aliases(db: AsyncSession, ids: set) -> set:
    """Adds each account's id and phone number to a set of ids (messages may use either form)."""
    if ids:
        res = await db.execute(select(User.id, User.phone_number).where(or_(User.id.in_(ids), User.phone_number.in_(ids))))
        for uid, phone in res.all():
            ids.update(x for x in (uid, phone) if x)
    return ids


# -----------------------------------------------------------------------------
# Who may see whom (statuses, online / last seen, profile "about")
#
# Owner O is visible to viewer V when O has reached out to V or keeps V as a contact:
#   - O has sent V at least one message, or
#   - O has V in their saved contacts (SavedContact, reported by O's phone).
# Only O's own actions count. Before, anyone V had *sent* a message to counted, so a stranger
# could send "hi" to a number and from then on see that person's statuses and live presence.
# A normal two-way chat (both have written) makes both sides visible to each other.
# -----------------------------------------------------------------------------
async def visible_owner_ids(db: AsyncSession, viewer: User) -> set:
    """Ids (and phone numbers) of everyone whose statuses / presence `viewer` may see, plus the AI agent."""
    mine = {i for i in (viewer.id, viewer.phone_number) if i}
    # They wrote to me (DISTINCT: one row per person, not per message; this runs on every connect)
    wrote_to_me = await db.execute(select(Message.sender_id).where(Message.receiver_id.in_(mine)).distinct())
    # They saved me as a contact
    saved_me = await db.execute(select(SavedContact.user_id).where(SavedContact.contact_id.in_(mine)).distinct())
    ids = {p for (p,) in [*wrote_to_me.all(), *saved_me.all()] if p and p not in mine}
    await _with_phone_aliases(db, ids)
    ids.add(AGENT_USER_ID)
    return ids


async def is_visible_to(db: AsyncSession, owner: User, viewer: User) -> bool:
    """Whether `viewer` may see `owner`'s statuses / presence / about (same rule as visible_owner_ids)."""
    if owner.id == viewer.id or owner.id == AGENT_USER_ID:
        return True
    owner_ids = [i for i in (owner.id, owner.phone_number) if i]
    viewer_ids = [i for i in (viewer.id, viewer.phone_number) if i]
    wrote = await db.execute(
        select(Message.id).where(Message.sender_id.in_(owner_ids), Message.receiver_id.in_(viewer_ids)).limit(1)
    )
    if wrote.first():
        return True
    saved = await db.execute(
        select(SavedContact.id).where(SavedContact.user_id == owner.id, SavedContact.contact_id.in_(viewer_ids)).limit(1)
    )
    return saved.first() is not None


async def audience_ids(db: AsyncSession, owner: User) -> set:
    """The reverse of visible_owner_ids: everyone who may see `owner` (for live status updates)."""
    mine = {i for i in (owner.id, owner.phone_number) if i}
    wrote_to = await db.execute(select(Message.receiver_id).where(Message.sender_id.in_(mine)).distinct())
    saved = await db.execute(select(SavedContact.contact_id).where(SavedContact.user_id == owner.id).distinct())
    return {p for (p,) in [*wrote_to.all(), *saved.all()] if p and p not in mine}


# Message metadata (e.g. a photo's URL) goes into the push only when this small: FCM data
# values must be strings and the whole push must stay under 4 KB (see notifications.py)
MAX_PUSH_METADATA_CHARS = 1500


def push_metadata_field(metadata: Any) -> dict:
    """{"metadata": "<json>"} for a chat push when the message has small metadata, else {}."""
    if not metadata:
        return {}
    try:
        text = json.dumps(metadata, separators=(",", ":"))
    except (TypeError, ValueError):
        return {}
    return {"metadata": text} if len(text) < MAX_PUSH_METADATA_CHARS else {}


def receipts_shared(*users: Optional[User]) -> bool:
    """Blue ticks only when everyone involved has read receipts on (like WhatsApp, it works both ways)."""
    return all(u.prefs().get("read_receipts", True) for u in users if u is not None)


async def find_client_message(db: AsyncSession, sender_id: str, client_msg_id: Optional[str]) -> Optional[Message]:
    """Message this sender already stored under the app-generated id, if any (a retried send)."""
    if not client_msg_id:
        return None
    res = await db.execute(
        select(Message).where(Message.sender_id == sender_id, Message.client_msg_id == client_msg_id)
    )
    return res.scalars().first()


async def dispatch_push_to_user(
    sender_id: str,
    receiver_id: str,
    content: str,
    data_payload: Optional[dict] = None,
):
    """Dispatches high-priority push notification to offline receiver with phone tolerance."""
    try:
        async with async_session_factory() as push_db:
            # 1. Look up receiver with active token prioritization (exact spellings of the same
            # full number only: a suffix match could pick someone else's phone, see identity.py)
            rec_forms = id_forms(receiver_id)
            r_stmt = (
                select(User)
                .where(or_(User.id.in_(rec_forms), User.phone_number.in_(rec_forms)))
                .order_by(desc(User.fcm_token.isnot(None)), desc(User.last_seen))
            )
            r_res = await push_db.execute(r_stmt)
            rec_user = r_res.scalars().first()

            # 2. Look up sender name
            send_forms = id_forms(sender_id)
            s_stmt = (
                select(User)
                .where(or_(User.id.in_(send_forms), User.phone_number.in_(send_forms)))
                .order_by(desc(User.last_seen))
            )
            s_res = await push_db.execute(s_stmt)
            sender_user = s_res.scalars().first()

            if rec_user and rec_user.fcm_token:
                prefs = rec_user.prefs()
                is_call_push = (data_payload or {}).get("type") in (
                    "incoming_call", "call_answered", "call_rejected", "call_ended",
                )
                # Calls always ring; message pushes follow the receiver's notification settings
                if not is_call_push:
                    if not prefs.get("msg_notifications", True):
                        logger.info("[Push] Skipped for %s: message notifications are off", receiver_id)
                        return
                    if not prefs.get("preview", True):
                        content = "New message"
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
                thread_key = None
                if (data_payload or {}).get("type") == "chat_message":
                    # One notification per chat (like WhatsApp): it lists this chat's unread
                    # messages and replaces the previous one (see notification_stack.py)
                    chat_id = sender_user.id if sender_user else sender_id
                    thread_key = chat_thread_key(chat_id)
                    sender_title, content = await direct_chat_notification(
                        push_db, chat_id, rec_user.id, sender_title, content, preview=prefs.get("preview", True)
                    )
                await send_push_notification(
                    fcm_token=rec_user.fcm_token,
                    sender_name=sender_title,
                    content=content,
                    data_payload=payload_dict,
                    silent=not is_call_push and not prefs.get("sound", True),
                    thread_key=thread_key,
                )
            else:
                logger.info("[Push] User %s has no active FCM token registered (rec_user=%s)", receiver_id, rec_user is not None)
    except Exception as err:
        logger.warning("Error dispatching offline push to %s: %s", receiver_id, err)


async def find_user(db: AsyncSession, any_id: str) -> Optional[User]:
    """A user by id or phone number, in any spelling of the same full number (see identity.py)."""
    uid = str(any_id or "").strip()
    if not uid:
        return None
    # An exact match first; other spellings of the number only when there is none
    exact = (await db.execute(select(User).where(or_(User.id == uid, User.phone_number == uid)))).scalars().first()
    if exact is not None:
        return exact
    forms = id_forms(uid)
    res = await db.execute(select(User).where(or_(User.id.in_(forms), User.phone_number.in_(forms))))
    return res.scalars().first()


async def resolve_user_context(user_opt: Optional[User], db: AsyncSession) -> User:
    """Returns the authenticated user. Unauthenticated callers are rejected rather than mapped to a shared demo account."""
    if user_opt:
        return user_opt
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired session. Please log in again.",
        headers={"WWW-Authenticate": "Bearer"},
    )
