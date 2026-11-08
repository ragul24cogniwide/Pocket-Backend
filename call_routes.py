"""Calls (voice & video): call sessions, signaling (REST and WebSocket) and ICE servers."""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import hashlib
import hmac
import re
import time

from fastapi import Depends, HTTPException
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from auth import (
    get_current_user,
    same_identity,
)
from connection_manager import manager
from identity import id_forms
from database import async_session_factory
from models import Message, SavedContact, User
from schemas import (
    CallAnswerRequest,
    CallEndRequest,
    CallOfferRequest,
    CallRejectRequest,
)
import calllog
from fastapi import APIRouter
import base64
from core import dispatch_push_to_user, find_user

logger = logging.getLogger("Main")

router = APIRouter()

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


_call_target_cache: dict[str, str] = {}


async def resolve_call_target(target_id: str) -> str:
    """The account id behind a call participant id (cached: every candidate of a call uses it)."""
    key = str(target_id or "").strip()
    if key in _call_target_cache:
        return _call_target_cache[key]
    resolved = key
    try:
        async with async_session_factory() as call_db:
            user_obj = (
                await call_db.execute(select(User).where(or_(User.id == key, User.phone_number == key)))
            ).scalars().first()
            if user_obj is None:
                # Ids saved by older app versions in another spelling (e.g. without the country
                # code): the same full number only, never a different number with the same ending
                forms = id_forms(key)
                user_obj = (
                    await call_db.execute(select(User).where(or_(User.id.in_(forms), User.phone_number.in_(forms))))
                ).scalars().first()
            if user_obj is not None:
                resolved = user_obj.id
    except Exception as e:
        logger.warning("[CallRouting] lookup failed for %s: %s", key, e)
    if len(_call_target_cache) > 5000:
        _call_target_cache.clear()
    _call_target_cache[key] = resolved
    return resolved


async def broadcast_call_frame(target_id: str, frame_type: str, data: dict, sender_id: Optional[str] = None) -> bool:
    """
    Delivers a call signaling frame to every open socket of the other participant, once.
    (Sending it to each spelling of their number used to deliver every frame several times.)
    """
    target = await resolve_call_target(target_id)
    if not target or (sender_id and same_identity(target, sender_id)):
        return False
    return await manager.send_personal_message(target, {"type": frame_type, "data": data})

# -----------------------------------------------------------------------------
# Calls (voice & video): signaling only. Audio/video flows directly between the two
# phones over WebRTC (DTLS-SRTP encrypted end to end); the server never sees the media.
# Every signaling message is bound to the call's two participants, so nobody else can
# answer, hang up, inject network candidates into, or listen in on someone's call.
# -----------------------------------------------------------------------------
CALL_ID_RE = re.compile(r"^[A-Za-z0-9_\-]{6,80}$")
MAX_SDP_CHARS = 30_000
MAX_ICE_CHARS = 4_000
MAX_AUDIO_CHUNK_CHARS = 64_000
TURN_CREDENTIAL_TTL_SECONDS = 6 * 3600
CALL_SIGNAL_TYPES = (
    "call_offer", "call_answer", "call_ice_candidate", "call_reject", "call_end",
    "call_media_state", "call_audio_chunk",
)


class CallError(Exception):
    def __init__(self, reason: str, status_code: int = 403):
        super().__init__(reason)
        self.reason = reason
        self.status_code = status_code


def call_party(session: dict, user_id: str) -> Optional[str]:
    """'caller' or 'receiver' when user_id takes part in this call, otherwise None."""
    if same_identity(session.get("caller_id"), user_id):
        return "caller"
    if same_identity(session.get("receiver_id"), user_id):
        return "receiver"
    return None


def caller_card(user: Optional[User], fallback_id: str) -> dict:
    name = (user.username if user else None) or f"User {str(fallback_id)[-4:]}"
    return {
        "caller_name": name,
        "caller_avatar": name[:2].upper(),
        "caller_color": (user.avatar_color if user else None) or "#FFB800",
        "caller_avatar_url": user.avatar_url if user else None,
    }


CALL_NOT_ALLOWED = "You can call people in your contacts, and anyone who has messaged or called you."


async def may_call(db: AsyncSession, caller: User, receiver: User) -> bool:
    """
    Who can ring whom (so strangers can't ring a number they guessed): the caller has the
    receiver in their phone contacts, or the receiver has reached out to the caller before
    (messaged or called them), so you can always call back.
    """
    saved = await db.execute(
        select(SavedContact.id).where(SavedContact.user_id == caller.id, SavedContact.contact_id == receiver.id).limit(1)
    )
    if saved.first():
        return True
    caller_ids = [i for i in (caller.id, caller.phone_number) if i]
    receiver_ids = [i for i in (receiver.id, receiver.phone_number) if i]
    messaged = await db.execute(
        select(Message.id).where(Message.sender_id.in_(receiver_ids), Message.receiver_id.in_(caller_ids)).limit(1)
    )
    if messaged.first():
        return True
    return await calllog.has_called(db, caller=receiver.id, receiver=caller.id)


async def process_call_signal(user_id: str, frame_type: str, data: dict) -> dict:
    """
    Validates one call signaling message from `user_id`, updates the call session and forwards
    it to the other participant only. Raises CallError when the message isn't allowed.
    """
    data = data if isinstance(data, dict) else {}
    call_id = str(data.get("call_id") or "")
    if not CALL_ID_RE.match(call_id):
        raise CallError("Invalid call id", 400)
    now_iso = datetime.now(timezone.utc).isoformat()
    session = await get_call_session(call_id)

    sdp = data.get("sdp")
    if sdp is not None and (not isinstance(sdp, str) or len(sdp) > MAX_SDP_CHARS):
        raise CallError("Invalid session description", 400)

    # ---- New call ----
    if frame_type == "call_offer":
        receiver_id = str(data.get("receiver_id") or "").strip()
        if not receiver_id or same_identity(receiver_id, user_id):
            raise CallError("Invalid receiver", 400)
        if session:
            # Re-sent offer from the same caller is fine; anyone else reusing the id is refused
            if call_party(session, user_id) != "caller" or not same_identity(session.get("receiver_id"), receiver_id):
                raise CallError("This call id is already in use", 409)
            if session.get("status") in ("ended", "rejected"):
                raise CallError("This call has already finished", 409)
        first_offer = not session
        async with async_session_factory() as db:
            caller = (await db.execute(select(User).where(User.id == user_id))).scalars().first()
            if first_offer:  # a re-sent offer for an allowed call is fine
                receiver = await find_user(db, receiver_id)
                if not receiver:
                    raise CallError("This number isn't on Flick.", 404)
                if not caller or not await may_call(db, caller, receiver):
                    raise CallError(CALL_NOT_ALLOWED, 403)
        card = caller_card(caller, user_id)
        session = {
            **(session or {}),
            "call_id": call_id,
            "caller_id": user_id,
            "receiver_id": receiver_id,
            "video": bool(data.get("video") or (session or {}).get("video")),
            "status": "ringing",
            **card,
            "created_at": (session or {}).get("created_at", now_iso),
            "updated_at": now_iso,
        }
        if sdp:
            session["offer_sdp"] = sdp
        await save_call_session(call_id, session)

        frame = {
            "call_id": call_id,
            "caller_id": user_id,
            "sender_id": user_id,
            "receiver_id": receiver_id,
            "video": session["video"],
            "sdp": session.get("offer_sdp"),
            **card,
        }
        delivered = await broadcast_call_frame(receiver_id, "call_offer", frame, sender_id=user_id)
        if first_offer:
            asyncio.create_task(calllog.record_offer(call_id, user_id, receiver_id, session["video"]))
            kind = "video" if session["video"] else "voice"
            asyncio.create_task(dispatch_push_to_user(
                sender_id=user_id,
                receiver_id=receiver_id,
                content=f"{'📹' if session['video'] else '📞'} Incoming {kind} call from {card['caller_name']}",
                data_payload={
                    "type": "incoming_call",
                    "call_id": call_id,
                    "caller_id": str(user_id),
                    "video": "1" if session["video"] else "0",
                    **{k: (v or "") for k, v in card.items()},
                },
            ))
        return {"status": "ringing", "call_id": call_id, "delivered": delivered}

    # ---- Everything else needs an existing call this user is part of ----
    if not session:
        raise CallError("Unknown call", 404)
    party = call_party(session, user_id)
    if not party:
        raise CallError("You are not part of this call", 403)
    other = session["receiver_id"] if party == "caller" else session["caller_id"]
    status = session.get("status")
    finished = status in ("ended", "rejected")
    out: dict = {}
    push = None

    if frame_type == "call_answer":
        if party != "receiver":
            raise CallError("Only the person being called can answer", 403)
        if status != "ringing":
            raise CallError("This call is no longer ringing", 409)
        session.update({"status": "answered", "answered_at": now_iso})
        asyncio.create_task(calllog.record_answer(call_id))
        if sdp:
            session["answer_sdp"] = sdp  # lets the caller recover it via /api/call/status if the socket frame is missed
        out = {"sdp": sdp, "video": session.get("video", False)}
        push = ("Call answered", {"type": "call_answered"})
    elif frame_type == "call_ice_candidate":
        if finished:
            return {"status": status, "call_id": call_id}
        cand = data.get("candidate")
        if len(json.dumps(cand or {})) > MAX_ICE_CHARS:
            raise CallError("Invalid network candidate", 400)
        out = {"candidate": cand}
    elif frame_type == "call_media_state":
        out = {"video_enabled": bool(data.get("video_enabled", True)), "audio_enabled": bool(data.get("audio_enabled", True))}
    elif frame_type == "call_audio_chunk":  # legacy relay used by older app versions
        chunk = data.get("chunk")
        if status != "answered" or not isinstance(chunk, str) or len(chunk) > MAX_AUDIO_CHUNK_CHARS:
            return {"status": status, "call_id": call_id}
        out = {"chunk": chunk}
    elif frame_type == "call_reject":
        if finished:
            return {"status": status, "call_id": call_id}
        reason = str(data.get("reason") or "declined")[:32]
        session.update({"status": "rejected", "reason": reason})
        asyncio.create_task(calllog.record_finish(call_id, "busy" if reason == "busy" else "declined"))
        out = {"reason": reason}
        push = ("Call declined", {"type": "call_rejected", "reason": reason})
    elif frame_type == "call_end":
        if finished:
            return {"status": status, "call_id": call_id}
        try:
            duration = max(0, int(data.get("duration") or 0))
        except (TypeError, ValueError):
            duration = 0
        session.update({"status": "ended", "duration": duration})
        asyncio.create_task(calllog.record_finish(call_id, None, duration))
        out = {"duration": duration}
        push = ("Call ended", {"type": "call_ended"})
    else:
        raise CallError("Unsupported call message", 400)

    if frame_type in ("call_answer", "call_reject", "call_end"):
        session["updated_at"] = now_iso
        await save_call_session(call_id, session)
    if frame_type in ("call_answer", "call_reject") and party == "receiver":
        # The callee may have the phone and Flick Web open: the device that didn't pick up stops ringing
        await manager.send_to_my_devices(user_id, {"type": "call_claimed", "data": {
            "call_id": call_id, "action": "answered" if frame_type == "call_answer" else "declined",
        }})

    out.update({
        "call_id": call_id,
        "sender_id": user_id,
        "caller_id": session["caller_id"],
        "receiver_id": session["receiver_id"],
    })
    delivered = await broadcast_call_frame(other, frame_type, out, sender_id=user_id)
    if push:
        asyncio.create_task(dispatch_push_to_user(
            sender_id=user_id, receiver_id=other, content=push[0],
            data_payload={**push[1], "call_id": call_id},
        ))
    return {"status": session.get("status"), "call_id": call_id, "delivered": delivered}


async def _rest_call_signal(user: User, frame_type: str, data: dict) -> dict:
    try:
        return await process_call_signal(user.id, frame_type, data)
    except CallError as ce:
        raise HTTPException(status_code=ce.status_code, detail=ce.reason)


@router.post("/api/call/offer")
async def call_offer_endpoint(payload: CallOfferRequest, current_user: User = Depends(get_current_user)):
    """Starts a call (voice or video). The caller is always the signed-in user."""
    return await _rest_call_signal(current_user, "call_offer", payload.model_dump())


@router.post("/api/call/answer")
async def call_answer_endpoint(payload: CallAnswerRequest, current_user: User = Depends(get_current_user)):
    """Answers a ringing call; only the person being called can do this."""
    return await _rest_call_signal(current_user, "call_answer", payload.model_dump())


@router.post("/api/call/reject")
async def call_reject_endpoint(payload: CallRejectRequest, current_user: User = Depends(get_current_user)):
    """Declines a call (either participant)."""
    return await _rest_call_signal(current_user, "call_reject", payload.model_dump())


@router.post("/api/call/end")
async def call_end_endpoint(payload: CallEndRequest, current_user: User = Depends(get_current_user)):
    """Hangs up a call (either participant)."""
    return await _rest_call_signal(current_user, "call_end", payload.model_dump())


@router.get("/api/call/status/{call_id}")
async def get_call_status(call_id: str, current_user: User = Depends(get_current_user)):
    """Call state for its two participants only (the callee also gets the offer, e.g. after a push wake-up)."""
    session = await get_call_session(call_id)
    party = call_party(session, current_user.id) if session else None
    if not party:
        return {"call_id": call_id, "status": "unknown"}
    out = {
        "call_id": call_id,
        "status": session.get("status", "unknown"),
        "video": bool(session.get("video")),
        "caller_id": session.get("caller_id"),
        "receiver_id": session.get("receiver_id"),
        "created_at": session.get("created_at"),
        "answered_at": session.get("answered_at"),
        **{k: session.get(k) for k in ("caller_name", "caller_avatar", "caller_color", "caller_avatar_url")},
    }
    # Each side can fetch what it needs from the other: the callee the offer (e.g. after a
    # push wake-up), the caller the answer (if the live socket message was missed)
    if party == "receiver" and session.get("status") == "ringing":
        out["sdp"] = session.get("offer_sdp")
    elif party == "caller" and session.get("status") == "answered":
        out["sdp"] = session.get("answer_sdp")
    return out


@router.get("/api/call/ice-servers")
async def get_ice_servers(current_user: User = Depends(get_current_user)):
    """
    Servers the phones use to find a direct path to each other. STUN works on most networks;
    a TURN relay (configure TURN_URLS plus TURN_SECRET, or TURN_USERNAME/TURN_PASSWORD) is
    needed when both phones are behind strict mobile-carrier NATs. Media through TURN stays
    end-to-end encrypted; the relay only forwards encrypted packets.
    """
    servers = [{"urls": ["stun:stun.l.google.com:19302", "stun:stun1.l.google.com:19302"]}]
    turn_urls = [u.strip() for u in os.getenv("TURN_URLS", "").split(",") if u.strip()]
    if turn_urls:
        secret = os.getenv("TURN_SECRET", "").strip()
        if secret:
            # Time-limited credentials (coturn "use-auth-secret" / TURN REST API)
            username = f"{int(time.time()) + TURN_CREDENTIAL_TTL_SECONDS}:{current_user.id}"
            credential = base64.b64encode(hmac.new(secret.encode(), username.encode(), hashlib.sha1).digest()).decode()
        else:
            username, credential = os.getenv("TURN_USERNAME", ""), os.getenv("TURN_PASSWORD", "")
        if username and credential:
            servers.append({"urls": turn_urls, "username": username, "credential": credential})
    return {"ice_servers": servers, "ttl": TURN_CREDENTIAL_TTL_SECONDS}
