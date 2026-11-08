"""The personal AI agent's own endpoints."""

import json
import logging
from datetime import datetime, timezone
from typing import Optional


from fastapi import Depends
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from auth import (
    get_current_user_optional,
)
from database import async_session_factory, get_db
from models import Message, MessageStatus, User, iso_utc
from schemas import (
    AgentChatRequest,
    AgentChatResponse,
)
from sync_service import (
    record_conversation_change,
)
from ai_agent import (
    process_personal_agent_message,
    ensure_personal_agent_user,
    AGENT_USER_ID,
    AGENT_NAME,
    AGENT_AVATAR,
    AGENT_AVATAR_COLOR,
    AGENT_QUOTE,
)
from fastapi import APIRouter
from core import find_client_message, resolve_user_context
from messaging import agent_reply_client_id, check_agent_limits, find_agent_reply

logger = logging.getLogger("Main")

router = APIRouter()

# -----------------------------------------------------------------------------
# Personal AI Agent Dedicated Endpoints
# -----------------------------------------------------------------------------
@router.get("/api/agent/profile")
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


@router.post("/api/agent/chat", response_model=AgentChatResponse)
async def chat_with_agent_endpoint(
    payload: AgentChatRequest,
    user_opt: Optional[User] = Depends(get_current_user_optional),
    db: AsyncSession = Depends(get_db),
):
    """
    Direct REST endpoint to chat with Personal AI Agent and execute calendar actions.

    No database connection is held while the AI thinks: the question is committed first, the
    agent ends its read transaction before calling Gemini (see call_gemini_agent), and the
    answer is saved in a new, short session afterwards.
    """
    current_user = await resolve_user_context(user_opt, db)
    await check_agent_limits(current_user.id, payload.content)
    await ensure_personal_agent_user(db)

    def stored_answer(question: Message, answered: Message) -> dict:
        meta = (answered.to_dict().get("metadata") or {})
        return {
            "reply": answered.content,
            "action": meta.get("action"),
            "meeting": meta.get("meeting"),
            "meetings": meta.get("meetings"),
            "suggested_actions": meta.get("suggested_actions"),
            "user_message_id": question.id,
            "message_id": answered.id,
            "timestamp": iso_utc(answered.timestamp),
        }

    # A retried question we already answered: return the stored reply instead of
    # asking the AI again (which could, for example, book the same meeting twice)
    previous = await find_client_message(db, current_user.id, payload.client_msg_id)
    if previous is not None:
        answered = await find_agent_reply(db, current_user.id, payload.client_msg_id)
        if answered is not None:
            return stored_answer(previous, answered)

    # 1. Save the question and commit it now (a retry arriving mid-processing then sees it,
    #    and the request's DB connection goes back to the pool before the AI call)
    now = datetime.now(timezone.utc)
    if previous is not None:
        user_msg = previous  # stored earlier but never answered: answer it now, don't duplicate it
    else:
        user_msg = Message(
            sender_id=current_user.id,
            receiver_id=AGENT_USER_ID,
            content=payload.content,
            timestamp=now,
            status=MessageStatus.READ,
            delivered_at=now,
            read_at=now,
            client_msg_id=payload.client_msg_id,
        )
        db.add(user_msg)
        try:
            await db.commit()
        except IntegrityError:
            # The same question (same client_msg_id) was stored by a parallel retry just now
            await db.rollback()
            await db.refresh(current_user)  # the rollback expired it; the agent reads it below
            user_msg = await find_client_message(db, current_user.id, payload.client_msg_id)
            if user_msg is None:
                raise
    user_msg_id = user_msg.id

    # 2. Process query via Personal AI Agent engine (it releases its connection before calling Gemini)
    res = await process_personal_agent_message(
        current_user=current_user,
        content=payload.content,
        db=db,
        tz_offset_minutes=payload.client_tz_offset_minutes,
    )
    await db.commit()  # meeting changes made by the agent, if any

    # 3. Save the answer in a new short session
    reply_metadata = {
        "is_agent": True,
        "action": res.get("action"),
        "meeting": res.get("meeting"),
        "meetings": res.get("meetings"),
        "suggested_actions": res.get("suggested_actions"),
    }
    async with async_session_factory() as save_db:
        agent_msg = Message(
            sender_id=AGENT_USER_ID,
            receiver_id=current_user.id,
            content=(res.get("reply") or "I am your personal AI assistant.").strip(),
            reply_to=json.dumps({"metadata": reply_metadata}),
            timestamp=datetime.now(timezone.utc),
            status=MessageStatus.READ,
            delivered_at=now,
            read_at=now,
            client_msg_id=(
                agent_reply_client_id(current_user.id, payload.client_msg_id) if payload.client_msg_id else None
            ),
        )
        save_db.add(agent_msg)
        try:
            await save_db.commit()
        except IntegrityError:
            # A parallel retry of this question already stored its answer: return that one
            await save_db.rollback()
            answered = await find_agent_reply(save_db, current_user.id, payload.client_msg_id)
            if answered is None:
                raise
            return stored_answer(user_msg, answered)

        try:
            await record_conversation_change(save_db, user_id=current_user.id, peer_id=AGENT_USER_ID, action="updated")
            await save_db.commit()
        except Exception:
            await save_db.rollback()

    res["user_message_id"] = user_msg_id
    res["message_id"] = agent_msg.id
    res["timestamp"] = iso_utc(agent_msg.timestamp)

    return res
