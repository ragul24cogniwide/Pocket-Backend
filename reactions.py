"""
Emoji reactions on messages (direct and group), like WhatsApp.

PUT /api/messages/{message_id}/reaction  {"emoji": "❤️"}  sets my reaction (replacing any other)
PUT /api/messages/{message_id}/reaction  {"emoji": null}  removes it

- Only the two people in a direct chat, or current members of a group, can react or see reactions.
  Anyone else gets the same 404 as for a message that doesn't exist.
- One reaction per person per message. The value must be an emoji (no text, no markup).
- Everyone in the chat (all their devices: phone and Flick Web) gets a live `message_reaction`
  frame with the message's full reaction list, so clients just replace what they show.
- The person who wrote the message also gets a push notification ("Bob reacted ❤️ to …"),
  following their notification / preview / sound settings.
"""

import asyncio
import logging
import unicodedata
from datetime import datetime, timezone
from typing import Iterable, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from auth import get_current_user
from connection_manager import manager
from database import async_session_factory, get_db
from models import Group, GroupMember, GroupMessage, Message, MessageReaction, User
from notifications import send_push_notification

logger = logging.getLogger("Reactions")

router = APIRouter(prefix="/api/messages", tags=["reactions"])

MAX_EMOJI_CODEPOINTS = 12
PUSH_PREVIEW_CHARS = 60


class ReactionRequest(BaseModel):
    emoji: Optional[str] = Field(default=None, max_length=64)


def valid_emoji(value: str) -> bool:
    """A single emoji (possibly with skin tone / ZWJ sequence / flag), not text."""
    if not value or len(value) > MAX_EMOJI_CODEPOINTS:
        return False
    has_symbol = False
    for ch in value:
        cp = ord(ch)
        cat = unicodedata.category(ch)
        if cat == "So" or 0x1F1E6 <= cp <= 0x1F1FF:  # symbols, regional-indicator flags
            has_symbol = True
        elif cat in ("Sk", "Mn", "Me"):  # skin tones, variation selectors, keycap
            continue
        elif cp == 0x200D or 0xE0020 <= cp <= 0xE007F:  # zero-width joiner, flag tags
            continue
        else:
            return False
    return has_symbol


async def reactions_for(db: AsyncSession, message_ids: Iterable[str]) -> dict:
    """{message_id: [{"user_id", "emoji"}, ...]} in the order people reacted."""
    ids = [m for m in dict.fromkeys(message_ids) if m]
    if not ids:
        return {}
    rows = (
        await db.execute(
            select(MessageReaction)
            .where(MessageReaction.message_id.in_(ids))
            .order_by(MessageReaction.created_at.asc())
        )
    ).scalars().all()
    out: dict = {}
    for r in rows:
        out.setdefault(r.message_id, []).append({"user_id": r.user_id, "emoji": r.emoji})
    return out


async def delete_reactions(db: AsyncSession, message_id: str) -> None:
    """When a message is deleted for everyone its reactions go too (caller commits)."""
    await db.execute(delete(MessageReaction).where(MessageReaction.message_id == message_id))


async def _audience(db: AsyncSession, message_id: str, user: User):
    """(group_id or None, user ids who should see this message's reactions). 404 for strangers."""
    mine = {user.id, user.phone_number}
    direct = await db.get(Message, message_id)
    if direct is not None:
        if direct.sender_id not in mine and direct.receiver_id not in mine:
            raise HTTPException(status_code=404, detail="Message not found")
        return None, direct, [direct.sender_id, direct.receiver_id]
    grp = await db.get(GroupMessage, message_id)
    if grp is not None and grp.kind == "text":
        members = (
            await db.execute(
                select(GroupMember).where(GroupMember.group_id == grp.group_id, GroupMember.left_at.is_(None))
            )
        ).scalars().all()
        member_ids = [m.user_id for m in members]
        if user.id in member_ids:
            return grp.group_id, grp, member_ids
    raise HTTPException(status_code=404, detail="Message not found")


@router.put("/{message_id}/reaction")
async def set_reaction(
    message_id: str,
    payload: ReactionRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    emoji = (payload.emoji or "").strip() or None
    if emoji is not None and not valid_emoji(emoji):
        raise HTTPException(status_code=400, detail="Reactions must be a single emoji")
    group_id, msg, audience = await _audience(db, message_id, current_user)

    existing = (
        await db.execute(
            select(MessageReaction).where(
                MessageReaction.message_id == message_id, MessageReaction.user_id == current_user.id
            )
        )
    ).scalar_one_or_none()
    if emoji is None:
        if existing is not None:
            await db.delete(existing)
    elif existing is not None:
        existing.emoji = emoji
        existing.created_at = datetime.now(timezone.utc)
    else:
        db.add(MessageReaction(message_id=message_id, group_id=group_id, user_id=current_user.id, emoji=emoji))
    await db.commit()

    reactions = (await reactions_for(db, [message_id])).get(message_id, [])
    frame = {
        "type": "message_reaction",
        "data": {
            "message_id": message_id,
            "group_id": group_id,
            "sender_id": msg.sender_id,  # who wrote the message
            "receiver_id": None if group_id else msg.receiver_id,
            "user_id": current_user.id,  # who reacted
            "name": current_user.username or "Someone",
            "emoji": emoji,
            "reactions": reactions,
            # for the chat list line "Bob reacted ❤️ to: “…”" (everyone here already has the message)
            "message_preview": " ".join(str(msg.content or "").split())[:PUSH_PREVIEW_CHARS],
        },
    }
    for uid in dict.fromkeys(audience):
        if uid and uid != current_user.id:
            await manager.send_personal_message(uid, frame)
    await manager.send_to_my_devices(current_user.id, frame)  # my phone / other browsers
    if emoji and msg.sender_id and msg.sender_id != current_user.id:
        asyncio.create_task(push_reaction(
            current_user.id, current_user.username or "Someone", msg.sender_id, message_id, msg.content, group_id, emoji,
        ))
    return {"message_id": message_id, "emoji": emoji, "reactions": reactions}


async def push_reaction(
    reactor_id: str, reactor_name: str, author_id: str, message_id: str, content: str, group_id: Optional[str], emoji: str
) -> None:
    """Notification to the message's author: "❤️ Bob reacted to: “See you at 5”"."""
    try:
        async with async_session_factory() as db:
            author = await db.get(User, author_id)
            group = await db.get(Group, group_id) if group_id else None
        if author is None or not author.fcm_token:
            return
        prefs = author.prefs()
        if not prefs.get("msg_notifications", True):
            return
        if prefs.get("preview", True):
            text = " ".join(str(content or "").split())
            if len(text) > PUSH_PREVIEW_CHARS:
                text = text[:PUSH_PREVIEW_CHARS].rstrip() + "…"
            about = f": “{text}”" if text else " your message"
        else:
            about = " your message"
        if group is not None:
            title, body = group.name, f"{reactor_name} reacted {emoji} to{about}"
            data = {"type": "reaction", "message_id": message_id, "group_id": group.id, "group_name": group.name,
                    "group_avatar_color": group.avatar_color or ""}
        else:
            title, body = reactor_name, f"Reacted {emoji} to{about}"
            # sender_id = the chat to open when the notification is tapped
            data = {"type": "reaction", "message_id": message_id, "sender_id": reactor_id}
        await send_push_notification(
            fcm_token=author.fcm_token, sender_name=title, content=body, data_payload=data,
            silent=not prefs.get("sound", True),
        )
    except Exception as e:  # a failed push never affects the reaction itself
        logger.warning("Reaction push to %s failed: %s", author_id, e)
