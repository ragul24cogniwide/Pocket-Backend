"""
Archived chats (like WhatsApp): a person can move a chat out of their main list.

PUT /api/chats/{peer_id}/archive  {"archived": true | false}

- Only affects the caller's own list; the other person sees nothing.
- Synced to all the caller's devices: the chat's sync snapshot carries `archived`, and their other
  devices (phone / Flick Web) get a live `chat_archived` frame.
- Archived chats stay archived when new messages arrive (WhatsApp's "Keep chats archived").
- You can archive chats you actually have: a person you've exchanged messages with, or a group
  you're (or were) in. The AI agent chat stays pinned and can't be archived.
"""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import delete, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from auth import get_current_user
from connection_manager import manager
from database import get_db
from models import ChatArchive, GroupMember, Message, User
from sync_service import AGENT_USER_ID, is_group_id, record_conversation_change

router = APIRouter(prefix="/api/chats", tags=["archive"])


class ArchiveRequest(BaseModel):
    archived: bool = True


async def _resolve_chat(db: AsyncSession, me: User, peer_id: str) -> str:
    """The chat's canonical id, or 404 if the caller has no such chat."""
    peer_id = (peer_id or "").strip()
    if peer_id == AGENT_USER_ID:
        raise HTTPException(status_code=400, detail="The Flick AI Agent chat can't be archived")
    if is_group_id(peer_id):
        member = (
            await db.execute(select(GroupMember.id).where(GroupMember.group_id == peer_id, GroupMember.user_id == me.id))
        ).first()
        if member is None:
            raise HTTPException(status_code=404, detail="Chat not found")
        return peer_id
    peer = (
        await db.execute(select(User).where(or_(User.id == peer_id, User.phone_number == peer_id)))
    ).scalars().first()
    if peer is None or peer.id == me.id:
        raise HTTPException(status_code=404, detail="Chat not found")
    mine = {me.id, me.phone_number}
    theirs = {peer.id, peer.phone_number}
    talked = (
        await db.execute(
            select(Message.id)
            .where(
                or_(
                    Message.sender_id.in_(mine) & Message.receiver_id.in_(theirs),
                    Message.sender_id.in_(theirs) & Message.receiver_id.in_(mine),
                )
            )
            .limit(1)
        )
    ).first()
    if talked is None:
        raise HTTPException(status_code=404, detail="Chat not found")
    return peer.id


@router.put("/{peer_id}/archive")
async def set_archived(
    peer_id: str,
    payload: ArchiveRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    chat_id = await _resolve_chat(db, current_user, peer_id)
    existing = (
        await db.execute(
            select(ChatArchive).where(ChatArchive.user_id == current_user.id, ChatArchive.peer_id == chat_id)
        )
    ).scalar_one_or_none()
    if payload.archived and existing is None:
        db.add(ChatArchive(user_id=current_user.id, peer_id=chat_id))
    elif not payload.archived and existing is not None:
        await db.delete(existing)
    await db.flush()
    # Other devices that were offline pick it up on their next sync
    await record_conversation_change(db, user_id=current_user.id, peer_id=chat_id, action="updated")
    await db.commit()
    await manager.send_to_my_devices(
        current_user.id, {"type": "chat_archived", "data": {"peer_id": chat_id, "archived": payload.archived}}
    )
    return {"peer_id": chat_id, "archived": payload.archived}


async def delete_archives_for(db: AsyncSession, user_id: str) -> None:
    """Account deletion: their archive list, and other people's archive entries for them."""
    await db.execute(delete(ChatArchive).where(or_(ChatArchive.user_id == user_id, ChatArchive.peer_id == user_id)))
