"""One-to-one messages: history, send, edit, delete."""

import logging
from typing import Optional
from datetime import datetime


from fastapi import Depends, HTTPException
from sqlalchemy import desc, false, func, or_, and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from auth import (
    get_current_user,
)
from connection_manager import manager
from identity import id_forms
from database import get_db
from models import Message, User
from schemas import (
    EditMessageRequest,
    SendMessageRequest,
)
from sync_service import (
    record_conversation_change,
)
import groups
import reactions
from groups import GroupError, is_group_id
from fastapi import APIRouter
from messaging import SendError, send_direct_message
from core import (
    find_user,
)

logger = logging.getLogger("Main")

router = APIRouter()

# -----------------------------------------------------------------------------
# Conversation History
# -----------------------------------------------------------------------------
@router.get("/api/messages/{other_user_id}")
async def get_messages_with_user(
    other_user_id: str,
    limit: int = 50,
    offset: int = 0,
    before: Optional[str] = None,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    A page of message history, oldest first: the latest `limit` messages, or the `limit` messages
    before the message whose id is `before` (the oldest one the app already has).

    `before` is how the app scrolls back: unlike `offset`, it can't skip a message when new ones
    arrive or old ones are deleted meanwhile. Messages with exactly the same time as `before` may
    come again (the app drops duplicates by id), so none is ever skipped. An unknown `before`
    (e.g. that message was deleted since) falls back to `offset`.

    A page shorter than `limit` means the history starts there: the app then treats everything it
    has that the server didn't return as deleted, so pages are only ever short at the start.
    """
    other_user_id = other_user_id.strip()
    if is_group_id(other_user_id):
        try:
            return await groups.group_history(current_user, other_user_id, limit, offset, before)
        except GroupError as ge:
            raise HTTPException(status_code=ge.status_code, detail=ge.reason)
    limit = max(1, min(int(limit), 100))
    offset = max(0, int(offset))

    # 1. Every stored spelling of my own number (and only mine: see identity.py). Matching by the
    # last 10 digits used to pull in the chats of another person whose number ends the same way.
    cur_ids = id_forms(current_user.id, current_user.phone_number)

    # 2. The same for the other person, plus their exact db id & phone_number
    other_ids = id_forms(other_user_id)
    o_user = await find_user(db, other_user_id)
    if o_user:
        other_ids |= id_forms(o_user.id, o_user.phone_number)

    in_this_chat = or_(
        Message.sender_id.in_(cur_ids) & Message.receiver_id.in_(other_ids),
        Message.sender_id.in_(other_ids) & Message.receiver_id.in_(cur_ids),
    )
    page_filter = []
    anchor = None
    if before:
        anchor = (await db.execute(select(Message).where(Message.id == str(before), in_this_chat))).scalars().first()
    if anchor is not None:
        page_filter = [Message.timestamp <= anchor.timestamp, Message.id != anchor.id]
        offset = 0

    stmt = (
        select(Message)
        .where(
            in_this_chat,
            *page_filter,
            # "Deleted for me" by this person: left out here, in SQL, before OFFSET/LIMIT. Filtering
            # after the LIMIT returned short pages, which the app took as "no older messages".
            # (COALESCE: rows from before these columns existed have NULL there.)
            ~or_(
                and_(Message.sender_id.in_(cur_ids), func.coalesce(Message.hidden_for_sender, false())),
                and_(Message.receiver_id.in_(cur_ids), func.coalesce(Message.hidden_for_receiver, false())),
            ),
        )
        .order_by(desc(Message.timestamp), desc(Message.id))
        .offset(offset)
        .limit(limit)
    )
    result = await db.execute(stmt)
    messages = result.scalars().all()
    ordered_messages = list(reversed(messages))
    reaction_map = await reactions.reactions_for(db, [m.id for m in ordered_messages])
    out = []
    for msg in ordered_messages:
        m = msg.to_dict()
        m["reactions"] = reaction_map.get(msg.id, [])
        # My message read while (or sent while) read receipts were off for either of us:
        # two grey ticks, permanently. Turning receipts on/off later doesn't change past ticks.
        if msg.receipt_hidden and m.get("sender_id") in cur_ids and str(m.get("status", "")).upper() == "READ":
            m["status"] = "DELIVERED"
        out.append(m)
    return out


@router.post("/api/messages/send")
async def send_message_rest(
    payload: SendMessageRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Sends a message via REST with immediate DB persistence and WebSocket dispatch."""
    receiver_id = payload.receiver_id
    sender_id = current_user.id
    content = payload.content.strip()
    client_temp_id = payload.temp_msg_id or f"temp_{int(datetime.now().timestamp() * 1000)}"

    if is_group_id(receiver_id):
        try:
            sent = await groups.send_group_message(
                current_user.id, receiver_id, content, payload.temp_msg_id, payload.reply_to, payload.metadata
            )
        except GroupError as ge:
            raise HTTPException(status_code=ge.status_code, detail=ge.reason)
        if not sent.get("duplicate"):
            await manager.send_to_my_devices(current_user.id, {"type": "message_sent_sync", "data": sent})
        return {"status": "success", "temp_msg_id": client_temp_id, **sent}

    try:
        result = await send_direct_message(
            sender_id, receiver_id, content, payload.temp_msg_id, payload.reply_to, payload.metadata,
            sender_name=current_user.username or "Flick User",
        )
    except SendError as se:
        raise HTTPException(status_code=se.status_code, detail=se.reason)
    message_dict = result.message
    return {
        "status": "success",
        **({"duplicate": True} if result.duplicate else {}),
        "temp_msg_id": client_temp_id,
        "message_id": message_dict["message_id"],
        **message_dict,
    }


@router.put("/api/messages/{message_id}")
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
        try:
            edited = await groups.edit_group_message(current_user, message_id, payload.content)
        except GroupError as ge:
            raise HTTPException(status_code=ge.status_code, detail=ge.reason)
        if edited is not None:
            return edited
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
    await manager.send_to_my_devices(current_user.id, edit_frame)

    return {"status": "success", "message": msg.to_dict(), "is_edited": True}


@router.delete("/api/messages/{message_id}")
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
        await groups.delete_group_message(current_user, message_id, for_everyone)
        return {"status": "success", "message_id": message_id, "deleted": True}

    mine = {current_user.id, current_user.phone_number}
    is_sender, is_receiver = msg.sender_id in mine, msg.receiver_id in mine
    if not (is_sender or is_receiver):
        # Not your conversation: same answer as a missing message, nothing is touched
        return {"status": "success", "message_id": message_id, "deleted": True}

    if for_everyone and is_sender:
        receiver_id = msg.receiver_id
        sender_id = msg.sender_id
        await reactions.delete_reactions(db, message_id)
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
        await manager.send_to_my_devices(current_user.id, delete_frame)
    else:
        # Delete for me only: hide it for this person; the other person keeps their copy
        other_id = msg.receiver_id if is_sender else msg.sender_id
        if is_sender:
            msg.hidden_for_sender = True
        if is_receiver:
            msg.hidden_for_receiver = True
        await db.commit()
        try:
            await record_conversation_change(db, user_id=current_user.id, peer_id=other_id, action="updated")
            await db.commit()
        except Exception:
            pass
        await manager.send_to_my_devices(current_user.id, {"type": "message_deleted", "data": {
            "message_id": message_id, "sender_id": msg.sender_id, "receiver_id": msg.receiver_id, "for_me": True,
        }})

    return {"status": "success", "message_id": message_id, "deleted": True}
