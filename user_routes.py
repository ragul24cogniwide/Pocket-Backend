"""User profiles, the chat list, conversation delta sync and phone-contact matching."""

import logging
from datetime import datetime, timezone
from typing import List, Optional

import time

from fastapi import Depends, HTTPException
from sqlalchemy import desc, func, or_, and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from auth import (
    get_current_user,
    get_current_user_optional,
    get_phone_user,
)
from connection_manager import manager
from identity import id_forms
from database import get_db
from models import Message, SavedContact, User, iso_utc
from schemas import (
    ChatContactItem,
    SaveContactsRequest,
    SyncContactsRequest,
    SyncContactsResponse,
    SyncConversationsResponse,
)
from sync_service import (
    get_conversation_sync_page,
)
from ai_agent import (
    ensure_personal_agent_user,
    AGENT_USER_ID,
    AGENT_NAME,
    AGENT_AVATAR,
    AGENT_AVATAR_COLOR,
    AGENT_QUOTE,
)
from fastapi import APIRouter
from core import (
    CONTACT_CHECK_MAX_NUMBERS_PER_DAY,
    PROFILE_LOOKUPS_PER_HOUR,
    _contact_check_log,
    chat_peer_ids,
    enforce_rate_limit,
    find_user,
    is_visible_to,
    resolve_user_context,
    visible_owner_ids,
)

logger = logging.getLogger("Main")

router = APIRouter()

# -----------------------------------------------------------------------------
# User Discovery & Contacts
# -----------------------------------------------------------------------------
@router.get("/api/users/profile/{user_id}")
@router.get("/api/users/{user_id}")
async def get_user_public_profile(
    user_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    A user's profile. Anyone signed in gets id / name / avatar (enough to start a chat with a
    number); about, online status and last seen only when that user lets the caller see them
    (see visible_owner_ids). Lookups are rate limited per caller so numbers can't be scanned.
    """
    await enforce_rate_limit(
        f"profile_lookup:{current_user.id}", PROFILE_LOOKUPS_PER_HOUR, 3600, "Too many profile lookups."
    )
    user = await find_user(db, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    profile = {
        "id": user.id,
        "phone_number": user.phone_number,
        "username": user.username,
        "avatar_url": user.avatar_url,
        "avatar_color": user.avatar_color or "#FFB800",
        "avatar": user.username[:2].upper() if user.username and len(user.username) >= 2 else "FK",
    }
    if await is_visible_to(db, user, current_user):
        profile.update({
            "quote": user.quote or "Hey there! I am using Flick.",
            "is_online": await manager.is_user_online(user.id, user.phone_number),
            "last_seen": user.visible_last_seen(viewer=current_user),
        })
    else:
        # Not visible: the keys stay (older apps read them) but say nothing about this person
        profile.update({"quote": None, "is_online": False, "last_seen": None})
    return profile


@router.get("/api/users", response_model=List[ChatContactItem])
async def list_available_contacts(
    user_opt: Optional[User] = Depends(get_current_user_optional),
    db: AsyncSession = Depends(get_db),
):
    """Lists registered users excluding current user, with real-time online status."""
    current_user = await resolve_user_context(user_opt, db)
    await ensure_personal_agent_user(db)
    # Only people you've chatted with: listing every user would let anyone harvest phone numbers
    peers = await chat_peer_ids(db, current_user)
    # Online / last seen / about only for people who let this user see them (visible_owner_ids);
    # someone you wrote to but who never wrote back or saved you stays in your list without them
    visible = await visible_owner_ids(db, current_user)
    stmt = (
        select(User)
        .where(User.id != current_user.id, or_(User.id.in_(peers), User.phone_number.in_(peers)))
        .order_by(User.username.asc())
    )
    result = await db.execute(stmt)
    users = result.scalars().all()

    contacts = []
    seen_identifiers = set()

    # Pre-calculate current_user identifiers (every spelling of this account's own number only)
    cur_ids = id_forms(current_user.id, current_user.phone_number)

    for u in users:
        # Avoid duplicate contacts pointing to the same account (two people can share a display name)
        norm_key = str(u.id)
        if norm_key in seen_identifiers:
            continue
        seen_identifiers.add(norm_key)

        shows_presence = u.id in visible or u.phone_number in visible
        is_online = shows_presence and await manager.is_user_online(u.id, u.phone_number)
        about = (u.quote or "Hey there! I am using Flick.") if shows_presence else None

        # Collect target user identifier aliases
        u_ids = id_forms(u.id, u.phone_number)

        # Get latest message between current_user and u
        msg_stmt = (
            select(Message)
            .where(
                or_(
                    Message.sender_id.in_(cur_ids) & Message.receiver_id.in_(u_ids),
                    Message.sender_id.in_(u_ids) & Message.receiver_id.in_(cur_ids),
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
                    quote=about,
                    online=is_online,
                    last_message=latest_msg.content if latest_msg else (about or "Tap to start conversation"),
                    time=(
                        iso_utc(latest_msg.timestamp)
                        if latest_msg and latest_msg.timestamp
                        else ""
                    ),
                    unread=unread_count,
                    last_seen=u.visible_last_seen(viewer=current_user) if shows_presence else None,
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
            iso_utc(latest_agent_msg.timestamp)
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
@router.get("/sync/conversations", response_model=SyncConversationsResponse)
@router.get("/api/sync/conversations", response_model=SyncConversationsResponse)
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
def _clean_contact_numbers(raw_numbers: List[str]) -> set[str]:
    cleaned_numbers = set()
    for num in raw_numbers:
        clean = "".join(filter(lambda c: c.isdigit() or c == '+', num.strip()))
        if clean:
            if not clean.startswith("+") and len(clean) == 10:
                cleaned_numbers.add("+91" + clean)
            cleaned_numbers.add(clean)
    return cleaned_numbers


def _allow_contact_check(user_id: str, numbers: set[str]) -> None:
    """Counts numbers this account hasn't checked today against its daily allowance (429 when over)."""
    now_ts = time.monotonic()
    checked = _contact_check_log[user_id]
    for num, at in list(checked.items()):
        if now_ts - at > 24 * 3600:
            del checked[num]
    new_numbers = numbers.difference(checked)
    if len(checked) + len(new_numbers) > CONTACT_CHECK_MAX_NUMBERS_PER_DAY:
        logger.warning("[Contacts] %s hit the daily contact-check limit", user_id)
        raise HTTPException(status_code=429, detail="Too many contact checks today. Try again tomorrow.")
    for num in new_numbers:
        checked[num] = now_ts


async def _users_with_numbers(db: AsyncSession, me: User, numbers: set[str]) -> list[User]:
    stmt = (
        select(User)
        .where(
            User.id != me.id,
            or_(User.phone_number.in_(numbers), User.id.in_(numbers)),
        )
        .order_by(User.username.asc())
    )
    result = await db.execute(stmt)
    seen, users = set(), []
    for u in result.scalars().all():
        if u.id not in seen:
            seen.add(u.id)
            users.append(u)
    return users


async def _contact_item(u: User, viewer: User, visible: set) -> ChatContactItem:
    """A contact on Flick. Online / last seen / about only when `u` lets `viewer` see them (visible_owner_ids)."""
    shows_presence = u.id in visible or u.phone_number in visible
    about = (u.quote or "Hey there! I am using Flick.") if shows_presence else None
    initials = (
        "".join([part[0] for part in u.username.split()[:2]]).upper()
        if u.username
        else "FK"
    )
    return ChatContactItem(
        id=u.id,
        phone_number=u.phone_number,
        name=u.username,
        avatar=initials,
        avatar_color=u.avatar_color,
        avatar_url=u.avatar_url,
        quote=about,
        online=shows_presence and await manager.is_user_online(u.id),
        last_message=about or "Available on Flick",
        time="",
        unread=0,
        last_seen=u.visible_last_seen(viewer=viewer) if shows_presence else None,
    )


@router.post("/api/contacts/sync", response_model=SyncContactsResponse)
async def sync_device_contacts(
    payload: SyncContactsRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Matches device phone contacts against registered Flick users.
    Returns only users who have registered and installed the app.
    """
    cleaned_numbers = _clean_contact_numbers(payload.phone_numbers)
    if not cleaned_numbers:
        return SyncContactsResponse(registered_users=[], total_synced=0)
    _allow_contact_check(current_user.id, cleaned_numbers)

    users = await _users_with_numbers(db, current_user, cleaned_numbers)
    visible = await visible_owner_ids(db, current_user)
    registered_contacts = [await _contact_item(u, current_user, visible) for u in users]
    return SyncContactsResponse(
        registered_users=registered_contacts,
        total_synced=len(registered_contacts),
    )


@router.put("/api/contacts/on-flick")
async def save_contacts_on_flick(
    payload: SaveContactsRequest,
    current_user: User = Depends(get_phone_user),
    db: AsyncSession = Depends(get_db),
):
    """
    The phone reports which of its contacts are on Flick (their numbers, after /api/contacts/sync),
    replacing the previous list, so Flick Web can show them too. An empty list clears it (the
    phone does that when contact access is turned off). Names are never sent.
    """
    from sqlalchemy import delete as sql_delete

    numbers = _clean_contact_numbers(payload.phone_numbers)
    if numbers:
        _allow_contact_check(current_user.id, numbers)
    users = await _users_with_numbers(db, current_user, numbers) if numbers else []
    await db.execute(sql_delete(SavedContact).where(SavedContact.user_id == current_user.id))
    for u in users:
        db.add(SavedContact(user_id=current_user.id, contact_id=u.id))
    await db.commit()
    return {"saved": len(users)}


@router.get("/api/contacts/on-flick", response_model=List[ChatContactItem])
async def list_contacts_on_flick(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Your phone contacts who are on Flick, as last reported by your phone (for Flick Web)."""
    result = await db.execute(
        select(User)
        .join(SavedContact, SavedContact.contact_id == User.id)
        .where(SavedContact.user_id == current_user.id)
        .order_by(User.username.asc())
    )
    visible = await visible_owner_ids(db, current_user)
    return [await _contact_item(u, current_user, visible) for u in result.scalars().all()]
