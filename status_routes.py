"""Statuses (24-hour stories)."""

import asyncio
import json
import logging
import uuid
from datetime import datetime, timedelta, timezone


from fastapi import Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import joinedload
from sqlalchemy.ext.asyncio import AsyncSession

from auth import (
    get_current_user,
)
from connection_manager import manager
from database import get_db
from models import User, Status
from schemas import (
    CreateStatusRequest,
)
from fastapi import APIRouter
from core import audience_ids, is_visible_to, visible_owner_ids

logger = logging.getLogger("Main")

router = APIRouter()

# -----------------------------------------------------------------------------
# Status & Stories Endpoints (24-Hour Ephemeral Status)
# -----------------------------------------------------------------------------
@router.post("/api/statuses")
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

    # Live update only for the people who can see it in Updates (see visible_owner_ids): people
    # this user has written to or saved, not strangers who merely wrote to them
    peers = await audience_ids(db, current_user)
    frame = {"type": "status_update", "data": st_dict}

    async def notify_peers():
        for peer in peers:
            try:
                await manager.send_personal_message(peer, frame)
            except Exception as e:
                logger.debug("status_update to %s failed: %s", peer, e)

    asyncio.create_task(notify_peers())
    return st_dict


@router.get("/api/statuses/feed")
async def get_status_feed(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Returns active 24h statuses (mine, and those of people who let me see them) with their profiles."""
    now = datetime.now(timezone.utc)
    mine = {i for i in (current_user.id, current_user.phone_number) if i}
    peers = await visible_owner_ids(db, current_user)
    # Filtered in SQL: only my statuses and those of people visible to me are loaded
    stmt = (
        select(Status)
        .options(joinedload(Status.user))
        .where(Status.expires_at > now, Status.user_id.in_(mine | peers))
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
        elif s.user_id in peers:
            st_dict.pop("viewers", None)  # who viewed a story is only for its owner
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


@router.post("/api/statuses/{status_id}/view")
async def mark_status_viewed(
    status_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Records that current user viewed the status."""
    stmt = select(Status).where(Status.id == status_id)
    result = await db.execute(stmt)
    st = result.scalar_one_or_none()
    if not st or st.user_id in (current_user.id, current_user.phone_number):
        if not st:
            raise HTTPException(status_code=404, detail="Status not found")
        return {"message": "Own status", "viewsCount": len(json.loads(st.viewers or "[]"))}
    owner = await db.get(User, st.user_id)
    if owner is None or not await is_visible_to(db, owner, current_user):
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


@router.delete("/api/statuses/{status_id}")
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
