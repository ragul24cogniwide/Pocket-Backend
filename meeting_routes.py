"""Calendar & meetings."""

import json
import logging
from datetime import datetime, timezone
from datetime import time as dt_time
from typing import Optional


from fastapi import Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from auth import (
    get_current_user_optional,
)
from database import get_db
from models import User, Meeting
from schemas import (
    MeetingCreate,
    MeetingUpdate,
    CheckConflictRequest,
)
from ai_agent import (
    check_meeting_conflicts,
)
from fastapi import APIRouter
from core import resolve_user_context

logger = logging.getLogger("Main")

router = APIRouter()

# -----------------------------------------------------------------------------
# Calendar & Meeting Management APIs (Microsoft Teams / Arattai style)
# -----------------------------------------------------------------------------
@router.get("/api/meetings")
async def list_user_meetings(
    date_str: Optional[str] = None, # YYYY-MM-DD
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    status: Optional[str] = None,
    user_opt: Optional[User] = Depends(get_current_user_optional),
    db: AsyncSession = Depends(get_db),
):
    """Retrieves all meetings for the current user, optionally filtered by date or status."""
    current_user = await resolve_user_context(user_opt, db)
    stmt = select(Meeting).where(Meeting.user_id == current_user.id)
    if status:
        stmt = stmt.where(Meeting.status == status)

    if date_str:
        try:
            d = datetime.strptime(date_str, "%Y-%m-%d").date()
            # dt_time (datetime.time), not the `time` module imported at the top of this file:
            # `time.min` raised AttributeError and the except below silently dropped the filter
            d_start = datetime.combine(d, dt_time.min).replace(tzinfo=timezone.utc)
            d_end = datetime.combine(d, dt_time.max).replace(tzinfo=timezone.utc)
            stmt = stmt.where(Meeting.start_time >= d_start, Meeting.start_time <= d_end)
        except Exception:
            pass
    elif start_date and end_date:
        try:
            sd = datetime.fromisoformat(start_date)
            ed = datetime.fromisoformat(end_date)
            stmt = stmt.where(Meeting.start_time >= sd, Meeting.start_time <= ed)
        except Exception:
            pass

    stmt = stmt.order_by(Meeting.start_time.asc())
    result = await db.execute(stmt)
    meetings = result.scalars().all()
    return [m.to_dict() for m in meetings]


@router.post("/api/meetings", status_code=status.HTTP_201_CREATED)
async def create_user_meeting(
    payload: MeetingCreate,
    user_opt: Optional[User] = Depends(get_current_user_optional),
    db: AsyncSession = Depends(get_db),
):
    """Creates a new calendar event for current user with conflict check."""
    current_user = await resolve_user_context(user_opt, db)

    # Check conflicts
    conflicts = await check_meeting_conflicts(db, current_user.id, payload.start_time, payload.end_time)
    has_conflict = len(conflicts) > 0

    attendees_json = json.dumps(payload.attendees or [current_user.username or "Flick User"])

    meeting = Meeting(
        user_id=current_user.id,
        title=payload.title,
        description=payload.description or "",
        start_time=payload.start_time,
        end_time=payload.end_time,
        timezone=payload.timezone or "UTC",
        location=payload.location or "",
        meeting_link=payload.meeting_link or None,
        attendees=attendees_json,
        status=payload.status or "SCHEDULED",
    )
    db.add(meeting)
    await db.commit()
    await db.refresh(meeting)

    result_dict = meeting.to_dict()
    result_dict["has_conflict"] = has_conflict
    if has_conflict:
        result_dict["conflicting_meetings"] = [c.to_dict() for c in conflicts]

    return result_dict


@router.get("/api/meetings/{meeting_id}")
async def get_user_meeting(
    meeting_id: str,
    user_opt: Optional[User] = Depends(get_current_user_optional),
    db: AsyncSession = Depends(get_db),
):
    """Fetches a single meeting owned by current user."""
    current_user = await resolve_user_context(user_opt, db)
    stmt = select(Meeting).where(Meeting.id == meeting_id, Meeting.user_id == current_user.id)
    res = await db.execute(stmt)
    meeting = res.scalar_one_or_none()
    if not meeting:
        raise HTTPException(status_code=404, detail="Meeting not found or access denied")
    return meeting.to_dict()


@router.put("/api/meetings/{meeting_id}")
async def update_user_meeting(
    meeting_id: str,
    payload: MeetingUpdate,
    user_opt: Optional[User] = Depends(get_current_user_optional),
    db: AsyncSession = Depends(get_db),
):
    """Updates an authorized meeting."""
    current_user = await resolve_user_context(user_opt, db)
    stmt = select(Meeting).where(Meeting.id == meeting_id, Meeting.user_id == current_user.id)
    res = await db.execute(stmt)
    meeting = res.scalar_one_or_none()
    if not meeting:
        raise HTTPException(status_code=404, detail="Meeting not found or access denied")

    if payload.title is not None:
        meeting.title = payload.title
    if payload.description is not None:
        meeting.description = payload.description
    if payload.start_time is not None:
        meeting.start_time = payload.start_time
    if payload.end_time is not None:
        meeting.end_time = payload.end_time
    if payload.location is not None:
        meeting.location = payload.location
    if payload.meeting_link is not None:
        meeting.meeting_link = payload.meeting_link
    if payload.timezone is not None:
        meeting.timezone = payload.timezone
    if payload.attendees is not None:
        meeting.attendees = json.dumps(payload.attendees)
    if payload.status is not None:
        meeting.status = payload.status

    meeting.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(meeting)
    return meeting.to_dict()


@router.delete("/api/meetings/{meeting_id}")
async def delete_user_meeting(
    meeting_id: str,
    user_opt: Optional[User] = Depends(get_current_user_optional),
    db: AsyncSession = Depends(get_db),
):
    """Deletes or cancels a meeting owned by current user."""
    current_user = await resolve_user_context(user_opt, db)
    stmt = select(Meeting).where(Meeting.id == meeting_id, Meeting.user_id == current_user.id)
    res = await db.execute(stmt)
    meeting = res.scalar_one_or_none()
    if not meeting:
        raise HTTPException(status_code=404, detail="Meeting not found or access denied")

    await db.delete(meeting)
    await db.commit()
    return {"status": "success", "message": "Meeting deleted successfully", "id": meeting_id}


@router.post("/api/meetings/check-conflicts")
async def check_conflicts_endpoint(
    payload: CheckConflictRequest,
    user_opt: Optional[User] = Depends(get_current_user_optional),
    db: AsyncSession = Depends(get_db),
):
    """Checks whether the requested start_time and end_time conflict with existing meetings."""
    current_user = await resolve_user_context(user_opt, db)
    conflicts = await check_meeting_conflicts(
        db, current_user.id, payload.start_time, payload.end_time, payload.exclude_meeting_id
    )
    return {
        "has_conflict": len(conflicts) > 0,
        "conflicts": [c.to_dict() for c in conflicts],
    }
