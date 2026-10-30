"""
Call log, kept on the server so every device (phone and Flick Web) shows the same history.

GET    /api/calls            my calls, newest first (only calls I took part in)
DELETE /api/calls            clear my call log (the other person keeps theirs)
DELETE /api/calls/{call_id}  remove one call from my log

The call signaling in main.py records each step (offer, answer, decline, hang-up) in the
background, so calls never wait on the database. Both people's devices get a live
`call_log_updated` frame.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import delete, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from auth import get_current_user
from connection_manager import manager
from database import async_session_factory, get_db
from models import CallRecord, User, iso_utc

logger = logging.getLogger("CallLog")

router = APIRouter(prefix="/api/calls", tags=["calls"])

RING_TIMEOUT = timedelta(minutes=2)  # still "ringing" after this (server restart, lost hang-up) = missed


def _utc(dt: Optional[datetime]) -> Optional[datetime]:
    return dt.replace(tzinfo=timezone.utc) if dt is not None and dt.tzinfo is None else dt


async def _notify(record: CallRecord) -> None:
    frame = {"type": "call_log_updated", "data": {"call_id": record.id}}
    for uid in {record.caller_id, record.receiver_id}:
        await manager.send_personal_message(uid, frame)


async def record_offer(call_id: str, caller_id: str, receiver: str, video: bool) -> None:
    try:
        async with async_session_factory() as db:
            if await db.get(CallRecord, call_id) is not None:
                return  # re-sent offer
            peer = (
                await db.execute(select(User).where(or_(User.id == receiver, User.phone_number == receiver)))
            ).scalars().first()
            if peer is None:
                return
            record = CallRecord(id=call_id, caller_id=caller_id, receiver_id=peer.id, video=bool(video))
            db.add(record)
            await db.commit()
        await _notify(record)
    except Exception as e:  # the call itself must never fail because of the log
        logger.warning("Call log (offer %s) failed: %s", call_id, e)


async def record_answer(call_id: str) -> None:
    await _update(call_id, answered=True)


async def record_finish(call_id: str, outcome: Optional[str] = None, duration: int = 0) -> None:
    """outcome: 'declined' / 'busy' for a rejected call; None for a hang-up."""
    await _update(call_id, outcome=outcome, duration=duration, finished=True)


async def _update(call_id: str, answered: bool = False, outcome: Optional[str] = None,
                  duration: int = 0, finished: bool = False) -> None:
    try:
        async with async_session_factory() as db:
            record = await db.get(CallRecord, call_id)
            if record is None:
                return
            now = datetime.now(timezone.utc)
            if answered and record.answered_at is None:
                record.answered_at = now
                record.status = "answered"
            if finished and record.status in ("ringing", "answered"):
                record.ended_at = now
                if outcome:
                    record.status = outcome
                elif record.answered_at is not None:
                    record.status = "completed"
                    # The caller's own count is best; otherwise answer -> hang-up time
                    record.duration = int(duration) if duration else int((now - _utc(record.answered_at)).total_seconds())
                else:
                    record.status = "missed"  # hung up before anyone answered
            await db.commit()
        await _notify(record)
    except Exception as e:
        logger.warning("Call log (%s) failed: %s", call_id, e)


def _status_for(record: CallRecord, mine_outgoing: bool) -> str:
    """What this person sees: completed | missed | declined | busy | no_answer | ongoing."""
    status = record.status
    if status in ("ringing", "answered") and datetime.now(timezone.utc) - _utc(record.started_at) > RING_TIMEOUT:
        status = "completed" if record.answered_at else "missed"
    if status in ("ringing", "answered"):
        return "ongoing"
    if mine_outgoing and status == "missed":
        return "no_answer"
    return status


@router.get("")
async def list_calls(limit: int = 100, current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    limit = max(1, min(int(limit), 200))
    me = current_user.id
    rows = (
        await db.execute(
            select(CallRecord)
            .where(
                or_(
                    (CallRecord.caller_id == me) & (CallRecord.hidden_for_caller.is_(False)),
                    (CallRecord.receiver_id == me) & (CallRecord.hidden_for_receiver.is_(False)),
                )
            )
            .order_by(CallRecord.started_at.desc())
            .limit(limit)
        )
    ).scalars().all()
    peer_ids = {r.receiver_id if r.caller_id == me else r.caller_id for r in rows}
    peers = {}
    if peer_ids:
        for u in (await db.execute(select(User).where(User.id.in_(peer_ids)))).scalars().all():
            peers[u.id] = u
    out = []
    for r in rows:
        outgoing = r.caller_id == me
        peer = peers.get(r.receiver_id if outgoing else r.caller_id)
        if peer is None:
            continue
        out.append({
            "id": r.id,
            "direction": "outgoing" if outgoing else "incoming",
            "video": r.video,
            "status": _status_for(r, outgoing),
            "started_at": iso_utc(r.started_at),
            "duration": r.duration or 0,
            "peer": {
                "id": peer.id,
                "name": peer.username,
                "phone_number": peer.phone_number,
                "avatar_color": peer.avatar_color,
                "avatar_url": peer.avatar_url,
            },
        })
    return {"calls": out}


async def _hide(db: AsyncSession, me: str, call_id: Optional[str] = None) -> int:
    hidden = 0
    for column, flag in ((CallRecord.caller_id, "hidden_for_caller"), (CallRecord.receiver_id, "hidden_for_receiver")):
        stmt = update(CallRecord).where(column == me)
        if call_id:
            stmt = stmt.where(CallRecord.id == call_id)
        res = await db.execute(stmt.values(**{flag: True}))
        hidden += res.rowcount or 0
    await db.commit()
    return hidden


@router.delete("")
async def clear_calls(current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    await _hide(db, current_user.id)
    await manager.send_to_my_devices(current_user.id, {"type": "call_log_updated", "data": {"cleared": True}})
    return {"cleared": True}


@router.delete("/{call_id}")
async def remove_call(call_id: str, current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    if not await _hide(db, current_user.id, call_id):
        raise HTTPException(status_code=404, detail="Call not found")
    await manager.send_to_my_devices(current_user.id, {"type": "call_log_updated", "data": {"call_id": call_id}})
    return {"removed": call_id}


async def delete_calls_for(db: AsyncSession, user_id: str) -> None:
    """Account deletion."""
    await db.execute(delete(CallRecord).where(or_(CallRecord.caller_id == user_id, CallRecord.receiver_id == user_id)))
