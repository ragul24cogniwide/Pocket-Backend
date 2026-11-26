"""
Couples (Flick for lovers): pairing two people, and what they share.

GET    /api/couple          my couple: {"status": "none" | "waiting" | "paired", ...}
POST   /api/couple/invite   make (or get back) my invite code, e.g. "FLK-8Q2M"
POST   /api/couple/join     {"code", "anniversary"?, "nickname"?}  pair with the person whose code it is
PATCH  /api/couple          {"anniversary"?, "nickname"?, "mood"?, "tz"?, "milestones"?}
DELETE /api/couple          unpair (or cancel my waiting code)

Someone can be in one couple at a time. Every change reaches both people's devices as a live
`couple_updated` frame, so the other phone refreshes at once.
"""

import json
import logging
import re
import secrets
from datetime import date, datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from auth import get_current_user
from connection_manager import manager
from core import enforce_rate_limit
from database import get_db
from models import Couple, User, iso_utc

logger = logging.getLogger("Couples")

router = APIRouter(prefix="/api/couple", tags=["couple"])

CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # no 0/O or 1/I to mix up
MAX_MILESTONES = 60
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class JoinBody(BaseModel):
    code: str
    anniversary: Optional[str] = None
    nickname: Optional[str] = None


class UpdateBody(BaseModel):
    anniversary: Optional[str] = None
    nickname: Optional[str] = None
    mood: Optional[str] = None
    tz: Optional[int] = None
    milestones: Optional[list[dict[str, Any]]] = None


def normalize_code(raw: str) -> str:
    """'flk 8q2m', 'FLK-8Q2M', '8q2m' all become 'FLK-8Q2M'."""
    s = re.sub(r"[^A-Z0-9]", "", str(raw or "").upper())
    if s.startswith("FLK"):
        s = s[3:]
    return f"FLK-{s}" if s else ""


def _valid_date(value: Optional[str]) -> Optional[str]:
    if value in (None, ""):
        return None
    v = str(value).strip()[:10]
    if not _DATE.match(v):
        raise HTTPException(status_code=400, detail="Dates look like 2025-10-10.")
    try:
        d = date.fromisoformat(v)
    except ValueError:
        raise HTTPException(status_code=400, detail="That date doesn't exist.")
    if d > date.today():
        raise HTTPException(status_code=400, detail="The anniversary can't be in the future.")
    return v


def _clean_text(value: Optional[str], limit: int) -> Optional[str]:
    if value is None:
        return None
    v = " ".join(str(value).split())[:limit]
    return v or None


def _clean_milestones(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for raw in items[:MAX_MILESTONES]:
        if not isinstance(raw, dict):
            continue
        title = _clean_text(raw.get("title"), 80)
        if not title:
            continue
        when = str(raw.get("date") or "").strip()[:10]
        if when and not _DATE.match(when):
            when = ""
        out.append({
            "id": str(raw.get("id") or secrets.token_hex(4))[:24],
            "title": title,
            "date": when or None,
            "note": _clean_text(raw.get("note"), 120),
        })
    return out


async def _my_couple(db: AsyncSession, me: str) -> Optional[Couple]:
    """My couple: a paired one first, else my own waiting code."""
    rows = (
        await db.execute(select(Couple).where(or_(Couple.user_a == me, Couple.user_b == me)))
    ).scalars().all()
    paired = [c for c in rows if c.user_b]
    if paired:
        return paired[0]
    return rows[0] if rows else None


def _milestones(c: Couple) -> list:
    try:
        v = json.loads(c.milestones) if c.milestones else []
        return v if isinstance(v, list) else []
    except (TypeError, ValueError):
        return []


async def _view(db: AsyncSession, c: Optional[Couple], me: str) -> dict:
    if c is None:
        return {"status": "none"}
    mine_a = c.user_a == me
    base = {
        "code": c.invite_code,
        "anniversary": c.anniversary,
        "nickname": c.nickname_a if mine_a else c.nickname_b,  # what I call them
        "my_mood": c.mood_a if mine_a else c.mood_b,
        "milestones": _milestones(c),
        "created_at": iso_utc(c.created_at),
    }
    if not c.user_b:
        return {"status": "waiting", **base}
    partner_id = c.user_b if mine_a else c.user_a
    partner = await db.get(User, partner_id)
    return {
        "status": "paired",
        **base,
        "paired_at": iso_utc(c.paired_at),
        "their_nickname": c.nickname_b if mine_a else c.nickname_a,  # what they call me
        "partner_mood": c.mood_b if mine_a else c.mood_a,
        "partner_tz": c.tz_b if mine_a else c.tz_a,
        "partner": {
            "id": partner_id,
            "phone_number": partner.phone_number if partner else partner_id,
            "name": partner.username if partner else partner_id,
            "avatar_url": partner.avatar_url if partner else None,
            "avatar_color": partner.avatar_color if partner else None,
        },
    }


async def _notify(c: Couple, *extra: Optional[str]) -> None:
    frame = {"type": "couple_updated", "data": {"couple_id": c.id}}
    for uid in {c.user_a, c.user_b, *extra} - {None}:
        try:
            await manager.send_personal_message(uid, frame)
        except Exception as e:  # a missed frame only delays the refresh
            logger.debug("couple_updated to %s failed: %s", uid, e)


async def _new_code(db: AsyncSession) -> str:
    for _ in range(20):
        code = "FLK-" + "".join(secrets.choice(CODE_ALPHABET) for _ in range(4))
        if (await db.execute(select(Couple.id).where(Couple.invite_code == code))).first() is None:
            return code
    raise HTTPException(status_code=503, detail="Could not make a code just now. Try again.")


@router.get("")
async def get_couple(current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    return await _view(db, await _my_couple(db, current_user.id), current_user.id)


@router.post("/invite")
async def make_invite(current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    me = current_user.id
    c = await _my_couple(db, me)
    if c is None:
        c = Couple(user_a=me, invite_code=await _new_code(db))
        db.add(c)
        await db.commit()
        await db.refresh(c)
    return await _view(db, c, me)


@router.post("/join")
async def join(body: JoinBody, current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    me = current_user.id
    await enforce_rate_limit(f"couple_join:{me}", 10, 600, "Too many tries.")
    code = normalize_code(body.code)
    if not code:
        raise HTTPException(status_code=400, detail="Enter their code.")
    mine = await _my_couple(db, me)
    if mine is not None and mine.user_b:
        raise HTTPException(status_code=409, detail="You're already paired. Unpair first.")
    c = (await db.execute(select(Couple).where(Couple.invite_code == code))).scalars().first()
    if c is None:
        raise HTTPException(status_code=404, detail="No one has that code. Check it and try again.")
    if c.user_a == me:
        raise HTTPException(status_code=400, detail="That's your own code. Send it to your person.")
    if c.user_b:
        raise HTTPException(status_code=409, detail="That code has already been used.")

    anniversary = _valid_date(body.anniversary)
    if mine is not None:  # my own unused code goes away; I'm joining theirs
        await db.delete(mine)
    c.user_b = me
    c.paired_at = datetime.now(timezone.utc)
    if anniversary and not c.anniversary:
        c.anniversary = anniversary
    nickname = _clean_text(body.nickname, 40)
    if nickname:
        c.nickname_b = nickname
    await db.commit()
    await db.refresh(c)
    await _notify(c)
    return await _view(db, c, me)


@router.patch("")
async def update(body: UpdateBody, current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    me = current_user.id
    c = await _my_couple(db, me)
    if c is None:
        raise HTTPException(status_code=404, detail="You're not paired yet.")
    mine_a = c.user_a == me
    sent = body.model_fields_set
    if "anniversary" in sent:
        c.anniversary = _valid_date(body.anniversary)
    if "nickname" in sent:
        nick = _clean_text(body.nickname, 40)
        if mine_a:
            c.nickname_a = nick
        else:
            c.nickname_b = nick
    if "mood" in sent:
        mood = _clean_text(body.mood, 40)
        if mine_a:
            c.mood_a = mood
        else:
            c.mood_b = mood
    if "tz" in sent and body.tz is not None:
        tz = max(-14 * 60, min(14 * 60, int(body.tz)))
        if mine_a:
            c.tz_a = tz
        else:
            c.tz_b = tz
    if "milestones" in sent and body.milestones is not None:
        c.milestones = json.dumps(_clean_milestones(body.milestones))
    await db.commit()
    await db.refresh(c)
    # Only the time zone changing is too small to wake the other phone
    if sent - {"tz"}:
        await _notify(c)
    return await _view(db, c, me)


@router.delete("")
async def unpair(current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    me = current_user.id
    c = await _my_couple(db, me)
    if c is None:
        return {"status": "none"}
    await db.delete(c)
    await db.commit()
    await _notify(c)
    return {"status": "none"}
