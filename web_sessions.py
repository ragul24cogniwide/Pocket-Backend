"""
Flick Web: using Flick in a browser, linked to the phone (like WhatsApp Web).

Linking
  1. The browser asks for a link code (POST /api/web/link/start). It gets an 8-character code to
     show, plus a private poll secret that never leaves the browser.
  2. On the phone: Linked devices -> Link a device -> type the code. The phone first sees which
     browser it is (/link/inspect), then approves (/link/approve). Only the phone app can do this;
     a linked browser can't link more browsers.
  3. The browser, polling with its secret, receives its own token once. Codes expire after
     2 minutes, work once, and wrong guesses are rate limited.

Sessions
  - Each browser has its own session (max MAX_LINKED_BROWSERS). Its token names the session, so
    logging it out from the phone (or from the browser) ends it immediately, live socket included.
    Logging out on the phone ends every linked browser too.
  - Sessions unused for WEB_SESSION_IDLE_DAYS are signed out automatically.
  - The browser opens its WebSocket with a one-time ticket (valid 30 s) instead of putting its
    token in the URL, where it could end up in access logs.
"""

import logging
import re
import secrets
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from auth import create_web_token, get_current_user, get_phone_user, web_session_id
from connection_manager import manager
from database import get_db
from models import User, WebSession

logger = logging.getLogger("FlickWeb")

router = APIRouter(prefix="/api/web", tags=["web"])

LINK_CODE_TTL_SECONDS = 120
LINK_CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # no 0/O, 1/I: easy to type from a screen
LINK_CODE_LENGTH = 8
MAX_LINKED_BROWSERS = 4
WS_TICKET_TTL_SECONDS = 30
# Wrong / repeated code entries allowed per phone account, and new codes per browser address
CODE_ATTEMPTS_PER_WINDOW = 10
CODE_ATTEMPT_WINDOW_SECONDS = 600
LINK_STARTS_PER_MINUTE = 30  # per network address (offices share one)
MAX_PENDING_LINKS = 5000

# Pending links and socket tickets live in memory: they last seconds to minutes, and the service
# runs as a single instance. {link_id: {...}}
_pending_links: dict[str, dict] = {}
_code_index: dict[str, str] = {}  # code -> link_id
_ws_tickets: dict[str, dict] = {}  # ticket -> {user_id, sid, expires}
_code_attempts: dict[str, deque] = defaultdict(deque)
_link_starts: dict[str, deque] = defaultdict(deque)


class LinkStartRequest(BaseModel):
    device_name: Optional[str] = Field(default=None, max_length=100)


class LinkPollRequest(BaseModel):
    link_id: str = Field(max_length=64)
    poll_secret: str = Field(max_length=128)


class LinkCodeRequest(BaseModel):
    code: str = Field(max_length=32)


def _now() -> float:
    return time.time()


def _prune() -> None:
    now = _now()
    for link_id, link in list(_pending_links.items()):
        # Approved links wait a little longer for the browser to collect its token
        if link["expires"] + (30 if link.get("status") == "approved" else 0) < now:
            _pending_links.pop(link_id, None)
            _code_index.pop(link.get("code"), None)
    for ticket, info in list(_ws_tickets.items()):
        if info["expires"] < now:
            _ws_tickets.pop(ticket, None)


def _rate_limited(log: dict, key: str, limit: int, window: float) -> bool:
    now = _now()
    q = log[key]
    while q and now - q[0] > window:
        q.popleft()
    if len(q) >= limit:
        return True
    q.append(now)
    return False


def normalize_code(raw: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(raw or "").upper())


_BROWSERS = (("Edg/", "Edge"), ("OPR/", "Opera"), ("Firefox/", "Firefox"), ("Chrome/", "Chrome"), ("Safari/", "Safari"))
_SYSTEMS = (("Windows", "Windows"), ("Mac OS X", "macOS"), ("Android", "Android"), ("iPhone", "iOS"),
            ("iPad", "iPadOS"), ("CrOS", "ChromeOS"), ("Linux", "Linux"))


def device_label(user_agent: str, requested: Optional[str] = None) -> str:
    """'Chrome on macOS', from the browser's User-Agent (what the phone shows before approving)."""
    ua = user_agent or ""
    browser = next((name for key, name in _BROWSERS if key in ua), "Browser")
    system = next((name for key, name in _SYSTEMS if key in ua), "")
    label = f"{browser} on {system}" if system else browser
    extra = re.sub(r"[^\w .,'()-]", "", (requested or "").strip())[:40]
    return f"{label} ({extra})" if extra and extra.lower() not in label.lower() else label


def client_ip(request: Request) -> str:
    # Render puts the visitor's address first in X-Forwarded-For
    fwd = request.headers.get("x-forwarded-for", "")
    return (fwd.split(",")[0].strip() if fwd else "") or (request.client.host if request.client else "unknown")


def _find_link_by_code(code: str) -> Optional[dict]:
    _prune()
    link_id = _code_index.get(normalize_code(code))
    link = _pending_links.get(link_id) if link_id else None
    if not link or link["status"] != "pending" or link["expires"] < _now():
        return None
    return link


def _link_for_code(user: User, code: str) -> dict:
    """The pending link a code belongs to. Wrong codes count against the account, so codes
    can't be guessed (CODE_ATTEMPTS_PER_WINDOW misses per 10 minutes)."""
    now = _now()
    misses = _code_attempts[user.id]
    while misses and now - misses[0] > CODE_ATTEMPT_WINDOW_SECONDS:
        misses.popleft()
    if len(misses) >= CODE_ATTEMPTS_PER_WINDOW:
        raise HTTPException(status_code=429, detail="Too many wrong codes. Try again in a few minutes.")
    link = _find_link_by_code(code)
    if not link:
        misses.append(now)
        raise HTTPException(status_code=404, detail="That code isn't valid or has expired. Check the code on your computer.")
    return link


async def active_sessions(db: AsyncSession, user_id: str) -> list[WebSession]:
    res = await db.execute(
        select(WebSession)
        .where(WebSession.user_id == user_id, WebSession.revoked_at.is_(None))
        .order_by(WebSession.last_active_at.desc())
    )
    return list(res.scalars().all())


# ---------------------------------------------------------------------------
# Browser side
# ---------------------------------------------------------------------------
@router.post("/link/start")
async def start_link(payload: LinkStartRequest, request: Request):
    """A new browser asks to be linked. Returns the code to show and its private poll secret."""
    if _rate_limited(_link_starts, client_ip(request), LINK_STARTS_PER_MINUTE, 60):
        raise HTTPException(status_code=429, detail="Too many requests. Wait a minute and try again.")
    _prune()
    if len(_pending_links) >= MAX_PENDING_LINKS:
        raise HTTPException(status_code=503, detail="Please try again in a moment.")
    while True:
        code = "".join(secrets.choice(LINK_CODE_ALPHABET) for _ in range(LINK_CODE_LENGTH))
        if code not in _code_index:
            break
    link_id = secrets.token_urlsafe(24)
    poll_secret = secrets.token_urlsafe(32)
    _pending_links[link_id] = {
        "code": code,
        "poll_secret": poll_secret,
        "device_name": device_label(request.headers.get("user-agent", ""), payload.device_name),
        "status": "pending",
        "expires": _now() + LINK_CODE_TTL_SECONDS,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    _code_index[code] = link_id
    return {
        "link_id": link_id,
        "poll_secret": poll_secret,
        "code": f"{code[:4]}-{code[4:]}",
        "expires_in": LINK_CODE_TTL_SECONDS,
    }


@router.post("/link/poll")
async def poll_link(payload: LinkPollRequest, db: AsyncSession = Depends(get_db)):
    """The browser checks whether the phone approved. The token is handed out exactly once."""
    _prune()
    link = _pending_links.get(payload.link_id)
    if not link or not secrets.compare_digest(link["poll_secret"], payload.poll_secret):
        return {"status": "expired"}
    if link["status"] == "pending":
        return {"status": "pending", "expires_in": max(0, int(link["expires"] - _now()))}
    # Approved: issue the token once, then forget the link
    _pending_links.pop(payload.link_id, None)
    _code_index.pop(link["code"], None)
    user = await db.get(User, link["user_id"])
    session = await db.get(WebSession, link["session_id"])
    if user is None or session is None or session.revoked_at is not None:
        return {"status": "expired"}
    token = create_web_token(user.id, user.phone_number, user.token_version or 0, session.id)
    return {"status": "approved", "token": token, "user": user.to_dict(), "session": session.to_dict(session.id)}


@router.post("/ws-ticket")
async def ws_ticket(current_user: User = Depends(get_current_user)):
    """One-time ticket (30 s) for opening the browser's WebSocket without a token in the URL."""
    sid = web_session_id(current_user)
    if not sid:
        raise HTTPException(status_code=403, detail="Only for Flick Web")
    _prune()
    ticket = secrets.token_urlsafe(32)
    _ws_tickets[ticket] = {"user_id": current_user.id, "sid": sid, "expires": _now() + WS_TICKET_TTL_SECONDS}
    return {"ticket": ticket, "expires_in": WS_TICKET_TTL_SECONDS}


async def consume_ws_ticket(ticket: str, db: AsyncSession) -> tuple[Optional[User], Optional[str]]:
    """The account and web session behind a socket ticket (each ticket works once)."""
    info = _ws_tickets.pop(str(ticket or ""), None)
    if not info or info["expires"] < _now():
        return None, None
    session = await db.get(WebSession, info["sid"])
    if session is None or session.revoked_at is not None or session.user_id != info["user_id"]:
        return None, None
    user = await db.get(User, info["user_id"])
    if user is not None:
        user.web_session_id = session.id
    return user, session.id


@router.get("/me")
async def web_me(current_user: User = Depends(get_current_user)):
    """Profile plus the privacy / notification settings (the browser shows and edits both)."""
    return {**current_user.to_dict(), "prefs": current_user.prefs(), "session_id": web_session_id(current_user)}


@router.post("/logout")
async def web_logout(current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """Logs this browser out (the phone and other browsers stay signed in)."""
    sid = web_session_id(current_user)
    if not sid:
        raise HTTPException(status_code=403, detail="Only for Flick Web")
    await revoke_session(db, current_user.id, sid)
    return {"message": "Logged out"}


# ---------------------------------------------------------------------------
# Phone side
# ---------------------------------------------------------------------------
@router.post("/link/inspect")
async def inspect_link(payload: LinkCodeRequest, current_user: User = Depends(get_phone_user)):
    """Shows which browser a code belongs to, so the person can check before approving."""
    link = _link_for_code(current_user, payload.code)
    return {"device_name": link["device_name"], "created_at": link["created_at"]}


@router.post("/link/approve")
async def approve_link(
    payload: LinkCodeRequest,
    current_user: User = Depends(get_phone_user),
    db: AsyncSession = Depends(get_db),
):
    """The phone approves a browser: creates its session; the browser picks up its token."""
    link = _link_for_code(current_user, payload.code)
    if len(await active_sessions(db, current_user.id)) >= MAX_LINKED_BROWSERS:
        raise HTTPException(
            status_code=409,
            detail=f"You can link up to {MAX_LINKED_BROWSERS} browsers. Log out of one first.",
        )
    session = WebSession(id=secrets.token_urlsafe(24), user_id=current_user.id, device_name=link["device_name"])
    db.add(session)
    await db.commit()
    link.update({"status": "approved", "user_id": current_user.id, "session_id": session.id})
    logger.info("Linked browser %s for %s", session.device_name, current_user.id)
    return {"status": "linked", "session": session.to_dict()}


@router.get("/sessions")
async def list_sessions(current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """Linked browsers of this account (shown on the phone, and in the browser's settings)."""
    sid = web_session_id(current_user)
    return {"sessions": [s.to_dict(sid) for s in await active_sessions(db, current_user.id)]}


@router.delete("/sessions/{session_id}")
async def logout_session(
    session_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Logs out one linked browser. The phone can log out any of them; a browser only itself."""
    sid = web_session_id(current_user)
    if sid and sid != session_id:
        raise HTTPException(status_code=403, detail="Use your phone to log out other devices.")
    if not await revoke_session(db, current_user.id, session_id):
        raise HTTPException(status_code=404, detail="Device not found")
    return {"message": "Logged out"}


async def revoke_session(db: AsyncSession, user_id: str, session_id: str) -> bool:
    session = await db.get(WebSession, session_id)
    if session is None or session.user_id != user_id or session.revoked_at is not None:
        return False
    session.revoked_at = datetime.now(timezone.utc)
    await db.commit()
    for ticket, info in list(_ws_tickets.items()):
        if info["sid"] == session_id:
            _ws_tickets.pop(ticket, None)
    await manager.close_linked_session(user_id, session_id)
    return True


async def revoke_all_sessions(db: AsyncSession, user_id: str) -> None:
    """Logging out on the phone (or deleting the account) signs out every linked browser."""
    ids = [s.id for s in await active_sessions(db, user_id)]
    if not ids:
        return
    await db.execute(
        update(WebSession).where(WebSession.id.in_(ids)).values(revoked_at=datetime.now(timezone.utc))
    )
    for sid in ids:
        await manager.close_linked_session(user_id, sid)
