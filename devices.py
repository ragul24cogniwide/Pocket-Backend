"""
Devices, and which device has which message (step 1 of keeping messages on phones, not here).

Every phone and linked browser is a Device. When a message is sent, a MessageDelivery row is
written for every device that has to get it (the receiver's devices, every group member's, and
the sender's own other devices); the row is ticked when that device has the message:

  - it confirms it over the socket ("device_ack", or the older "ack_delivered" /
    "ack_group_delivered" frames),
  - it reports it from a push notification while the app was closed (POST /api/messages/delivered),
  - or it downloads it with the chat history (GET /api/messages/{chat}).

WATCH MODE: nothing is deleted because of this yet. The report (GET /api/admin/delivery-report)
shows whether every device really gets every message; only once it does will messages be removed
from the server after delivery. Everything here is best-effort: a failure is logged and never
gets in the way of sending or receiving a message.

How a request names its device:
  - the phone app sends its key in the "X-Flick-Device" header (and ?device= on the socket),
    made when it logs in (see the app's services/api.js),
  - a linked browser is known by its web session ("web:<session id>"),
  - an older app version that sends no key counts as the account's "legacy-phone" device.
"""

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import and_, delete, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from auth import WEB_SESSION_IDLE_DAYS, get_current_user, web_session_id
from core import require_admin
from database import async_session_factory, get_db
from models import Device, MessageDelivery, User

logger = logging.getLogger("Devices")

router = APIRouter()

DEVICE_HEADER = "x-flick-device"
LEGACY_PHONE_KEY = "legacy-phone"
DEVICE_KEY_RE = re.compile(r"^[A-Za-z0-9_-]{8,64}$")
# A phone not seen for this long is gone (a browser: when its web session expires)
PHONE_ACTIVE_DAYS = 30
# last_seen_at is written at most this often
TOUCH_EVERY = timedelta(hours=1)
# Bookkeeping kept for the report: delivered rows this long, never-delivered rows longer
DELIVERED_RETENTION_DAYS = 30
PENDING_RETENTION_DAYS = 60
# Messages to/from the AI agent stay on the server (the AI has to read them): not tracked
AGENT_USER_ID = "ai_assistant"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _aware(dt: Optional[datetime]) -> Optional[datetime]:
    return dt.replace(tzinfo=timezone.utc) if dt is not None and dt.tzinfo is None else dt


def clean_key(raw: Optional[str]) -> Optional[str]:
    """A phone's device key as sent by the app, or None if it's missing / not a valid key."""
    key = str(raw or "").strip()
    return key if DEVICE_KEY_RE.match(key) and key != LEGACY_PHONE_KEY else None


# ---------------------------------------------------------------------------
# Which device is this?
# ---------------------------------------------------------------------------
async def _get_or_create(db: AsyncSession, user_id: str, client_key: str, kind: str, **fields) -> tuple[Device, bool]:
    """(device, created). Never brings a revoked device back (only /api/devices/register does)."""
    device = (
        await db.execute(select(Device).where(Device.user_id == user_id, Device.client_key == client_key))
    ).scalars().first()
    now = _now()
    if device is None:
        device = Device(user_id=user_id, client_key=client_key, kind=kind, created_at=now, last_seen_at=now, **fields)
        db.add(device)
        await db.flush()
        return device, True
    for name, value in fields.items():
        if value is not None and getattr(device, name) != value:
            setattr(device, name, value)
    if now - (_aware(device.last_seen_at) or now) > TOUCH_EVERY:
        device.last_seen_at = now
    return device, False


async def _active_phone(db: AsyncSession, user_id: str) -> Optional[Device]:
    return (
        await db.execute(
            select(Device)
            .where(Device.user_id == user_id, Device.kind == "phone", Device.revoked_at.is_(None))
            .order_by(Device.last_seen_at.desc())
        )
    ).scalars().first()


async def resolve_device(db: AsyncSession, user: User, client_key: Optional[str] = None) -> Optional[Device]:
    """
    The device a request / socket comes from (created the first time it's seen). Commits.
    None only if the bookkeeping fails (never stops the request).
    """
    try:
        sid = web_session_id(user)
        if sid:
            device, _ = await _get_or_create(db, user.id, f"web:{sid}", "web", platform="web", web_session_id=sid)
        else:
            key = clean_key(client_key)
            if key:
                device, created = await _get_or_create(db, user.id, key, "phone")
                if created:
                    await _retire_other_phones(db, user.id, keep=device.id)
            else:
                # An app version from before device keys: its phone, unless a newer app on this
                # account already registered itself (then requests without a key are from that one)
                device = await _active_phone(db, user.id)
                if device is None or device.client_key == LEGACY_PHONE_KEY:
                    device, _ = await _get_or_create(db, user.id, LEGACY_PHONE_KEY, "phone", name="Phone (older app)")
        await db.commit()
        return device
    except Exception as e:
        logger.warning("Could not resolve the device of %s: %s", user.id, e)
        await db.rollback()
        return None


async def _retire_other_phones(db: AsyncSession, user_id: str, keep: str) -> None:
    """One phone per account: a newly registered one replaces the previous ones."""
    await db.execute(
        update(Device)
        .where(Device.user_id == user_id, Device.kind == "phone", Device.id != keep, Device.revoked_at.is_(None))
        .values(revoked_at=_now())
    )


def device_id_of(device) -> Optional[str]:
    """The id of a resolved device, or None (no device, or a route function called directly)."""
    return device.id if isinstance(device, Device) else None


async def current_device(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[Device]:
    """FastAPI dependency: the caller's device (from the X-Flick-Device header / web session)."""
    return await resolve_device(db, current_user, request.headers.get(DEVICE_HEADER))


# ---------------------------------------------------------------------------
# Revoking
# ---------------------------------------------------------------------------
async def revoke_web_device(db: AsyncSession, user_id: str, session_id: str) -> None:
    """A browser logged out (or was unlinked)."""
    await db.execute(
        update(Device)
        .where(Device.user_id == user_id, Device.web_session_id == session_id, Device.revoked_at.is_(None))
        .values(revoked_at=_now())
    )


async def revoke_all_devices(db: AsyncSession, user_id: str) -> None:
    """Logged out on the phone: the phone and every linked browser are signed out."""
    await db.execute(
        update(Device).where(Device.user_id == user_id, Device.revoked_at.is_(None)).values(revoked_at=_now())
    )


async def delete_user_devices(db: AsyncSession, user_id: str, message_ids: Iterable[str] = ()) -> None:
    """Account deleted: its devices, their delivery rows, and the rows of the messages it sent."""
    ids = [str(i) for i in message_ids if i]
    await db.execute(delete(MessageDelivery).where(MessageDelivery.user_id == user_id))
    for start in range(0, len(ids), 500):
        await db.execute(delete(MessageDelivery).where(MessageDelivery.message_id.in_(ids[start:start + 500])))
    await db.execute(delete(Device).where(Device.user_id == user_id))


# ---------------------------------------------------------------------------
# Who has to get a message, and who has it
# ---------------------------------------------------------------------------
async def active_devices(db: AsyncSession, user_ids: Iterable[str]) -> list[tuple[str, str]]:
    """(device id, owner) of the devices of these people that are in use: a phone seen in the
    last 30 days, a browser within its web session's idle limit."""
    ids = [u for u in dict.fromkeys(user_ids) if u]
    if not ids:
        return []
    now = _now()
    rows = await db.execute(
        select(Device.id, Device.user_id).where(
            Device.user_id.in_(ids),
            Device.revoked_at.is_(None),
            or_(
                and_(Device.kind == "phone", Device.last_seen_at >= now - timedelta(days=PHONE_ACTIVE_DAYS)),
                and_(Device.kind == "web", Device.last_seen_at >= now - timedelta(days=WEB_SESSION_IDLE_DAYS)),
            ),
        )
    )
    return [(r.id, r.user_id) for r in rows.all()]


async def expect_deliveries(
    message_id: str,
    message_ts: datetime,
    recipient_ids: Iterable[str],
    sender_id: str,
    sender_device_id: Optional[str],
    group_id: Optional[str] = None,
) -> int:
    """
    Writes "has to get it" for every device of the recipients, and the sender's own other devices
    (not the one it was sent from). Call BEFORE the message is sent out, so a quick confirmation
    always finds its row. Returns how many rows were written. Never raises.
    """
    if sender_id == AGENT_USER_ID or AGENT_USER_ID in recipient_ids:
        return 0
    try:
        async with async_session_factory() as db:
            devices = await active_devices(db, [*recipient_ids, sender_id])
            rows = [
                MessageDelivery(
                    message_id=message_id,
                    chat_kind="group" if group_id else "direct",
                    group_id=group_id,
                    message_ts=message_ts,
                    user_id=owner,
                    device_id=device_id,
                )
                for device_id, owner in devices
                if device_id != sender_device_id
            ]
            if rows:
                db.add_all(rows)
                await db.commit()
            return len(rows)
    except Exception as e:
        logger.warning("Could not record the expected deliveries of %s: %s", message_id, e)
        return 0


async def mark_delivered(device_id: Optional[str], message_ids: Iterable[str], via: str) -> int:
    """This device has these messages. Only rows that were expected (and not yet ticked) change."""
    ids = [str(i) for i in dict.fromkeys(message_ids or []) if i][:500]
    if not device_id or not ids:
        return 0
    try:
        async with async_session_factory() as db:
            result = await db.execute(
                update(MessageDelivery)
                .where(
                    MessageDelivery.device_id == device_id,
                    MessageDelivery.message_id.in_(ids),
                    MessageDelivery.delivered_at.is_(None),
                )
                .values(delivered_at=_now(), delivered_via=via)
            )
            await db.commit()
            return result.rowcount or 0
    except Exception as e:
        logger.warning("Could not record deliveries for device %s: %s", device_id, e)
        return 0


async def mark_group_delivered(device_id: Optional[str], group_id: str, until: Optional[datetime] = None, via: str = "socket") -> int:
    """This device has the group's messages up to `until` (older apps confirm a whole group)."""
    if not device_id or not group_id:
        return 0
    try:
        async with async_session_factory() as db:
            result = await db.execute(
                update(MessageDelivery)
                .where(
                    MessageDelivery.device_id == device_id,
                    MessageDelivery.group_id == group_id,
                    MessageDelivery.message_ts <= (until or _now()),
                    MessageDelivery.delivered_at.is_(None),
                )
                .values(delivered_at=_now(), delivered_via=via)
            )
            await db.commit()
            return result.rowcount or 0
    except Exception as e:
        logger.warning("Could not record group deliveries for device %s: %s", device_id, e)
        return 0


async def forget_message(db: AsyncSession, message_id: str) -> None:
    """Deleted for everyone: nobody has to get it any more."""
    await db.execute(delete(MessageDelivery).where(MessageDelivery.message_id == message_id))


async def prune_deliveries(db: AsyncSession) -> int:
    """Old bookkeeping goes (run with the change-log pruning, every few hours)."""
    now = _now()
    result = await db.execute(
        delete(MessageDelivery).where(
            or_(
                MessageDelivery.delivered_at < now - timedelta(days=DELIVERED_RETENTION_DAYS),
                MessageDelivery.created_at < now - timedelta(days=PENDING_RETENTION_DAYS),
            )
        )
    )
    await db.commit()
    return result.rowcount or 0


# ---------------------------------------------------------------------------
# The report: does every device get every message?
# ---------------------------------------------------------------------------
AGE_BUCKETS = [("under_1h", timedelta(hours=1)), ("1h_to_1d", timedelta(days=1)), ("1d_to_7d", timedelta(days=7))]


async def delivery_report(db: AsyncSession, days: int = 7) -> dict:
    """Counts only (no message text, no full phone numbers)."""
    now = _now()
    since = now - timedelta(days=days)
    in_window = MessageDelivery.created_at >= since
    pending = MessageDelivery.delivered_at.is_(None)

    totals = (
        await db.execute(
            select(
                func.count(MessageDelivery.id),
                func.count(MessageDelivery.delivered_at),
                func.count(func.distinct(MessageDelivery.message_id)),
            ).where(in_window)
        )
    ).one()
    expected, delivered, messages = int(totals[0] or 0), int(totals[1] or 0), int(totals[2] or 0)

    messages_pending = (
        await db.execute(select(func.count(func.distinct(MessageDelivery.message_id))).where(in_window, pending))
    ).scalar_one() or 0

    by_kind = {}
    rows = await db.execute(
        select(Device.kind, func.count(MessageDelivery.id), func.count(MessageDelivery.delivered_at))
        .join(Device, Device.id == MessageDelivery.device_id)
        .where(in_window)
        .group_by(Device.kind)
    )
    for kind, exp, got in rows.all():
        by_kind[kind] = {"expected": int(exp), "delivered": int(got), "pending": int(exp) - int(got)}

    by_via = {
        (via or "unknown"): int(n)
        for via, n in (
            await db.execute(
                select(MessageDelivery.delivered_via, func.count(MessageDelivery.id))
                .where(in_window, MessageDelivery.delivered_at.is_not(None))
                .group_by(MessageDelivery.delivered_via)
            )
        ).all()
    }

    age = {}
    lower = None
    for name, upper in AGE_BUCKETS:
        conds = [in_window, pending, MessageDelivery.created_at >= now - upper]
        if lower is not None:
            conds.append(MessageDelivery.created_at < now - lower)
        age[name] = int((await db.execute(select(func.count(MessageDelivery.id)).where(*conds))).scalar_one() or 0)
        lower = upper
    age["over_7d"] = int(
        (
            await db.execute(
                select(func.count(MessageDelivery.id)).where(in_window, pending, MessageDelivery.created_at < now - lower)
            )
        ).scalar_one()
        or 0
    )

    # Devices still owing the most, with when they were last seen (owner shown as the last 4 digits)
    behind = []
    rows = await db.execute(
        select(
            Device.id, Device.kind, Device.user_id, Device.last_seen_at, Device.revoked_at,
            func.count(MessageDelivery.id).label("owed"),
            func.min(MessageDelivery.created_at).label("oldest"),
        )
        .join(Device, Device.id == MessageDelivery.device_id)
        .where(in_window, pending)
        .group_by(Device.id, Device.kind, Device.user_id, Device.last_seen_at, Device.revoked_at)
        .order_by(func.count(MessageDelivery.id).desc())
        .limit(10)
    )
    for r in rows.all():
        behind.append({
            "device_id": r.id,
            "kind": r.kind,
            "owner": f"…{str(r.user_id)[-4:]}",
            "owed": int(r.owed),
            "oldest_pending": (_aware(r.oldest) or now).isoformat(),
            "last_seen_at": (_aware(r.last_seen_at) or now).isoformat(),
            "revoked": r.revoked_at is not None,
        })

    devices = {
        kind: int(n)
        for kind, n in (
            await db.execute(
                select(Device.kind, func.count(Device.id)).where(Device.revoked_at.is_(None)).group_by(Device.kind)
            )
        ).all()
    }

    return {
        "window_days": days,
        "messages": messages,
        "messages_fully_delivered": messages - int(messages_pending),
        "messages_with_a_device_missing_them": int(messages_pending),
        "deliveries": {"expected": expected, "delivered": delivered, "pending": expected - delivered},
        "delivered_percent": round(100.0 * delivered / expected, 2) if expected else None,
        "by_device_kind": by_kind,
        "delivered_via": by_via,
        "pending_by_age": age,
        "devices_most_behind": behind,
        "active_devices": devices,
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
class RegisterDeviceRequest(BaseModel):
    device_key: str = Field(..., min_length=8, max_length=64)
    platform: Optional[str] = Field(None, max_length=20)
    name: Optional[str] = Field(None, max_length=100)
    app_version: Optional[str] = Field(None, max_length=30)
    fcm_token: Optional[str] = Field(None, max_length=4096)


@router.post("/api/devices/register")
async def register_device(
    payload: RegisterDeviceRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """The phone app announces itself after signing in (and on every start)."""
    if web_session_id(current_user):
        raise HTTPException(status_code=403, detail="Browsers are registered when they're linked")
    key = clean_key(payload.device_key)
    if not key:
        raise HTTPException(status_code=400, detail="Invalid device key")
    device, _ = await _get_or_create(
        db, current_user.id, key, "phone",
        platform=(payload.platform or "")[:20] or None,
        name=(payload.name or "")[:100] or None,
        app_version=(payload.app_version or "")[:30] or None,
        fcm_token=payload.fcm_token or None,
    )
    # This install is the phone in use now (also after being replaced by another phone earlier)
    device.revoked_at = None
    device.last_seen_at = _now()
    await _retire_other_phones(db, current_user.id, keep=device.id)
    await db.commit()
    return {"status": "success", "device": device.to_dict(current_id=device.id)}


@router.get("/api/devices")
async def list_my_devices(
    device: Optional[Device] = Depends(current_device),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """This account's devices in use (the phone and linked browsers)."""
    rows = (
        await db.execute(
            select(Device).where(Device.user_id == current_user.id, Device.revoked_at.is_(None)).order_by(Device.kind)
        )
    ).scalars().all()
    return [d.to_dict(current_id=device_id_of(device)) for d in rows]


@router.get("/api/admin/delivery-report", dependencies=[Depends(require_admin)])
async def delivery_report_endpoint(days: int = 7, db: AsyncSession = Depends(get_db)):
    """Does every device get every message? (watch mode: see this module's docstring)"""
    return await delivery_report(db, days=max(1, min(int(days), 60)))
