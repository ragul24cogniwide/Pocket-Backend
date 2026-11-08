"""Sign-in (phone OTP), sign-out, account deletion and the signed-in user's profile."""

import json
import logging
from datetime import datetime, timedelta, timezone


from fastapi import Depends, HTTPException, Request
from sqlalchemy import desc, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from auth import (
    create_access_token,
    generate_otp_code,
    get_current_user,
    get_phone_user,
    web_session_id,
)
from connection_manager import manager
from database import get_db
from models import Message, OtpRecord, SavedContact, User, Status, Meeting, iso_utc
from schemas import (
    AuthResponse,
    SendOtpRequest,
    SendOtpResponse,
    UpdateFcmTokenRequest,
    UpdatePreferencesRequest,
    UpdateProfileRequest,
    UserProfileResponse,
    VerifyOtpRequest,
)
from sync_service import (
    record_conversation_change,
    record_profile_change_for_contacts,
)
import archive
import rate_limits
import calllog
import groups
import web_sessions
from fastapi import APIRouter
from core import (
    ALLOW_MASTER_OTP,
    DEV_MODE_EXPOSE_OTP,
    MASTER_OTP_CODE,
    OTP_EXPIRY_MINUTES,
    OTP_MAX_SENDS_PER_HOUR,
    OTP_MAX_SENDS_PER_IP_PER_HOUR,
    OTP_MAX_VERIFY_ATTEMPTS,
    OTP_RESEND_COOLDOWN_SECONDS,
    client_ip,
    deliver_otp_sms,
    enforce_rate_limit,
    normalize_phone,
)

logger = logging.getLogger("Main")

router = APIRouter()

# -----------------------------------------------------------------------------
# Authentication & OTP Endpoints
# -----------------------------------------------------------------------------
@router.post("/api/auth/send-otp", response_model=SendOtpResponse)
async def send_otp(payload: SendOtpRequest, request: Request, db: AsyncSession = Depends(get_db)):
    """
    Generates a 6-digit OTP for the provided phone number.
    In testing/dev mode, returns the OTP in the response for easy instant login.
    """
    phone = normalize_phone(payload.phone_number)
    digits = phone.lstrip("+")
    if not digits.isdigit() or not (8 <= len(digits) <= 15):
        raise HTTPException(status_code=400, detail="Enter a valid phone number.")

    # Throttle (shared across server processes via Redis): per client IP, then per number:
    # one code per cooldown window and a cap per hour
    await enforce_rate_limit(
        f"otp_send_ip:{client_ip(request)}", OTP_MAX_SENDS_PER_IP_PER_HOUR, 3600,
        "Too many login codes requested from this network.",
    )
    sends_now, cooldown_left = await rate_limits.incr_counter(f"otp_cooldown:{phone}", OTP_RESEND_COOLDOWN_SECONDS)
    if sends_now > 1:
        raise HTTPException(
            status_code=429,
            detail=f"Please wait {cooldown_left}s before requesting another code.",
            headers={"Retry-After": str(cooldown_left)},
        )
    await enforce_rate_limit(
        f"otp_send_phone:{phone}", OTP_MAX_SENDS_PER_HOUR, 3600,
        "Too many codes requested for this number.",
    )

    otp_code = generate_otp_code(6)
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=OTP_EXPIRY_MINUTES)

    # Invalidate previous unused OTPs for this number
    await db.execute(
        update(OtpRecord)
        .where(OtpRecord.phone_number == phone, OtpRecord.is_used == False)
        .values(is_used=True)
    )

    otp_record = OtpRecord(
        phone_number=phone,
        otp_code=otp_code,
        expires_at=expires_at,
        is_used=False,
    )
    db.add(otp_record)
    await db.commit()

    # A new code gets a fresh set of verify attempts (the old code was just invalidated above)
    await rate_limits.reset_counter(f"otp_verify:{phone}")

    sms_sent = await deliver_otp_sms(phone, otp_code)
    if not sms_sent and not DEV_MODE_EXPOSE_OTP:
        logger.error("OTP for %s could not be delivered: no SMS provider configured.", phone)
        raise HTTPException(
            status_code=503,
            detail="We couldn't send a code right now. Please try again later.",
        )

    if DEV_MODE_EXPOSE_OTP:
        logger.info("Generated OTP for %s: %s (dev mode, expires in %d mins)", phone, otp_code, OTP_EXPIRY_MINUTES)
    else:
        logger.info("Sent OTP to %s (expires in %d mins)", phone, OTP_EXPIRY_MINUTES)

    return SendOtpResponse(
        message="OTP sent successfully.",
        phone_number=phone,
        dev_otp=otp_code if DEV_MODE_EXPOSE_OTP else None,
        expires_in_seconds=OTP_EXPIRY_MINUTES * 60,
    )


@router.post("/api/auth/verify-otp", response_model=AuthResponse)
async def verify_otp(payload: VerifyOtpRequest, db: AsyncSession = Depends(get_db)):
    """
    Verifies the OTP code for the phone number.
    If valid, logs in the existing user or creates a new user, returning a JWT token.
    """
    phone = normalize_phone(payload.phone_number)
    code = (payload.otp_code or "").strip()

    # Count this attempt BEFORE looking at the code, atomically (Redis INCR, shared by every
    # server process; in-memory fallback without Redis). Checking first and counting after an
    # awaited query let many parallel guesses all pass the check. The count lives as long as a
    # code does and is cleared by a successful login or a new code.
    attempt_key = f"otp_verify:{phone}"
    attempts, _ = await rate_limits.incr_counter(attempt_key, OTP_EXPIRY_MINUTES * 60)
    if attempts > OTP_MAX_VERIFY_ATTEMPTS:
        raise HTTPException(status_code=429, detail="Too many incorrect attempts. Request a new code.")

    now = datetime.now(timezone.utc)

    # Check OTP record
    stmt = (
        select(OtpRecord)
        .where(
            OtpRecord.phone_number == phone,
            OtpRecord.otp_code == code,
            OtpRecord.is_used == False,
            OtpRecord.expires_at > now,
        )
        .order_by(desc(OtpRecord.created_at))
    )
    result = await db.execute(stmt)
    otp_record = result.scalars().first()

    if not otp_record and not (ALLOW_MASTER_OTP and code == MASTER_OTP_CODE):
        if attempts >= OTP_MAX_VERIFY_ATTEMPTS:
            # Burn any outstanding code so it can't be brute-forced further
            await db.execute(
                update(OtpRecord)
                .where(OtpRecord.phone_number == phone, OtpRecord.is_used == False)
                .values(is_used=True)
            )
            await db.commit()
            raise HTTPException(status_code=429, detail="Too many incorrect attempts. Request a new code.")
        raise HTTPException(status_code=400, detail="Invalid or expired OTP code.")

    if otp_record:
        # Claim the code atomically: of two requests racing with the same code only one wins
        claimed = await db.execute(
            update(OtpRecord)
            .where(OtpRecord.id == otp_record.id, OtpRecord.is_used == False)  # noqa: E712
            .values(is_used=True)
        )
        await db.commit()
        if claimed.rowcount != 1:
            raise HTTPException(status_code=400, detail="Invalid or expired OTP code.")
    await rate_limits.reset_counter(attempt_key)

    # Find or Create User
    user_stmt = select(User).where(or_(User.id == phone, User.phone_number == phone))
    user_res = await db.execute(user_stmt)
    user = user_res.scalars().first()
    is_new = user is None

    if not user:
        colors = ["#FFB800", "#10B981", "#3B82F6", "#8B5CF6", "#EC4899", "#F97316", "#14B8A6", "#EF4444"]
        # From the digits themselves: the length is the same for every number in a country, which
        # used to give everyone in India the same green
        avatar_color = colors[sum(int(d) for d in phone if d.isdigit()) % len(colors)]
        username = payload.username or f"User {phone[-4:]}"

        user = User(
            id=phone,  # Use phone as unique ID for straightforward routing
            phone_number=phone,
            username=username,
            avatar_color=avatar_color,
            quote="Hey there! I am using Flick.",
            is_online=True,
            last_seen=now,
        )
        db.add(user)
        await db.commit()
        await db.refresh(user)

    token = create_access_token(user_id=user.id, phone_number=user.phone_number, token_version=user.token_version or 0)

    return AuthResponse(
        access_token=token,
        token_type="bearer",
        user=user.to_dict(),
        is_new_user=is_new,
    )


@router.post("/api/auth/logout")
async def logout(current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    """Signs out: revokes this account's tokens and detaches the push token from the phone."""
    sid = web_session_id(current_user)
    if sid:
        # A linked browser signs out only itself; the phone stays logged in
        await web_sessions.revoke_session(db, current_user.id, sid)
        return {"message": "Logged out"}
    await web_sessions.revoke_all_sessions(db, current_user.id)
    current_user.token_version = (current_user.token_version or 0) + 1  # every older token stops working
    current_user.fcm_token = None
    current_user.is_online = False
    current_user.last_seen = datetime.now(timezone.utc)
    await db.commit()
    # The phone's open socket(s) were authenticated with the now-revoked token: close them too
    # (4401 = "log out"), on whichever server holds them
    await manager.close_user_sockets(current_user.id, reason="Logged out")
    return {"message": "Logged out"}


@router.delete("/api/auth/me")
async def delete_my_account(current_user: User = Depends(get_phone_user), db: AsyncSession = Depends(get_db)):
    """Permanently deletes the authenticated account and everything it owns."""
    from sqlalchemy import delete as sql_delete
    from models import ConversationChangeLog, UserSyncSequence

    me = current_user.id
    peer_rows = await db.execute(
        select(Message.sender_id, Message.receiver_id).where(
            or_(Message.sender_id == me, Message.receiver_id == me)
        )
    )
    peers = {p for row in peer_rows.all() for p in row if p and p != me}

    await db.execute(sql_delete(Message).where(or_(Message.sender_id == me, Message.receiver_id == me)))
    await db.execute(sql_delete(Status).where(Status.user_id == me))
    await db.execute(sql_delete(Meeting).where(Meeting.user_id == me))
    await db.execute(sql_delete(OtpRecord).where(OtpRecord.phone_number == current_user.phone_number))
    await db.execute(sql_delete(ConversationChangeLog).where(ConversationChangeLog.user_id == me))
    await db.execute(sql_delete(UserSyncSequence).where(UserSyncSequence.user_id == me))
    await groups.remove_user_from_all_groups(db, current_user)
    await web_sessions.revoke_all_sessions(db, me)
    from models import MessageReaction
    await db.execute(sql_delete(MessageReaction).where(MessageReaction.user_id == me))
    await archive.delete_archives_for(db, me)
    await calllog.delete_calls_for(db, me)
    await db.execute(
        sql_delete(SavedContact).where(or_(SavedContact.user_id == me, SavedContact.contact_id == me))
    )

    # Tell everyone this user chatted with to drop the conversation on their next sync
    for peer in peers:
        try:
            await record_conversation_change(db, user_id=peer, peer_id=me, action="deleted")
        except Exception as sync_err:
            logger.warning("Failed to record account deletion for %s: %s", peer, sync_err)

    await db.delete(current_user)
    await db.commit()
    # Close the deleted account's phone socket(s) and any browser still connected
    await manager.close_user_sockets(me, reason="Account deleted")
    logger.info("Deleted account %s", me)
    return {"message": "Account deleted"}


@router.get("/api/auth/me", response_model=UserProfileResponse)
@router.get("/api/users/profile/me", response_model=UserProfileResponse)
@router.get("/api/users/me", response_model=UserProfileResponse)
async def get_my_profile(current_user: User = Depends(get_current_user)):
    """Returns the authenticated user's profile to maintain login state."""
    return UserProfileResponse(
        id=current_user.id,
        phone_number=current_user.phone_number,
        username=current_user.username,
        avatar_color=current_user.avatar_color,
        avatar_url=current_user.avatar_url,
        quote=current_user.quote or "Hey there! I am using Flick.",
        is_online=current_user.is_online,
        last_seen=iso_utc(current_user.last_seen),
    )


@router.put("/api/users/profile", response_model=UserProfileResponse)
@router.patch("/api/users/profile", response_model=UserProfileResponse)
@router.post("/api/users/profile", response_model=UserProfileResponse)
@router.put("/api/profile", response_model=UserProfileResponse)
@router.patch("/api/profile", response_model=UserProfileResponse)
@router.post("/api/profile", response_model=UserProfileResponse)
async def update_user_profile(
    payload: UpdateProfileRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Updates the authenticated user's profile (username, avatar_url, avatar_color, quote)."""
    try:
        # user_id / phone_number in the body are ignored: you can only edit your own profile
        target_user = current_user

        if payload.username is not None and payload.username.strip():
            target_user.username = payload.username.strip()
        if payload.avatar_url is not None:
            target_user.avatar_url = payload.avatar_url.strip() if payload.avatar_url else None
        if payload.avatar_color is not None and payload.avatar_color.strip():
            target_user.avatar_color = payload.avatar_color.strip()
        if payload.quote is not None:
            target_user.quote = payload.quote.strip() if payload.quote else "Hey there! I am using Flick."

        # Propagate profile updates to conversation changelog for delta sync
        try:
            await record_profile_change_for_contacts(db, target_user)
        except Exception as sync_err:
            logger.warning("Failed to record profile change for sync: %s", sync_err)

        await db.commit()
        await db.refresh(target_user)

        return UserProfileResponse(
            id=target_user.id,
            phone_number=target_user.phone_number,
            username=target_user.username,
            avatar_color=target_user.avatar_color,
            avatar_url=target_user.avatar_url,
            quote=target_user.quote or "Hey there! I am using Flick.",
            is_online=target_user.is_online,
            last_seen=iso_utc(target_user.last_seen),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error updating user profile: %s", e, exc_info=True)
        await db.rollback()
        raise HTTPException(status_code=500, detail="Couldn’t update your profile. Please try again.")


@router.post("/api/users/fcm-token")
async def update_fcm_token(
    payload: UpdateFcmTokenRequest,
    current_user: User = Depends(get_phone_user),
    db: AsyncSession = Depends(get_db),
):
    """Registers this device's FCM push token for the authenticated user."""
    # A device token belongs to one account at a time: detach it from whoever had it before
    await db.execute(
        update(User)
        .where(User.fcm_token == payload.fcm_token, User.id != current_user.id)
        .values(fcm_token=None)
    )
    current_user.fcm_token = payload.fcm_token
    await db.commit()
    logger.info("Updated FCM token for user %s", current_user.id)
    return {"message": "FCM token updated successfully"}


@router.put("/api/users/preferences")
async def update_my_preferences(
    payload: UpdatePreferencesRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Saves the privacy / notification settings the server enforces for other users."""
    prefs = current_user.prefs()
    updates = payload.model_dump(exclude_none=True)
    if "last_seen" in updates and updates["last_seen"] not in ("everyone", "nobody"):
        raise HTTPException(status_code=400, detail="last_seen must be 'everyone' or 'nobody'")
    prefs.update(updates)
    current_user.preferences = json.dumps(prefs)
    await db.commit()

    aliases = [a for a in (current_user.id, current_user.phone_number) if a]
    if prefs["last_seen"] == "nobody":
        manager.last_seen_hidden.update(aliases)
    else:
        manager.last_seen_hidden.difference_update(aliases)
    return prefs
