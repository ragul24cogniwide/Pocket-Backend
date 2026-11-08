"""The real-time WebSocket gateway (/ws/{user_id})."""

import asyncio
import json
import logging
from datetime import datetime, timezone


from fastapi import WebSocket, WebSocketDisconnect
from sqlalchemy import or_, select, update

from auth import (
    user_from_token,
    web_session_id,
)
from connection_manager import manager, PHONE_DEVICE
from database import async_session_factory
from models import Message, User
from sync_service import (
    record_conversation_change,
)
import groups
import web_sessions
from groups import GroupError, is_group_id
from fastapi import APIRouter
from call_routes import CALL_SIGNAL_TYPES, CallError, process_call_signal
from core import (
    MAX_WS_FRAME_CHARS,
    find_user,
    is_visible_to,
    receipts_shared,
    visible_owner_ids,
)
from messaging import (
    SendError,
    clear_chat_notification,
    flush_offline_messages,
    mark_messages_delivered,
    read_group_and_clear,
    send_direct_message,
)

logger = logging.getLogger("Main")

router = APIRouter()

# -----------------------------------------------------------------------------
# Real-Time WebSocket Gateway
# -----------------------------------------------------------------------------
WS_CLOSE_UNAUTHORIZED = 4401  # app-defined close code: the client should log the user out
# The app pings every 10s. Silence this long means the phone is gone (lost signal, switched
# networks) even if TCP hasn't noticed yet, so mark it offline now instead of minutes later.
WS_IDLE_TIMEOUT_SECONDS = 25
WS_LINKED_IDLE_TIMEOUT_SECONDS = 90


@router.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    """
    Core bidirectional real-time communication endpoint.
    Handles:
    - User connection and Redis Pub/Sub subscription
    - Guaranteed message persistence and delivery
    - Offline queue replay
    - Typing indicators (typing_start / typing_stop)
    - Status receipts (SENT -> DELIVERED -> READ)
    - Heartbeat keep-alive
    """
    # Authenticate before registering the socket: the token must belong to the user in the path.
    # Without this, anyone could connect as any phone number and read or send that person's messages.
    # The app sends it as "Authorization: Bearer <jwt>" so it never appears in URL access logs;
    # ?token=<jwt> is accepted for clients that can't set headers (e.g. browsers).
    auth_header = websocket.headers.get("authorization", "")
    token = auth_header[7:].strip() if auth_header.lower().startswith("bearer ") else websocket.query_params.get("token")
    ticket = websocket.query_params.get("ticket")
    async with async_session_factory() as db:
        # Flick Web opens its socket with a one-time ticket (see web_sessions.py)
        if ticket:
            user_obj, web_sid = await web_sessions.consume_ws_ticket(ticket, db)
        else:
            user_obj = await user_from_token(token, db)
            web_sid = web_session_id(user_obj)
        if not user_obj or user_id.strip() not in {user_obj.id, user_obj.phone_number}:
            await websocket.accept()
            await websocket.close(code=WS_CLOSE_UNAUTHORIZED, reason="Invalid or expired session")
            return
        user_id = user_obj.id
        user_obj.is_online = True
        await db.commit()
        alias_ids = [user_obj.id, user_obj.phone_number]
        if user_obj.prefs().get("last_seen") == "nobody":
            manager.last_seen_hidden.update(a for a in alias_ids if a)
        else:
            manager.last_seen_hidden.difference_update(alias_ids)

    if web_sid:
        # A linked browser joins the phone's connection instead of replacing it
        await manager.connect_linked(user_id, websocket, web_sid, alias_ids)
    else:
        await manager.connect(user_id, websocket)
        manager.register_aliases(user_id, alias_ids)
    # Browser tabs in the background may only get to ping about once a minute
    idle_timeout = WS_LINKED_IDLE_TIMEOUT_SECONDS if web_sid else WS_IDLE_TIMEOUT_SECONDS
    await manager.refresh_presence(user_id)
    async with async_session_factory() as db:
        me_obj = await db.get(User, user_id)
        my_peer_ids = await visible_owner_ids(db, me_obj) if me_obj else set()
    # Presence changes reach this user only for people who let them see it: who wrote to them
    # or saved them as a contact (visible_owner_ids), not anyone they merely wrote to
    manager.watch_presence(user_id, my_peer_ids)
    try:
        await manager.send_online_snapshot(websocket, my_peer_ids)
    except Exception:
        pass

    # Flush any unread messages stored while this user was offline
    asyncio.create_task(flush_offline_messages(user_id))
    asyncio.create_task(groups.mark_delivered_on_connect(user_id))

    try:
        while True:
            try:
                raw_data = await asyncio.wait_for(websocket.receive_text(), timeout=idle_timeout)
            except asyncio.TimeoutError:
                logger.info("No frames from %s for %ss, treating as disconnected", user_id, idle_timeout)
                try:
                    await websocket.close(code=1001)
                except Exception:
                    pass
                raise WebSocketDisconnect(code=1001)
            if len(raw_data) > MAX_WS_FRAME_CHARS:
                logger.warning("Dropped oversized frame (%d chars) from %s", len(raw_data), user_id)
                continue
            try:
                payload = json.loads(raw_data)
            except json.JSONDecodeError:
                logger.warning("Received invalid JSON from %s", user_id)
                continue

            frame_type = payload.get("type")
            data = payload.get("data", {})

            # -----------------------------------------------------------------
            # 1. SEND MESSAGE
            # -----------------------------------------------------------------
            if frame_type in ("send_message", "chat_message"):
                receiver_id = data.get("receiver_id")
                content = (data.get("content") or "").strip()
                client_msg_id = data.get("temp_msg_id") or data.get("temp_id")
                client_temp_id = client_msg_id or f"temp_{int(datetime.now(timezone.utc).timestamp()*1000)}"

                if not receiver_id or not content:
                    continue
                if is_group_id(receiver_id):
                    try:
                        sent = await groups.send_group_message(
                            user_id, receiver_id, content, client_msg_id, data.get("reply_to"), data.get("metadata")
                        )
                    except GroupError as ge:
                        await websocket.send_text(json.dumps({"type": "message_error", "data": {
                            "temp_msg_id": client_msg_id, "reason": ge.reason}}))
                        continue
                    await websocket.send_text(json.dumps({"type": "ack_sent", "data": {
                        "temp_id": client_temp_id,
                        "temp_msg_id": client_temp_id,
                        "message_id": sent["message_id"],
                        "group_id": receiver_id,
                        "timestamp": sent["timestamp"],
                        "status": sent["status"],
                        **({"duplicate": True} if sent.get("duplicate") else {}),
                    }}))
                    if not sent.get("duplicate"):
                        await manager.send_to_my_devices(
                            user_id, {"type": "message_sent_sync", "data": sent}, exclude=websocket
                        )
                    continue
                async def send_ack(msg: dict, duplicate: bool = False) -> None:
                    """ack_sent to this socket: the message is stored (sent before anyone else hears of it)."""
                    frame = {"type": "ack_sent", "data": {
                        "temp_id": client_temp_id,
                        "temp_msg_id": client_temp_id,
                        "message_id": msg["message_id"],
                        "timestamp": msg["timestamp"],
                        "status": msg["status"],
                        **({"duplicate": True} if duplicate else {}),
                    }}
                    if duplicate:
                        await websocket.send_text(json.dumps(frame))
                        return
                    try:
                        await websocket.send_text(json.dumps(frame))
                    except Exception:
                        await manager.send_personal_message(user_id, frame)

                # Stored once, then delivered everywhere (shared with REST: messaging.send_direct_message).
                # The app re-sends queued messages after reconnecting; one that already arrived is
                # just confirmed again instead of being stored and forwarded twice.
                try:
                    result = await send_direct_message(
                        user_id, receiver_id, content, client_msg_id, data.get("reply_to"), data.get("metadata"),
                        exclude_ws=websocket, on_stored=send_ack,
                    )
                except SendError as se:
                    await websocket.send_text(json.dumps({"type": "message_error", "data": {
                        "temp_msg_id": client_msg_id, "reason": se.reason}}))
                    continue
                if result.duplicate:
                    await send_ack(result.message, duplicate=True)


            # -----------------------------------------------------------------
            # 2. TYPING INDICATORS
            # -----------------------------------------------------------------
            elif frame_type in ("typing_start", "typing_stop"):
                receiver_id = data.get("receiver_id")
                if is_group_id(receiver_id):
                    asyncio.create_task(groups.relay_typing(user_id, receiver_id, frame_type == "typing_start"))
                elif receiver_id:
                    typing_frame = {
                        "type": frame_type,
                        "data": {
                            "sender_id": user_id,
                            "receiver_id": receiver_id,
                        },
                    }
                    await manager.send_personal_message(receiver_id, typing_frame)

            # -----------------------------------------------------------------
            # 3. READ & DELIVERY RECEIPTS
            # -----------------------------------------------------------------
            elif frame_type == "ack_group_delivered":
                # A group message reached this device: grey double ticks for its sender (only for
                # groups this person is a member of; see groups.mark_delivered_on_connect)
                group_id = data.get("group_id")
                if is_group_id(group_id):
                    asyncio.create_task(groups.mark_delivered_on_connect(user_id, only=[group_id]))

            elif frame_type == "ack_delivered":
                # The app got a message over the socket: grey double ticks for its sender
                if data.get("message_id"):
                    await mark_messages_delivered(user_id, [data.get("message_id")])

            elif frame_type == "mark_read":
                sender_id = data.get("sender_id")  # original sender of the messages being read
                if sender_id:
                    # Read on one device: the unread badge clears on the others too
                    await manager.send_to_my_devices(
                        user_id, {"type": "read_sync", "data": {"peer_id": sender_id}}, exclude=websocket
                    )
                if is_group_id(sender_id):
                    if web_sid:  # read on Flick Web: the phone's notification for it goes too
                        asyncio.create_task(read_group_and_clear(user_id, sender_id))
                    else:
                        asyncio.create_task(groups.mark_group_read(user_id, sender_id))
                elif sender_id:
                    now = datetime.now(timezone.utc)
                    async with async_session_factory() as db:
                        reader = await db.get(User, user_id)
                        sender_res = await db.execute(
                            select(User).where(or_(User.id == sender_id, User.phone_number == sender_id))
                        )
                        sender = sender_res.scalars().first()
                        # Settings at the moment of reading decide; nothing read now can turn blue later
                        hidden_now = not receipts_shared(reader, sender)

                        unread_res = await db.execute(
                            select(Message.id, Message.receipt_hidden).where(
                                Message.sender_id == sender_id,
                                Message.receiver_id == user_id,
                                Message.status != "READ",
                            )
                        )
                        unread_rows = unread_res.all()
                        if unread_rows:
                            await db.execute(
                                update(Message)
                                .where(Message.id.in_([r.id for r in unread_rows]))
                                .values(status="READ", read_at=now)
                            )
                            if hidden_now:
                                await db.execute(
                                    update(Message)
                                    .where(Message.id.in_([r.id for r in unread_rows]))
                                    .values(receipt_hidden=True)
                                )
                            await db.commit()

                            try:
                                await record_conversation_change(db, user_id=user_id, peer_id=sender_id, action="updated")
                                await db.commit()
                            except Exception as sync_err:
                                logger.warning("Failed to record conversation change for mark_read: %s", sync_err)

                            if web_sid:
                                # Read on Flick Web: remove the chat's notification from the phone
                                # (the phone removes it by itself when it's read there)
                                asyncio.create_task(clear_chat_notification(user_id, sender.id if sender else sender_id))

                    # The reader's unread badge clears either way. The sender only gets blue ticks
                    # for messages whose receipts are shareable (decided per message).
                    visible_ids = [] if hidden_now else [r.id for r in unread_rows if not r.receipt_hidden]
                    if not visible_ids:
                        continue
                    ack_read_frame = {
                        "type": "ack_read",
                        "data": {
                            "reader_id": user_id,
                            "sender_id": sender_id,
                            "message_ids": visible_ids,
                            "read_at": now.isoformat(),
                        },
                    }
                    await manager.send_personal_message(sender_id, ack_read_frame)

            # -----------------------------------------------------------------
            # 4. REAL-TIME CALL AUDIO CHUNK FORWARDING (Zero-Latency Relay)
            # -----------------------------------------------------------------
            elif frame_type in CALL_SIGNAL_TYPES:
                # Calls: only the two participants of a call can signal it (see process_call_signal)
                try:
                    await process_call_signal(user_id, frame_type, data)
                except CallError as ce:
                    await websocket.send_text(json.dumps({
                        "type": "call_error",
                        "data": {"call_id": (data or {}).get("call_id"), "frame": frame_type, "reason": ce.reason},
                    }))

            # -----------------------------------------------------------------
            # 5. HEARTBEAT / PING & PRESENCE CONTROL
            # -----------------------------------------------------------------
            elif frame_type == "ping":
                await manager.refresh_presence(user_id)
                await websocket.send_text(json.dumps({"type": "pong", "timestamp": datetime.now(timezone.utc).isoformat()}))

            elif frame_type == "presence_online":
                # The phone app came to the foreground, or a Flick Web tab is being used
                manager.set_device_active(user_id, web_sid or PHONE_DEVICE, True)
                async with async_session_factory() as db:
                    await db.execute(
                        update(User)
                        .where(or_(User.id == user_id, User.phone_number == user_id))
                        .values(is_online=True)
                    )
                    await db.commit()
                manager.mark_active(user_id)
                await manager.refresh_presence(user_id)
                await manager.broadcast_presence(user_id, is_online=True)

            elif frame_type == "presence_offline":
                # The phone app went to the background, or a Flick Web tab was hidden / left idle
                if manager.set_device_active(user_id, web_sid or PHONE_DEVICE, False):
                    continue  # still in use on another device: still online
                now_utc = datetime.now(timezone.utc)
                async with async_session_factory() as db:
                    await db.execute(
                        update(User)
                        .where(or_(User.id == user_id, User.phone_number == user_id))
                        .values(is_online=False, last_seen=now_utc)
                    )
                    await db.commit()
                manager.mark_away(user_id)
                await manager.presence_remove(manager._aliases_for(user_id) + alias_ids, user_id=user_id)
                await manager.broadcast_presence(user_id, is_online=False, last_seen=now_utc.isoformat())

            elif frame_type == "query_online_snapshot":
                # The chat list re-syncs all green dots at once (on open, and as a periodic safety net)
                async with async_session_factory() as db:
                    me_obj = await db.get(User, user_id)
                    my_peer_ids = await visible_owner_ids(db, me_obj) if me_obj else set()
                manager.watch_presence(user_id, my_peer_ids)  # people who wrote / saved me since the last refresh
                await manager.send_online_snapshot(websocket, my_peer_ids)

            elif frame_type == "query_presence":
                target_id = data.get("user_id")
                if target_id:
                    target_last_seen = None
                    is_target_online = False
                    target_visible = False
                    async with async_session_factory() as db:
                        target_user = await find_user(db, str(target_id))
                        me_obj = await db.get(User, user_id) if target_user else None
                        # Only people who let me see them (wrote to me / saved me, see
                        # visible_owner_ids). Anyone else, or an unknown id: offline, no last
                        # seen, and no live updates, so a stranger learns nothing.
                        target_visible = bool(target_user and me_obj and await is_visible_to(db, target_user, me_obj))
                        if target_visible:
                            is_target_online = await manager.is_user_online(target_user.id, target_user.phone_number)
                            # Hidden if they chose "Nobody", or if I did (then I can't see anyone's)
                            target_last_seen = (
                                None if manager.is_last_seen_hidden(user_id) else target_user.visible_last_seen()
                            )
                    try:
                        aliases = [target_id]
                        if target_user:
                            aliases.extend([target_user.id, target_user.phone_number])
                        if target_visible:
                            # They opened this chat: keep them posted when this person comes and goes
                            manager.watch_presence(user_id, aliases)
                        await websocket.send_text(json.dumps({
                            "type": "presence_update",
                            "data": {
                                "user_id": target_id,
                                "aliases": [a for a in aliases if a],
                                "is_online": is_target_online,
                                "last_seen": target_last_seen,
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                            }
                        }))
                    except Exception:
                        pass

    except WebSocketDisconnect:
        logger.info("WebSocket disconnected for user %s", user_id)
        if web_sid:
            await manager.disconnect_linked(user_id, websocket)
        else:
            await manager.disconnect(user_id, websocket)
        if not await manager.is_user_online(user_id):
            now_utc = datetime.now(timezone.utc)
            async with async_session_factory() as db:
                await db.execute(
                    update(User)
                    .where(or_(User.id == user_id, User.phone_number == user_id))
                    .values(is_online=False, last_seen=now_utc)
                )
                await db.commit()
    except Exception as e:
        logger.error("WebSocket runtime error for %s: %s", user_id, e)
        if web_sid:
            await manager.disconnect_linked(user_id, websocket)
        else:
            await manager.disconnect(user_id, websocket)
        if not await manager.is_user_online(user_id):
            now_utc = datetime.now(timezone.utc)
            async with async_session_factory() as db:
                await db.execute(
                    update(User)
                    .where(or_(User.id == user_id, User.phone_number == user_id))
                    .values(is_online=False, last_seen=now_utc)
                )
                await db.commit()
