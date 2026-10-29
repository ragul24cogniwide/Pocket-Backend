"""
Delta Sync Service for Flick Messenger.
Manages per-user change sequence versioning, tombstones, and conversation delta queries.
"""

import base64
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import and_, desc, func, or_, select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from models import ConversationChangeLog, Group, GroupMember, GroupMessage, Message, User, UserSyncSequence, iso_utc
from schemas import ChatContactItem, SyncConversationsResponse

logger = logging.getLogger("SyncService")

AGENT_USER_ID = "ai_assistant"
AGENT_NAME = "Flick AI Agent"
AGENT_AVATAR = "AI"
AGENT_AVATAR_COLOR = "#FFB800"
AGENT_QUOTE = "Your Personal AI Collaboration Assistant ⚡"

GROUP_ID_PREFIX = "grp_"


def is_group_id(value) -> bool:
    return isinstance(value, str) and value.startswith(GROUP_ID_PREFIX)


def group_initials(name: str) -> str:
    """Avatar letters of a group: the start of its name ("Office Team" -> "OT", "Family" -> "F")."""
    parts = (name or "").split()
    if not parts:
        return "G"
    return "".join(p[0] for p in parts[:2]).upper()


# Retention period for change logs and tombstones (30 days minimum)
CHANGE_LOG_RETENTION_DAYS = 30


def encode_cursor(version: int, timestamp: Optional[float] = None) -> str:
    """Encodes a monotonically increasing sequence integer and epoch timestamp into an opaque cursor string."""
    ts = int(timestamp if timestamp is not None else datetime.now(timezone.utc).timestamp())
    payload = json.dumps({"v": int(version), "t": ts})
    return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("utf-8")


def decode_cursor(cursor_str: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    """Decodes an opaque cursor string into (version, timestamp_epoch). Returns (None, None) if invalid."""
    if not cursor_str:
        return None, None
    raw = cursor_str.strip()
    if raw.isdigit():
        return int(raw), None
    try:
        decoded = base64.urlsafe_b64decode(raw.encode("utf-8")).decode("utf-8")
        if decoded.startswith("{"):
            data = json.loads(decoded)
            return int(data.get("v", 0)), data.get("t")
        if decoded.startswith("cur_v1_"):
            return int(decoded.replace("cur_v1_", "")), None
        if decoded.isdigit():
            return int(decoded), None
    except Exception:
        pass
    return None, None


async def get_user_current_version(db: AsyncSession, user_id: str) -> int:
    """Gets the current change sequence version for a user, or 0 if none recorded yet."""
    stmt = select(UserSyncSequence.current_version).where(UserSyncSequence.user_id == user_id)
    res = await db.execute(stmt)
    val = res.scalar_one_or_none()
    return int(val) if val is not None else 0


async def get_next_sync_version(db: AsyncSession, user_id: str) -> int:
    """Atomically increments and returns the monotonically increasing version for a user."""
    stmt = select(UserSyncSequence).where(UserSyncSequence.user_id == user_id)
    res = await db.execute(stmt)
    seq = res.scalar_one_or_none()
    if not seq:
        # Check max existing version in change log
        max_stmt = select(func.max(ConversationChangeLog.version)).where(ConversationChangeLog.user_id == user_id)
        max_res = await db.execute(max_stmt)
        curr = max_res.scalar_one() or 0
        seq = UserSyncSequence(user_id=user_id, current_version=curr + 1)
        db.add(seq)
        await db.flush()
        return seq.current_version
    else:
        seq.current_version += 1
        await db.flush()
        return seq.current_version


async def build_conversation_snapshot(
    db: AsyncSession, user_id: str, peer_id: str, version: int = 0
) -> Optional[dict]:
    """Builds a conversation dictionary for user_id chatting with peer_id without volatile online state."""
    if is_group_id(peer_id):
        return await build_group_snapshot(db, user_id, peer_id, version=version)
    if peer_id == AGENT_USER_ID:
        agent_msg_stmt = (
            select(Message)
            .where(
                or_(
                    and_(Message.sender_id == user_id, Message.receiver_id == AGENT_USER_ID),
                    and_(Message.sender_id == AGENT_USER_ID, Message.receiver_id == user_id),
                )
            )
            .order_by(desc(Message.timestamp))
            .limit(1)
        )
        msg_res = await db.execute(agent_msg_stmt)
        latest_agent_msg = msg_res.scalars().first()

        return {
            "id": AGENT_USER_ID,
            "phone_number": "+00000000000",
            "name": AGENT_NAME,
            "avatar": AGENT_AVATAR,
            "avatar_color": AGENT_AVATAR_COLOR,
            "avatar_url": None,
            "quote": AGENT_QUOTE,
            "online": False,  # Volatile presence is WebSocket-only
            "last_message": latest_agent_msg.content if latest_agent_msg else "Your personal AI agent is ready. Ask me anything!",
            "time": iso_utc(latest_agent_msg.timestamp) if latest_agent_msg and latest_agent_msg.timestamp else "",
            "unread": 0,
            "last_seen": None,
            "version": version,
        }

    peer_stmt = select(User).where(User.id == peer_id)
    peer_res = await db.execute(peer_stmt)
    peer = peer_res.scalar_one_or_none()
    if not peer:
        return None

    # Calculate identifier aliases
    cur_ids = {user_id}
    cur_digits = "".join(filter(str.isdigit, str(user_id)))
    cur_last10 = cur_digits[-10:] if len(cur_digits) >= 7 else cur_digits
    if cur_last10:
        cur_ids.add(cur_last10)
        cur_ids.add(f"+91{cur_last10}")

    u_ids = {peer.id, peer.phone_number}
    u_digits = "".join(filter(str.isdigit, str(peer.id)))
    u_last10 = u_digits[-10:] if len(u_digits) >= 7 else u_digits
    if u_last10:
        u_ids.add(u_last10)
        u_ids.add(f"+91{u_last10}")
    cur_ids = {str(x) for x in cur_ids if x}
    u_ids = {str(x) for x in u_ids if x}

    # Latest message
    msg_stmt = (
        select(Message)
        .where(
            or_(
                Message.sender_id.in_(cur_ids) & Message.receiver_id.in_(u_ids),
                Message.sender_id.in_(u_ids) & Message.receiver_id.in_(cur_ids),
                (
                    Message.sender_id.endswith(cur_last10) & Message.receiver_id.endswith(u_last10)
                    if cur_last10 and u_last10 else False
                ),
                (
                    Message.sender_id.endswith(u_last10) & Message.receiver_id.endswith(cur_last10)
                    if cur_last10 and u_last10 else False
                ),
            )
        )
        .order_by(desc(Message.timestamp))
        .limit(1)
    )
    msg_res = await db.execute(msg_stmt)
    latest_msg = msg_res.scalars().first()

    # Unread count
    unread_stmt = select(func.count(Message.id)).where(
        Message.sender_id.in_(u_ids),
        Message.receiver_id.in_(cur_ids),
        Message.status != "READ",
    )
    unread_res = await db.execute(unread_stmt)
    unread_count = unread_res.scalar_one() or 0

    initials = (
        "".join([part[0] for part in peer.username.split()[:2]]).upper()
        if peer.username
        else "FK"
    )

    return {
        "id": peer.id,
        "phone_number": peer.phone_number,
        "name": peer.username,
        "avatar": initials,
        "avatar_color": peer.avatar_color or "#FFB800",
        "avatar_url": peer.avatar_url,
        "quote": peer.quote or "Hey there! I am using Flick.",
        "online": False,  # Client maintains in-memory presence over WebSocket
        "last_message": latest_msg.content if latest_msg else (peer.quote or "Tap to start conversation"),
        "time": iso_utc(latest_msg.timestamp) if latest_msg and latest_msg.timestamp else "",
        "unread": unread_count,
        "last_seen": None,
        "version": version,
    }


async def build_group_snapshot(db: AsyncSession, user_id: str, group_id: str, version: int = 0) -> Optional[dict]:
    """
    Chat-list row of a group for one member. None when they're not (and never were) in it, which
    the sync turns into a "deleted" row. Former members keep the row, frozen at the time they left.
    """
    group = await db.get(Group, group_id)
    if not group:
        return None
    member = (
        await db.execute(
            select(GroupMember).where(GroupMember.group_id == group_id, GroupMember.user_id == user_id)
        )
    ).scalar_one_or_none()
    if not member:
        return None

    # Only what this member could see: from when they joined until they left
    visible = [GroupMessage.group_id == group_id, GroupMessage.timestamp >= member.joined_at]
    if member.left_at is not None:
        visible.append(GroupMessage.timestamp <= member.left_at)

    recent = (
        await db.execute(select(GroupMessage).where(*visible).order_by(desc(GroupMessage.timestamp)).limit(20))
    ).scalars().all()
    latest = next((m for m in recent if user_id not in m.hidden_ids()), None)

    preview = "Tap to start chatting"
    if latest is not None:
        if latest.kind == "system" or not latest.sender_id:
            preview = latest.content
        elif latest.sender_id == user_id:
            preview = f"You: {latest.content}"
        else:
            sender = await db.get(User, latest.sender_id)
            preview = f"{(sender.username if sender else None) or 'Someone'}: {latest.content}"

    read_from = max(member.last_read_at or member.joined_at, member.joined_at)
    unread = (
        await db.execute(
            select(func.count(GroupMessage.id)).where(
                *visible,
                GroupMessage.timestamp > read_from,
                GroupMessage.kind == "text",
                GroupMessage.sender_id != user_id,
            )
        )
    ).scalar_one() or 0
    member_count = (
        await db.execute(
            select(func.count(GroupMember.id)).where(GroupMember.group_id == group_id, GroupMember.left_at.is_(None))
        )
    ).scalar_one() or 0

    when = latest.timestamp if latest is not None else group.created_at
    return {
        "id": group.id,
        "phone_number": "",
        "name": group.name,
        "avatar": group_initials(group.name),
        "avatar_color": group.avatar_color or "#3B82F6",
        "avatar_url": group.avatar_url,
        "quote": group.description or None,
        "online": False,
        "last_message": preview,
        "time": iso_utc(when) if when else "",
        "unread": unread,
        "last_seen": None,
        "version": version,
        "is_group": True,
        "member_count": member_count,
        "is_member": member.left_at is None,
    }


async def record_conversation_change(
    db: AsyncSession, user_id: str, peer_id: str, action: str = "updated"
) -> int:
    """
    Bumps the user's sync version and records a change log entry (or tombstone).
    Presence changes (online / last_seen) MUST NOT invoke this function.
    """
    if not user_id or not peer_id:
        return 0

    new_version = await get_next_sync_version(db, user_id)

    payload_str = None
    if action in ("created", "updated"):
        snapshot = await build_conversation_snapshot(db, user_id, peer_id, version=new_version)
        if snapshot:
            payload_str = json.dumps(snapshot)
        else:
            action = "deleted"

    entry = ConversationChangeLog(
        user_id=user_id,
        peer_id=peer_id,
        version=new_version,
        action=action,
        payload=payload_str,
        created_at=datetime.now(timezone.utc),
    )
    db.add(entry)
    await db.flush()
    return new_version


async def record_profile_change_for_contacts(db: AsyncSession, updated_user: User):
    """
    When a user updates profile fields (name, avatar, quote), record an updated
    conversation change for all users who have had conversations with them.
    """
    # Find all distinct user IDs who have exchanged messages with this user
    stmt = (
        select(Message.sender_id, Message.receiver_id)
        .where(or_(Message.sender_id == updated_user.id, Message.receiver_id == updated_user.id))
        .distinct()
    )
    res = await db.execute(stmt)
    rows = res.all()
    peer_ids = set()
    for s_id, r_id in rows:
        if s_id and s_id != updated_user.id:
            peer_ids.add(s_id)
        if r_id and r_id != updated_user.id:
            peer_ids.add(r_id)

    for peer_id in peer_ids:
        try:
            await record_conversation_change(db, user_id=peer_id, peer_id=updated_user.id, action="updated")
        except Exception as e:
            logger.warning("Failed to record profile change for peer %s: %s", peer_id, e)


async def build_all_conversations_for_user(
    db: AsyncSession, current_user: User, base_version: int = 1
) -> List[dict]:
    """
    Generates the full list of conversations for current_user: only people they have actually
    exchanged messages with (like any chat app), not every registered user.
    """
    my_ids = {i for i in (current_user.id, current_user.phone_number) if i}
    pair_rows = await db.execute(
        select(Message.sender_id, Message.receiver_id).where(
            or_(Message.sender_id.in_(my_ids), Message.receiver_id.in_(my_ids))
        )
    )
    peer_ids = {p for row in pair_rows.all() for p in row if p and p not in my_ids and p != AGENT_USER_ID}
    if not peer_ids:
        users = []
    else:
        stmt = (
            select(User)
            .where(or_(User.id.in_(peer_ids), User.phone_number.in_(peer_ids)), User.id != current_user.id)
            .order_by(User.username.asc())
        )
        users = (await db.execute(stmt)).scalars().all()

    conversations = []
    seen_identifiers = set()

    for u in users:
        if u.id in seen_identifiers:
            continue
        seen_identifiers.add(u.id)

        snap = await build_conversation_snapshot(db, current_user.id, u.id, version=base_version)
        if snap:
            conversations.append(snap)

    # Groups this person is (or was) in
    group_ids = (
        await db.execute(select(GroupMember.group_id).where(GroupMember.user_id == current_user.id))
    ).scalars().all()
    for gid in set(group_ids):
        snap = await build_group_snapshot(db, current_user.id, gid, version=base_version)
        if snap:
            conversations.append(snap)

    # Prepend AI agent
    agent_snap = await build_conversation_snapshot(db, current_user.id, AGENT_USER_ID, version=base_version)
    if agent_snap:
        conversations = [agent_snap] + conversations

    return conversations


async def get_conversation_sync_page(
    db: AsyncSession, current_user: User, cursor_str: Optional[str] = None, limit: int = 50
) -> Dict[str, Any]:
    """
    Evaluates cursor and returns paginated delta changes or full sync.
    """
    limit = max(1, min(limit, 200))
    cursor_version, cursor_ts = decode_cursor(cursor_str)

    # 1. Check if cursor is missing or invalid
    if cursor_version is None or cursor_version < 0:
        logger.info("Sync for %s: missing or invalid cursor -> full sync", current_user.id)
        curr_ver = await get_user_current_version(db, current_user.id)
        all_convs = await build_all_conversations_for_user(db, current_user, base_version=curr_ver)
        return {
            "cursor": encode_cursor(curr_ver),
            "has_more": False,
            "full_sync": True,
            "created": all_convs,
            "updated": [],
            "deleted": [],
        }

    # 1b. Cursor from a "future" the server doesn't know: the sync history was reset or wiped
    #     (e.g. database restored or tables cleared). The client must replace its cache.
    curr_ver = await get_user_current_version(db, current_user.id)
    if cursor_version > curr_ver:
        logger.info(
            "Sync for %s: cursor v%s is ahead of server v%s (history reset) -> full sync",
            current_user.id, cursor_version, curr_ver,
        )
        all_convs = await build_all_conversations_for_user(db, current_user, base_version=curr_ver)
        return {
            "cursor": encode_cursor(curr_ver),
            "has_more": False,
            "full_sync": True,
            "created": all_convs,
            "updated": [],
            "deleted": [],
        }

    # 2. Check if cursor timestamp is older than retained history (30 days retention check)
    now_ts = datetime.now(timezone.utc).timestamp()
    if cursor_ts and (now_ts - cursor_ts > CHANGE_LOG_RETENTION_DAYS * 86400):
        logger.info("Sync for %s: cursor timestamp is > 30 days old -> full sync", current_user.id)
        curr_ver = await get_user_current_version(db, current_user.id)
        all_convs = await build_all_conversations_for_user(db, current_user, base_version=curr_ver)
        return {
            "cursor": encode_cursor(curr_ver),
            "has_more": False,
            "full_sync": True,
            "created": all_convs,
            "updated": [],
            "deleted": [],
        }

    # 3. Check if changes prior to cursor have been pruned from DB
    oldest_log_stmt = (
        select(ConversationChangeLog.version)
        .where(ConversationChangeLog.user_id == current_user.id)
        .order_by(ConversationChangeLog.version.asc())
        .limit(1)
    )
    oldest_res = await db.execute(oldest_log_stmt)
    oldest_ver = oldest_res.scalar_one_or_none()

    if oldest_ver is not None and oldest_ver > 1 and cursor_version < oldest_ver:
        # Some versions were pruned from the table!
        logger.info(
            "Sync for %s: cursor version %s is older than pruned history threshold %s -> full sync",
            current_user.id,
            cursor_version,
            oldest_ver,
        )
        curr_ver = await get_user_current_version(db, current_user.id)
        all_convs = await build_all_conversations_for_user(db, current_user, base_version=curr_ver)
        return {
            "cursor": encode_cursor(curr_ver),
            "has_more": False,
            "full_sync": True,
            "created": all_convs,
            "updated": [],
            "deleted": [],
        }

    # 3. Delta query: changes with version > cursor_version
    delta_stmt = (
        select(ConversationChangeLog)
        .where(
            ConversationChangeLog.user_id == current_user.id,
            ConversationChangeLog.version > cursor_version,
        )
        .order_by(ConversationChangeLog.version.asc())
        .limit(limit + 1)
    )
    delta_res = await db.execute(delta_stmt)
    logs = delta_res.scalars().all()

    if not logs:
        # No changes since last sync
        return {
            "cursor": cursor_str if cursor_str else encode_cursor(cursor_version),
            "has_more": False,
            "full_sync": False,
            "created": [],
            "updated": [],
            "deleted": [],
        }

    has_more = len(logs) > limit
    page_logs = logs[:limit]
    new_cursor_version = page_logs[-1].version

    # Consolidate changes by peer_id in chronological order
    peer_actions: Dict[str, Tuple[str, Optional[dict]]] = {}
    for log in page_logs:
        pid = log.peer_id
        action = log.action
        payload_dict = None
        if log.payload:
            try:
                payload_dict = json.loads(log.payload)
            except Exception:
                payload_dict = None

        if action == "deleted":
            peer_actions[pid] = ("deleted", None)
        elif action == "created":
            peer_actions[pid] = ("created", payload_dict)
        elif action == "updated":
            # If it was created previously in this page, preserve 'created' status with newer data
            prev_action, _ = peer_actions.get(pid, (None, None))
            if prev_action == "created":
                peer_actions[pid] = ("created", payload_dict)
            else:
                peer_actions[pid] = ("updated", payload_dict)

    created_list = []
    updated_list = []
    deleted_list = []

    for pid, (action, payload) in peer_actions.items():
        if action == "deleted":
            deleted_list.append(pid)
        elif action == "created" and payload:
            created_list.append(payload)
        elif action == "updated" and payload:
            updated_list.append(payload)

    return {
        "cursor": encode_cursor(new_cursor_version),
        "has_more": has_more,
        "full_sync": False,
        "created": created_list,
        "updated": updated_list,
        "deleted": deleted_list,
    }


async def prune_expired_change_logs(db: AsyncSession, days: int = CHANGE_LOG_RETENTION_DAYS) -> int:
    """Removes change log entries older than the retention period."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    stmt = delete(ConversationChangeLog).where(ConversationChangeLog.created_at < cutoff)
    res = await db.execute(stmt)
    await db.commit()
    return res.rowcount or 0
