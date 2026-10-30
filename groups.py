"""
Group chats for Flick.

- Anyone can create a group and becomes its admin. Admins add / remove people, rename the group
  and make others admin. Anyone can leave; if the last admin leaves, the longest-standing member
  becomes admin.
- Every action checks membership on the server: only current members can read new messages,
  send, see who is typing or see the member list. People who left keep the history up to the
  moment they left, read-only (like WhatsApp). New members only see messages from when they joined.
- Messages go to every member's open sockets at once; members who are offline get a push and
  catch up through the conversation sync and the history endpoint.
- Ticks: grey double tick once the message reached every member, blue once every member read it
  (in groups read receipts are always shared, as on WhatsApp).
"""

import asyncio
import json
import logging
import re
import secrets
import string
from datetime import datetime, timezone
from typing import Iterable, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import desc, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from auth import get_current_user
from connection_manager import manager
from database import async_session_factory, get_db
from models import Group, GroupMember, GroupMessage, User, iso_utc
from notifications import send_push_notification
from schemas import CreateGroupRequest, GroupAdminRequest, GroupMembersRequest, UpdateGroupRequest
from sync_service import AGENT_USER_ID, GROUP_ID_PREFIX, group_initials, is_group_id, record_conversation_change

logger = logging.getLogger("Groups")

router = APIRouter(prefix="/api/groups", tags=["groups"])

GROUP_ID_RE = re.compile(r"^grp_[a-z]{24}$")
MAX_GROUP_MEMBERS = 256
MAX_MESSAGE_CHARS = 8000
MAX_METADATA_CHARS = 20_000
HISTORY_MAX_PAGE = 100
GROUP_COLORS = ["#3B82F6", "#10B981", "#8B5CF6", "#F97316", "#EC4899", "#14B8A6", "#EF4444", "#6366F1"]
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f​-‏‪-‮⁦-⁩]")


class GroupError(Exception):
    def __init__(self, reason: str, status_code: int = 400):
        super().__init__(reason)
        self.reason = reason
        self.status_code = status_code


def _http(err: GroupError) -> HTTPException:
    return HTTPException(status_code=err.status_code, detail=err.reason)


def utc(dt: Optional[datetime]) -> Optional[datetime]:
    """SQLite hands back naive datetimes; compare everything as aware UTC."""
    if dt is None:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def new_group_id() -> str:
    # Letters only: ids never contain digits, so phone-number matching can't confuse two groups
    return GROUP_ID_PREFIX + "".join(secrets.choice(string.ascii_lowercase) for _ in range(24))


def valid_group_id(value) -> bool:
    return isinstance(value, str) and bool(GROUP_ID_RE.match(value))


def clean_text(raw: Optional[str], max_len: int) -> str:
    """Strips control / bidi-override characters (used to fake names) and extra whitespace."""
    text = _CONTROL_CHARS.sub("", raw or "")
    return re.sub(r"\s+", " ", text).strip()[:max_len]


def display_name(user: Optional[User]) -> str:
    return (user.username if user else None) or "Someone"


# ---------------------------------------------------------------------------
# Lookups and permission checks
# ---------------------------------------------------------------------------
async def get_membership(db: AsyncSession, group_id: str, user_id: str) -> Optional[GroupMember]:
    return (
        await db.execute(select(GroupMember).where(GroupMember.group_id == group_id, GroupMember.user_id == user_id))
    ).scalar_one_or_none()


async def require_member(db: AsyncSession, group_id: str, user_id: str, active: bool = True):
    """
    The group and the caller's membership. Strangers get the same 404 as for a group that
    doesn't exist, so group ids can't be probed.
    """
    if not valid_group_id(group_id):
        raise GroupError("Group not found", 404)
    group = await db.get(Group, group_id)
    member = await get_membership(db, group_id, user_id) if group else None
    if not group or not member:
        raise GroupError("Group not found", 404)
    if active and member.left_at is not None:
        raise GroupError("You're no longer a member of this group", 403)
    return group, member


async def require_admin(db: AsyncSession, group_id: str, user_id: str):
    group, member = await require_member(db, group_id, user_id)
    if member.role != "admin":
        raise GroupError("Only group admins can do that", 403)
    return group, member


async def active_members(db: AsyncSession, group_id: str) -> list:
    return list(
        (
            await db.execute(
                select(GroupMember)
                .where(GroupMember.group_id == group_id, GroupMember.left_at.is_(None))
                .order_by(GroupMember.joined_at.asc(), GroupMember.id.asc())
            )
        ).scalars().all()
    )


async def users_by_id(db: AsyncSession, ids: Iterable[str]) -> dict:
    ids = {i for i in ids if i}
    if not ids:
        return {}
    rows = (await db.execute(select(User).where(User.id.in_(ids)))).scalars().all()
    return {u.id: u for u in rows}


async def resolve_people(db: AsyncSession, raw_ids: Iterable[str], me: User) -> list:
    """Real Flick accounts for the given ids / phone numbers (exact match only). Unknown -> 404."""
    wanted, missing, found = [], [], {}
    for raw in raw_ids:
        val = str(raw or "").strip()
        if val and val not in wanted:
            wanted.append(val)
    if not wanted:
        return []
    rows = (
        await db.execute(select(User).where(or_(User.id.in_(wanted), User.phone_number.in_(wanted))))
    ).scalars().all()
    for u in rows:
        found[u.id] = u
        found[u.phone_number] = u
    people = {}
    for val in wanted:
        u = found.get(val)
        if u is None:
            missing.append(val)
        elif u.id not in (me.id, AGENT_USER_ID):
            people[u.id] = u
    if missing:
        raise GroupError("Some of these people aren't on Flick", 404)
    return list(people.values())


# ---------------------------------------------------------------------------
# Ticks
# ---------------------------------------------------------------------------
def receipt_marks(members: list, sender_id: str):
    """
    (delivered_upto, read_upto) for messages `sender_id` sent: the times up to which every other
    current member has received / read the group. None when there's nobody else.
    """
    others = [m for m in members if m.user_id != sender_id and m.left_at is None]
    if not others:
        return None, None
    read = min(utc(m.last_read_at) or utc(m.joined_at) for m in others)
    delivered = min(
        max(utc(m.last_delivered_at) or utc(m.joined_at), utc(m.last_read_at) or utc(m.joined_at)) for m in others
    )
    return delivered, read


def tick_for(ts: datetime, marks) -> str:
    delivered, read = marks
    ts = utc(ts)
    if read is not None and ts <= read:
        return "READ"
    if delivered is not None and ts <= delivered:
        return "DELIVERED"
    return "SENT"


async def push_receipt_changes(group_id: str, before: dict, members: list) -> None:
    """Tells each sender whose ticks moved (someone got / read the messages) the new marks."""
    frames = []
    for m in members:
        after = receipt_marks(members, m.user_id)
        if after != before.get(m.user_id) and after != (None, None):
            frames.append(
                manager.send_personal_message(
                    m.user_id,
                    {
                        "type": "group_receipt",
                        "data": {
                            "group_id": group_id,
                            "delivered_upto": iso_utc(after[0]),
                            "read_upto": iso_utc(after[1]),
                        },
                    },
                )
            )
    if frames:
        await asyncio.gather(*frames, return_exceptions=True)


def marks_by_sender(members: list) -> dict:
    return {m.user_id: receipt_marks(members, m.user_id) for m in members}


# ---------------------------------------------------------------------------
# Fan-out helpers
# ---------------------------------------------------------------------------
async def fanout(user_ids: Iterable[str], frame: dict, exclude: Optional[str] = None) -> None:
    targets = [uid for uid in dict.fromkeys(user_ids) if uid and uid != exclude]
    if targets:
        await asyncio.gather(*(manager.send_personal_message(uid, frame) for uid in targets), return_exceptions=True)


async def record_changes(group_id: str, user_ids: Iterable[str]) -> None:
    """Updates each member's chat-list row (unread count, last message) for the delta sync."""
    async with async_session_factory() as db:
        for uid in dict.fromkeys(user_ids):
            try:
                await record_conversation_change(db, user_id=uid, peer_id=group_id, action="updated")
            except Exception as e:
                logger.warning("Group sync change for %s failed: %s", uid, e)
        await db.commit()


def _background(coro) -> None:
    task = asyncio.create_task(coro)
    task.add_done_callback(
        lambda t: not t.cancelled() and t.exception()
        and logger.warning("Group background task failed: %s", t.exception())
    )


async def notify_group_changed(group_id: str, user_ids: Iterable[str], action: str) -> None:
    """Open chat lists / group screens refresh; offline members pick it up through the sync."""
    ids = list(dict.fromkeys(user_ids))
    await record_changes(group_id, ids)
    await fanout(ids, {"type": "group_updated", "data": {"group_id": group_id, "action": action}})


async def add_system_message(
    db: AsyncSession, group: Group, actor: User, event: str, text: str, targets: Optional[list] = None
) -> GroupMessage:
    msg = GroupMessage(
        group_id=group.id,
        sender_id=actor.id,
        kind="system",
        content=text,
        extra=json.dumps({
            "metadata": {
                "type": "group_event",
                "event": event,
                "actor_id": actor.id,
                "actor_name": display_name(actor),
                "targets": [{"id": u.id, "name": display_name(u)} for u in (targets or [])],
            }
        }),
        timestamp=datetime.now(timezone.utc),
    )
    db.add(msg)
    await db.flush()
    return msg


async def broadcast_system(msg: GroupMessage, group: Group, actor: User, member_ids: Iterable[str]) -> None:
    """Everyone in the group sees the notice, including whoever made the change (on all their screens)."""
    frame = {"type": "group_message", "data": {**msg.to_dict(actor), **group_card(group)}}
    await fanout(member_ids, frame)


def group_card(group: Group) -> dict:
    return {
        "group_name": group.name,
        "group_avatar": group_initials(group.name),
        "group_avatar_color": group.avatar_color,
        "group_avatar_url": group.avatar_url,
    }


async def group_info(db: AsyncSession, group: Group, me: GroupMember) -> dict:
    members = await active_members(db, group.id)
    people = await users_by_id(db, [m.user_id for m in members])
    rows = []
    for m in members:
        u = people.get(m.user_id)
        if not u:
            continue
        rows.append({
            "id": u.id,
            "phone_number": u.phone_number,
            "name": u.username,
            "avatar_color": u.avatar_color,
            "avatar_url": u.avatar_url,
            "quote": u.quote,
            "role": m.role,
            "is_creator": u.id == group.created_by,
            "joined_at": iso_utc(m.joined_at),
        })
    # Admins first, then by name; me first of all
    rows.sort(key=lambda r: (r["id"] != me.user_id, r["role"] != "admin", (r["name"] or "").lower()))
    return {
        "id": group.id,
        "name": group.name,
        "description": group.description or "",
        "avatar": group_initials(group.name),
        "avatar_color": group.avatar_color,
        "avatar_url": group.avatar_url,
        "created_by": group.created_by,
        "created_at": iso_utc(group.created_at),
        "is_group": True,
        "is_member": me.left_at is None,
        "my_role": me.role if me.left_at is None else None,
        "member_count": len(rows),
        "members": rows if me.left_at is None else [],
    }


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------
async def send_group_message(
    sender_id: str,
    group_id: str,
    content: str,
    client_msg_id: Optional[str] = None,
    reply_to=None,
    metadata=None,
) -> dict:
    """Stores a group message and delivers it to every member. Returns the sender's ack data."""
    content = (content or "").strip()
    if not content:
        raise GroupError("Message is empty")
    if len(content) > MAX_MESSAGE_CHARS or len(json.dumps(metadata or {})) > MAX_METADATA_CHARS:
        raise GroupError("Message is too long", 413)

    async with async_session_factory() as db:
        group, me = await require_member(db, group_id, sender_id)

        if client_msg_id:
            # A retry of a send that already arrived (lost ack on a flaky network): confirm it again
            dup = (
                await db.execute(
                    select(GroupMessage).where(
                        GroupMessage.group_id == group_id,
                        GroupMessage.sender_id == sender_id,
                        GroupMessage.client_msg_id == client_msg_id,
                    )
                )
            ).scalars().first()
            if dup is not None:
                members = await active_members(db, group_id)
                return {**dup.to_dict(), "status": tick_for(dup.timestamp, receipt_marks(members, sender_id)), "duplicate": True}

        members = await active_members(db, group_id)
        others = [m for m in members if m.user_id != sender_id]
        online = await asyncio.gather(*(manager.is_user_online(m.user_id) for m in others))
        online_ids = {m.user_id for m, on in zip(others, online) if on}

        now = datetime.now(timezone.utc)
        extra = {}
        if reply_to:
            extra["reply_to"] = reply_to
        if metadata:
            extra["metadata"] = metadata
        msg = GroupMessage(
            group_id=group_id,
            sender_id=sender_id,
            kind="text",
            content=content,
            extra=json.dumps(extra) if extra else None,
            timestamp=now,
            client_msg_id=client_msg_id,
        )
        db.add(msg)
        for m in members:
            if m.user_id in online_ids:
                m.last_delivered_at = now
            elif m.user_id == sender_id:
                # Sending means you've seen the chat up to here
                m.last_delivered_at = now
                m.last_read_at = now
        sender = await db.get(User, sender_id)
        await db.commit()
        await db.refresh(msg)

        message_dict = msg.to_dict(sender)
        status = "DELIVERED" if others and len(online_ids) == len(others) else "SENT"
        offline_users = list((await users_by_id(db, [m.user_id for m in others if m.user_id not in online_ids])).values())

    frame = {"type": "group_message", "data": {**message_dict, **group_card(group)}}
    await fanout([m.user_id for m in others if m.user_id in online_ids], frame)
    _background(record_changes(group_id, [m.user_id for m in members]))
    if offline_users:
        _background(push_group_message(group, sender, content, offline_users, message_dict["message_id"]))
    return {**message_dict, "status": status}


async def push_group_message(group: Group, sender: User, content: str, recipients: list, message_id: str) -> None:
    for user in recipients:
        if not user.fcm_token:
            continue
        prefs = user.prefs()
        if not prefs.get("msg_notifications", True):
            continue
        body = content if prefs.get("preview", True) else "New message"
        try:
            await send_push_notification(
                fcm_token=user.fcm_token,
                sender_name=group.name,
                content=f"{display_name(sender)}: {body}",
                data_payload={
                    "type": "group_message",
                    "group_id": group.id,
                    "group_name": group.name,
                    "group_avatar_color": group.avatar_color or "",
                    "message_id": message_id,
                },
                silent=not prefs.get("sound", True),
            )
        except Exception as e:
            logger.warning("Group push to %s failed: %s", user.id, e)


async def group_history(user: User, group_id: str, limit: int = 50, offset: int = 0) -> list:
    limit = max(1, min(int(limit), HISTORY_MAX_PAGE))
    offset = max(0, int(offset))
    async with async_session_factory() as db:
        _, me = await require_member(db, group_id, user.id, active=False)
        visible = [GroupMessage.group_id == group_id, GroupMessage.timestamp >= me.joined_at]
        if me.left_at is not None:
            visible.append(GroupMessage.timestamp <= me.left_at)
        rows = (
            await db.execute(
                select(GroupMessage).where(*visible).order_by(desc(GroupMessage.timestamp)).offset(offset).limit(limit)
            )
        ).scalars().all()
        members = await active_members(db, group_id)
        marks = receipt_marks(members, user.id)
        senders = await users_by_id(db, [m.sender_id for m in rows])

    out = []
    for msg in reversed(rows):
        if user.id in msg.hidden_ids():
            continue
        d = msg.to_dict(senders.get(msg.sender_id))
        d["status"] = tick_for(msg.timestamp, marks) if msg.sender_id == user.id and msg.kind == "text" else "READ"
        out.append(d)
    return out


async def find_group_message(db: AsyncSession, message_id: str) -> Optional[GroupMessage]:
    return await db.get(GroupMessage, message_id)


async def edit_group_message(user: User, message_id: str, content: str) -> Optional[dict]:
    """None when the id isn't a group message (the caller then treats it as a direct message)."""
    content = (content or "").strip()
    async with async_session_factory() as db:
        msg = await find_group_message(db, message_id)
        if msg is None:
            return None
        await require_member(db, msg.group_id, user.id)
        if msg.sender_id != user.id or msg.kind != "text":
            raise GroupError("You can only edit your own messages", 403)
        if not content or len(content) > MAX_MESSAGE_CHARS:
            raise GroupError("Message is empty or too long")
        msg.content = content
        msg.is_edited = True
        await db.commit()
        member_ids = [m.user_id for m in await active_members(db, msg.group_id)]
        data = msg.to_dict(user)
    edit_frame = {"type": "message_edited", "data": {"message_id": message_id, "group_id": data["group_id"],
                                                     "sender_id": user.id, "content": content, "is_edited": True}}
    await fanout(member_ids, edit_frame, exclude=user.id)
    await manager.send_to_my_devices(user.id, edit_frame)  # the editor's phone / browser
    _background(record_changes(data["group_id"], member_ids))
    return {"status": "success", "message": data, "is_edited": True}


async def delete_group_message(user: User, message_id: str, for_everyone: bool) -> bool:
    """False when the id isn't a group message. Strangers get a silent no-op, like direct messages."""
    async with async_session_factory() as db:
        msg = await find_group_message(db, message_id)
        if msg is None:
            return False
        member = await get_membership(db, msg.group_id, user.id)
        if member is None:
            return True
        group_id = msg.group_id
        if for_everyone and msg.sender_id == user.id and member.left_at is None and msg.kind == "text":
            await db.delete(msg)
            await db.commit()
            member_ids = [m.user_id for m in await active_members(db, group_id)]
            delete_frame = {"type": "message_deleted",
                            "data": {"message_id": message_id, "group_id": group_id, "sender_id": user.id}}
            await fanout(member_ids, delete_frame, exclude=user.id)
            await manager.send_to_my_devices(user.id, delete_frame)
            _background(record_changes(group_id, member_ids))
        else:
            hidden = msg.hidden_ids()
            hidden.add(user.id)
            msg.hidden_for = json.dumps(sorted(hidden))
            await db.commit()
            _background(record_changes(group_id, [user.id]))
            await manager.send_to_my_devices(user.id, {"type": "message_deleted", "data": {
                "message_id": message_id, "group_id": group_id, "sender_id": msg.sender_id, "for_me": True}})
    return True


# ---------------------------------------------------------------------------
# Live signals: typing, read, delivered
# ---------------------------------------------------------------------------
async def relay_typing(user_id: str, group_id: str, typing: bool) -> None:
    async with async_session_factory() as db:
        try:
            await require_member(db, group_id, user_id)
        except GroupError:
            return
        member_ids = [m.user_id for m in await active_members(db, group_id)]
        me = await db.get(User, user_id)
    await fanout(
        member_ids,
        {
            "type": "group_typing",
            "data": {
                "group_id": group_id,
                "user_id": user_id,
                "name": display_name(me),
                "avatar_color": me.avatar_color if me else None,
                "avatar_url": me.avatar_url if me else None,
                "typing": bool(typing),
            },
        },
        exclude=user_id,
    )


async def mark_group_read(user_id: str, group_id: str) -> None:
    async with async_session_factory() as db:
        try:
            _, me = await require_member(db, group_id, user_id, active=False)
        except GroupError:
            return
        now = datetime.now(timezone.utc)
        if me.left_at is not None:
            now = utc(me.left_at)
        if utc(me.last_read_at) and utc(me.last_read_at) >= now:
            return
        members = await active_members(db, group_id)
        before = marks_by_sender(members)
        me.last_read_at = now
        me.last_delivered_at = max(utc(me.last_delivered_at) or now, now)
        await db.commit()
        members = await active_members(db, group_id)
    await record_changes(group_id, [user_id])
    if me.left_at is None:
        await push_receipt_changes(group_id, before, members)


async def mark_delivered_on_connect(user_id: str) -> None:
    """A member came online: everything sent to their groups so far has now reached them."""
    async with async_session_factory() as db:
        mine = (
            await db.execute(
                select(GroupMember).where(GroupMember.user_id == user_id, GroupMember.left_at.is_(None))
            )
        ).scalars().all()
        group_ids = [m.group_id for m in mine]
    now = datetime.now(timezone.utc)
    for gid in group_ids:
        async with async_session_factory() as db:
            members = await active_members(db, gid)
            before = marks_by_sender(members)
            changed = False
            for m in members:
                if m.user_id == user_id:
                    m.last_delivered_at = now
                    changed = True
            if not changed:
                continue
            await db.commit()
            members = await active_members(db, gid)
        await push_receipt_changes(gid, before, members)


async def remove_user_from_all_groups(db: AsyncSession, user: User) -> None:
    """Account deletion: leave every group (so admins are handed over) and drop the memberships."""
    rows = (await db.execute(select(GroupMember).where(GroupMember.user_id == user.id))).scalars().all()
    for row in rows:
        if row.left_at is None:
            group = await db.get(Group, row.group_id)
            if group:
                await _leave(db, group, row, user, actor=user)
        await db.delete(row)
    await db.flush()


async def _leave(db: AsyncSession, group: Group, member: GroupMember, person: User, actor: User) -> GroupMessage:
    member.left_at = datetime.now(timezone.utc)
    was_admin = member.role == "admin"
    member.role = "member"
    await db.flush()  # the session doesn't autoflush: later member queries must see this
    if actor.id == person.id:
        msg = await add_system_message(db, group, actor, "left", f"{display_name(person)} left")
    else:
        msg = await add_system_message(db, group, actor, "removed", f"{display_name(actor)} removed {display_name(person)}", [person])
    remaining = [m for m in await active_members(db, group.id) if m.user_id != member.user_id]
    if was_admin and remaining and not any(m.role == "admin" for m in remaining):
        remaining[0].role = "admin"  # never leave a group without an admin
    return msg


# ---------------------------------------------------------------------------
# REST endpoints
# ---------------------------------------------------------------------------
@router.post("")
async def create_group(payload: CreateGroupRequest, current_user: User = Depends(get_current_user),
                       db: AsyncSession = Depends(get_db)):
    name = clean_text(payload.name, 60)
    if not name:
        raise HTTPException(status_code=400, detail="Give the group a name")
    try:
        people = await resolve_people(db, payload.member_ids, current_user)
    except GroupError as e:
        raise _http(e)
    if not people:
        raise HTTPException(status_code=400, detail="Add at least one person to the group")
    if len(people) + 1 > MAX_GROUP_MEMBERS:
        raise HTTPException(status_code=400, detail=f"A group can have up to {MAX_GROUP_MEMBERS} members")

    color = payload.avatar_color if payload.avatar_color in GROUP_COLORS else secrets.choice(GROUP_COLORS)
    now = datetime.now(timezone.utc)
    group = Group(
        id=new_group_id(),
        name=name,
        description=clean_text(payload.description, 500) or None,
        avatar_color=color,
        created_by=current_user.id,
        created_at=now,
    )
    db.add(group)
    await db.flush()
    for u in [current_user, *people]:
        db.add(GroupMember(
            group_id=group.id,
            user_id=u.id,
            role="admin" if u.id == current_user.id else "member",
            joined_at=now,
            last_delivered_at=now,
            last_read_at=now,
        ))
    await db.flush()
    msg = await add_system_message(db, group, current_user, "created", f"{display_name(current_user)} created the group “{name}”")
    me = await get_membership(db, group.id, current_user.id)
    info = await group_info(db, group, me)
    await db.commit()

    member_ids = [current_user.id, *[u.id for u in people]]
    await broadcast_system(msg, group, current_user, [u.id for u in people])
    await notify_group_changed(group.id, member_ids, "created")
    return info


@router.get("/{group_id}")
async def get_group(group_id: str, current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    try:
        group, me = await require_member(db, group_id, current_user.id, active=False)
    except GroupError as e:
        raise _http(e)
    return await group_info(db, group, me)


@router.patch("/{group_id}")
async def update_group(group_id: str, payload: UpdateGroupRequest, current_user: User = Depends(get_current_user),
                       db: AsyncSession = Depends(get_db)):
    try:
        group, me = await require_admin(db, group_id, current_user.id)
    except GroupError as e:
        raise _http(e)
    events = []
    if payload.name is not None:
        name = clean_text(payload.name, 60)
        if not name:
            raise HTTPException(status_code=400, detail="Give the group a name")
        if name != group.name:
            group.name = name
            events.append(("renamed", f"{display_name(current_user)} changed the group name to “{name}”"))
    if payload.description is not None:
        desc_text = clean_text(payload.description, 500) or None
        if desc_text != group.description:
            group.description = desc_text
            events.append(("description", f"{display_name(current_user)} changed the group description"))
    if payload.avatar_color is not None and payload.avatar_color in GROUP_COLORS:
        group.avatar_color = payload.avatar_color
    msgs = [await add_system_message(db, group, current_user, ev, text) for ev, text in events]
    info = await group_info(db, group, me)
    await db.commit()
    member_ids = [m["id"] for m in info["members"]]
    for msg in msgs:
        await broadcast_system(msg, group, current_user, member_ids)
    await notify_group_changed(group.id, member_ids, "updated")
    return info


@router.post("/{group_id}/members")
async def add_members(group_id: str, payload: GroupMembersRequest, current_user: User = Depends(get_current_user),
                      db: AsyncSession = Depends(get_db)):
    try:
        group, me = await require_admin(db, group_id, current_user.id)
        people = await resolve_people(db, payload.member_ids, current_user)
    except GroupError as e:
        raise _http(e)
    current = {m.user_id for m in await active_members(db, group_id)}
    newcomers = [u for u in people if u.id not in current]
    if not newcomers:
        raise HTTPException(status_code=400, detail="They're already in the group")
    if len(current) + len(newcomers) > MAX_GROUP_MEMBERS:
        raise HTTPException(status_code=400, detail=f"A group can have up to {MAX_GROUP_MEMBERS} members")

    now = datetime.now(timezone.utc)
    for u in newcomers:
        row = await get_membership(db, group_id, u.id)
        if row is None:
            db.add(GroupMember(group_id=group_id, user_id=u.id, role="member", joined_at=now,
                               last_delivered_at=now, last_read_at=now))
        else:
            # Re-added: a fresh start, they only see messages from now on
            row.left_at = None
            row.role = "member"
            row.joined_at = now
            row.last_delivered_at = now
            row.last_read_at = now
    await db.flush()
    names = ", ".join(display_name(u) for u in newcomers)
    msg = await add_system_message(db, group, current_user, "added", f"{display_name(current_user)} added {names}", newcomers)
    info = await group_info(db, group, me)
    await db.commit()
    member_ids = [m["id"] for m in info["members"]]
    await broadcast_system(msg, group, current_user, member_ids)
    await notify_group_changed(group.id, member_ids, "members")
    return info


@router.delete("/{group_id}/members/{user_id}")
async def remove_member(group_id: str, user_id: str, current_user: User = Depends(get_current_user),
                        db: AsyncSession = Depends(get_db)):
    if user_id == current_user.id:
        return await leave_group(group_id, current_user, db)
    try:
        group, me = await require_admin(db, group_id, current_user.id)
    except GroupError as e:
        raise _http(e)
    target = await get_membership(db, group_id, user_id)
    if target is None or target.left_at is not None:
        raise HTTPException(status_code=404, detail="They're not in this group")
    if user_id == group.created_by:
        raise HTTPException(status_code=403, detail="The group creator can't be removed")
    person = await db.get(User, user_id)
    member_ids_before = [m.user_id for m in await active_members(db, group_id)]
    msg = await _leave(db, group, target, person, actor=current_user)
    info = await group_info(db, group, me)
    await db.commit()
    await broadcast_system(msg, group, current_user, member_ids_before)
    await notify_group_changed(group.id, member_ids_before, "members")
    return info


@router.post("/{group_id}/leave")
async def leave_group_endpoint(group_id: str, current_user: User = Depends(get_current_user),
                               db: AsyncSession = Depends(get_db)):
    return await leave_group(group_id, current_user, db)


async def leave_group(group_id: str, current_user: User, db: AsyncSession) -> dict:
    try:
        group, me = await require_member(db, group_id, current_user.id)
    except GroupError as e:
        raise _http(e)
    member_ids_before = [m.user_id for m in await active_members(db, group_id)]
    msg = await _leave(db, group, me, current_user, actor=current_user)
    info = await group_info(db, group, me)
    await db.commit()
    await broadcast_system(msg, group, current_user, member_ids_before)
    await notify_group_changed(group.id, member_ids_before, "members")
    return info


@router.post("/{group_id}/admins/{user_id}")
async def set_admin(group_id: str, user_id: str, payload: GroupAdminRequest,
                    current_user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    try:
        group, me = await require_admin(db, group_id, current_user.id)
    except GroupError as e:
        raise _http(e)
    target = await get_membership(db, group_id, user_id)
    if target is None or target.left_at is not None:
        raise HTTPException(status_code=404, detail="They're not in this group")
    if not payload.admin:
        if user_id == group.created_by and user_id != current_user.id:
            raise HTTPException(status_code=403, detail="The group creator stays an admin")
        admins = [m for m in await active_members(db, group_id) if m.role == "admin"]
        if len(admins) <= 1 and target.role == "admin":
            raise HTTPException(status_code=400, detail="A group needs at least one admin")
    target.role = "admin" if payload.admin else "member"
    info = await group_info(db, group, me)
    await db.commit()
    await notify_group_changed(group.id, [m["id"] for m in info["members"]], "admins")
    return info


@router.delete("/{group_id}/chat")
async def delete_group_chat(group_id: str, current_user: User = Depends(get_current_user),
                            db: AsyncSession = Depends(get_db)):
    """Removes a group you've left from your chat list (leave it first)."""
    try:
        _, me = await require_member(db, group_id, current_user.id, active=False)
    except GroupError as e:
        raise _http(e)
    if me.left_at is None:
        raise HTTPException(status_code=400, detail="Leave the group before deleting it")
    await db.delete(me)
    await db.flush()
    await record_conversation_change(db, user_id=current_user.id, peer_id=group_id, action="deleted")
    await db.commit()
    return {"status": "success", "group_id": group_id, "deleted": True}


__all__ = [
    "router",
    "GroupError",
    "is_group_id",
    "send_group_message",
    "group_history",
    "edit_group_message",
    "delete_group_message",
    "relay_typing",
    "mark_group_read",
    "mark_delivered_on_connect",
    "remove_user_from_all_groups",
]
