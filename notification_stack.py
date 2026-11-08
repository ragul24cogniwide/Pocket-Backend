"""
What a chat's notification says: one notification per chat, like WhatsApp.

Every push of a chat replaces that chat's previous notification (notifications.chat_thread_key),
so each one carries the whole picture: the chat's unread messages, one line each (oldest first,
the newest few), and in groups each line starts with who wrote it:

    Sadha (3 messages)            Family (2 messages)
    Hi                            Amma: Dinner ready?
    Are you coming?               Appa: Come fast
    📷 Photo
"""

from typing import Optional

from sqlalchemy import desc, false, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from identity import id_forms
from models import GroupMember, GroupMessage, Message, User

MAX_LINES = 6        # unread messages listed; older ones only count towards "(N messages)"
MAX_LINE_CHARS = 120
MAX_BODY_CHARS = 600  # notifications.MAX_PUSH_BODY_CHARS


def _line(text: Optional[str]) -> str:
    text = " ".join(str(text or "").split())  # one line per message
    return text if len(text) <= MAX_LINE_CHARS else text[: MAX_LINE_CHARS - 1] + "…"


def _title(name: str, count: int) -> str:
    return name if count <= 1 else f"{name} ({count} messages)"


def _body(lines: list[str], count: int, preview: bool) -> str:
    if not preview:
        return "New message" if count <= 1 else f"{count} new messages"
    # Drop the oldest lines until it fits; the newest message is always there
    while len(lines) > 1 and len("\n".join(lines)) > MAX_BODY_CHARS:
        lines = lines[1:]
    return "\n".join(lines)[:MAX_BODY_CHARS]


async def direct_chat_notification(
    db: AsyncSession, sender_id: str, receiver_id: str, sender_name: str, latest: str, preview: bool = True
) -> tuple[str, str]:
    """(title, body) for a one-to-one chat: the sender's messages the receiver hasn't read."""
    unread = [
        Message.sender_id.in_(id_forms(sender_id)),
        Message.receiver_id.in_(id_forms(receiver_id)),
        Message.status != "READ",
        ~func.coalesce(Message.hidden_for_receiver, false()),
    ]
    count = (await db.execute(select(func.count(Message.id)).where(*unread))).scalar_one() or 0
    rows = (
        await db.execute(select(Message.content).where(*unread).order_by(desc(Message.timestamp)).limit(MAX_LINES))
    ).scalars().all()
    lines = [_line(c) for c in reversed(rows)] or [_line(latest)]
    count = max(count, 1)
    return _title(sender_name, count), _body(lines, count, preview)


async def group_chat_notification(
    db: AsyncSession, group_id: str, group_name: str, reader: User, latest_line: str, preview: bool = True
) -> tuple[str, str]:
    """(title, body) for a group: messages since this member last read it, each with its sender."""
    member = (
        await db.execute(select(GroupMember).where(GroupMember.group_id == group_id, GroupMember.user_id == reader.id))
    ).scalars().first()
    since = (member.last_read_at or member.joined_at) if member else None
    conds = [GroupMessage.group_id == group_id, GroupMessage.kind == "text", GroupMessage.sender_id != reader.id]
    if since is not None:
        conds.append(GroupMessage.timestamp > since)
    rows = (
        await db.execute(select(GroupMessage).where(*conds).order_by(desc(GroupMessage.timestamp)).limit(50))
    ).scalars().all()
    rows = [m for m in rows if reader.id not in m.hidden_ids()]  # "deleted for me"
    count = max(len(rows), 1)
    names = {}
    sender_ids = {m.sender_id for m in rows[:MAX_LINES] if m.sender_id}
    if sender_ids:
        for u in (await db.execute(select(User).where(User.id.in_(sender_ids)))).scalars().all():
            names[u.id] = (u.username or "").strip() or u.phone_number
    lines = [_line(f"{names.get(m.sender_id, 'Someone')}: {m.content}") for m in reversed(rows[:MAX_LINES])]
    return _title(group_name, count), _body(lines or [_line(latest_line)], count, preview)
