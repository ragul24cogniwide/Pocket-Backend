"""
SQLAlchemy 2.0 Async ORM Models for Flick Messenger.
"""

from typing import Optional
import enum
import json
import uuid
from datetime import datetime, timezone
from sqlalchemy import Boolean, DateTime, Enum, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database import Base


class MessageStatus(str, enum.Enum):
    """Lifecycle state of a message."""
    SENT = "SENT"
    DELIVERED = "DELIVERED"
    READ = "READ"


def iso_utc(dt: Optional[datetime]) -> Optional[str]:
    """
    ISO-8601 in UTC with an explicit offset ("...+00:00"). Some databases hand back datetimes
    without a timezone; sent as-is, phones would read them as local time and be hours off.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


# Settings other users are affected by, so the server has to enforce them
DEFAULT_USER_PREFS = {
    "last_seen": "everyone",  # 'everyone' | 'nobody'
    "read_receipts": True,
    "msg_notifications": True,
    "preview": True,
    "sound": True,
}


class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(
        String(64), primary_key=True, index=True
    )
    phone_number: Mapped[str] = mapped_column(
        String(50), unique=True, index=True, nullable=False
    )
    username: Mapped[str] = mapped_column(
        String(100), default="Flick User", nullable=False
    )
    avatar_color: Mapped[str] = mapped_column(
        String(20), default="#FFB800", nullable=False
    )
    avatar_url: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, default=None
    )
    quote: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, default="Hey there! I am using Flick."
    )
    fcm_token: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, default=None
    )
    # Incremented on logout; tokens carry it ("tv") so older tokens are rejected
    token_version: Mapped[Optional[int]] = mapped_column(Integer, default=0, nullable=True)
    # JSON privacy / notification settings (see privacy_prefs in main.py)
    preferences: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, default=None
    )
    is_online: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )
    last_seen: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    # Relationships
    sent_messages: Mapped[list["Message"]] = relationship(
        "Message",
        back_populates="sender",
        foreign_keys="Message.sender_id",
        cascade="all, delete-orphan",
    )
    received_messages: Mapped[list["Message"]] = relationship(
        "Message",
        back_populates="receiver",
        foreign_keys="Message.receiver_id",
        cascade="all, delete-orphan",
    )

    def prefs(self) -> dict:
        """Privacy / notification settings with defaults filled in."""
        try:
            saved = json.loads(self.preferences) if self.preferences else {}
        except (TypeError, ValueError):
            saved = {}
        return {**DEFAULT_USER_PREFS, **(saved if isinstance(saved, dict) else {})}

    def hides_last_seen(self) -> bool:
        return self.prefs().get("last_seen") == "nobody"

    def visible_last_seen(self, viewer: Optional["User"] = None) -> Optional[str]:
        """
        Last-seen time as `viewer` may see it. Works both ways (like WhatsApp): hidden when this
        user chose "Nobody", and also when the viewer did (they can't see anyone's either).
        Online status is separate and always visible.
        """
        if not self.last_seen or self.hides_last_seen() or (viewer is not None and viewer.hides_last_seen()):
            return None
        return iso_utc(self.last_seen)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "phone_number": self.phone_number,
            "username": self.username,
            "avatar_color": self.avatar_color,
            "avatar_url": self.avatar_url,
            "quote": self.quote or "Hey there! I am using Flick.",
            "is_online": self.is_online,
            "last_seen": self.visible_last_seen(),
            "created_at": iso_utc(self.created_at),
        }


class OtpRecord(Base):
    __tablename__ = "otp_records"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    phone_number: Mapped[str] = mapped_column(
        String(50), index=True, nullable=False
    )
    otp_code: Mapped[str] = mapped_column(
        String(6), nullable=False
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    is_used: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )


class Message(Base):
    __tablename__ = "messages"

    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        index=True,
    )
    sender_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    receiver_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    content: Mapped[str] = mapped_column(
        Text, nullable=False
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True,
    )
    status: Mapped[str] = mapped_column(
        String(20),
        default="SENT",
        nullable=False,
    )
    reply_to: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, default=None
    )
    is_edited: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )
    delivered_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    read_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    # True when this message must never show blue ticks to its sender: either person had read
    # receipts off when it was sent or when it was read. Fixed at that moment, so changing the
    # setting later doesn't rewrite past ticks (old blue stays blue, "off" messages stay grey).
    receipt_hidden: Mapped[Optional[bool]] = mapped_column(
        Boolean, default=False, nullable=True
    )
    # "Delete for me": hidden only for that person, the other still has the message
    hidden_for_sender: Mapped[Optional[bool]] = mapped_column(Boolean, default=False, nullable=True)
    hidden_for_receiver: Mapped[Optional[bool]] = mapped_column(Boolean, default=False, nullable=True)
    # Id the app generated for this message; makes retries over a flaky network idempotent
    client_msg_id: Mapped[Optional[str]] = mapped_column(
        String(64), nullable=True, index=True
    )

    # Relationships
    sender: Mapped["User"] = relationship(
        "User", foreign_keys=[sender_id], back_populates="sent_messages"
    )
    receiver: Mapped["User"] = relationship(
        "User", foreign_keys=[receiver_id], back_populates="received_messages"
    )

    def to_dict(self) -> dict:
        parsed_reply = None
        parsed_metadata = None
        if self.reply_to:
            try:
                raw_data = json.loads(self.reply_to)
                if isinstance(raw_data, dict):
                    if "metadata" in raw_data or "reply_to" in raw_data:
                        parsed_metadata = raw_data.get("metadata")
                        parsed_reply = raw_data.get("reply_to")
                    else:
                        parsed_reply = raw_data
                else:
                    parsed_reply = {"text": str(raw_data)}
            except Exception:
                parsed_reply = {"text": self.reply_to}

        return {
            "id": self.id,
            "message_id": self.id,
            "sender_id": self.sender_id,
            "receiver_id": self.receiver_id,
            "content": self.content,
            "reply_to": parsed_reply,
            "metadata": parsed_metadata,
            "timestamp": iso_utc(self.timestamp or datetime.now(timezone.utc)),
            "status": self.status.value if hasattr(self.status, 'value') else str(self.status),
            "is_edited": self.is_edited or False,
            "delivered_at": iso_utc(self.delivered_at),
            "read_at": iso_utc(self.read_at),
            "client_msg_id": self.client_msg_id,
        }



class Status(Base):
    __tablename__ = "statuses"

    id: Mapped[str] = mapped_column(
        String(64),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        index=True,
    )
    user_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    type: Mapped[str] = mapped_column(
        String(20), default="image", nullable=False
    )
    media_url: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, default=None
    )
    caption: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, default=None
    )
    bg_gradient: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True, default="#1E293B"
    )
    viewers: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, default="[]"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True,
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        index=True,
    )

    user: Mapped["User"] = relationship("User")

    def to_dict(self) -> dict:
        parsed_viewers = []
        if self.viewers:
            try:
                parsed_viewers = json.loads(self.viewers)
            except Exception:
                parsed_viewers = []

        user_name = "Flick User"
        avatar = "FK"
        avatar_color = "#FFB800"
        avatar_url = None
        try:
            if self.user:
                user_name = self.user.username or "Flick User"
                avatar = self.user.username[:2].upper() if self.user.username else "FK"
                avatar_color = self.user.avatar_color or "#FFB800"
                avatar_url = self.user.avatar_url
        except Exception:
            pass

        return {
            "id": self.id,
            "user_id": self.user_id,
            "userName": user_name,
            "avatar": avatar,
            "avatarColor": avatar_color,
            "avatarUrl": avatar_url,
            "type": self.type,
            "uri": self.media_url,
            "caption": self.caption,
            "bgGradient": self.bg_gradient,
            "viewsCount": len(parsed_viewers),
            "viewers": parsed_viewers,
            "created_at": iso_utc(self.created_at),
            "time": iso_utc(self.created_at),
        }


class Meeting(Base):
    __tablename__ = "meetings"

    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        index=True,
    )
    user_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    title: Mapped[str] = mapped_column(
        String(255), nullable=False
    )
    description: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, default=None
    )
    start_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    end_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    timezone: Mapped[str] = mapped_column(
        String(50), default="UTC", nullable=False
    )
    location: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, default=None
    )
    meeting_link: Mapped[Optional[str]] = mapped_column(
        String(500), nullable=True, default=None
    )
    attendees: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, default="[]"
    )
    status: Mapped[str] = mapped_column(
        String(20), default="SCHEDULED", nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    user: Mapped["User"] = relationship("User")

    @property
    def duration_minutes(self) -> int:
        if self.start_time and self.end_time:
            delta = self.end_time - self.start_time
            return max(1, int(delta.total_seconds() / 60))
        return 30

    def to_dict(self) -> dict:
        parsed_attendees = []
        if self.attendees:
            try:
                raw_att = json.loads(self.attendees)
                if isinstance(raw_att, list):
                    parsed_attendees = raw_att
                else:
                    parsed_attendees = [str(raw_att)]
            except Exception:
                parsed_attendees = [a.strip() for a in self.attendees.split(",") if a.strip()]

        duration_mins = 30
        if self.start_time and self.end_time:
            delta = self.end_time - self.start_time
            duration_mins = max(1, int(delta.total_seconds() / 60))

        return {
            "id": self.id,
            "user_id": self.user_id,
            "title": self.title,
            "description": self.description or "",
            "start_time": iso_utc(self.start_time),
            "end_time": iso_utc(self.end_time),
            "duration_minutes": duration_mins,
            "timezone": self.timezone or "UTC",
            "location": self.location or "",
            "meeting_link": self.meeting_link or "",
            "attendees": parsed_attendees,
            "status": self.status,
            "created_at": iso_utc(self.created_at),
            "updated_at": iso_utc(self.updated_at),
        }


class ConversationChangeLog(Base):
    __tablename__ = "conversation_change_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(String(64), nullable=False)
    peer_id: Mapped[str] = mapped_column(String(64), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    action: Mapped[str] = mapped_column(String(20), nullable=False)  # 'created', 'updated', 'deleted'
    payload: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON serialized conversation dict
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (
        Index("ix_conv_change_user_version", "user_id", "version"),
        Index("ix_conv_change_user_peer", "user_id", "peer_id"),
        Index("ix_conv_change_user_created", "user_id", "created_at"),
    )

    def to_dict(self) -> dict:
        parsed_payload = None
        if self.payload:
            try:
                parsed_payload = json.loads(self.payload)
            except Exception:
                parsed_payload = None
        return {
            "id": self.id,
            "user_id": self.user_id,
            "peer_id": self.peer_id,
            "version": self.version,
            "action": self.action,
            "payload": parsed_payload,
            "created_at": iso_utc(self.created_at),
        }


class UserSyncSequence(Base):
    __tablename__ = "user_sync_sequences"

    user_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    current_version: Mapped[int] = mapped_column(Integer, default=0, nullable=False)




class Group(Base):
    """A group chat. Ids look like "grp_" + 24 lowercase letters (no digits, so they can never
    be mistaken for a phone number by the number-matching helpers)."""
    __tablename__ = "chat_groups"

    id: Mapped[str] = mapped_column(String(64), primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True, default=None)
    avatar_color: Mapped[str] = mapped_column(String(20), default="#3B82F6", nullable=False)
    avatar_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True, default=None)
    created_by: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )


class GroupMember(Base):
    """
    One person's membership of a group. A member who leaves or is removed keeps the row with
    `left_at` set: they still see the history up to that moment, read-only (like WhatsApp).
    """
    __tablename__ = "group_members"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    group_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("chat_groups.id", ondelete="CASCADE"), nullable=False, index=True
    )
    user_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    role: Mapped[str] = mapped_column(String(10), default="member", nullable=False)  # 'admin' | 'member'
    joined_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    left_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    # Everything up to these times has reached / been read by this member (ticks + unread count)
    last_delivered_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_read_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        UniqueConstraint("group_id", "user_id", name="uq_group_member"),
    )

    @property
    def active(self) -> bool:
        return self.left_at is None


class GroupMessage(Base):
    __tablename__ = "group_messages"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4()), index=True
    )
    group_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("chat_groups.id", ondelete="CASCADE"), nullable=False, index=True
    )
    sender_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    kind: Mapped[str] = mapped_column(String(10), default="text", nullable=False)  # 'text' | 'system'
    content: Mapped[str] = mapped_column(Text, nullable=False)
    # JSON {"reply_to": ..., "metadata": ...}, same as Message.reply_to
    extra: Mapped[Optional[str]] = mapped_column(Text, nullable=True, default=None)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False, index=True
    )
    is_edited: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    client_msg_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    # JSON list of user ids who chose "delete for me"
    hidden_for: Mapped[Optional[str]] = mapped_column(Text, nullable=True, default=None)

    __table_args__ = (Index("ix_group_msg_group_ts", "group_id", "timestamp"),)

    def hidden_ids(self) -> set:
        try:
            ids = json.loads(self.hidden_for) if self.hidden_for else []
        except (TypeError, ValueError):
            ids = []
        return set(ids) if isinstance(ids, list) else set()

    def to_dict(self, sender: Optional["User"] = None) -> dict:
        reply_to = metadata = None
        if self.extra:
            try:
                parsed = json.loads(self.extra)
                if isinstance(parsed, dict):
                    reply_to, metadata = parsed.get("reply_to"), parsed.get("metadata")
            except (TypeError, ValueError):
                pass
        return {
            "id": self.id,
            "message_id": self.id,
            "group_id": self.group_id,
            "sender_id": self.sender_id,
            "sender_name": (sender.username if sender else None) or ("" if self.kind == "system" else "Flick User"),
            "sender_color": sender.avatar_color if sender else None,
            "sender_avatar_url": sender.avatar_url if sender else None,
            "kind": self.kind,
            "content": self.content,
            "reply_to": reply_to,
            "metadata": metadata,
            "timestamp": iso_utc(self.timestamp or datetime.now(timezone.utc)),
            "is_edited": self.is_edited or False,
            "client_msg_id": self.client_msg_id,
        }


class WebSession(Base):
    """A browser linked to a phone account (Flick Web). The phone approves it with a one-time
    code; logging out on the phone, or from Linked devices, revokes it."""
    __tablename__ = "web_sessions"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    device_name: Mapped[str] = mapped_column(String(100), default="Browser", nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    last_active_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    revoked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    def to_dict(self, current_sid: Optional[str] = None) -> dict:
        return {
            "id": self.id,
            "device_name": self.device_name,
            "created_at": iso_utc(self.created_at),
            "last_active_at": iso_utc(self.last_active_at),
            "current": self.id == current_sid,
        }


class MessageReaction(Base):
    """An emoji reaction to a direct or group message. One per person per message (like
    WhatsApp): reacting again replaces it, reacting with the same emoji removes it."""
    __tablename__ = "message_reactions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    message_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    group_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    user_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    emoji: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (UniqueConstraint("message_id", "user_id", name="uq_reaction_message_user"),)


class ChatArchive(Base):
    """A chat someone archived (hidden from their main chat list, on all their devices). It stays
    archived when new messages arrive, like WhatsApp's "Keep chats archived"."""
    __tablename__ = "chat_archives"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    peer_id: Mapped[str] = mapped_column(String(64), nullable=False)  # a user id or a group id
    archived_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (UniqueConstraint("user_id", "peer_id", name="uq_chat_archive"),)


class CallRecord(Base):
    """One voice / video call, for the call log of both people (phone and Flick Web).
    status: ringing | completed | missed | declined | busy"""
    __tablename__ = "call_records"

    id: Mapped[str] = mapped_column(String(80), primary_key=True)  # the call id
    caller_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    receiver_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    video: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    status: Mapped[str] = mapped_column(String(16), default="ringing", nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False, index=True
    )
    answered_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    duration: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    # "Clear call log" hides it for that person only
    hidden_for_caller: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    hidden_for_receiver: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
