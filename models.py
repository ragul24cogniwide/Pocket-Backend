"""
SQLAlchemy 2.0 Async ORM Models for Flick Messenger.
"""

from typing import Optional
import enum
import json
import uuid
from datetime import datetime, timezone
from sqlalchemy import Boolean, DateTime, Enum, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database import Base


class MessageStatus(str, enum.Enum):
    """Lifecycle state of a message."""
    SENT = "SENT"
    DELIVERED = "DELIVERED"
    READ = "READ"


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

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "phone_number": self.phone_number,
            "username": self.username,
            "avatar_color": self.avatar_color,
            "avatar_url": self.avatar_url,
            "quote": self.quote or "Hey there! I am using Flick.",
            "is_online": self.is_online,
            "last_seen": self.last_seen.isoformat() if self.last_seen else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
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
            "timestamp": self.timestamp.isoformat() if self.timestamp else datetime.now(timezone.utc).isoformat(),
            "status": self.status.value if hasattr(self.status, 'value') else str(self.status),
            "is_edited": self.is_edited or False,
            "delivered_at": self.delivered_at.isoformat() if self.delivered_at else None,
            "read_at": self.read_at.isoformat() if self.read_at else None,
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
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "time": self.created_at.strftime("%I:%M %p") if self.created_at else "Just now",
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
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_minutes": duration_mins,
            "timezone": self.timezone or "UTC",
            "location": self.location or "",
            "meeting_link": self.meeting_link or "",
            "attendees": parsed_attendees,
            "status": self.status,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


