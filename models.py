"""
SQLAlchemy 2.0 Async ORM Models for Pocket Messenger.
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
        String(100), default="Pocket User", nullable=False
    )
    avatar_color: Mapped[str] = mapped_column(
        String(20), default="#FFB800", nullable=False
    )
    avatar_url: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, default=None
    )
    quote: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, default="Hey there! I am using Pocket."
    )
    fcm_token: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, default=None
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
            "quote": self.quote or "Hey there! I am using Pocket.",
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
        if self.reply_to:
            try:
                parsed_reply = json.loads(self.reply_to)
            except Exception:
                parsed_reply = {"text": self.reply_to}

        return {
            "id": self.id,
            "message_id": self.id,
            "sender_id": self.sender_id,
            "receiver_id": self.receiver_id,
            "content": self.content,
            "reply_to": parsed_reply,
            "timestamp": self.timestamp.isoformat() if self.timestamp else datetime.now(timezone.utc).isoformat(),
            "status": self.status.value if hasattr(self.status, 'value') else str(self.status),
            "delivered_at": self.delivered_at.isoformat() if self.delivered_at else None,
            "read_at": self.read_at.isoformat() if self.read_at else None,
        }
