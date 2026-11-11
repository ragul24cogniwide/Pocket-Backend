"""Devices and per-device message delivery (step 1 of keeping messages on phones; see devices.py)

Revision ID: 0002_devices
Revises: 0001_baseline
Create Date: 2026-11-11

Two new tables. The app's startup (init_db's create_all) may already have created them, so each
is created only if it isn't there yet.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0002_devices"
down_revision: Union[str, Sequence[str], None] = "0001_baseline"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _has_table(name: str) -> bool:
    return name in sa.inspect(op.get_bind()).get_table_names()


def upgrade() -> None:
    if not _has_table("devices"):
        op.create_table(
            "devices",
            sa.Column("id", sa.String(64), primary_key=True),
            sa.Column("user_id", sa.String(64), nullable=False),
            sa.Column("client_key", sa.String(80), nullable=False),
            sa.Column("kind", sa.String(10), nullable=False),
            sa.Column("platform", sa.String(20), nullable=True),
            sa.Column("name", sa.String(100), nullable=True),
            sa.Column("app_version", sa.String(30), nullable=True),
            sa.Column("web_session_id", sa.String(64), nullable=True),
            sa.Column("fcm_token", sa.Text(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
            sa.UniqueConstraint("user_id", "client_key", name="uq_devices_user_key"),
        )
        op.create_index("ix_devices_user_id", "devices", ["user_id"])
        op.create_index("ix_devices_web_session_id", "devices", ["web_session_id"])

    if not _has_table("message_deliveries"):
        op.create_table(
            "message_deliveries",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("message_id", sa.String(36), nullable=False),
            sa.Column("chat_kind", sa.String(10), nullable=False),
            sa.Column("group_id", sa.String(64), nullable=True),
            sa.Column("message_ts", sa.DateTime(timezone=True), nullable=False),
            sa.Column("user_id", sa.String(64), nullable=False),
            sa.Column("device_id", sa.String(64), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("delivered_via", sa.String(10), nullable=True),
            sa.UniqueConstraint("message_id", "device_id", name="uq_message_deliveries_message_device"),
        )
        op.create_index("ix_message_deliveries_message_id", "message_deliveries", ["message_id"])
        op.create_index("ix_message_deliveries_group_id", "message_deliveries", ["group_id"])
        op.create_index("ix_message_deliveries_user_id", "message_deliveries", ["user_id"])
        op.create_index("ix_message_deliveries_created_at", "message_deliveries", ["created_at"])
        op.create_index("ix_message_deliveries_device_pending", "message_deliveries", ["device_id", "delivered_at"])


def downgrade() -> None:
    op.drop_table("message_deliveries")
    op.drop_table("devices")
