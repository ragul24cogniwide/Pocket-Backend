"""
Unit and integration tests for Delta Sync functionality in Flick Messenger.
"""

import asyncio
import os
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, delete, or_

from database import async_session_factory, init_db, engine
from models import Base, User, Message, ConversationChangeLog, UserSyncSequence
from sync_service import (
    decode_cursor,
    encode_cursor,
    get_conversation_sync_page,
    get_user_current_version,
    record_conversation_change,
    record_profile_change_for_contacts,
)


async def test_cursor_encoding_decoding():
    ver = 42
    cursor = encode_cursor(ver)
    assert isinstance(cursor, str)
    decoded_ver, decoded_ts = decode_cursor(cursor)
    assert decoded_ver == 42
    assert decoded_ts is not None

    # Direct integer string fallback
    d_int, _ = decode_cursor("100")
    assert d_int == 100
    assert decode_cursor(None) == (None, None)
    assert decode_cursor("") == (None, None)
    assert decode_cursor("invalid_garbage") == (None, None)


async def test_full_sync_and_delta_sync_lifecycle():
    await init_db()

    async with async_session_factory() as db:
        # Create test users
        u1 = User(id="test_sync_u1", phone_number="+919999900001", username="Sync User 1", is_online=False)
        u2 = User(id="test_sync_u2", phone_number="+919999900002", username="Sync User 2", is_online=False)

        await db.merge(u1)
        await db.merge(u2)
        await db.commit()

        # Clean any old test records
        await db.execute(delete(Message).where(or_(Message.sender_id.in_(["test_sync_u1", "test_sync_u2"]), Message.receiver_id.in_(["test_sync_u1", "test_sync_u2"]))))
        await db.execute(delete(ConversationChangeLog).where(ConversationChangeLog.user_id.in_(["test_sync_u1", "test_sync_u2"])))
        await db.execute(delete(UserSyncSequence).where(UserSyncSequence.user_id.in_(["test_sync_u1", "test_sync_u2"])))
        await db.commit()

        # 1. First sync without cursor -> full sync
        page1 = await get_conversation_sync_page(db, u1, cursor_str=None, limit=10)
        assert page1["full_sync"] is True
        assert page1["has_more"] is False
        cursor1 = page1["cursor"]
        initial_ver, _ = decode_cursor(cursor1)
        assert initial_ver is not None

        # 2. Immediate second sync with same cursor -> 0 changes
        page2 = await get_conversation_sync_page(db, u1, cursor_str=cursor1, limit=10)
        assert page2["full_sync"] is False
        assert page2["has_more"] is False
        assert len(page2["created"]) == 0
        assert len(page2["updated"]) == 0
        assert len(page2["deleted"]) == 0
        assert page2["cursor"] == cursor1

        # 3. Presence updates MUST NOT bump version
        u2.is_online = True
        u2.last_seen = datetime.now(timezone.utc)
        await db.commit()

        page_presence = await get_conversation_sync_page(db, u1, cursor_str=cursor1, limit=10)
        assert page_presence["full_sync"] is False
        assert len(page_presence["updated"]) == 0  # Still 0 changes!

        # 4. New message -> bump version and record change
        new_msg = Message(
            sender_id=u2.id,
            receiver_id=u1.id,
            content="Hello from test delta sync!",
            status="SENT",
            timestamp=datetime.now(timezone.utc),
        )
        db.add(new_msg)
        await db.commit()

        await record_conversation_change(db, user_id=u1.id, peer_id=u2.id, action="updated")
        await record_conversation_change(db, user_id=u2.id, peer_id=u1.id, action="updated")
        await db.commit()

        # 5. Delta sync for u1 -> exactly 1 updated conversation
        page3 = await get_conversation_sync_page(db, u1, cursor_str=cursor1, limit=10)
        assert page3["full_sync"] is False
        assert len(page3["updated"]) == 1
        updated_conv = page3["updated"][0]
        assert updated_conv["id"] == u2.id
        assert updated_conv["last_message"] == "Hello from test delta sync!"
        assert updated_conv["unread"] == 1
        cursor2 = page3["cursor"]
        ver2, _ = decode_cursor(cursor2)
        assert ver2 > initial_ver

        # 6. Pagination test
        # Record 3 separate changes for u1
        p_extra1 = "test_peer_a"
        p_extra2 = "test_peer_b"
        await record_conversation_change(db, user_id=u1.id, peer_id=p_extra1, action="created")
        await record_conversation_change(db, user_id=u1.id, peer_id=p_extra2, action="created")
        await db.commit()

        # Request page with limit = 1
        p_page_1 = await get_conversation_sync_page(db, u1, cursor_str=cursor2, limit=1)
        assert p_page_1["has_more"] is True
        cursor_page_1 = p_page_1["cursor"]

        p_page_2 = await get_conversation_sync_page(db, u1, cursor_str=cursor_page_1, limit=10)
        assert p_page_2["has_more"] is False

        # 7. Invalid or corrupted cursor -> falls back to full sync
        p_corrupt = await get_conversation_sync_page(db, u1, cursor_str="not-a-valid-cursor", limit=10)
        assert p_corrupt["full_sync"] is True

        # Clean up
        await db.execute(delete(ConversationChangeLog).where(ConversationChangeLog.user_id.in_(["test_sync_u1", "test_sync_u2"])))
        await db.execute(delete(UserSyncSequence).where(UserSyncSequence.user_id.in_(["test_sync_u1", "test_sync_u2"])))
        await db.execute(delete(Message).where(Message.sender_id.in_(["test_sync_u1", "test_sync_u2"])))
        await db.execute(delete(User).where(User.id.in_(["test_sync_u1", "test_sync_u2"])))
        await db.commit()


if __name__ == "__main__":
    asyncio.run(test_cursor_encoding_decoding())
    asyncio.run(test_full_sync_and_delta_sync_lifecycle())
    print("All backend delta sync tests passed successfully!")
