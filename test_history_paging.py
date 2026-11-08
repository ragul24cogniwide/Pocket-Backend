"""
Scrolling back through history (what a freshly installed app does to get old chats back):
asking for "the page before the oldest message I have" until a short page must return every
message exactly once, even when new messages arrive or messages share a timestamp, and a page
is only ever short at the very start of the history (the app treats a short page as "nothing
older" and drops what the server didn't return).
"""

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, or_

from database import async_session_factory, init_db
from models import Group, GroupMember, GroupMessage, Message, User

ME = "+919700000001"
PEER = "+919700000002"
GROUP = "grp_" + "pagingtest" * 2 + "pagi"  # grp_ + 24 letters, like real ids
PAGE = 30


async def _reset():
    await init_db()
    async with async_session_factory() as db:
        await db.execute(delete(Message).where(or_(Message.sender_id.in_([ME, PEER]), Message.receiver_id.in_([ME, PEER]))))
        await db.execute(delete(GroupMessage).where(GroupMessage.group_id == GROUP))
        await db.execute(delete(GroupMember).where(GroupMember.group_id == GROUP))
        await db.execute(delete(Group).where(Group.id == GROUP))
        for uid in (ME, PEER):
            await db.merge(User(id=uid, phone_number=uid, username=uid))
        await db.commit()


async def _add_direct(n: int, start: datetime, same_time_every: int = 0) -> list[str]:
    """n messages, one a second (or `same_time_every` in a row with the very same timestamp)."""
    ids = []
    async with async_session_factory() as db:
        for i in range(n):
            step = i // same_time_every if same_time_every else i
            m = Message(
                sender_id=ME if i % 2 else PEER, receiver_id=PEER if i % 2 else ME,
                content=f"m{i}", timestamp=start + timedelta(seconds=step),
            )
            db.add(m)
            await db.flush()
            ids.append(m.id)
        await db.commit()
    return ids


async def _page(other: str, before=None, offset=0) -> list[dict]:
    from message_routes import get_messages_with_user
    async with async_session_factory() as db:
        me = await db.get(User, ME)
        return await get_messages_with_user(other, limit=PAGE, offset=offset, before=before, current_user=me, db=db)


async def _scroll_back_everything(other: str, between_pages=None) -> list[str]:
    """What the app does after a fresh install: latest page, then older pages until a short one."""
    page = await _page(other)
    seen = [m["message_id"] for m in page]
    while len(page) == PAGE:
        if between_pages:
            await between_pages()
        oldest = page[0]["message_id"]  # pages come oldest first
        page = await _page(other, before=oldest, offset=len(seen))
        seen += [m["message_id"] for m in page]
    return seen


async def test_fresh_install_gets_every_message_back():
    await _reset()
    ids = await _add_direct(95, datetime(2026, 1, 1, tzinfo=timezone.utc))
    seen = await _scroll_back_everything(PEER)
    assert set(seen) == set(ids)
    assert len(seen) == len(ids)  # nothing twice


async def test_new_messages_while_scrolling_back_skip_nothing():
    await _reset()
    ids = await _add_direct(95, datetime(2026, 1, 1, tzinfo=timezone.utc))
    later = datetime(2026, 2, 1, tzinfo=timezone.utc)

    async def someone_writes():
        nonlocal later
        later += timedelta(seconds=1)
        await _add_direct(5, later)  # pushes every older message 5 places further back

    seen = await _scroll_back_everything(PEER, between_pages=someone_writes)
    assert set(ids) <= set(seen)


async def test_messages_with_the_same_timestamp_are_never_skipped():
    await _reset()
    # 7 messages share each timestamp, so page boundaries fall inside groups of equal times
    ids = await _add_direct(95, datetime(2026, 1, 1, tzinfo=timezone.utc), same_time_every=7)
    seen = await _scroll_back_everything(PEER)
    assert set(seen) == set(ids)  # repeats of equal-time messages are fine (the app drops them by id)


async def test_unknown_before_falls_back_to_offset():
    await _reset()
    await _add_direct(40, datetime(2026, 1, 1, tzinfo=timezone.utc))
    by_offset = await _page(PEER, offset=PAGE)
    assert [m["content"] for m in await _page(PEER, before="deleted-meanwhile", offset=PAGE)] == [m["content"] for m in by_offset]
    assert len(by_offset) == 10


async def test_deleted_for_me_messages_never_make_a_group_page_short():
    await _reset()
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    async with async_session_factory() as db:
        db.add(Group(id=GROUP, name="Paging", created_by=ME))
        for uid in (ME, PEER):
            db.add(GroupMember(group_id=GROUP, user_id=uid, role="member", joined_at=start - timedelta(days=1)))
        ids = []
        for i in range(80):
            # I deleted 25 of the newest messages "for me"
            hidden = json.dumps([ME]) if 50 <= i < 75 else None
            gm = GroupMessage(group_id=GROUP, sender_id=PEER, content=f"g{i}", timestamp=start + timedelta(seconds=i), hidden_for=hidden)
            db.add(gm)
            await db.flush()
            ids.append((gm.id, hidden is None))
        await db.commit()

    first = await _page(GROUP)
    assert len(first) == PAGE  # before: only 5 came back, and the app thought that was the whole history
    seen = await _scroll_back_everything(GROUP)
    assert set(seen) == {i for i, shown in ids if shown}
