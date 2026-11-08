"""
Two different people whose numbers end with the same 10 digits must never be mixed up
(+1 415 555 1234 vs +91 41555 51234), while every spelling older app versions stored for an
Indian number keeps matching it, so existing chats still load.
"""

from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, or_

from connection_manager import ConnectionManager
from database import async_session_factory, init_db
from identity import canonical_id, id_forms, same_identity
from models import Message, User

INDIA = "+914155551234"   # Indian user
US = "+14155551234"       # US user: same last 10 digits
FRIEND = "+919000000001"  # someone both of them chat with


# ---------------------------------------------------------------------------
# The rule itself
# ---------------------------------------------------------------------------
def test_canonical_id_matches_the_apps_rules():
    for spelling in ("+91 41555 51234", "914155551234", "4155551234", "04155551234", "0091 4155551234", INDIA):
        assert canonical_id(spelling) == INDIA, spelling
    for spelling in ("+1 415 555 1234", "14155551234", "001 415 555 1234", US):
        assert canonical_id(spelling) == US, spelling
    # Ids that aren't phone numbers are compared exactly
    for other in ("ai_assistant", "grp_abc123", "1", "test_sync_u1", ""):
        assert canonical_id(other) == other


def test_numbers_with_the_same_ending_are_different_people():
    assert not same_identity(INDIA, US)
    assert not id_forms(INDIA) & id_forms(US)
    assert not id_forms(US) & id_forms(INDIA)
    # The bare 10 digits mean the Indian number (India is the app's default country), never the US one
    assert "4155551234" in id_forms(INDIA)
    assert "4155551234" not in id_forms(US)


def test_old_spellings_of_an_indian_number_still_match_it():
    forms = id_forms(INDIA)
    # Everything the old code matched for this person (id, +91 + last 10, last 10, digits)
    assert {INDIA, "914155551234", "4155551234", "04155551234"} <= forms
    assert same_identity("4155551234", INDIA)
    assert same_identity("+91 41555 51234", INDIA)
    assert not same_identity(None, None)
    assert not same_identity("", "")


def test_non_phone_ids_have_only_themselves_as_forms():
    assert id_forms("ai_assistant") == {"ai_assistant"}
    assert id_forms("1") == {"1"}
    assert id_forms(None, "") == set()


# ---------------------------------------------------------------------------
# Chat history, user lookup and live delivery against a real (test) database
# ---------------------------------------------------------------------------
async def _seed():
    await init_db()
    ids = [INDIA, US, FRIEND]
    async with async_session_factory() as db:
        await db.execute(delete(Message).where(or_(Message.sender_id.in_(id_forms(*ids)), Message.receiver_id.in_(id_forms(*ids)))))
        for uid, name in ((INDIA, "India User"), (US, "US User"), (FRIEND, "Friend")):
            await db.merge(User(id=uid, phone_number=uid, username=name, is_online=False))
        now = datetime.now(timezone.utc)
        db.add_all([
            # Indian user <-> friend, one of them stored the way an older app version spelled ids
            Message(sender_id=INDIA, receiver_id=FRIEND, content="india-1", timestamp=now - timedelta(minutes=5)),
            Message(sender_id=FRIEND, receiver_id="4155551234", content="india-legacy", timestamp=now - timedelta(minutes=4)),
            # US user <-> friend
            Message(sender_id=US, receiver_id=FRIEND, content="us-1", timestamp=now - timedelta(minutes=3)),
            Message(sender_id=FRIEND, receiver_id=US, content="us-2", timestamp=now - timedelta(minutes=2)),
        ])
        await db.commit()


async def _history(me_id: str, other_id: str) -> set[str]:
    from message_routes import get_messages_with_user
    async with async_session_factory() as db:
        me = await db.get(User, me_id)
        rows = await get_messages_with_user(other_id, limit=50, offset=0, current_user=me, db=db)
    return {m["content"] for m in rows}


async def test_chat_history_is_not_shared_between_colliding_numbers():
    await _seed()
    assert await _history(US, FRIEND) == {"us-1", "us-2"}
    # The Indian user's chat, including the message stored under the old 10-digit spelling
    assert await _history(INDIA, FRIEND) == {"india-1", "india-legacy"}
    # The friend sees both chats separately
    assert await _history(FRIEND, US) == {"us-1", "us-2"}
    assert await _history(FRIEND, INDIA) == {"india-1", "india-legacy"}
    # Opening the chat with an old spelling of the number still finds the right person
    assert await _history(FRIEND, "4155551234") == {"india-1", "india-legacy"}
    assert await _history(FRIEND, "14155551234") == {"us-1", "us-2"}


async def test_chat_list_row_is_not_shared():
    """The chat-list row (delta sync): last message and unread count are each person's own."""
    from sync_service import _build_snapshot
    await _seed()
    async with async_session_factory() as db:
        us_row = await _build_snapshot(db, US, FRIEND)
        india_row = await _build_snapshot(db, INDIA, FRIEND)
    assert us_row["last_message"] == "us-2"
    assert us_row["unread"] == 1          # only "us-2" was sent to the US user
    assert india_row["last_message"] == "india-legacy"
    assert india_row["unread"] == 1       # only "india-legacy", stored under the old spelling


async def test_find_user_picks_the_right_person():
    from core import find_user
    await _seed()
    async with async_session_factory() as db:
        assert (await find_user(db, "4155551234")).id == INDIA
        assert (await find_user(db, "+91 41555 51234")).id == INDIA
        assert (await find_user(db, "14155551234")).id == US
        assert (await find_user(db, US)).id == US
        assert await find_user(db, "+444155551234") is None  # same ending, nobody with that number


class FakeSocket:
    def __init__(self):
        self.sent: list[str] = []

    async def accept(self):
        pass

    async def close(self, code: int = 1000, reason: str = ""):
        pass

    async def send_text(self, text: str):
        self.sent.append(text)


async def test_live_messages_reach_only_the_right_person():
    manager = ConnectionManager()  # no Redis: this server's sockets only
    india_ws, us_ws = FakeSocket(), FakeSocket()
    await manager.connect(INDIA, india_ws)
    await manager.connect(US, us_ws)

    assert await manager.send_personal_message(US, {"type": "chat_message", "data": {"content": "for-us"}})
    assert any("for-us" in t for t in us_ws.sent)
    assert not any("for-us" in t for t in india_ws.sent)

    # An old 10-digit spelling means the Indian number
    assert await manager.send_personal_message("4155551234", {"type": "chat_message", "data": {"content": "for-india"}})
    assert any("for-india" in t for t in india_ws.sent)
    assert not any("for-india" in t for t in us_ws.sent)

    # A number that merely ends the same way reaches nobody
    assert not await manager.send_personal_message("+444155551234", {"type": "chat_message", "data": {"content": "x"}})

    # Going offline / away is tracked per person too
    manager.mark_away(US)
    assert manager.is_away(US)
    assert not manager.is_away(INDIA)
    assert await manager.is_user_online(INDIA)
