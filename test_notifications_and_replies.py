"""
WhatsApp-style notifications (one per chat, listing its unread messages, removed when the chat is
read elsewhere) and replies that quote a message by the id its phone gave it while sending.
"""

import json
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import delete, or_

import notifications
from auth import create_access_token
from database import async_session_factory, init_db
from models import Group, GroupMember, GroupMessage, Message, User
from notification_stack import direct_chat_notification, group_chat_notification

SADHA = "+919600000001"
RAGUL = "+919600000002"
AMMA = "+919600000003"
GROUP = "grp_" + "notifytest" * 2 + "noti"


async def _reset():
    await init_db()
    async with async_session_factory() as db:
        ids = [SADHA, RAGUL, AMMA]
        await db.execute(delete(Message).where(or_(Message.sender_id.in_(ids), Message.receiver_id.in_(ids))))
        await db.execute(delete(GroupMessage).where(GroupMessage.group_id == GROUP))
        await db.execute(delete(GroupMember).where(GroupMember.group_id == GROUP))
        await db.execute(delete(Group).where(Group.id == GROUP))
        for uid, name in ((SADHA, "Sadha"), (RAGUL, "Ragul"), (AMMA, "Amma")):
            await db.merge(User(id=uid, phone_number=uid, username=name, fcm_token=f"token-{uid}"))
        await db.commit()


def _msg(sender, receiver, text, minutes, status="DELIVERED", **kw):
    t = datetime(2026, 11, 8, 9, 0, tzinfo=timezone.utc) + timedelta(minutes=minutes)
    return Message(sender_id=sender, receiver_id=receiver, content=text, timestamp=t, status=status, **kw)


async def test_one_notification_lists_the_unread_messages_of_the_chat():
    await _reset()
    async with async_session_factory() as db:
        db.add_all([
            _msg(SADHA, RAGUL, "old and read", 0, status="READ"),
            _msg(SADHA, RAGUL, "Hi", 1),
            _msg(SADHA, RAGUL, "Are you coming?\nreply soon", 2),
            _msg(SADHA, RAGUL, "📷 Photo", 3),
            _msg(RAGUL, SADHA, "my own message", 4),
            _msg(SADHA, RAGUL, "deleted for me", 5, hidden_for_receiver=True),
        ])
        await db.commit()
        title, body = await direct_chat_notification(db, SADHA, RAGUL, "Sadha", "📷 Photo")
        assert title == "Sadha (3 messages)"
        assert body == "Hi\nAre you coming? reply soon\n📷 Photo"
        # Previews off: only how many
        assert (await direct_chat_notification(db, SADHA, RAGUL, "Sadha", "x", preview=False))[1] == "3 new messages"


async def test_a_single_message_has_a_plain_title_and_long_chats_keep_the_newest():
    await _reset()
    async with async_session_factory() as db:
        db.add(_msg(SADHA, RAGUL, "only one", 1))
        await db.commit()
        assert await direct_chat_notification(db, SADHA, RAGUL, "Sadha", "only one") == ("Sadha", "only one")
        db.add_all([_msg(SADHA, RAGUL, f"line {i}", 10 + i) for i in range(12)])
        await db.commit()
        title, body = await direct_chat_notification(db, SADHA, RAGUL, "Sadha", "line 11")
        assert title == "Sadha (13 messages)"
        assert body.split("\n") == [f"line {i}" for i in range(6, 12)]


async def test_group_notification_names_who_wrote_each_line():
    await _reset()
    start = datetime(2026, 11, 8, 9, 0, tzinfo=timezone.utc)
    async with async_session_factory() as db:
        db.add(Group(id=GROUP, name="Family", created_by=SADHA))
        db.add(GroupMember(group_id=GROUP, user_id=RAGUL, joined_at=start - timedelta(days=1), last_read_at=start))
        db.add_all([
            GroupMessage(group_id=GROUP, sender_id=AMMA, content="read already", timestamp=start - timedelta(minutes=1)),
            GroupMessage(group_id=GROUP, sender_id=AMMA, content="Dinner ready?", timestamp=start + timedelta(minutes=1)),
            GroupMessage(group_id=GROUP, sender_id=SADHA, content="Come fast", timestamp=start + timedelta(minutes=2)),
            GroupMessage(group_id=GROUP, sender_id=RAGUL, content="mine", timestamp=start + timedelta(minutes=3)),
            GroupMessage(group_id=GROUP, sender_id=None, kind="system", content="Amma joined", timestamp=start + timedelta(minutes=4)),
        ])
        await db.commit()
        ragul = await db.get(User, RAGUL)
        title, body = await group_chat_notification(db, GROUP, "Family", ragul, "Sadha: Come fast")
    assert title == "Family (2 messages)"
    assert body == "Amma: Dinner ready?\nSadha: Come fast"


async def test_push_carries_the_chat_key(monkeypatch):
    """The push replaces the chat's previous notification (same key) instead of adding another."""
    await _reset()
    sent = []

    async def fake_send(**kw):
        sent.append(kw)
        return True

    import core
    monkeypatch.setattr(core, "send_push_notification", fake_send)
    async with async_session_factory() as db:
        db.add_all([_msg(SADHA, RAGUL, "Hi", 1), _msg(SADHA, RAGUL, "Hello?", 2)])
        await db.commit()
    await core.dispatch_push_to_user(SADHA, RAGUL, "Hello?", {"type": "chat_message", "message_id": "m"})
    assert sent[0]["thread_key"] == f"chat_{SADHA}"
    assert sent[0]["sender_name"] == "Sadha (2 messages)"
    assert sent[0]["content"] == "Hi\nHello?"

    # Calls are unchanged: no chat key, their own text
    sent.clear()
    await core.dispatch_push_to_user(SADHA, RAGUL, "Incoming call", {"type": "incoming_call"})
    assert sent[0]["thread_key"] is None and sent[0]["content"] == "Incoming call"


# ---------------------------------------------------------------------------
# Through the real app: replies, and reading on Flick Web
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def client():
    from main import app
    with TestClient(app) as c:
        yield c


def _token(client, phone):
    async def tv():
        async with async_session_factory() as db:
            return (await db.get(User, phone)).token_version or 0
    return create_access_token(phone, phone, client.portal.call(tv))


def test_reply_to_a_message_quoted_by_its_temporary_id_gets_the_server_id(client):
    client.portal.call(_reset)
    s, r = _token(client, SADHA), _token(client, RAGUL)
    h = lambda t: {"Authorization": f"Bearer {t}"}
    original = client.post("/api/messages/send", headers=h(r), json={
        "receiver_id": SADHA, "content": "intha photova thaan", "temp_msg_id": "temp_123_abc",
    }).json()
    # Sadha quoted it while it still had the id Ragul's phone gave it
    reply = client.post("/api/messages/send", headers=h(s), json={
        "receiver_id": RAGUL, "content": "Yendha pht", "temp_msg_id": "temp_456_def",
        "reply_to": {"id": "temp_123_abc", "sender": "Ragul", "text": "intha photova thaan"},
    }).json()
    assert reply["reply_to"]["id"] == original["message_id"]
    assert reply["reply_to"]["text"] == "intha photova thaan"
    # A quote of a real id, or of something unknown, is stored as it was
    other = client.post("/api/messages/send", headers=h(s), json={
        "receiver_id": RAGUL, "content": "x", "reply_to": {"id": "not-a-message", "text": "?"},
    }).json()
    assert other["reply_to"]["id"] == "not-a-message"


def test_reading_on_flick_web_removes_the_phone_notification(client, monkeypatch):
    client.portal.call(_reset)
    cleared = []

    async def fake_clear(token, key):
        cleared.append((token, key))
        return True

    import messaging
    monkeypatch.setattr(messaging, "send_clear_notification", fake_clear)

    async def setup():
        async with async_session_factory() as db:
            db.add(_msg(SADHA, RAGUL, "unread", 1))
            await db.commit()
    client.portal.call(setup)

    r = _token(client, RAGUL)
    # From the phone: no push (the phone removes its own notification)
    with client.websocket_connect(f"/ws/{RAGUL}?token={r}") as ws:
        ws.send_text(json.dumps({"type": "mark_read", "data": {"sender_id": SADHA}}))
        ws.send_text(json.dumps({"type": "ping"}))
        while json.loads(ws.receive_text()).get("type") != "pong":
            pass
    assert cleared == []

    # From Flick Web: the phone is told to remove that chat's notification
    client.portal.call(setup)
    import gateway
    original = gateway.user_from_token

    async def as_web(token, db):
        user = await original(token, db)
        if user is not None:
            user.web_session_id = "web-session-1"
        return user

    monkeypatch.setattr(gateway, "user_from_token", as_web)
    with client.websocket_connect(f"/ws/{RAGUL}?token={r}") as ws:
        ws.send_text(json.dumps({"type": "mark_read", "data": {"sender_id": SADHA}}))
        ws.send_text(json.dumps({"type": "ping"}))
        while json.loads(ws.receive_text()).get("type") != "pong":
            pass
    client.portal.call(lambda: __import__("asyncio").sleep(0.2))
    assert cleared == [(f"token-{RAGUL}", f"chat_{SADHA}")]
