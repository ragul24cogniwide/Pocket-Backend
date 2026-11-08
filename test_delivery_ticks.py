"""
Ticks like WhatsApp: one grey tick = on the server, two grey ticks = reached the other person's
phone (over the socket, or as a push notification while their app was closed), blue = read.
"""

import json
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import delete, or_, select

from auth import create_access_token
from database import async_session_factory, init_db
from models import Group, GroupMember, GroupMessage, Message, User

ALICE = "+919500000001"
BOB = "+919500000002"
EVE = "+919500000003"
GROUP = "grp_" + "deliverytest" * 2


@pytest.fixture(scope="module")
def client():
    from main import app
    with TestClient(app) as c:
        yield c


def _reset(client):
    async def run():
        await init_db()
        async with async_session_factory() as db:
            ids = [ALICE, BOB, EVE]
            await db.execute(delete(Message).where(or_(Message.sender_id.in_(ids), Message.receiver_id.in_(ids))))
            await db.execute(delete(GroupMessage).where(GroupMessage.group_id == GROUP))
            await db.execute(delete(GroupMember).where(GroupMember.group_id == GROUP))
            await db.execute(delete(Group).where(Group.id == GROUP))
            for uid in ids:
                await db.merge(User(id=uid, phone_number=uid, username=uid))
            await db.commit()
    client.portal.call(run)


def _token(client, phone):
    async def tv():
        async with async_session_factory() as db:
            return (await db.get(User, phone)).token_version or 0
    return create_access_token(phone, phone, client.portal.call(tv))


def _h(token):
    return {"Authorization": f"Bearer {token}"}


def _status(client, message_id):
    async def get():
        async with async_session_factory() as db:
            m = await db.get(Message, message_id)
            return str(m.status.value if hasattr(m.status, "value") else m.status)
    return client.portal.call(get)


def _next(ws, wanted):
    for _ in range(30):
        frame = json.loads(ws.receive_text())
        if frame.get("type") == wanted:
            return frame
    raise AssertionError(f"no {wanted}")


def test_push_delivery_turns_one_tick_into_two_for_the_sender(client):
    _reset(client)
    a, b = _token(client, ALICE), _token(client, BOB)
    with client.websocket_connect(f"/ws/{ALICE}?token={a}") as wa:
        # Bob's app is closed: the message waits on the server (one tick) and goes out as a push
        sent = client.post("/api/messages/send", headers=_h(a), json={"receiver_id": BOB, "content": "hi", "temp_msg_id": "t1"}).json()
        mid = sent["message_id"]
        assert _status(client, mid) == "SENT"

        # Bob's phone got the push: it says so, and Alice sees two ticks right away
        r = client.post("/api/messages/delivered", headers=_h(b), json={"message_ids": [mid]}).json()
        assert r["delivered"] == 1
        assert _status(client, mid) == "DELIVERED"
        frame = _next(wa, "ack_delivered")["data"]
        assert frame["message_id"] == mid and frame["receiver_id"] == BOB

        # Again (a repeated push): nothing changes
        assert client.post("/api/messages/delivered", headers=_h(b), json={"message_ids": [mid]}).json()["delivered"] == 0


def test_nobody_else_can_mark_a_message_delivered_and_read_stays_read(client):
    _reset(client)
    a, b, e = _token(client, ALICE), _token(client, BOB), _token(client, EVE)
    mid = client.post("/api/messages/send", headers=_h(a), json={"receiver_id": BOB, "content": "x"}).json()["message_id"]
    # Not addressed to Eve (or to Alice, who sent it): no fake receipts
    assert client.post("/api/messages/delivered", headers=_h(e), json={"message_ids": [mid]}).json()["delivered"] == 0
    assert client.post("/api/messages/delivered", headers=_h(a), json={"message_ids": [mid]}).json()["delivered"] == 0
    assert _status(client, mid) == "SENT"
    assert client.post("/api/messages/delivered", json={"message_ids": [mid]}).status_code == 401

    # Read first, then a late push confirmation: still read (blue), never back to grey
    with client.websocket_connect(f"/ws/{BOB}?token={b}") as wb:
        wb.send_text(json.dumps({"type": "mark_read", "data": {"sender_id": ALICE}}))
        wb.send_text(json.dumps({"type": "ping"}))
        _next(wb, "pong")
    assert _status(client, mid) == "READ"
    client.post("/api/messages/delivered", headers=_h(b), json={"message_ids": [mid]})
    assert _status(client, mid) == "READ"


def test_socket_delivery_still_works(client):
    _reset(client)
    a, b = _token(client, ALICE), _token(client, BOB)
    mid = client.post("/api/messages/send", headers=_h(a), json={"receiver_id": BOB, "content": "y"}).json()["message_id"]
    with client.websocket_connect(f"/ws/{BOB}?token={b}") as wb:
        wb.send_text(json.dumps({"type": "ack_delivered", "data": {"message_id": mid, "sender_id": ALICE}}))
        wb.send_text(json.dumps({"type": "ping"}))
        _next(wb, "pong")
    assert _status(client, mid) == "DELIVERED"


def test_group_push_delivery_reaches_the_member(client):
    _reset(client)
    b = _token(client, BOB)
    start = datetime.now(timezone.utc) - timedelta(minutes=5)

    async def setup():
        async with async_session_factory() as db:
            db.add(Group(id=GROUP, name="G", created_by=ALICE))
            db.add(GroupMember(group_id=GROUP, user_id=ALICE, joined_at=start, last_delivered_at=start, last_read_at=start))
            db.add(GroupMember(group_id=GROUP, user_id=BOB, joined_at=start, last_delivered_at=start, last_read_at=start))
            await db.commit()
    client.portal.call(setup)

    client.post("/api/messages/delivered", headers=_h(b), json={"group_ids": [GROUP, "not-a-group"]})

    async def delivered_at():
        async with async_session_factory() as db:
            m = (await db.execute(select(GroupMember).where(GroupMember.group_id == GROUP, GroupMember.user_id == BOB))).scalars().first()
            return m.last_delivered_at
    got = client.portal.call(delivered_at)
    assert got is not None and got.replace(tzinfo=got.tzinfo or timezone.utc) > start + timedelta(minutes=4)
