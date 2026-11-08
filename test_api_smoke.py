"""
End-to-end checks of the whole app (routes, WebSocket gateway, delivery), run in-process against
the throwaway test database (see conftest.py). They guard refactors of main.py / the routers and
of the shared send path used by REST and the WebSocket.
"""

import json

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from auth import create_access_token

ALICE = "+919800000001"
BOB = "+919800000002"


@pytest.fixture(scope="module")
def client():
    from main import app
    with TestClient(app) as c:
        yield c


def _login(client: TestClient, phone: str) -> str:
    """Creates the account (if needed) through the real sign-in, returns a bearer token."""
    import asyncio
    from database import async_session_factory
    from models import User

    async def ensure():
        async with async_session_factory() as db:
            user = await db.get(User, phone)
            if user is None:
                user = User(id=phone, phone_number=phone, username=f"User {phone[-4:]}")
                db.add(user)
                await db.commit()
            return user.token_version or 0

    tv = client.portal.call(ensure) if hasattr(client, "portal") and client.portal else asyncio.run(ensure())
    return create_access_token(phone, phone, tv)


def _h(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def test_health(client):
    assert client.get("/api/health").json()["status"] == "active"


def test_protected_routes_need_a_token(client):
    assert client.get("/api/users").status_code == 401
    assert client.post("/api/messages/send", json={"receiver_id": BOB, "content": "x"}).status_code == 401


def test_rest_send_history_edit_delete(client):
    a, b = _login(client, ALICE), _login(client, BOB)
    sent = client.post("/api/messages/send", headers=_h(a), json={
        "receiver_id": BOB, "content": "hello bob", "temp_msg_id": "t-rest-1",
    }).json()
    # ("status" in the answer is the message's own SENT / DELIVERED, as it always was)
    assert sent["temp_msg_id"] == "t-rest-1" and sent["status"] in ("SENT", "DELIVERED")
    mid = sent["message_id"]

    # A retry with the same temp id is answered with the first message, not stored twice
    again = client.post("/api/messages/send", headers=_h(a), json={
        "receiver_id": BOB, "content": "hello bob", "temp_msg_id": "t-rest-1",
    }).json()
    assert again["message_id"] == mid and again.get("duplicate") is True

    history = client.get(f"/api/messages/{ALICE}", headers=_h(b)).json()
    assert [m["content"] for m in history if m["message_id"] == mid] == ["hello bob"]

    edited = client.put(f"/api/messages/{mid}", headers=_h(a), json={"content": "hello again"}).json()
    assert edited["is_edited"] is True
    assert client.put(f"/api/messages/{mid}", headers=_h(b), json={"content": "hijack"}).status_code == 403

    client.delete(f"/api/messages/{mid}", headers=_h(a))
    history = client.get(f"/api/messages/{ALICE}", headers=_h(b)).json()
    assert mid not in {m["message_id"] for m in history}


def test_rest_send_to_unknown_number(client):
    a = _login(client, ALICE)
    r = client.post("/api/messages/send", headers=_h(a), json={"receiver_id": "+919811111111", "content": "x"})
    assert r.status_code == 404


def test_websocket_rejects_a_missing_or_wrong_token(client):
    a = _login(client, ALICE)
    for url in (f"/ws/{ALICE}", f"/ws/{BOB}?token={a}"):
        with client.websocket_connect(url) as ws:
            with pytest.raises(WebSocketDisconnect) as exc:
                ws.receive_text()
            assert exc.value.code == 4401


def _next(ws, wanted: str, limit: int = 30, **match) -> dict:
    for _ in range(limit):
        frame = json.loads(ws.receive_text())
        data = frame.get("data") or {}
        if frame.get("type") == wanted and all(data.get(k) == v for k, v in match.items()):
            return frame
    raise AssertionError(f"no {wanted} frame")


def test_websocket_chat_between_two_people(client):
    a, b = _login(client, ALICE), _login(client, BOB)
    with client.websocket_connect(f"/ws/{ALICE}?token={a}") as wa, client.websocket_connect(f"/ws/{BOB}?token={b}") as wb:
        wa.send_text(json.dumps({"type": "ping"}))
        assert _next(wa, "pong")

        wa.send_text(json.dumps({"type": "send_message", "data": {
            "receiver_id": BOB, "content": "live hi", "temp_msg_id": "t-ws-1",
            "metadata": {"kind": "text"},
        }}))
        ack = _next(wa, "ack_sent")["data"]
        assert ack["temp_msg_id"] == "t-ws-1" and ack["status"] == "DELIVERED"
        got = _next(wb, "chat_message", content="live hi")["data"]
        assert got["content"] == "live hi" and got["sender_id"] == ALICE and got["message_id"] == ack["message_id"]

        # Resent after a reconnect: confirmed again, not delivered twice
        wa.send_text(json.dumps({"type": "send_message", "data": {
            "receiver_id": BOB, "content": "live hi", "temp_msg_id": "t-ws-1",
        }}))
        dup = _next(wa, "ack_sent")["data"]
        assert dup["message_id"] == ack["message_id"] and dup.get("duplicate") is True

        # Unknown receiver / too long: an error for that message, the socket stays open
        wa.send_text(json.dumps({"type": "send_message", "data": {"receiver_id": "+919811111111", "content": "x", "temp_msg_id": "t-ws-2"}}))
        assert _next(wa, "message_error")["data"]["temp_msg_id"] == "t-ws-2"
        wa.send_text(json.dumps({"type": "send_message", "data": {"receiver_id": BOB, "content": "x" * 70000, "temp_msg_id": "t-ws-3"}}))
        assert _next(wa, "message_error")["data"]["temp_msg_id"] == "t-ws-3"

    history = client.get(f"/api/messages/{BOB}", headers=_h(a)).json()
    assert [m["content"] for m in history].count("live hi") == 1
