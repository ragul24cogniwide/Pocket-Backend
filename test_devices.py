"""
Step 1 of keeping messages on phones: every device is known, and the server records which device
has which message (watch mode: nothing is deleted). See devices.py.
"""

import json
import secrets
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import delete, or_, select

import devices
from auth import create_access_token, create_web_token
from database import async_session_factory, init_db
from models import Device, Group, GroupMember, GroupMessage, Message, MessageDelivery, User, WebSession

ALICE = "+919300000001"
BOB = "+919300000002"
CAROL = "+919300000003"
GROUP = "grp_" + "devicetest" * 2 + "devi"
ALICE_PHONE = "alicephone0001"
BOB_PHONE = "bobphone000001"


@pytest.fixture(scope="module")
def client():
    from main import app
    with TestClient(app) as c:
        yield c


def run(client, coro_fn):
    return client.portal.call(coro_fn)


def reset(client):
    async def go():
        await init_db()
        people = [ALICE, BOB, CAROL]
        async with async_session_factory() as db:
            await db.execute(delete(MessageDelivery).where(MessageDelivery.user_id.in_(people)))
            await db.execute(delete(Device).where(Device.user_id.in_(people)))
            await db.execute(delete(WebSession).where(WebSession.user_id.in_(people)))
            await db.execute(delete(Message).where(or_(Message.sender_id.in_(people), Message.receiver_id.in_(people))))
            await db.execute(delete(GroupMessage).where(GroupMessage.group_id == GROUP))
            await db.execute(delete(GroupMember).where(GroupMember.group_id == GROUP))
            await db.execute(delete(Group).where(Group.id == GROUP))
            for uid in people:
                await db.merge(User(id=uid, phone_number=uid, username=uid, token_version=0))
            await db.commit()
    run(client, go)


def phone_auth(uid, key=None):
    h = {"Authorization": f"Bearer {create_access_token(uid, uid, 0)}"}
    if key:
        h["X-Flick-Device"] = key
    return h


def link_browser(client, uid) -> tuple[str, str]:
    """(web token, session id) of a browser linked to uid's phone."""
    sid = secrets.token_urlsafe(18)

    async def go():
        async with async_session_factory() as db:
            db.add(WebSession(id=sid, user_id=uid, device_name="Chrome"))
            await db.commit()
    run(client, go)
    return create_web_token(uid, uid, 0, sid), sid


def register(client, uid, key, **extra):
    return client.post("/api/devices/register", headers=phone_auth(uid), json={"device_key": key, "platform": "android", **extra})


def rows(client, message_id=None):
    """{(owner, device kind, client key): (delivered?, via)} of the delivery rows."""
    async def go():
        async with async_session_factory() as db:
            q = select(MessageDelivery, Device).join(Device, Device.id == MessageDelivery.device_id)
            if message_id:
                q = q.where(MessageDelivery.message_id == message_id)
            out = {}
            for d, dev in (await db.execute(q)).all():
                out[(d.user_id, dev.kind, dev.client_key)] = (d.delivered_at is not None, d.delivered_via)
            return out
    return run(client, go)


def device_of(client, uid, key):
    async def go():
        async with async_session_factory() as db:
            return (await db.execute(select(Device).where(Device.user_id == uid, Device.client_key == key))).scalars().first()
    return run(client, go)


def next_frame(ws, wanted):
    for _ in range(40):
        f = json.loads(ws.receive_text())
        if f.get("type") == wanted:
            return f
    raise AssertionError(f"no {wanted}")


def ping(ws):
    """Waits until the server has handled everything sent before."""
    ws.send_text(json.dumps({"type": "ping"}))
    next_frame(ws, "pong")


# ---------------------------------------------------------------------------
# Devices
# ---------------------------------------------------------------------------
def test_a_phone_registers_and_a_new_phone_replaces_it(client):
    reset(client)
    r = register(client, ALICE, ALICE_PHONE, name="Pixel 8", app_version="1.4.0")
    assert r.status_code == 200 and r.json()["device"]["kind"] == "phone"
    listed = client.get("/api/devices", headers=phone_auth(ALICE, ALICE_PHONE)).json()
    assert [(d["name"], d["current"]) for d in listed] == [("Pixel 8", True)]

    # Signing in on a new phone: the old one is no longer the account's phone
    register(client, ALICE, "alicenewphone1")
    assert device_of(client, ALICE, ALICE_PHONE).revoked_at is not None
    assert device_of(client, ALICE, "alicenewphone1").revoked_at is None
    # A request from the old phone doesn't take the account back (only registering does)
    client.get("/api/devices", headers=phone_auth(ALICE, ALICE_PHONE))
    assert device_of(client, ALICE, "alicenewphone1").revoked_at is None
    # The old phone registering again (signed in again there) does
    register(client, ALICE, ALICE_PHONE)
    assert device_of(client, ALICE, ALICE_PHONE).revoked_at is None
    assert device_of(client, ALICE, "alicenewphone1").revoked_at is not None


def test_device_keys_are_per_account_and_validated(client):
    reset(client)
    register(client, ALICE, "sharedkey0001")
    register(client, BOB, "sharedkey0001")  # same key, other account: a different device
    assert device_of(client, ALICE, "sharedkey0001").id != device_of(client, BOB, "sharedkey0001").id
    assert register(client, ALICE, "bad key!").status_code == 422 or register(client, ALICE, "bad key!").status_code == 400
    assert register(client, ALICE, "legacy-phone").status_code == 400
    web_token, _ = link_browser(client, ALICE)
    r = client.post("/api/devices/register", headers={"Authorization": f"Bearer {web_token}"}, json={"device_key": "webwantsakey"})
    assert r.status_code == 403


# ---------------------------------------------------------------------------
# Who has to get a message, and who has it
# ---------------------------------------------------------------------------
def test_a_message_is_expected_on_every_device_but_the_one_it_was_sent_from(client):
    reset(client)
    register(client, ALICE, ALICE_PHONE)
    register(client, BOB, BOB_PHONE)
    _, alice_sid = link_browser(client, ALICE)
    bob_web, bob_sid = link_browser(client, BOB)
    client.get("/api/devices", headers={"Authorization": f"Bearer {bob_web}"})  # Bob's browser shows up
    alice_web, _ = alice_sid, None
    client.get("/api/devices", headers={"Authorization": f"Bearer {create_web_token(ALICE, ALICE, 0, alice_sid)}"})

    sent = client.post("/api/messages/send", headers=phone_auth(ALICE, ALICE_PHONE),
                       json={"receiver_id": BOB, "content": "hi", "temp_msg_id": "t-1"}).json()
    got = rows(client, sent["message_id"])
    assert set(got) == {
        (BOB, "phone", BOB_PHONE),
        (BOB, "web", f"web:{bob_sid}"),
        (ALICE, "web", f"web:{alice_sid}"),  # the sender's other device shows it too
    }
    assert all(not delivered for delivered, _ in got.values())


def test_each_way_a_device_gets_a_message_is_recorded(client):
    reset(client)
    register(client, ALICE, ALICE_PHONE)
    register(client, BOB, BOB_PHONE)
    bob_web, bob_sid = link_browser(client, BOB)
    client.get("/api/devices", headers={"Authorization": f"Bearer {bob_web}"})

    m1 = client.post("/api/messages/send", headers=phone_auth(ALICE, ALICE_PHONE), json={"receiver_id": BOB, "content": "one"}).json()["message_id"]
    m2 = client.post("/api/messages/send", headers=phone_auth(ALICE, ALICE_PHONE), json={"receiver_id": BOB, "content": "two"}).json()["message_id"]

    # Bob's phone, over its socket (new app: device_ack; the older ack_delivered counts too)
    token = create_access_token(BOB, BOB, 0)
    with client.websocket_connect(f"/ws/{BOB}?token={token}&device={BOB_PHONE}") as ws:
        ws.send_text(json.dumps({"type": "device_ack", "data": {"message_ids": [m1]}}))
        ws.send_text(json.dumps({"type": "ack_delivered", "data": {"message_id": m2, "sender_id": ALICE}}))
        ping(ws)
    assert rows(client, m1)[(BOB, "phone", BOB_PHONE)] == (True, "socket")
    assert rows(client, m2)[(BOB, "phone", BOB_PHONE)] == (True, "socket")

    # Bob's browser, by downloading the chat
    history = client.get(f"/api/messages/{ALICE}", headers={"Authorization": f"Bearer {bob_web}"}).json()
    assert {m["message_id"] for m in history} >= {m1, m2}
    assert rows(client, m1)[(BOB, "web", f"web:{bob_sid}")] == (True, "history")

    # A push while the app was closed
    m3 = client.post("/api/messages/send", headers=phone_auth(ALICE, ALICE_PHONE), json={"receiver_id": BOB, "content": "three"}).json()["message_id"]
    client.post("/api/messages/delivered", headers=phone_auth(BOB, BOB_PHONE), json={"message_ids": [m3]})
    assert rows(client, m3)[(BOB, "phone", BOB_PHONE)] == (True, "push")
    # A device can't tick a row that isn't its own
    client.post("/api/messages/delivered", headers=phone_auth(CAROL, "carolphone001"), json={"message_ids": [m3]})
    assert rows(client, m3)[(BOB, "web", f"web:{bob_sid}")] == (False, None)


def test_group_messages_are_expected_on_every_members_devices(client):
    reset(client)
    for uid, key in ((ALICE, ALICE_PHONE), (BOB, BOB_PHONE), (CAROL, "carolphone001")):
        register(client, uid, key)
    start = datetime.now(timezone.utc) - timedelta(minutes=5)

    async def setup():
        async with async_session_factory() as db:
            db.add(Group(id=GROUP, name="G", created_by=ALICE))
            for uid in (ALICE, BOB, CAROL):
                db.add(GroupMember(group_id=GROUP, user_id=uid, joined_at=start, last_delivered_at=start, last_read_at=start))
            await db.commit()
    run(client, setup)

    g1 = client.post("/api/messages/send", headers=phone_auth(ALICE, ALICE_PHONE), json={"receiver_id": GROUP, "content": "dinner?"}).json()["message_id"]
    g2 = client.post("/api/messages/send", headers=phone_auth(ALICE, ALICE_PHONE), json={"receiver_id": GROUP, "content": "8pm"}).json()["message_id"]
    assert set(rows(client, g1)) == {(BOB, "phone", BOB_PHONE), (CAROL, "phone", "carolphone001")}

    # New app: confirms the message; older app: confirms "the group so far"
    with client.websocket_connect(f"/ws/{BOB}?token={create_access_token(BOB, BOB, 0)}&device={BOB_PHONE}") as ws:
        ws.send_text(json.dumps({"type": "ack_group_delivered", "data": {"group_id": GROUP, "message_id": g1}}))
        ping(ws)
    assert rows(client, g1)[(BOB, "phone", BOB_PHONE)][0] and not rows(client, g2)[(BOB, "phone", BOB_PHONE)][0]
    with client.websocket_connect(f"/ws/{CAROL}?token={create_access_token(CAROL, CAROL, 0)}&device=carolphone001") as ws:
        ws.send_text(json.dumps({"type": "ack_group_delivered", "data": {"group_id": GROUP}}))
        ping(ws)
    assert rows(client, g1)[(CAROL, "phone", "carolphone001")][0] and rows(client, g2)[(CAROL, "phone", "carolphone001")][0]

    # Deleted for everyone: nobody has to get it any more
    client.delete(f"/api/messages/{g2}?for_everyone=true", headers=phone_auth(ALICE, ALICE_PHONE))
    assert rows(client, g2) == {}


def test_older_app_versions_are_one_legacy_phone_until_the_new_app_registers(client):
    reset(client)
    # Old app: no device key anywhere
    with client.websocket_connect(f"/ws/{BOB}?token={create_access_token(BOB, BOB, 0)}") as ws:
        ping(ws)
    legacy = device_of(client, BOB, devices.LEGACY_PHONE_KEY)
    assert legacy is not None and legacy.revoked_at is None
    m = client.post("/api/messages/send", headers=phone_auth(ALICE), json={"receiver_id": BOB, "content": "x"}).json()["message_id"]
    assert set(rows(client, m)) == {(BOB, "phone", devices.LEGACY_PHONE_KEY)}
    with client.websocket_connect(f"/ws/{BOB}?token={create_access_token(BOB, BOB, 0)}") as ws:
        ws.send_text(json.dumps({"type": "ack_delivered", "data": {"message_id": m, "sender_id": ALICE}}))
        ping(ws)
    assert rows(client, m)[(BOB, "phone", devices.LEGACY_PHONE_KEY)] == (True, "socket")

    # Updated app registers: it replaces the legacy phone, and requests without a key are it now
    register(client, BOB, BOB_PHONE)
    assert device_of(client, BOB, devices.LEGACY_PHONE_KEY).revoked_at is not None
    client.get("/api/devices", headers=phone_auth(BOB))
    assert device_of(client, BOB, devices.LEGACY_PHONE_KEY).revoked_at is not None


def test_signed_out_inactive_and_ai_agent_are_not_expected(client):
    reset(client)
    register(client, ALICE, ALICE_PHONE)
    register(client, BOB, BOB_PHONE)
    bob_web, bob_sid = link_browser(client, BOB)
    client.get("/api/devices", headers={"Authorization": f"Bearer {bob_web}"})

    # Bob unlinks his browser; his phone hasn't been seen for 31 days
    client.post("/api/web/logout", headers={"Authorization": f"Bearer {bob_web}"})

    async def age_phone():
        async with async_session_factory() as db:
            d = (await db.execute(select(Device).where(Device.user_id == BOB, Device.client_key == BOB_PHONE))).scalars().first()
            d.last_seen_at = datetime.now(timezone.utc) - timedelta(days=31)
            await db.commit()
    run(client, age_phone)
    assert device_of(client, BOB, f"web:{bob_sid}").revoked_at is not None
    m = client.post("/api/messages/send", headers=phone_auth(ALICE, ALICE_PHONE), json={"receiver_id": BOB, "content": "x"}).json()["message_id"]
    assert rows(client, m) == {}

    # The AI agent's chat stays on the server: not tracked
    n = run(client, lambda: devices.expect_deliveries("agent-msg", datetime.now(timezone.utc), [devices.AGENT_USER_ID], ALICE, None))
    assert n == 0


def test_phone_logout_signs_out_every_device_and_account_deletion_removes_them(client):
    reset(client)
    register(client, ALICE, ALICE_PHONE)
    register(client, BOB, BOB_PHONE)
    alice_web, alice_sid = link_browser(client, ALICE)
    client.get("/api/devices", headers={"Authorization": f"Bearer {alice_web}"})
    m = client.post("/api/messages/send", headers=phone_auth(ALICE, ALICE_PHONE), json={"receiver_id": BOB, "content": "bye"}).json()["message_id"]
    assert rows(client, m)

    client.post("/api/auth/logout", headers=phone_auth(ALICE, ALICE_PHONE))
    assert device_of(client, ALICE, ALICE_PHONE).revoked_at is not None
    assert device_of(client, ALICE, f"web:{alice_sid}").revoked_at is not None

    async def bump_back():  # sign in again (logout bumped the token version)
        async with async_session_factory() as db:
            u = await db.get(User, ALICE)
            return u.token_version
    tv = run(client, bump_back)
    h = {"Authorization": f"Bearer {create_access_token(ALICE, ALICE, tv)}", "X-Flick-Device": ALICE_PHONE}
    assert client.delete("/api/auth/me", headers=h).status_code == 200
    assert device_of(client, ALICE, ALICE_PHONE) is None
    assert rows(client, m) == {}  # the rows of the messages Alice sent went too


def test_push_token_is_kept_on_the_phone_device(client):
    reset(client)
    register(client, BOB, BOB_PHONE)
    client.post("/api/users/fcm-token", headers=phone_auth(BOB, BOB_PHONE), json={"fcm_token": "tok-123"})
    assert device_of(client, BOB, BOB_PHONE).fcm_token == "tok-123"


# ---------------------------------------------------------------------------
# The report, robustness, pruning
# ---------------------------------------------------------------------------
def test_the_report_shows_who_is_behind_and_needs_the_admin_key(client, monkeypatch):
    reset(client)
    register(client, ALICE, ALICE_PHONE)
    register(client, BOB, BOB_PHONE)
    bob_web, _ = link_browser(client, BOB)
    client.get("/api/devices", headers={"Authorization": f"Bearer {bob_web}"})
    import core
    monkeypatch.setattr(core, "ADMIN_API_KEY", "")
    assert client.get("/api/admin/delivery-report").status_code == 404  # no admin key set: hidden
    monkeypatch.setattr(core, "ADMIN_API_KEY", "secret")
    assert client.get("/api/admin/delivery-report", headers={"X-Admin-Key": "wrong"}).status_code == 403
    report = lambda: client.get("/api/admin/delivery-report", headers={"X-Admin-Key": "secret"}).json()
    kind = lambda r, k, f: r["by_device_kind"].get(k, {}).get(f, 0)
    before = report()  # (other tests' messages are in the same test database: compare differences)

    ids = [
        client.post("/api/messages/send", headers=phone_auth(ALICE, ALICE_PHONE), json={"receiver_id": BOB, "content": f"m{i}"}).json()["message_id"]
        for i in range(3)
    ]
    client.post("/api/messages/delivered", headers=phone_auth(BOB, BOB_PHONE), json={"message_ids": ids})
    after = report()

    assert after["deliveries"]["expected"] - before["deliveries"]["expected"] == 6  # Bob's phone + browser, x3
    assert after["deliveries"]["delivered"] - before["deliveries"]["delivered"] == 3
    assert kind(after, "phone", "pending") - kind(before, "phone", "pending") == 0
    assert kind(after, "web", "pending") - kind(before, "web", "pending") == 3
    assert after["delivered_via"].get("push", 0) - before["delivered_via"].get("push", 0) == 3
    assert after["messages_with_a_device_missing_them"] - before["messages_with_a_device_missing_them"] == 3
    bob_web_row = [d for d in after["devices_most_behind"] if d["owner"] == f"…{BOB[-4:]}"]
    assert bob_web_row and bob_web_row[0]["kind"] == "web" and bob_web_row[0]["owed"] == 3
    assert "content" not in json.dumps(after) and BOB not in json.dumps(after)


def test_messages_still_send_if_the_bookkeeping_fails(client, monkeypatch):
    reset(client)
    register(client, BOB, BOB_PHONE)

    async def broken(*a, **k):
        raise RuntimeError("database hiccup")
    monkeypatch.setattr(devices, "active_devices", broken)
    r = client.post("/api/messages/send", headers=phone_auth(ALICE, ALICE_PHONE), json={"receiver_id": BOB, "content": "still works"})
    assert r.status_code == 200 and r.json()["content"] == "still works"


def test_old_bookkeeping_is_pruned(client):
    reset(client)

    async def go():
        async with async_session_factory() as db:
            now = datetime.now(timezone.utc)
            db.add_all([
                MessageDelivery(message_id="old-done", chat_kind="direct", message_ts=now, user_id=BOB, device_id="d1",
                                created_at=now - timedelta(days=40), delivered_at=now - timedelta(days=31)),
                MessageDelivery(message_id="old-never", chat_kind="direct", message_ts=now, user_id=BOB, device_id="d1",
                                created_at=now - timedelta(days=61)),
                MessageDelivery(message_id="recent", chat_kind="direct", message_ts=now, user_id=BOB, device_id="d1",
                                created_at=now - timedelta(days=5)),
            ])
            await db.commit()
            removed = await devices.prune_deliveries(db)
            left = [r.message_id for r in (await db.execute(select(MessageDelivery).where(MessageDelivery.user_id == BOB))).scalars()]
            return removed, left
    removed, left = run(client, go)
    assert removed == 2 and left == ["recent"]


def test_an_instant_confirmation_always_finds_its_row(client):
    """The receiver is online and confirms the moment the message arrives: the "has to get it"
    row was written before the message went out, so the confirmation is never lost."""
    reset(client)
    register(client, ALICE, ALICE_PHONE)
    register(client, BOB, BOB_PHONE)
    start = datetime.now(timezone.utc) - timedelta(minutes=5)

    async def setup():
        async with async_session_factory() as db:
            db.add(Group(id=GROUP, name="G", created_by=ALICE))
            for uid in (ALICE, BOB):
                db.add(GroupMember(group_id=GROUP, user_id=uid, joined_at=start, last_delivered_at=start, last_read_at=start))
            await db.commit()
    run(client, setup)

    a_tok, b_tok = create_access_token(ALICE, ALICE, 0), create_access_token(BOB, BOB, 0)
    with client.websocket_connect(f"/ws/{ALICE}?token={a_tok}&device={ALICE_PHONE}") as wa, \
            client.websocket_connect(f"/ws/{BOB}?token={b_tok}&device={BOB_PHONE}") as wb:
        for target, frame in ((BOB, "chat_message"), (GROUP, "group_message")):
            wa.send_text(json.dumps({"type": "send_message", "data": {"receiver_id": target, "content": f"live {frame}"}}))
            got = next_frame(wb, frame)["data"]
            wb.send_text(json.dumps({"type": "device_ack", "data": {"message_ids": [got["message_id"]]}}))
            ping(wb)
            assert rows(client, got["message_id"]) == {(BOB, "phone", BOB_PHONE): (True, "socket")}
