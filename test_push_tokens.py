"""Dead push tokens (another Firebase project, or uninstalled app) are forgotten, not retried forever."""

import pytest
from fastapi.testclient import TestClient

import notifications
from database import async_session_factory, init_db
from models import User

ME = "+919400000401"


@pytest.fixture(scope="module")
def client():
    from main import app
    with TestClient(app) as c:
        async def users():
            await init_db()
            async with async_session_factory() as db:
                await db.merge(User(id=ME, phone_number=ME, username="Meera", fcm_token="old-project-token"))
                await db.commit()
        c.portal.call(users)
        yield c


def token_of(client):
    async def read():
        async with async_session_factory() as db:
            return (await db.get(User, ME)).fcm_token
    return client.portal.call(read)


def test_a_token_from_another_firebase_project_is_removed(client, monkeypatch):
    if not hasattr(notifications, "messaging"):
        pytest.skip("firebase_admin not installed")

    def refuse(_message):
        raise Exception("SenderId mismatch")

    monkeypatch.setattr(notifications, "firebase_initialized", True)
    monkeypatch.setattr(notifications.messaging, "send", refuse)
    sent = client.portal.call(lambda: notifications.send_push_notification("old-project-token", "Arjun", "hi"))
    assert sent is False
    assert token_of(client) is None


def test_other_errors_keep_the_token(client, monkeypatch):
    async def reset():
        async with async_session_factory() as db:
            (await db.get(User, ME)).fcm_token = "good-token"
            await db.commit()
    client.portal.call(reset)

    def flaky(_message):
        raise Exception("Internal error, try again")

    monkeypatch.setattr(notifications, "firebase_initialized", True)
    monkeypatch.setattr(notifications.messaging, "send", flaky)
    assert client.portal.call(lambda: notifications.send_push_notification("good-token", "Arjun", "hi")) is False
    assert token_of(client) == "good-token"
