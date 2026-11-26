"""Meetings (calendar): create, list, overlap warnings and the conflict check."""

import pytest
from fastapi.testclient import TestClient

from auth import create_access_token
from database import async_session_factory, init_db
from models import User

ME = "+919400000301"
OTHER = "+919400000302"


@pytest.fixture(scope="module")
def client():
    from main import app
    with TestClient(app) as c:
        async def users():
            await init_db()
            async with async_session_factory() as db:
                for uid in (ME, OTHER):
                    await db.merge(User(id=uid, phone_number=uid, username=uid))
                await db.commit()
        c.portal.call(users)
        yield c


def h(uid):
    return {"Authorization": f"Bearer {create_access_token(uid, uid, 0)}"}


def meeting(title, start, end):
    return {"title": title, "start_time": f"2026-12-01T{start}:00+00:00", "end_time": f"2026-12-01T{end}:00+00:00"}


def test_overlapping_meetings_are_flagged(client):
    first = client.post("/api/meetings", headers=h(ME), json=meeting("Dinner date", "18:00", "19:00"))
    assert first.status_code == 201, first.text
    assert first.json()["has_conflict"] is False

    clash = client.post("/api/meetings", headers=h(ME), json=meeting("Movie", "18:30", "20:00"))
    assert clash.json()["has_conflict"] is True
    assert [m["title"] for m in clash.json()["conflicting_meetings"]] == ["Dinner date"]

    # Back to back is fine
    after = client.post("/api/meetings", headers=h(ME), json=meeting("Walk", "20:00", "20:30"))
    assert after.json()["has_conflict"] is False


def test_conflict_check_and_privacy(client):
    r = client.post(
        "/api/meetings/check-conflicts",
        headers=h(ME),
        json={"start_time": "2026-12-01T18:15:00+00:00", "end_time": "2026-12-01T18:45:00+00:00"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["has_conflict"] is True
    assert {m["title"] for m in body["conflicts"]} == {"Dinner date", "Movie"}

    # Someone else's calendar never shows mine
    other = client.post(
        "/api/meetings/check-conflicts",
        headers=h(OTHER),
        json={"start_time": "2026-12-01T18:15:00+00:00", "end_time": "2026-12-01T18:45:00+00:00"},
    )
    assert other.json() == {"has_conflict": False, "conflicts": []}


def test_the_ai_agent_is_gone(client):
    # Its routes no longer exist, and nothing can be sent to its old account
    assert client.post("/api/agent/chat", headers=h(ME), json={"content": "hi"}).status_code in (404, 405)
    r = client.post("/api/messages/send", headers=h(ME), json={"receiver_id": "ai_assistant", "content": "hi"})
    assert r.status_code == 410, r.text
