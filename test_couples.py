"""Couples: invite code, joining, what each side sees, updates and unpairing."""

import pytest
from fastapi.testclient import TestClient

from auth import create_access_token
from couple_routes import normalize_code
from database import async_session_factory, init_db
from models import User

A = "+919400000201"
B = "+919400000202"
C = "+919400000203"


@pytest.fixture(scope="module")
def client():
    from main import app
    with TestClient(app) as c:
        async def users():
            await init_db()
            async with async_session_factory() as db:
                for uid, name in ((A, "Meera"), (B, "Arjun"), (C, "Kavya")):
                    await db.merge(User(id=uid, phone_number=uid, username=name))
                await db.commit()
        c.portal.call(users)
        yield c


def h(uid):
    return {"Authorization": f"Bearer {create_access_token(uid, uid, 0)}"}


def test_codes_are_read_loosely():
    assert normalize_code("flk 8q2m") == "FLK-8Q2M"
    assert normalize_code("8Q2M") == "FLK-8Q2M"
    assert normalize_code("FLK-8Q2M") == "FLK-8Q2M"
    assert normalize_code("  ") == ""


def test_pairing_flow(client):
    assert client.get("/api/couple", headers=h(A)).json() == {"status": "none"}

    invite = client.post("/api/couple/invite", headers=h(A)).json()
    assert invite["status"] == "waiting"
    code = invite["code"]
    assert code.startswith("FLK-") and len(code) == 8
    # Asking again gives the same code back
    assert client.post("/api/couple/invite", headers=h(A)).json()["code"] == code

    # My own code can't pair me with myself
    assert client.post("/api/couple/join", headers=h(A), json={"code": code}).status_code == 400
    assert client.post("/api/couple/join", headers=h(B), json={"code": "FLK-ZZZZ"}).status_code == 404
    # A future anniversary is refused
    assert client.post("/api/couple/join", headers=h(B), json={"code": code, "anniversary": "2999-01-01"}).status_code == 400

    joined = client.post(
        "/api/couple/join", headers=h(B), json={"code": code.lower().replace("-", " "), "anniversary": "2025-10-10", "nickname": "Kutty"}
    )
    assert joined.status_code == 200, joined.text
    b_view = joined.json()
    assert b_view["status"] == "paired"
    assert b_view["partner"]["id"] == A and b_view["partner"]["name"] == "Meera"
    assert b_view["anniversary"] == "2025-10-10"
    assert b_view["nickname"] == "Kutty"

    a_view = client.get("/api/couple", headers=h(A)).json()
    assert a_view["status"] == "paired"
    assert a_view["partner"]["id"] == B
    assert a_view["their_nickname"] == "Kutty"
    assert a_view["nickname"] is None

    # The code can't be used again
    assert client.post("/api/couple/join", headers=h(C), json={"code": code}).status_code == 409


def test_each_side_keeps_its_own_nickname_and_mood(client):
    r = client.patch("/api/couple", headers=h(A), json={"nickname": "  Babe  ", "mood": "missing you", "tz": 330})
    assert r.status_code == 200, r.text
    assert r.json()["nickname"] == "Babe" and r.json()["my_mood"] == "missing you"
    b = client.get("/api/couple", headers=h(B)).json()
    assert b["their_nickname"] == "Babe"
    assert b["partner_mood"] == "missing you"
    assert b["partner_tz"] == 330
    assert b["nickname"] == "Kutty"


def test_milestones_are_shared_and_cleaned(client):
    r = client.patch(
        "/api/couple",
        headers=h(B),
        json={"milestones": [{"title": "First date", "date": "2025-10-10"}, {"title": ""}, {"title": "Trip", "date": "bad"}]},
    )
    assert r.status_code == 200, r.text
    items = client.get("/api/couple", headers=h(A)).json()["milestones"]
    assert [m["title"] for m in items] == ["First date", "Trip"]
    assert items[0]["date"] == "2025-10-10" and items[1]["date"] is None
    assert all(m["id"] for m in items)


def test_someone_paired_cannot_join_another_code(client):
    other = client.post("/api/couple/invite", headers=h(C)).json()["code"]
    assert client.post("/api/couple/join", headers=h(A), json={"code": other}).status_code == 409


def test_unpair(client):
    assert client.delete("/api/couple", headers=h(B)).json() == {"status": "none"}
    assert client.get("/api/couple", headers=h(A)).json() == {"status": "none"}
    # Now free to pair again, e.g. with C's waiting code
    r = client.post("/api/couple/join", headers=h(A), json={"code": client.get("/api/couple", headers=h(C)).json()["code"]})
    assert r.status_code == 200 and r.json()["partner"]["id"] == C


def test_needs_login(client):
    assert client.get("/api/couple").status_code == 401
