"""Slow networks: photos and voice notes as plain files (no base64), and gzip-compressed API answers."""

import base64

import pytest
from fastapi.testclient import TestClient

from auth import create_access_token
from database import async_session_factory, init_db
from models import User

ME = "+919400000101"
PEER = "+919400000102"
JPEG = b"\xff\xd8\xff\xe0" + b"photo-bytes" * 50


@pytest.fixture(scope="module")
def client():
    from main import app
    with TestClient(app) as c:
        async def users():
            await init_db()
            async with async_session_factory() as db:
                for uid in (ME, PEER):
                    await db.merge(User(id=uid, phone_number=uid, username=uid))
                await db.commit()
        c.portal.call(users)
        yield c


@pytest.fixture(scope="module")
def auth(client):
    return {"Authorization": f"Bearer {create_access_token(ME, ME, 0)}"}


def test_a_photo_can_be_sent_as_a_file(client, auth):
    r = client.post("/api/upload/image", headers=auth, data={"ext": "jpg"}, files={"file": ("photo.jpg", JPEG, "image/jpeg")})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["size"] == len(JPEG)
    assert body["media_url"].endswith(".jpg")
    stored = client.get(body["media_url"])
    assert stored.status_code == 200
    assert stored.content == JPEG


def test_the_type_comes_from_the_file_when_ext_is_missing(client, auth):
    r = client.post("/api/upload/image", headers=auth, files={"file": ("x", b"\x89PNG....", "image/png")})
    assert r.status_code == 200, r.text
    assert r.json()["media_url"].endswith(".png")


def test_base64_photos_from_older_apps_still_work(client, auth):
    r = client.post("/api/upload/image", headers=auth, json={"image_base64": base64.b64encode(JPEG).decode(), "ext": "jpg"})
    assert r.status_code == 200, r.text
    assert r.json()["size"] == len(JPEG)


def test_bad_photo_uploads_are_refused(client, auth):
    assert client.post("/api/upload/image", headers=auth, files={"file": ("a.exe", b"MZ", "application/x-msdownload")}).status_code == 400
    assert client.post("/api/upload/image", headers=auth, files={"file": ("p.jpg", b"", "image/jpeg")}).status_code == 400
    assert client.post("/api/upload/image", headers=auth, data={"ext": "jpg"}).status_code == 400  # no file
    assert client.post("/api/upload/image", headers=auth, json=["not", "an", "object"]).status_code == 400
    assert client.post("/api/upload/image", files={"file": ("p.jpg", JPEG, "image/jpeg")}).status_code == 401


def test_a_voice_note_can_be_sent_as_a_file(client, auth):
    audio = b"\x00\x00\x00\x18ftypM4A " + b"aac" * 100
    r = client.post("/api/upload/audio", headers=auth, data={"duration": "4"}, files={"file": ("voice.m4a", audio, "audio/mp4")})
    assert r.status_code == 200, r.text
    assert r.json()["duration"] == 4
    assert r.json()["media_url"].endswith(".m4a")
    legacy = client.post("/api/upload/audio", headers=auth, json={"audio_base64": base64.b64encode(audio).decode(), "duration": 4})
    assert legacy.status_code == 200, legacy.text


def test_api_answers_are_gzipped_but_media_is_not(client, auth):
    for i in range(30):
        client.post("/api/messages/send", headers=auth, json={"receiver_id": PEER, "content": f"message number {i} " * 5})
    history = client.get(f"/api/messages/{PEER}?limit=30", headers={**auth, "Accept-Encoding": "gzip"})
    assert history.status_code == 200
    assert history.headers.get("content-encoding") == "gzip"
    assert len(history.json()) >= 30  # still reads as normal JSON

    plain = client.get(f"/api/messages/{PEER}?limit=30", headers={**auth, "Accept-Encoding": "identity"})
    assert plain.headers.get("content-encoding") is None

    url = client.post("/api/upload/image", headers=auth, files={"file": ("p.jpg", JPEG * 20, "image/jpeg")}).json()["media_url"]
    media = client.get(url, headers={"Accept-Encoding": "gzip"})
    assert media.status_code == 200
    assert media.headers.get("content-encoding") is None
