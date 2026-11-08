"""Sharing documents (any file up to 20 MB), like WhatsApp."""

import pytest
from fastapi.testclient import TestClient

from auth import create_access_token
from core import MAX_DOCUMENT_BYTES
from database import async_session_factory, init_db
from models import User

ME = "+919400000001"
PEER = "+919400000002"


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


def _upload(client, auth, name, data, mime="application/octet-stream"):
    return client.post("/api/upload/document", headers=auth, files={"file": (name, data, mime)})


def test_upload_and_download_a_pdf(client, auth):
    data = b"%PDF-1.4 fake pdf " * 1000
    r = _upload(client, auth, "Trip plan.pdf", data, "application/pdf")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["file_name"] == "Trip plan.pdf" and body["size"] == len(data) and body["mime_type"] == "application/pdf"
    assert body["media_url"].startswith("/uploads/documents/doc_") and body["media_url"].endswith("_Trip_plan.pdf")

    got = client.get(body["media_url"])
    assert got.status_code == 200 and got.content == data
    assert got.headers["content-type"] == "application/pdf"
    assert got.headers["content-disposition"] == 'inline; filename="Trip_plan.pdf"'


def test_other_files_download_and_never_run_as_a_page(client, auth):
    page = b"<html><script>alert(document.cookie)</script></html>"
    url = _upload(client, auth, "evil.html", page, "text/html").json()["media_url"]
    got = client.get(url)
    assert got.headers["content-type"] == "application/octet-stream"
    assert got.headers["content-disposition"].startswith("attachment;")
    assert got.headers["x-content-type-options"] == "nosniff"

    docx = _upload(client, auth, "../../Report (final).docx", b"PK\x03\x04 word", "application/vnd.openxmlformats")
    assert docx.json()["media_url"].endswith("_Report_final.docx")


def test_exactly_20_mb_is_fine_and_more_is_refused(client, auth):
    ok = _upload(client, auth, "big.zip", b"z" * MAX_DOCUMENT_BYTES)
    assert ok.status_code == 200, ok.text
    assert ok.json()["size"] == MAX_DOCUMENT_BYTES
    too_big = _upload(client, auth, "bigger.zip", b"z" * (MAX_DOCUMENT_BYTES + 1))
    assert too_big.status_code == 413
    assert "20 MB" in too_big.json()["detail"]


def test_empty_files_and_strangers_are_refused(client, auth):
    assert _upload(client, auth, "empty.txt", b"").status_code == 400
    assert client.post("/api/upload/document", files={"file": ("a.txt", b"x", "text/plain")}).status_code == 401
    # Other requests keep their 15 MB limit
    r = client.post("/api/upload/image", headers=auth, content=b"x" * (16 * 1024 * 1024))
    assert r.status_code == 413


def test_a_document_message_is_sent_like_any_message(client, auth):
    url = _upload(client, auth, "notes.txt", b"hello", "text/plain").json()["media_url"]
    meta = {"type": "document", "media_url": url, "file_name": "notes.txt", "size": 5, "mime_type": "text/plain"}
    sent = client.post("/api/messages/send", headers=auth, json={"receiver_id": PEER, "content": "📄 notes.txt", "metadata": meta}).json()
    assert sent["metadata"] == meta
    history = client.get(f"/api/messages/{ME}", headers={"Authorization": f"Bearer {create_access_token(PEER, PEER, 0)}"}).json()
    assert any(m["metadata"] == meta and m["content"] == "📄 notes.txt" for m in history)
