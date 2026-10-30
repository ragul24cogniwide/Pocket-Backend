"""
Uploaded photos and voice notes.

Files used to live only on the server's disk, which Render wipes on every redeploy and restart,
so older photos turned into broken images. Now every upload is stored in the database; the
disk copy is just a cache.

GET /uploads/{kind}/{name}   (same URLs as before, so existing messages keep working)
  1. from the disk cache when it's there
  2. otherwise from the database (and the cache is filled again)
  3. 404 for files uploaded before this change that the disk has since lost
"""

import logging
import os
import re
import uuid
from typing import Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, Response

from database import async_session_factory
from models import MediaFile

logger = logging.getLogger("Media")

router = APIRouter(tags=["media"])

UPLOAD_ROOT = "uploads"
KINDS = {"images", "audio"}
NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,120}$")
CONTENT_TYPES = {
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "png": "image/png",
    "gif": "image/gif",
    "webp": "image/webp",
    "heic": "image/heic",
    "m4a": "audio/mp4",
}
# Names are random, so a file never changes: browsers may keep it
CACHE_HEADERS = {
    "Cache-Control": "public, max-age=31536000, immutable",
    "X-Content-Type-Options": "nosniff",
}

for _kind in KINDS:
    os.makedirs(os.path.join(UPLOAD_ROOT, _kind), exist_ok=True)


def new_name(prefix: str, ext: str) -> str:
    """photo_<128 random bits>.jpg: unguessable, so a link can't be found by trying names."""
    return f"{prefix}_{uuid.uuid4().hex}.{ext}"


def _cache_path(kind: str, name: str) -> str:
    return os.path.join(UPLOAD_ROOT, kind, name)


def _write_cache(kind: str, name: str, data: bytes) -> None:
    try:
        with open(_cache_path(kind, name), "wb") as f:
            f.write(data)
    except OSError as e:
        logger.debug("Media cache write failed for %s: %s", name, e)


async def save_media(kind: str, name: str, data: bytes, owner_id: Optional[str]) -> str:
    """Stores the file (database first, then the disk cache). Returns its /uploads/... path."""
    ext = name.rsplit(".", 1)[-1].lower()
    async with async_session_factory() as db:
        db.add(MediaFile(
            name=name, kind=kind, content_type=CONTENT_TYPES.get(ext, "application/octet-stream"),
            size=len(data), data=data, owner_id=owner_id,
        ))
        await db.commit()
    _write_cache(kind, name, data)
    return f"/{UPLOAD_ROOT}/{kind}/{name}"


@router.get("/uploads/{kind}/{name}")
async def serve_media(kind: str, name: str):
    if kind not in KINDS or not NAME_RE.match(name) or name.startswith("."):
        raise HTTPException(status_code=404, detail="Not found")
    ext = name.rsplit(".", 1)[-1].lower()
    media_type = CONTENT_TYPES.get(ext, "application/octet-stream")
    path = _cache_path(kind, name)
    if os.path.isfile(path):
        return FileResponse(path, media_type=media_type, headers=CACHE_HEADERS)
    async with async_session_factory() as db:
        row = await db.get(MediaFile, name)
    if row is None or row.kind != kind:
        raise HTTPException(status_code=404, detail="Not found")
    _write_cache(kind, name, row.data)
    return Response(content=row.data, media_type=row.content_type or media_type, headers=CACHE_HEADERS)
