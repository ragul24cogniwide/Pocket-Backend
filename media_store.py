"""
Uploaded photos and voice notes.

Where files live:
- Cloudflare R2 (object storage, free up to 10 GB) when the R2_* variables are set. The database
  keeps only a small record of each file (name, type, size, owner), not the file itself.
- Otherwise, in the database (the old way). Files uploaded before R2 was set up stay there until
  `python migrate_media_to_r2.py` copies them.
- The server's disk is only a cache: Render wipes it on every redeploy and restart.

GET /uploads/{kind}/{name}   (same URLs as before, so existing messages and both apps keep working)
  1. from the disk cache when it's there
  2. otherwise from the database copy, if there is one (files from before R2)
  3. otherwise from R2 (and the cache is filled again)
  4. 404 when none of them has it; names without a database record never reach R2

R2 settings (Render: Environment; locally: .env):
  R2_ACCOUNT_ID         the id in your Cloudflare dashboard URL (dash.cloudflare.com/<id>/...)
  R2_ACCESS_KEY_ID      from R2 > Manage API tokens > Create API token
  R2_SECRET_ACCESS_KEY  shown once when the token is created
  R2_BUCKET             the bucket name, e.g. flick-media
  R2_ENDPOINT           optional: another S3-compatible endpoint (used by tests)
"""

import asyncio
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
KINDS = {"images", "audio", "documents"}
# Documents are downloaded, never shown by the browser (an uploaded .html or .svg must not run
# as a page of this site); only PDFs open in the browser's viewer
INLINE_DOCUMENT_EXTS = {"pdf"}
NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,120}$")
CONTENT_TYPES = {
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "png": "image/png",
    "gif": "image/gif",
    "webp": "image/webp",
    "heic": "image/heic",
    "m4a": "audio/mp4",
    "pdf": "application/pdf",
}
# Names are random, so a file never changes: browsers may keep it
CACHE_HEADERS = {
    "Cache-Control": "public, max-age=31536000, immutable",
    "X-Content-Type-Options": "nosniff",
}

for _kind in KINDS:
    os.makedirs(os.path.join(UPLOAD_ROOT, _kind), exist_ok=True)


# -----------------------------------------------------------------------------
# Cloudflare R2 (S3-compatible)
# -----------------------------------------------------------------------------
def _env(name: str) -> str:
    return os.getenv(name, "").strip().strip('"\'')


R2_BUCKET = _env("R2_BUCKET")
_r2_endpoint = _env("R2_ENDPOINT") or (
    f"https://{_env('R2_ACCOUNT_ID')}.r2.cloudflarestorage.com" if _env("R2_ACCOUNT_ID") else ""
)
_r2_client = None

if R2_BUCKET and _r2_endpoint and _env("R2_ACCESS_KEY_ID") and _env("R2_SECRET_ACCESS_KEY"):
    try:
        import boto3
        from botocore.config import Config

        _r2_client = boto3.client(
            "s3",
            endpoint_url=_r2_endpoint,
            aws_access_key_id=_env("R2_ACCESS_KEY_ID"),
            aws_secret_access_key=_env("R2_SECRET_ACCESS_KEY"),
            region_name="auto",
            config=Config(
                signature_version="s3v4",
                retries={"max_attempts": 3, "mode": "standard"},
                connect_timeout=10,
                read_timeout=30,
                # Cloudflare's recommended settings for current boto3 versions
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
            ),
        )
        logger.info("Media files are stored in R2 bucket %s", R2_BUCKET)
    except Exception as e:
        logger.error("R2 is configured but could not be set up (%s); files go to the database", e)
        _r2_client = None
else:
    logger.info("R2 is not configured (R2_* variables); media files are stored in the database")


def r2_enabled() -> bool:
    return _r2_client is not None


def _r2_key(kind: str, name: str) -> str:
    return f"{kind}/{name}"


async def r2_put(kind: str, name: str, data: bytes, content_type: str) -> None:
    await asyncio.to_thread(
        _r2_client.put_object,
        Bucket=R2_BUCKET,
        Key=_r2_key(kind, name),
        Body=data,
        ContentType=content_type,
        CacheControl=CACHE_HEADERS["Cache-Control"],
    )


async def r2_get(kind: str, name: str) -> Optional[bytes]:
    """The file's bytes from R2, or None if it isn't there."""
    def fetch() -> Optional[bytes]:
        try:
            obj = _r2_client.get_object(Bucket=R2_BUCKET, Key=_r2_key(kind, name))
        except _r2_client.exceptions.NoSuchKey:
            return None
        return obj["Body"].read()

    return await asyncio.to_thread(fetch)


async def r2_size(kind: str, name: str) -> Optional[int]:
    """Size of the file in R2, or None if it isn't there."""
    def head() -> Optional[int]:
        try:
            return int(_r2_client.head_object(Bucket=R2_BUCKET, Key=_r2_key(kind, name))["ContentLength"])
        except Exception:
            return None

    return await asyncio.to_thread(head)


# -----------------------------------------------------------------------------
# Saving and serving
# -----------------------------------------------------------------------------
def new_name(prefix: str, ext: str) -> str:
    """photo_<128 random bits>.jpg: unguessable, so a link can't be found by trying names."""
    return f"{prefix}_{uuid.uuid4().hex}.{ext}"


def document_name(original: str) -> str:
    """
    doc_<random>_<the original name, made safe>: unguessable like every upload, and a download
    keeps a recognisable name (report.pdf -> doc_1f2e..._report.pdf).
    """
    base = os.path.basename(str(original or "").replace("\\", "/")).strip() or "file"
    stem, dot, ext = base.rpartition(".")
    if not dot:
        stem, ext = base, ""
    clean = lambda t: re.sub(r"[^A-Za-z0-9_.-]+", "_", t).strip("._") or "file"
    ext = re.sub(r"[^A-Za-z0-9]", "", ext)[:10].lower()
    prefix = f"doc_{uuid.uuid4().hex}_"
    room = 120 - len(prefix) - (len(ext) + 1 if ext else 0)
    return prefix + clean(stem)[:room] + (f".{ext}" if ext else "")


def headers_for(kind: str, name: str) -> dict:
    if kind != "documents":
        return CACHE_HEADERS
    ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
    readable = name.split("_", 2)[-1] if name.startswith("doc_") else name
    disposition = "inline" if ext in INLINE_DOCUMENT_EXTS else "attachment"
    return {**CACHE_HEADERS, "Content-Disposition": f'{disposition}; filename="{readable}"'}


def media_type_for(kind: str, name: str) -> str:
    ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
    if kind == "documents" and ext not in INLINE_DOCUMENT_EXTS:
        return "application/octet-stream"
    return CONTENT_TYPES.get(ext, "application/octet-stream")


def _cache_path(kind: str, name: str) -> str:
    return os.path.join(UPLOAD_ROOT, kind, name)


def _write_cache(kind: str, name: str, data: bytes) -> None:
    try:
        with open(_cache_path(kind, name), "wb") as f:
            f.write(data)
    except OSError as e:
        logger.debug("Media cache write failed for %s: %s", name, e)


async def save_media(kind: str, name: str, data: bytes, owner_id: Optional[str]) -> str:
    """Stores the file (R2 if set up, else the database; then the disk cache). Returns its /uploads/... path."""
    content_type = media_type_for(kind, name)
    stored = b""  # an empty database copy means "the file is in R2"
    if r2_enabled():
        try:
            await r2_put(kind, name, data, content_type)
        except Exception as e:
            # Never lose an upload: keep it in the database instead
            logger.error("R2 upload failed for %s (%s); keeping it in the database", name, e)
            stored = data
    else:
        stored = data
    async with async_session_factory() as db:
        db.add(MediaFile(
            name=name, kind=kind, content_type=content_type,
            size=len(data), data=stored, owner_id=owner_id,
        ))
        await db.commit()
    _write_cache(kind, name, data)
    return f"/{UPLOAD_ROOT}/{kind}/{name}"


@router.get("/uploads/{kind}/{name}")
async def serve_media(kind: str, name: str):
    if kind not in KINDS or not NAME_RE.match(name) or name.startswith("."):
        raise HTTPException(status_code=404, detail="Not found")
    media_type = media_type_for(kind, name)
    headers = headers_for(kind, name)
    path = _cache_path(kind, name)
    if os.path.isfile(path):
        return FileResponse(path, media_type=media_type, headers=headers)
    async with async_session_factory() as db:
        row = await db.get(MediaFile, name)
    # Every upload has a database record, so a name without one doesn't exist: answer 404 without
    # asking R2 (made-up names can't run up R2 read charges)
    if row is None or row.kind != kind:
        raise HTTPException(status_code=404, detail="Not found")
    data = row.data or None
    if data is None and r2_enabled():
        try:
            data = await r2_get(kind, name)
        except Exception as e:
            logger.error("R2 read failed for %s: %s", name, e)
            raise HTTPException(status_code=503, detail="Media temporarily unavailable")
    if data is None:
        raise HTTPException(status_code=404, detail="Not found")
    _write_cache(kind, name, data)
    return Response(
        content=data, media_type=media_type if kind == "documents" else (row.content_type or media_type), headers=headers
    )
