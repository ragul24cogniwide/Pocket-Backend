"""Photo, voice-note and document uploads."""

import logging


from fastapi import Depends, File, HTTPException, Request, UploadFile

from auth import (
    get_current_user,
)
from models import User
from fastapi import APIRouter
import base64
import media_store
from core import (
    ALLOWED_IMAGE_EXTS,
    MAX_UPLOAD_BASE64_CHARS,
    MAX_DOCUMENT_BYTES,
    MAX_UPLOAD_BYTES,
    UPLOADS_PER_HOUR,
    enforce_rate_limit,
)

logger = logging.getLogger("Main")

router = APIRouter()

# Content types the image endpoint accepts as a file upload, mapped to the extension it stores
_IMAGE_TYPE_EXTS = {
    "image/jpeg": "jpg",
    "image/jpg": "jpg",
    "image/png": "png",
    "image/gif": "gif",
    "image/webp": "webp",
    "image/heic": "heic",
}


def _is_multipart(request: Request) -> bool:
    return request.headers.get("content-type", "").lower().startswith("multipart/form-data")


async def _read_upload(file, too_big: str) -> bytes:
    """Reads a multipart file part in pieces, refusing it as soon as it passes MAX_UPLOAD_BYTES."""
    if file is None or not hasattr(file, "read"):
        raise HTTPException(status_code=400, detail="A file is required")
    chunks, size = [], 0
    while True:
        chunk = await file.read(256 * 1024)
        if not chunk:
            break
        size += len(chunk)
        if size > MAX_UPLOAD_BYTES:
            raise HTTPException(status_code=413, detail=too_big)
        chunks.append(chunk)
    if size == 0:
        raise HTTPException(status_code=400, detail="The file is empty")
    return b"".join(chunks)


async def _json_body(request: Request) -> dict:
    try:
        payload = await request.json()
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid request body")
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Invalid request body")
    return payload


# -----------------------------------------------------------------------------
# Media & Audio Upload Endpoint
# -----------------------------------------------------------------------------
@router.post("/api/upload/image")
@router.post("/api/upload/media")
async def upload_image(request: Request, current_user: User = Depends(get_current_user)):
    """
    Saves a photo and returns its URL. Two ways to send it:
    - multipart/form-data with a "file" part (and optionally "ext"): the raw file, about a third
      smaller than base64, which matters a lot on slow mobile networks;
    - JSON {"image_base64": ..., "ext": ...}: what older app versions send.
    """
    try:
        if _is_multipart(request):
            form = await request.form()
            upload = form.get("file")
            file_ext = str(form.get("ext") or "").replace(".", "").lower()
            if not file_ext:
                content_type = (getattr(upload, "content_type", "") or "").split(";")[0].strip().lower()
                file_ext = _IMAGE_TYPE_EXTS.get(content_type, "")
            if not file_ext:
                filename = getattr(upload, "filename", "") or ""
                file_ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else "jpg"
            if file_ext == "jpeg":
                file_ext = "jpg"
            if file_ext not in ALLOWED_IMAGE_EXTS:
                raise HTTPException(status_code=400, detail="Unsupported image type")
            await enforce_rate_limit(f"upload:{current_user.id}", UPLOADS_PER_HOUR, 3600, "Too many uploads.")
            image_bytes = await _read_upload(upload, "Image is too large (max 10 MB)")
        else:
            payload = await _json_body(request)
            image_base64 = payload.get("image_base64") or payload.get("base64") or payload.get("media_base64")
            file_ext = str(payload.get("ext") or "jpg").replace(".", "").lower()

            if not image_base64:
                raise HTTPException(status_code=400, detail="Image base64 data required")

            if not isinstance(image_base64, str):
                raise HTTPException(status_code=400, detail="Image base64 data required")
            if "," in image_base64:
                # Extract extension if in data URI (e.g. data:image/png;base64,...)
                if "data:image/" in image_base64:
                    try:
                        header = image_base64.split(";")[0]
                        file_ext = header.split("/")[1].lower()
                    except Exception:
                        pass
                image_base64 = image_base64.split(",", 1)[1]

            if file_ext not in ALLOWED_IMAGE_EXTS:
                raise HTTPException(status_code=400, detail="Unsupported image type")
            # Refuse oversized data before decoding it (decoding would allocate it all first)
            if len(image_base64) > MAX_UPLOAD_BASE64_CHARS:
                raise HTTPException(status_code=413, detail="Image is too large (max 10 MB)")
            await enforce_rate_limit(f"upload:{current_user.id}", UPLOADS_PER_HOUR, 3600, "Too many uploads.")

            image_bytes = base64.b64decode(image_base64)
            if len(image_bytes) > MAX_UPLOAD_BYTES:
                raise HTTPException(status_code=413, detail="Image is too large (max 10 MB)")
        file_id = media_store.new_name("photo", file_ext)
        media_url = await media_store.save_media("images", file_id, image_bytes, current_user.id)
        logger.info("Saved photo (%d bytes) as %s", len(image_bytes), file_id)

        return {
            "status": "success",
            "media_url": media_url,
            "url": media_url,
            "file_name": file_id,
            "size": len(image_bytes),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Image upload failed: %s", e)
        raise HTTPException(status_code=500, detail="Image upload failed")


@router.post("/api/upload/audio")
async def upload_audio(request: Request, current_user: User = Depends(get_current_user)):
    """
    Saves a recorded voice note (.m4a AAC) and returns its URL. Sent either as
    multipart/form-data ("file" plus "duration") or as JSON {"audio_base64", "duration"}.
    """
    try:
        if _is_multipart(request):
            form = await request.form()
            duration = form.get("duration") or 0
            try:
                duration = float(duration)
                duration = int(duration) if duration.is_integer() else duration
            except (TypeError, ValueError):
                duration = 0
            await enforce_rate_limit(f"upload:{current_user.id}", UPLOADS_PER_HOUR, 3600, "Too many uploads.")
            audio_bytes = await _read_upload(form.get("file"), "Voice note is too large (max 10 MB)")
        else:
            payload = await _json_body(request)
            audio_base64 = payload.get("audio_base64") or payload.get("base64")
            duration = payload.get("duration", 0)

            if not audio_base64 or not isinstance(audio_base64, str):
                raise HTTPException(status_code=400, detail="Audio base64 data required")

            if "," in audio_base64:
                audio_base64 = audio_base64.split(",", 1)[1]
            # Refuse oversized data before decoding it (decoding would allocate it all first)
            if len(audio_base64) > MAX_UPLOAD_BASE64_CHARS:
                raise HTTPException(status_code=413, detail="Voice note is too large (max 10 MB)")
            await enforce_rate_limit(f"upload:{current_user.id}", UPLOADS_PER_HOUR, 3600, "Too many uploads.")

            audio_bytes = base64.b64decode(audio_base64)
            if len(audio_bytes) > MAX_UPLOAD_BYTES:
                raise HTTPException(status_code=413, detail="Voice note is too large (max 10 MB)")
        file_id = media_store.new_name("voice", "m4a")
        media_url = await media_store.save_media("audio", file_id, audio_bytes, current_user.id)
        logger.info("Saved voice note (%d bytes, %ss) as %s", len(audio_bytes), duration, file_id)

        return {
            "status": "success",
            "media_url": media_url,
            "url": media_url,
            "duration": duration,
            "file_name": file_id,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Audio upload failed: %s", e)
        raise HTTPException(status_code=500, detail="Audio upload failed")


@router.post("/api/upload/document")
async def upload_document(file: UploadFile = File(...), current_user: User = Depends(get_current_user)):
    """
    Any file (PDF, Word, Excel, zip, ...) up to 20 MB, sent as a normal file upload (multipart).
    Returns its URL plus the name, size and type to show in the chat. Served as a download (see
    media_store.headers_for), so a file can never run as a page of this site.
    """
    await enforce_rate_limit(f"upload:{current_user.id}", UPLOADS_PER_HOUR, 3600, "Too many uploads.")
    chunks, size = [], 0
    while True:
        chunk = await file.read(1024 * 1024)
        if not chunk:
            break
        size += len(chunk)
        if size > MAX_DOCUMENT_BYTES:
            raise HTTPException(status_code=413, detail="Documents can be up to 20 MB")
        chunks.append(chunk)
    if size == 0:
        raise HTTPException(status_code=400, detail="The file is empty")
    original = file.filename or "file"
    name = media_store.document_name(original)
    try:
        media_url = await media_store.save_media("documents", name, b"".join(chunks), current_user.id)
    except Exception as e:
        logger.error("Document upload failed: %s", e)
        raise HTTPException(status_code=500, detail="Document upload failed")
    logger.info("Saved document (%d bytes) as %s", size, name)
    return {
        "status": "success",
        "media_url": media_url,
        "url": media_url,
        "file_name": (original.replace("\\", "/").rsplit("/", 1)[-1] or "file")[:200],
        "size": size,
        "mime_type": (file.content_type or "application/octet-stream")[:100],
    }
