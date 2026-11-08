"""Photo and voice-note uploads."""

import logging


from fastapi import Depends, HTTPException

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
    MAX_UPLOAD_BYTES,
    UPLOADS_PER_HOUR,
    enforce_rate_limit,
)

logger = logging.getLogger("Main")

router = APIRouter()

# -----------------------------------------------------------------------------
# Media & Audio Upload Endpoint
# -----------------------------------------------------------------------------
@router.post("/api/upload/image")
@router.post("/api/upload/media")
async def upload_image(payload: dict, current_user: User = Depends(get_current_user)):
    """
    Saves image / photo (.jpg/.png) from base64 data to cloud server and returns streaming URL.
    """
    try:
        image_base64 = payload.get("image_base64") or payload.get("base64") or payload.get("media_base64")
        file_ext = str(payload.get("ext") or "jpg").replace(".", "").lower()

        if not image_base64:
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
        if not isinstance(image_base64, str) or len(image_base64) > MAX_UPLOAD_BASE64_CHARS:
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
async def upload_audio(payload: dict, current_user: User = Depends(get_current_user)):
    """
    Saves recorded voice audio (.m4a AAC) to cloud server and returns streaming URL.
    """
    try:
        audio_base64 = payload.get("audio_base64") or payload.get("base64")
        duration = payload.get("duration", 0)

        if not audio_base64:
            raise HTTPException(status_code=400, detail="Audio base64 data required")

        if "," in audio_base64:
            audio_base64 = audio_base64.split(",", 1)[1]
        # Refuse oversized data before decoding it (decoding would allocate it all first)
        if not isinstance(audio_base64, str) or len(audio_base64) > MAX_UPLOAD_BASE64_CHARS:
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
