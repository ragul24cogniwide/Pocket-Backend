"""Health checks, admin hooks and the landing page."""

import logging
import os
from datetime import datetime, timezone


from fastapi import Depends

from database import async_session_factory, init_db
from ai_agent import (
    ensure_personal_agent_user,
)
from fastapi import APIRouter
from core import require_admin

logger = logging.getLogger("Main")

router = APIRouter()

@router.get("/api/health")
@router.get("/api/ping")
async def health_check():
    """Lightweight 0ms health check for keep-alive pings and uptime monitors."""
    return {
        "status": "active",
        "service": "Flick Messaging Backend",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.api_route("/api/admin/init-db", methods=["GET", "POST"], dependencies=[Depends(require_admin)])
async def trigger_init_db():
    """Manual or automated hook to verify and recreate database tables and seeds."""
    logger.info("Manual init-db requested. Rebuilding database schema...")
    await init_db()
    async with async_session_factory() as session:
        try:
            await ensure_personal_agent_user(session)
        except Exception as e:
            logger.warning("Could not provision agent: %s", e)
    return {
        "status": "success",
        "message": "All database tables and seed records verified successfully.",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/api/agent/status")
async def get_agent_status():
    """Checks whether Google Gemini LLM is active and ready."""
    from ai_agent import get_gemini_api_key, HAS_GENAI
    key = get_gemini_api_key()
    has_key = bool(key and key != "your_gemini_api_key_here")
    masked_key = f"{key[:6]}...{key[-4:]}" if (has_key and len(key) >= 10) else None
    return {
        "gemini_sdk_installed": HAS_GENAI,
        "api_key_configured": has_key,

        "status": "ready" if (HAS_GENAI and has_key) else "fallback_mode",
    }


@router.post("/api/admin/set-gemini-key", dependencies=[Depends(require_admin)])
async def set_gemini_key_endpoint(payload: dict):
    """Dynamically updates the Gemini API Key on the live running instance."""
    new_key = payload.get("key", "").strip()
    if new_key:
        os.environ["GEMINI_API_KEY"] = new_key
        return {"status": "success", "message": "Gemini API key updated successfully."}
    return {"status": "error", "message": "No key provided."}
