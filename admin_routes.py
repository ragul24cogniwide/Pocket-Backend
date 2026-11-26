"""Health checks, admin hooks and the landing page."""

import logging
from datetime import datetime, timezone


from fastapi import Depends

from database import init_db
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
    return {
        "status": "success",
        "message": "All database tables and seed records verified successfully.",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


