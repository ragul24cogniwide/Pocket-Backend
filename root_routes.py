"""The landing page ("/")."""

import logging
import os


from fastapi.responses import FileResponse

from fastapi import APIRouter

logger = logging.getLogger("Main")

router = APIRouter()

# -----------------------------------------------------------------------------
# Web & Testbed Endpoint
# -----------------------------------------------------------------------------
@router.get("/")
async def root():
    """Serves the test interface for dual-user simulation."""
    # The dual-user test page renders message text as HTML, so it's never served in production
    if os.getenv("SERVE_TEST_CLIENT", "false").lower() == "true" and os.path.exists("test_client.html"):
        return FileResponse("test_client.html")
    return {"message": "Flick Real-Time Messaging API is active"}
