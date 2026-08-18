"""
Push Notification Service for Pocket Messenger using Firebase Cloud Messaging (FCM).
Delivers high-priority background push notifications when recipient is offline / app is closed.
"""

import os
import logging
from typing import Optional

logger = logging.getLogger("Notifications")

firebase_initialized = False
try:
    import firebase_admin
    from firebase_admin import credentials, messaging

    # Look for firebase service account credentials file
    cred_path = os.getenv("FIREBASE_CREDENTIALS_PATH", "firebase-service-account.json")
    if os.path.exists(cred_path):
        cred = credentials.Certificate(cred_path)
        firebase_admin.initialize_app(cred)
        firebase_initialized = True
        logger.info("Firebase Admin SDK initialized with credentials from %s", cred_path)
    else:
        # Check if default application credentials exist
        try:
            firebase_admin.initialize_app()
            firebase_initialized = True
            logger.info("Firebase Admin SDK initialized with Application Default Credentials")
        except Exception:
            logger.info("Firebase credentials not found (%s). Push notifications will run in mock mode until credentials are provided.", cred_path)
except Exception as e:
    logger.warning("Firebase Admin initialization skipped: %s", e)


async def send_push_notification(
    fcm_token: Optional[str],
    sender_name: str,
    content: str,
    data_payload: Optional[dict] = None,
) -> bool:
    """
    Sends a high-priority push notification to an offline user via FCM.
    """
    if not fcm_token:
        return False

    if not firebase_initialized:
        logger.info("[Mock FCM] Push to %s... Title: %s, Body: %s", fcm_token[:12], sender_name, content)
        return True

    try:
        data_strings = {k: str(v) for k, v in (data_payload or {}).items()}
        message = messaging.Message(
            notification=messaging.Notification(
                title=sender_name,
                body=content,
            ),
            data=data_strings,
            token=fcm_token,
            android=messaging.AndroidConfig(
                priority="high",
                notification=messaging.AndroidNotification(
                    sound="default",
                    channel_id="pocket_messages",
                    priority="high",
                    default_sound=True,
                    default_vibrate_timings=True,
                ),
            ),
        )
        response = messaging.send(message)
        logger.info("FCM push sent successfully: %s", response)
        return True
    except Exception as e:
        logger.error("Failed to send FCM push notification: %s", e)
        return False
