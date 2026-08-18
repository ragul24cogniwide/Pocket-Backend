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
    import glob
    import json

    # 1. Check raw JSON env variable
    cred_json_str = os.getenv("FIREBASE_CREDENTIALS_JSON")
    if cred_json_str:
        try:
            cred_dict = json.loads(cred_json_str)
            cred = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(cred)
            firebase_initialized = True
            logger.info("Firebase Admin SDK initialized from FIREBASE_CREDENTIALS_JSON env var")
        except Exception as e:
            logger.warning("Error parsing FIREBASE_CREDENTIALS_JSON: %s", e)

    # 2. Check explicit path env variable or default local JSON file
    if not firebase_initialized:
        possible_files = [
            os.getenv("FIREBASE_CREDENTIALS_PATH", "pocket-c078e-firebase-adminsdk-fbsvc-6e06639f59.json"),
            "pocket-c078e-firebase-adminsdk-fbsvc-6e06639f59.json",
            "firebase-service-account.json",
        ] + glob.glob("*firebase*.json")

        for f_path in possible_files:
            if f_path and os.path.exists(f_path):
                try:
                    cred = credentials.Certificate(f_path)
                    firebase_admin.initialize_app(cred)
                    firebase_initialized = True
                    logger.info("Firebase Admin SDK initialized with credentials from %s", f_path)
                    break
                except Exception as err:
                    logger.warning("Failed to init with %s: %s", f_path, err)

    if not firebase_initialized:
        # Check Application Default Credentials
        try:
            firebase_admin.initialize_app()
            firebase_initialized = True
            logger.info("Firebase Admin SDK initialized with Application Default Credentials")
        except Exception:
            logger.info("Push notifications will run in mock mode until credentials are provided.")
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
