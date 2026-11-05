"""
Push Notification Service for Pocket Messenger using Firebase Cloud Messaging (FCM).
Delivers high-priority background push notifications when recipient is offline / app is closed.
"""

import glob
import json
import os
import logging
import asyncio
from datetime import timedelta
from typing import Optional

logger = logging.getLogger("Notifications")

firebase_initialized = False

BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))


def _credential_files() -> list:
    """Service-account files to try, in order. None of them belong in git (see .gitignore)."""
    paths = []
    for var in ("FIREBASE_CREDENTIALS_PATH", "GOOGLE_APPLICATION_CREDENTIALS"):
        value = os.getenv(var, "").strip().strip("\"'")
        if value:
            paths.append(value if os.path.isabs(value) else os.path.join(BACKEND_DIR, value))
    # Render "Secret Files" are mounted in /etc/secrets; locally the file sits next to this one
    paths += sorted(glob.glob("/etc/secrets/*firebase*.json"))
    paths += sorted(glob.glob(os.path.join(BACKEND_DIR, "*firebase-adminsdk*.json")))
    return list(dict.fromkeys(paths))


# The service-account key is never written in the source code. It is loaded, in this order, from:
#   1. FIREBASE_CREDENTIALS_JSON   the whole JSON file as one env var (Render: Environment)
#   2. a file: FIREBASE_CREDENTIALS_PATH / GOOGLE_APPLICATION_CREDENTIALS, a Render Secret File in
#      /etc/secrets, or the *firebase-adminsdk*.json next to this file (local development)
#   3. Application Default Credentials (Google Cloud hosting)
try:
    # FIREBASE_DISABLED=true (local test servers): never contact Firebase, log pushes instead
    if os.getenv("FIREBASE_DISABLED", "").strip().lower() == "true":
        raise RuntimeError("disabled by FIREBASE_DISABLED")
    import firebase_admin
    from firebase_admin import credentials, messaging

    cred_json_str = os.getenv("FIREBASE_CREDENTIALS_JSON", "").strip()
    if cred_json_str:
        try:
            firebase_admin.initialize_app(credentials.Certificate(json.loads(cred_json_str)))
            firebase_initialized = True
            logger.info("Firebase Admin SDK initialized from FIREBASE_CREDENTIALS_JSON")
        except Exception as e:
            logger.warning("FIREBASE_CREDENTIALS_JSON is set but could not be used: %s", e)

    if not firebase_initialized:
        for cred_path in _credential_files():
            if not os.path.isfile(cred_path):
                continue
            try:
                firebase_admin.initialize_app(credentials.Certificate(cred_path))
                firebase_initialized = True
                logger.info("Firebase Admin SDK initialized from %s", os.path.basename(cred_path))
                break
            except Exception as e:
                logger.warning("Could not use Firebase credentials file %s: %s", os.path.basename(cred_path), e)

    if not firebase_initialized:
        try:
            firebase_admin.initialize_app()
            firebase_initialized = True
            logger.info("Firebase Admin SDK initialized with Application Default Credentials")
        except Exception:
            logger.error(
                "Push notifications are OFF: no Firebase credentials found. Set FIREBASE_CREDENTIALS_JSON "
                "to the contents of the service-account JSON file (Render: Environment)."
            )
except Exception as e:
    logger.warning("Firebase Admin initialization skipped: %s", e)


# FCM refuses messages over 4 KB in total. Pictures sent as data: URIs (profile photos) or long
# texts must never be put in a push: they made every call alert fail with "Message is too large".
MAX_PUSH_FIELD_CHARS = 512
# Fields allowed to be longer than MAX_PUSH_FIELD_CHARS: a chat message's metadata (JSON, e.g. a
# photo's URL and size) so the phone can show photo messages from the push; main.py only
# includes it when it's under 1500 characters
PUSH_FIELD_LIMITS = {"metadata": 1500}
MAX_PUSH_DATA_CHARS = 2500
MAX_PUSH_BODY_CHARS = 600
MAX_PUSH_TITLE_CHARS = 120


def fit_push_data(data_payload: Optional[dict]) -> dict:
    """String-only push data that fits FCM's size limit: oversized fields are left out."""
    data = {}
    for key, value in (data_payload or {}).items():
        if value is None:
            continue
        text = str(value)
        if len(text) > PUSH_FIELD_LIMITS.get(str(key), MAX_PUSH_FIELD_CHARS) or text.startswith("data:"):
            logger.info("[Push] Left out oversized field %s (%d chars)", key, len(text))
            continue
        data[str(key)] = text
    # Still too big in total: drop the longest fields first (ids and types are short and stay)
    while data and sum(len(k) + len(v) for k, v in data.items()) > MAX_PUSH_DATA_CHARS:
        longest = max(data, key=lambda k: len(data[k]))
        logger.info("[Push] Left out field %s to fit the size limit", longest)
        data.pop(longest)
    return data


async def send_push_notification(
    fcm_token: Optional[str],
    sender_name: str,
    content: str,
    data_payload: Optional[dict] = None,
    silent: bool = False,
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
        data_strings = fit_push_data(data_payload)
        content = (content or "")[:MAX_PUSH_BODY_CHARS]
        sender_name = (sender_name or "Flick")[:MAX_PUSH_TITLE_CHARS]
        is_call = data_strings.get("type") == "incoming_call"
        channel_id = "pocket_messages"
        is_video_call = is_call and data_strings.get("video") == "1"
        display_title = (
            (f"📹 Incoming video call: {sender_name}" if is_video_call else f"📞 Incoming voice call: {sender_name}")
            if is_call and not sender_name.startswith(("📞", "📹"))
            else sender_name
        )

        message = messaging.Message(
            notification=messaging.Notification(
                title=display_title,
                body=content,
            ),
            data=data_strings,
            token=fcm_token,
            android=messaging.AndroidConfig(
                priority="high",
                ttl=timedelta(seconds=60) if is_call else timedelta(days=1),
                notification=messaging.AndroidNotification(
                    sound=None if silent else "default",
                    channel_id=channel_id,
                    priority="max" if is_call else "high",
                    default_sound=not silent,
                    default_vibrate_timings=True,
                    visibility="public" if is_call else "private",
                    sticky=True if is_call else False,
                ),
            ),
            apns=messaging.APNSConfig(
                headers={
                    "apns-priority": "10",
                    "apns-push-type": "alert",
                },
                payload=messaging.APNSPayload(
                    aps=messaging.Aps(
                        sound=None if silent else "default",
                        badge=1,
                        content_available=True,
                    )
                ),
            ),
        )
        response = await asyncio.to_thread(messaging.send, message)
        logger.info("FCM push sent successfully: %s", response)
        return True
    except Exception as e:
        logger.error("Failed to send FCM push notification: %s", e)
        return False

