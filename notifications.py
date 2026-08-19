"""
Push Notification Service for Pocket Messenger using Firebase Cloud Messaging (FCM).
Delivers high-priority background push notifications when recipient is offline / app is closed.
"""

import os
import logging
from typing import Optional

logger = logging.getLogger("Notifications")

firebase_initialized = False

DEFAULT_FIREBASE_CRED = {
    "type": "service_account",
    "project_id": "pocket-c078e",
    "private_key_id": "6e06639f5998eafe8b1d34718dd95a7c2aa23724",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDMVQqyiTE5cVnk\nNGIZs51Klnin3QqkwLrO7RZqTAPphl/CT4s5Cb55IiccaAYSWHbxeQ8+JoBjxYCY\niGdPATtzxM/RWUr6a4lLzn21Lq5jSooDxs/jkr/7S9/qGNonHFs1mUhoMXLXMhrJ\nFBPywlPLKOalnaG3wpRhnpni1A1tcTXpbB0KT6ZfEPsfI9wuwR8qzXgDJ1zHF+uj\nnY1XFOMZMD5xh2gGguP/u4OFP6XECJ/6MBomslaov6X8mHB88QlcVaQab+N7EeaQ\nWEb+oMUcnVc0XUASGx1IKekOAsUtp/Qh4y2hjbAXJwxyD6N4kMwTksxrSheY7a62\nSdPxxVQXAgMBAAECggEAArXgsmk0ocMq+PpAwX/8QeN1BIGkeJ4Co8DnSo/pWJrK\n6AJfkuk8cqCFxl7hzgtXN6dVuDxMIGQ5azPzFaTIlE02PZcHmYyRF939stInDJY1\ngD4EK2lzR6aX5TeSLY7eUECpxy8gbg0KwJxRObuAkKqca7dMd68Pbpg59TLm1GOH\nzRNWkevTCxqqtq5cOFeQvI5HCZ+IrKLb6vyQjj/1eo5r7kKxxhDkI4MeXA/SJvle\nimfoclM/XspAjpTYicxIOlo6sigWUBAsoXK5QlkdHfAz4jpj/1po0kNlCn90KUIZ\nPHCAkwdmplXU8v48iFccWwva0tHZ7CzofKy02EwTRQKBgQDk2N7VRb1BF8AdBcse\nBwU5zdqsgW8pPl7lv5GRFMm/R/WliEi8yoqc+0TE8FuYMgNrB6oHe+SNSikkFrdI\n3CvwSrQ9PAxhgpkSz0yz4TxlkGl3a2Wvbmvou4K1ZPuDd+pSbansYWzb6HoX9UpJ\n/kaFt8HsWKxF/T14YHNsoJAL3QKBgQDkk4piIzVdEM9rqUnvVz3QsQoQnOF289sw\nUWGTBxmUCpzdadBbBtBiQxmJLAnxc6v89ibdg48YveqLqiQleqG7Eb64xCGQ2pkV\nPQ/OX6QRuSPyGMeFnYztSt16BqOB96v56RRcvZ2QuTVfEhUk+2ipR0Ptri7sV0AS\nWeFq/0EqgwKBgDZubvIDWHR3FbbRffJycQfCstR9JNeGgkPbQOBlNWdN0lvBAqwE\n8NtN2JmPIfodSzrV49dL3JzOmuJ+lLG7zKem3SADfF5lFcunivLuC9OMeclxvgnw\nFbKRhxFmJ3yptQ5ODzCuK5pSvVedfEIFPPjpMDLrFG7BQTG0nz+jIR/xAoGBAN81\nbMdU1oGhLsxVrot4yDaJC+kZKds9WugeMIihQEse6fwVno+lYczy9XbMeJE+gc8u\nmNlr5Usl+mqUpWOsE09YjsRjUtvfe+oPjOXc450jDIUXyY8jQUFgAFNvNDBwqZIM\nGARQbhOrqQDD6b4JfDUCEMWDePL2aO0CtGwGA579AoGBAJv3+uUXCnYKiqzn5z/F\nplFKdSvJUi3tQWDCPea9QrKyqXqDAMppJghk7j3ZyNFs47OBFVibyc4Ws8o7Sao1\now4W8RPPZqxM8fDCfoLzn/d3JT+2qB1xz0NKSjwTF5mFuL84IBhnCqJPYmUlwXK6\nPbmT4IPKNqMBzHSlcwioJ5tY\n-----END PRIVATE KEY-----\n",
    "client_email": "firebase-adminsdk-fbsvc@pocket-c078e.iam.gserviceaccount.com",
    "client_id": "106422140632442178065",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-fbsvc%40pocket-c078e.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com",
}

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

    # 2. Check embedded default certificate
    if not firebase_initialized:
        try:
            cred = credentials.Certificate(DEFAULT_FIREBASE_CRED)
            firebase_admin.initialize_app(cred)
            firebase_initialized = True
            logger.info("Firebase Admin SDK successfully initialized with embedded production service certificate!")
        except Exception as err:
            logger.warning("Failed to init with embedded credentials: %s", err)

    if not firebase_initialized:
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
