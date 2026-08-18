"""
Pydantic v2 schemas for authentication, users, and real-time messaging payloads.
"""

from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field


# Auth & OTP Schemas
class SendOtpRequest(BaseModel):
    phone_number: str = Field(..., description="Phone number with country code, e.g. +919876543210")


class SendOtpResponse(BaseModel):
    message: str
    phone_number: str
    # In development / testing mode, the server returns the generated OTP to easily display in the UI!
    dev_otp: Optional[str] = None
    expires_in_seconds: int


class VerifyOtpRequest(BaseModel):
    phone_number: str
    otp_code: str
    username: Optional[str] = "Pocket User"


class AuthResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: dict


# User Profile
class UserProfileResponse(BaseModel):
    id: str
    phone_number: str
    username: str
    avatar_color: str
    avatar_url: Optional[str] = None
    quote: Optional[str] = "Hey there! I am using Pocket."
    is_online: bool
    last_seen: Optional[str] = None


class UpdateProfileRequest(BaseModel):
    username: Optional[str] = None
    avatar_url: Optional[str] = None
    avatar_color: Optional[str] = None
    quote: Optional[str] = None
    fcm_token: Optional[str] = None


class UpdateFcmTokenRequest(BaseModel):
    fcm_token: str


# Contact & Chat Overview
class ChatContactItem(BaseModel):
    id: str
    phone_number: str
    name: str
    avatar: str
    avatar_color: str
    avatar_url: Optional[str] = None
    quote: Optional[str] = None
    online: bool
    last_message: Optional[str] = None
    time: Optional[str] = None
    unread: int = 0
    last_seen: Optional[str] = None


class SyncContactsRequest(BaseModel):
    phone_numbers: List[str]


class SyncContactsResponse(BaseModel):
    registered_users: List[ChatContactItem]
    total_synced: int


class SendMessageRequest(BaseModel):
    receiver_id: str
    content: str
    temp_msg_id: Optional[str] = None
    reply_to: Optional[Any] = None
