"""
Pydantic v2 schemas for authentication, users, and real-time messaging payloads.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
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
    username: Optional[str] = "Flick User"


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
    quote: Optional[str] = "Hey there! I am using Flick."
    is_online: bool
    last_seen: Optional[str] = None


class UpdateProfileRequest(BaseModel):
    username: Optional[str] = None
    avatar_url: Optional[str] = None
    avatar_color: Optional[str] = None
    quote: Optional[str] = None
    fcm_token: Optional[str] = None
    user_id: Optional[str] = None
    phone_number: Optional[str] = None


class UpdateFcmTokenRequest(BaseModel):
    fcm_token: str
    user_id: Optional[str] = None
    phone_number: Optional[str] = None


# Contact & Chat Overview
class ChatContactItem(BaseModel):
    id: str
    phone_number: str
    name: str
    avatar: str
    avatar_color: str
    avatar_url: Optional[str] = None
    quote: Optional[str] = None
    online: bool = False
    last_message: Optional[str] = None
    time: Optional[str] = None
    unread: int = 0
    last_seen: Optional[str] = None
    version: Optional[int] = None


class SyncConversationsResponse(BaseModel):
    cursor: str
    has_more: bool = False
    full_sync: bool = False
    created: List[ChatContactItem] = []
    updated: List[ChatContactItem] = []
    deleted: List[str] = []


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
    metadata: Optional[Any] = None


class CreateStatusRequest(BaseModel):
    type: str = "image" # 'image' | 'video' | 'text'
    media_url: Optional[str] = None
    caption: Optional[str] = None
    bg_gradient: Optional[str] = "#1E293B"


class EditMessageRequest(BaseModel):
    content: str


# Voice Calling Schemas
class CallOfferRequest(BaseModel):
    call_id: str
    caller_id: str
    receiver_id: str
    caller_name: Optional[str] = "Flick User"
    caller_avatar: Optional[str] = "FK"
    caller_avatar_url: Optional[str] = None
    caller_color: Optional[str] = "#FFB800"


class CallAnswerRequest(BaseModel):
    call_id: str
    caller_id: str
    receiver_id: str
    sdp: Optional[str] = None


class CallRejectRequest(BaseModel):
    call_id: str
    caller_id: Optional[str] = None
    receiver_id: Optional[str] = None
    other_user_id: Optional[str] = None
    reason: Optional[str] = "declined"


class CallEndRequest(BaseModel):
    call_id: str
    caller_id: Optional[str] = None
    receiver_id: Optional[str] = None
    other_user_id: Optional[str] = None
    duration: Optional[int] = 0
    reason: Optional[str] = "ended"


# Meeting / Calendar Schemas
class MeetingCreate(BaseModel):
    title: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    start_time: datetime
    end_time: datetime
    timezone: Optional[str] = "UTC"
    location: Optional[str] = None
    meeting_link: Optional[str] = None
    attendees: Optional[List[str]] = None
    status: Optional[str] = "SCHEDULED"


class MeetingUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    timezone: Optional[str] = None
    location: Optional[str] = None
    meeting_link: Optional[str] = None
    attendees: Optional[List[str]] = None
    status: Optional[str] = None


class MeetingResponse(BaseModel):
    id: str
    user_id: str
    title: str
    description: Optional[str] = None
    start_time: str
    end_time: str
    duration_minutes: int = 30
    timezone: str = "UTC"
    location: Optional[str] = None
    meeting_link: Optional[str] = None
    attendees: List[str] = []
    status: str = "SCHEDULED"
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class CheckConflictRequest(BaseModel):
    start_time: datetime
    end_time: datetime
    exclude_meeting_id: Optional[str] = None


# Personal AI Agent Schemas
class AgentChatRequest(BaseModel):
    content: str
    client_tz_offset_minutes: Optional[int] = 0
    conversation_id: Optional[str] = None


class AgentChatResponse(BaseModel):
    reply: str
    action: Optional[str] = None # e.g. "meeting_scheduled", "schedule_list", "meeting_updated", "meeting_cancelled", "conversation_summary", "clarification_needed"
    meeting: Optional[Dict[str, Any]] = None
    meetings: Optional[List[Dict[str, Any]]] = None
    suggested_actions: Optional[List[str]] = None
    user_message_id: Optional[str] = None
    message_id: Optional[str] = None
    timestamp: Optional[str] = None



