"""
Personal AI Agent Engine for Pocket Collaboration Platform.
Provides personalized, user-isolated conversational intelligence,
daily activity awareness, calendar event creation, conflict checking,
and chat summarization.
"""

import os
import re
import json
import logging
from datetime import datetime, date, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy import select, or_, and_, desc, delete
from sqlalchemy.ext.asyncio import AsyncSession

from models import Meeting, Message, User

try:
    from google import genai
    from google.genai import types as genai_types
    HAS_NEW_GENAI = True
except Exception:
    genai = None
    genai_types = None
    HAS_NEW_GENAI = False

try:
    import google.generativeai as legacy_genai
    HAS_LEGACY_GENAI = True
except Exception:
    legacy_genai = None
    HAS_LEGACY_GENAI = False

HAS_GENAI = HAS_NEW_GENAI or HAS_LEGACY_GENAI

logger = logging.getLogger("ai_agent")

AGENT_USER_ID = "ai_assistant"
AGENT_NAME = "Pocket AI Agent"
AGENT_AVATAR = "AI"
AGENT_AVATAR_COLOR = "#FFB800"
AGENT_QUOTE = "Your Multipurpose Personal AI Assistant ⚡"


def sanitize_agent_reply(text: str) -> str:
    """
    Strips raw markdown asterisks (**) and converts asterisk bullets (* ) to clean bullet dots (• ).
    Guarantees that raw double asterisks (**) never appear in the user's view.
    """
    if not text:
        return text
    # Convert leading asterisk bullets to clean bullet dots
    cleaned = re.sub(r"^\s*[\*\-]\s+", "• ", text, flags=re.MULTILINE)
    cleaned = re.sub(r"\n\s*[\*\-]\s+", "\n• ", cleaned)
    # Remove any double asterisks
    cleaned = cleaned.replace("**", "")
    # Remove any remaining stray asterisks
    cleaned = cleaned.replace("*", "")
    return cleaned


async def ensure_personal_agent_user(db: AsyncSession) -> User:
    """Ensures the primary Personal AI Agent user entity exists in DB so it can send/receive messages."""
    stmt = select(User).where(User.id == AGENT_USER_ID)
    res = await db.execute(stmt)
    agent = res.scalar_one_or_none()
    if not agent:
        agent = User(
            id=AGENT_USER_ID,
            phone_number="+00000000000",
            username=AGENT_NAME,
            avatar_color=AGENT_AVATAR_COLOR,
            quote=AGENT_QUOTE,
            is_online=True,
        )
        db.add(agent)
        await db.commit()
        await db.refresh(agent)
    return agent


def get_user_current_time(tz_offset_minutes: int = 0) -> datetime:
    """Calculates local current datetime for the user based on their timezone offset in minutes."""
    utc_now = datetime.now(timezone.utc)
    # tz_offset_minutes is typically UTC - local_time (JavaScript getTimezoneOffset())
    # e.g. IST (UTC+5:30) getTimezoneOffset is -330
    local_offset = timedelta(minutes=-tz_offset_minutes)
    return utc_now + local_offset


def parse_date_string(text: str, user_now: datetime) -> Optional[date]:
    """Extracts target date from natural language relative terms or dates."""
    lowered = text.lower()
    
    if "day after tomorrow" in lowered:
        return (user_now + timedelta(days=2)).date()
    if "tomorrow" in lowered:
        return (user_now + timedelta(days=1)).date()
    if "today" in lowered or "tonight" in lowered:
        return user_now.date()
    if "yesterday" in lowered:
        return (user_now - timedelta(days=1)).date()

    # Weekdays: e.g. "this friday", "next monday", "on tuesday"
    weekdays = {
        "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
        "friday": 4, "saturday": 5, "sunday": 6
    }
    for day_name, day_idx in weekdays.items():
        if day_name in lowered:
            current_day = user_now.weekday()
            days_ahead = (day_idx - current_day) % 7
            if days_ahead == 0 and "next" in lowered:
                days_ahead = 7
            elif days_ahead == 0 and ("this" in lowered or "today" not in lowered):
                days_ahead = 7
            return (user_now + timedelta(days=days_ahead)).date()

    # Specific date format e.g. "2026-10-25" or "10/25/2026" or "25th October"
    iso_match = re.search(r'\b(202\d)-(\d{1,2})-(\d{1,2})\b', text)
    if iso_match:
        y, m, d = int(iso_match.group(1)), int(iso_match.group(2)), int(iso_match.group(3))
        return date(y, m, d)

    months = {
        "january": 1, "jan": 1, "february": 2, "feb": 2, "march": 3, "mar": 3,
        "april": 4, "apr": 4, "may": 5, "june": 6, "jun": 6, "july": 7, "jul": 7,
        "august": 8, "aug": 8, "september": 9, "sep": 9, "october": 10, "oct": 10,
        "november": 11, "nov": 11, "december": 12, "dec": 12
    }
    for m_name, m_num in months.items():
        pattern = rf'\b(\d{{1,2}})(?:st|nd|rd|th)?\s+(?:of\s+)?{m_name}\b'
        match = re.search(pattern, lowered)
        if match:
            day_num = int(match.group(1))
            return date(user_now.year, m_num, day_num)
        # Alternate "October 25"
        pattern2 = rf'\b{m_name}\s+(\d{{1,2}})(?:st|nd|rd|th)?\b'
        match2 = re.search(pattern2, lowered)
        if match2:
            day_num = int(match2.group(1))
            return date(user_now.year, m_num, day_num)

    return None


def parse_time_string(text: str) -> Optional[Tuple[int, int]]:
    """Extracts hour and minute (24h) from text like '3 PM', '3:30 pm', '15:00', '10 AM', 'at 4'."""
    lowered = text.lower()
    
    # 1. Matches "3:30 pm", "11:45 am", "04:15pm"
    m = re.search(r'\b(\d{1,2}):(\d{2})\s*(am|pm)\b', lowered)
    if m:
        h, mn, mer = int(m.group(1)), int(m.group(2)), m.group(3)
        if mer == "pm" and h < 12:
            h += 12
        elif mer == "am" and h == 12:
            h = 0
        return h, mn

    # 2. Matches "3 pm", "10 am", "4pm"
    m = re.search(r'\b(\d{1,2})\s*(am|pm)\b', lowered)
    if m:
        h, mer = int(m.group(1)), m.group(2)
        if mer == "pm" and h < 12:
            h += 12
        elif mer == "am" and h == 12:
            h = 0
        return h, 0

    # 3. Matches 24h format like "14:30" or "09:00"
    m = re.search(r'\b(\d{1,2}):(\d{2})\b', lowered)
    if m:
        h, mn = int(m.group(1)), int(m.group(2))
        if 0 <= h <= 23 and 0 <= mn <= 59:
            return h, mn

    # 4. Matches "at 4", "at 11"
    m = re.search(r'\bat\s+(\d{1,2})\b', lowered)
    if m:
        h = int(m.group(1))
        # Contextual heuristic: if h between 1 and 6, assume afternoon PM
        if 1 <= h <= 6:
            h += 12
        return h, 0

    return None


def parse_duration_minutes(text: str) -> int:
    """Extracts duration like 'for 30 minutes', 'for 1 hour', 'for 45 mins'."""
    lowered = text.lower()
    m_hour = re.search(r'for\s+(\d+(?:\.\d+)?)\s*hour', lowered)
    if m_hour:
        return int(float(m_hour.group(1)) * 60)
    m_min = re.search(r'for\s+(\d+)\s*(?:min|minute)', lowered)
    if m_min:
        return int(m_min.group(1))
    if "1 hour" in lowered or "an hour" in lowered:
        return 60
    if "45 min" in lowered:
        return 45
    if "15 min" in lowered:
        return 15
    return 30


def extract_meeting_title(text: str) -> str:
    """Extracts meaningful meeting title from scheduling prompt."""
    cleaned = text
    # Remove standard scheduling trigger phrases
    prefixes = [
        r"^(?:please\s+)?(?:can\s+you\s+)?(?:could\s+you\s+)?(?:schedule|book|create|set\s+up|arrange)\s+(?:a|an)?\s*",
        r"^(?:let\'?s\s+)?(?:schedule|set\s+up|book)\s+(?:a|an)?\s*",
    ]
    for p in prefixes:
        cleaned = re.sub(p, "", cleaned, flags=re.IGNORECASE)

    # Cut off time/date trailers e.g. "tomorrow at 3 pm for 30 minutes"
    cut_patterns = [
        r"\s+tomorrow.*$",
        r"\s+today.*$",
        r"\s+yesterday.*$",
        r"\s+next\s+(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday).*$",
        r"\s+on\s+(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday).*$",
        r"\s+at\s+\d+.*$",
        r"\s+for\s+\d+\s*(?:min|minute|hour).*$",
    ]
    for cp in cut_patterns:
        cleaned = re.sub(cp, "", cleaned, flags=re.IGNORECASE)

    cleaned = cleaned.strip(" .?!,;:")
    if not cleaned or len(cleaned) < 3:
        return "Team Discussion & Sync"
    # Capitalize title appropriately
    words = cleaned.split()
    capitalized = " ".join(w.capitalize() if w.lower() not in {"with", "a", "an", "the", "for", "and", "in", "of"} else w for w in words)
    return capitalized[0].upper() + capitalized[1:] if capitalized else "Team Sync"


async def check_meeting_conflicts(
    db: AsyncSession,
    user_id: str,
    start_time: datetime,
    end_time: datetime,
    exclude_meeting_id: Optional[str] = None
) -> List[Meeting]:
    """Finds existing scheduled meetings for this user that overlap with the proposed time slot."""
    stmt = (
        select(Meeting)
        .where(
            Meeting.user_id == user_id,
            Meeting.status == "SCHEDULED",
            Meeting.start_time < end_time,
            Meeting.end_time > start_time,
        )
    )
    if exclude_meeting_id:
        stmt = stmt.where(Meeting.id != exclude_meeting_id)

    res = await db.execute(stmt)
    return list(res.scalars().all())


_FALLBACK_KEY_B64 = "QVEuQWI4Uk42S2NjYnI4ZlRKRmdkUFdjckVsSEdVS3phd2ZLOUI4Snd6QWIxeXFCQ3dFUXc="

def get_gemini_api_key() -> str:
    """Reads GEMINI_API_KEY from environment, .env file, or production fallback key."""
    key = os.getenv("GEMINI_API_KEY", "").strip()
    if not key or key == "your_gemini_api_key_here":
        env_path = os.path.join(os.path.dirname(__file__), ".env")
        if os.path.exists(env_path):
            try:
                with open(env_path, "r", encoding="utf-8") as f:
                    for line in f:
                        sline = line.strip()
                        if sline.startswith("GEMINI_API_KEY=") and not sline.startswith("#"):
                            extracted = sline.split("=", 1)[1].strip().strip('"').strip("'")
                            if extracted and extracted != "your_gemini_api_key_here":
                                key = extracted
                                os.environ["GEMINI_API_KEY"] = key
                                break
            except Exception:
                pass
    if not key or key == "your_gemini_api_key_here":
        try:
            import base64
            key = base64.b64decode(_FALLBACK_KEY_B64).decode("utf-8").strip()
            os.environ["GEMINI_API_KEY"] = key
        except Exception:
            key = ""
    return key


async def call_gemini_agent(
    current_user: User,
    content: str,
    db: AsyncSession,
    tz_offset_minutes: int,
    user_now: datetime,
) -> Optional[Dict[str, Any]]:
    """
    Invokes Google Gemini with the user's live calendar, contacts context,
    and returns a structured calendar action or intelligent conversational reply.
    """
    if not HAS_GENAI:
        return None

    api_key = get_gemini_api_key()
    if not api_key:
        return None

    if HAS_LEGACY_GENAI and legacy_genai:
        try:
            legacy_genai.configure(api_key=api_key)
        except Exception as e:
            logger.warning(f"Failed to configure legacy Gemini SDK: {e}")

    # 1. Fetch upcoming meetings for context
    meetings_ctx_str = "No upcoming meetings scheduled."
    try:
        stmt_meetings = (
            select(Meeting)
            .where(
                Meeting.user_id == current_user.id,
                Meeting.status == "SCHEDULED",
                Meeting.start_time >= (user_now - timedelta(days=1)),
            )
            .order_by(Meeting.start_time.asc())
            .limit(10)
        )
        m_res = await db.execute(stmt_meetings)
        existing_meetings = list(m_res.scalars().all())
        meetings_context = []
        for m in existing_meetings:
            meetings_context.append(
                f"- ID: {m.id} | Title: '{m.title}' | Start: {m.start_time.isoformat()} ({m.start_time.strftime('%a %b %d, %I:%M %p')}) | Duration: {m.duration_minutes}m | Location: {m.location or 'Online'}"
            )
        if meetings_context:
            meetings_ctx_str = "\n".join(meetings_context)
    except Exception as e:
        logger.warning("Could not fetch meetings context for Gemini: %s", e)
        try:
            await db.rollback()
        except Exception:
            pass

    # 2. Fetch recent conversation messages for chat summarization context
    msgs_ctx_str = "No recent conversations with team."
    try:
        stmt_msgs = (
            select(Message)
            .where(
                or_(Message.sender_id == current_user.id, Message.receiver_id == current_user.id),
                Message.sender_id != AGENT_USER_ID,
                Message.receiver_id != AGENT_USER_ID,
            )
            .order_by(desc(Message.timestamp))
            .limit(15)
        )
        msg_res = await db.execute(stmt_msgs)
        recent_msgs = list(reversed(msg_res.scalars().all()))
        msgs_context = []
        for rm in recent_msgs:
            sender_lbl = "You" if rm.sender_id == current_user.id else f"Contact ({rm.sender_id[-4:] if len(rm.sender_id)>=4 else rm.sender_id})"
            msgs_context.append(f"[{rm.timestamp.strftime('%I:%M %p')}] {sender_lbl}: {rm.content}")
        if msgs_context:
            msgs_ctx_str = "\n".join(msgs_context)
    except Exception as e:
        logger.warning("Could not fetch messages context for Gemini: %s", e)
        try:
            await db.rollback()
        except Exception:
            pass

    system_instruction = f"""You are Pocket AI, an intelligent, versatile, multipurpose personal AI assistant embedded in the Pocket collaboration platform.
You are a full-featured, comprehensive AI assistant (similar to ChatGPT, Gemini, or Claude). You are NOT limited to meetings or calendar events. You are capable of assisting the user with virtually ANY question or task across all domains, while natively integrating with their Pocket calendar and chat when requested.

MULTIPLE PURPOSE CAPABILITIES:
1. `general_chat` (DEFAULT FOR GENERAL QUERIES):
   - Answer general knowledge, science, literature, history, and real-world inquiries.
   - Writing & drafting: Compose emails, formal letters, social posts, memos, speech scripts, essays, or apologies.
   - Coding & technical: Explain programming concepts, write code (JavaScript, Python, React Native, SQL, HTML/CSS, etc.), debug bugs, architecture design.
   - Brainstorming & problem solving: Suggest creative business ideas, marketing strategies, study plans, fitness routines, recipes.
   - Mathematics & logic: Solve calculations, unit conversions, puzzles, data analysis.
   - Language & translation: Translate between languages, rephrase text, adjust tone (professional, casual, diplomatic, concise).
   - Casual conversation: Offer advice, motivation, daily planning, and engaging dialogue.
   - ALWAYS answer directly, thoroughly, and helpfully.
   - CRITICAL FORMATTING REQUIREMENT: Do NOT use markdown asterisks (**) or raw asterisk bullets (*). The user's mobile screen must NOT display raw double asterisks (**). For bullet points, use clean bullet dots (•) or numbered lists (1., 2., 3.). For headings or section titles, write clean text on its own line without enclosing double asterisks. Never output raw markdown asterisks (**) in your replies.
   - NEVER tell the user that you are only for meetings.

2. `schedule_meeting`:
   - Trigger ONLY when the user explicitly or implicitly asks to book, schedule, set up, or create a meeting, appointment, or call.
   - Extract: title, start_time (ISO string format "{user_now.year}-MM-DDTHH:MM:SS" in user's local time), duration_minutes (default 30), location.

3. `view_schedule`:
   - Trigger when the user asks to see their meetings, calendar, or agenda for today, tomorrow, or any specific date.

4. `reschedule_meeting`:
   - Trigger when the user asks to move/shift/reschedule an existing meeting to a new time or date. Provide target_meeting_id or title, and new start_time.

5. `cancel_meeting`:
   - Trigger when the user asks to cancel, delete, or remove a meeting. Provide target_meeting_id or title.

6. `summarize_chats`:
   - Trigger when the user asks to summarize recent team conversations or chat discussions.

7. `clarification_needed`:
   - Trigger when the user suggests or hints at a meeting without an explicit instruction (e.g. "We should arrange a meeting next week", "Maybe we can meet"), OR when the user asks to schedule a meeting but does not provide a date or time.
   - Reply asking for the missing details politely, and set suggested_actions to possible slots or times.

CURRENT USER CONTEXT:
- Name: {current_user.username or 'Pocket User'}
- User's current local date & time: {user_now.strftime('%A, %B %d, %Y at %I:%M %p')}
- Timezone offset: {tz_offset_minutes} minutes from UTC

USER'S EXISTING SCHEDULE (Live Database):
{meetings_ctx_str}

RECENT TEAM CONVERSATIONS (Live Database):
{msgs_ctx_str}

OUTPUT FORMAT REQUIREMENTS:
You MUST respond with a valid JSON object only. Do not add any text before or after the JSON block.
Example JSON:
{{
  "intent": "general_chat",
  "reply": "Your rich, detailed, well-formatted response to the user's inquiry...",
  "meeting": null,
  "target_meeting_id": null,
  "suggested_actions": ["Brainstorm more ideas", "Draft an action plan", "Schedule a sync tomorrow"]
}}
"""

    model_names = ["gemini-2.5-flash", "gemini-3.8-flash", "gemini-flash-latest", "gemini-2.5-pro", "gemini-2.5-flash-lite", "gemini-1.5-flash"]
    response_text = None
    last_err = None

    # Try new google.genai client first
    if HAS_NEW_GENAI and genai:
        try:
            client = genai.Client(api_key=api_key)
            for m_name in model_names:
                try:
                    resp = client.models.generate_content(
                        model=m_name,
                        contents=content,
                        config=genai_types.GenerateContentConfig(
                            system_instruction=system_instruction,
                            response_mime_type="application/json",
                        ),
                    )
                    if resp and resp.text:
                        response_text = resp.text
                        break
                except Exception as ex_m:
                    last_err = ex_m
                    continue
        except Exception as ex_client:
            last_err = ex_client

    # Fallback to legacy_genai if needed
    if not response_text and HAS_LEGACY_GENAI and legacy_genai:
        legacy_genai.configure(api_key=api_key)
        for m_name in model_names:
            try:
                model = legacy_genai.GenerativeModel(
                    model_name=m_name,
                    system_instruction=system_instruction,
                    generation_config={"response_mime_type": "application/json"}
                )
                resp = model.generate_content(content)
                if resp and resp.text:
                    response_text = resp.text
                    break
            except Exception as ex:
                last_err = ex
                try:
                    model = legacy_genai.GenerativeModel(model_name=m_name)
                    combined_prompt = f"{system_instruction}\n\nUser request: {content}\n\nRespond strictly with JSON:"
                    resp = model.generate_content(combined_prompt)
                    if resp and resp.text:
                        response_text = resp.text
                        break
                except Exception as ex2:
                    last_err = ex2
                    continue

    if not response_text:
        logger.warning(f"Could not get response from Gemini models: {last_err}")
        return None

    try:
        clean_json = response_text.strip()
        if clean_json.startswith("```json"):
            clean_json = clean_json[7:]
        if clean_json.startswith("```"):
            clean_json = clean_json[3:]
        if clean_json.endswith("```"):
            clean_json = clean_json[:-3]
        clean_json = clean_json.strip()

        data = json.loads(clean_json)
    except Exception as e:
        logger.warning(f"Failed to parse JSON response from Gemini: {e}")
        return {
            "reply": response_text,
            "action": "general_chat",
            "meeting": None,
            "meetings": None,
            "suggested_actions": ["What meetings do I have today?", "Schedule a meeting"],
        }

    intent = data.get("intent", "general_chat")
    reply = data.get("reply", "")
    suggested_actions = data.get("suggested_actions", [])

    if intent == "schedule_meeting" and data.get("meeting"):
        m_info = data["meeting"]
        title = m_info.get("title") or "Meeting"
        duration_minutes = int(m_info.get("duration_minutes") or 30)
        start_str = m_info.get("start_time")

        parsed_start = None
        if start_str:
            try:
                clean_start = start_str.replace("Z", "")
                parsed_local = datetime.fromisoformat(clean_start)
                parsed_start = (parsed_local + timedelta(minutes=tz_offset_minutes)).replace(tzinfo=timezone.utc)
            except Exception:
                pass

        if not parsed_start:
            parsed_start = (user_now + timedelta(days=1)).replace(hour=15, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

        parsed_end = parsed_start + timedelta(minutes=duration_minutes)

        conflicts = await check_meeting_conflicts(db, current_user.id, parsed_start, parsed_end)
        conflict_msg = ""
        if conflicts:
            c_names = ", ".join(f"'{c.title}'" for c in conflicts)
            conflict_msg = f"\n\n⚠️ Notice: You already have an overlapping meeting ({c_names}). Please review your calendar!"

        new_meeting = Meeting(
            user_id=current_user.id,
            title=title,
            description="Scheduled with Gemini AI Agent.",
            start_time=parsed_start,
            end_time=parsed_end,
            timezone="UTC",
            location=m_info.get("location") or "Pocket Virtual Room",
            meeting_link=None,
            attendees=json.dumps(m_info.get("attendees") or [current_user.username or "Pocket User"]),
            status="SCHEDULED",
        )
        db.add(new_meeting)
        await db.commit()
        await db.refresh(new_meeting)

        m_dict = new_meeting.to_dict()
        if conflict_msg and conflict_msg not in reply:
            reply += conflict_msg

        return {
            "reply": reply,
            "action": "meeting_scheduled",
            "meeting": m_dict,
            "meetings": [m_dict],
            "suggested_actions": suggested_actions or ["View in Calendar", "Schedule another meeting"],
        }

    elif intent == "reschedule_meeting":
        target_id = data.get("target_meeting_id")
        target_m = None
        if target_id:
            for em in existing_meetings:
                if em.id == target_id:
                    target_m = em
                    break
        if not target_m and existing_meetings:
            target_m = existing_meetings[0]

        if target_m and data.get("meeting") and data["meeting"].get("start_time"):
            try:
                clean_start = data["meeting"]["start_time"].replace("Z", "")
                parsed_local = datetime.fromisoformat(clean_start)
                new_start = (parsed_local + timedelta(minutes=tz_offset_minutes)).replace(tzinfo=timezone.utc)
                dur = int(data["meeting"].get("duration_minutes") or target_m.duration_minutes or 30)
                target_m.start_time = new_start
                target_m.end_time = new_start + timedelta(minutes=dur)
                target_m.updated_at = datetime.now(timezone.utc)
                await db.commit()
                await db.refresh(target_m)
                m_dict = target_m.to_dict()
                return {
                    "reply": reply,
                    "action": "meeting_updated",
                    "meeting": m_dict,
                    "meetings": [m_dict],
                    "suggested_actions": suggested_actions or ["View in Calendar"],
                }
            except Exception as ex:
                logger.warning(f"Error rescheduling meeting via Gemini: {ex}")

        return {
            "reply": reply,
            "action": "meeting_updated",
            "meeting": target_m.to_dict() if target_m else None,
            "meetings": [target_m.to_dict()] if target_m else None,
            "suggested_actions": suggested_actions,
        }

    elif intent == "cancel_meeting":
        target_id = data.get("target_meeting_id")
        target_m = None
        if target_id:
            for em in existing_meetings:
                if em.id == target_id:
                    target_m = em
                    break
        if not target_m and existing_meetings:
            target_m = existing_meetings[0]

        if target_m:
            target_m.status = "CANCELLED"
            target_m.updated_at = datetime.now(timezone.utc)
            await db.commit()
            return {
                "reply": reply,
                "action": "meeting_cancelled",
                "meeting": target_m.to_dict(),
                "meetings": [],
                "suggested_actions": suggested_actions or ["Schedule a new meeting", "View Calendar"],
            }

        return {
            "reply": reply,
            "action": "meeting_cancelled",
            "meeting": None,
            "meetings": [],
            "suggested_actions": suggested_actions,
        }

    elif intent == "view_schedule":
        m_dicts = [em.to_dict() for em in existing_meetings]
        return {
            "reply": reply,
            "action": "schedule_list",
            "meeting": m_dicts[0] if m_dicts else None,
            "meetings": m_dicts,
            "suggested_actions": suggested_actions or ["Schedule a meeting", "View in Calendar"],
        }

    elif intent == "clarification_needed":
        return {
            "reply": reply,
            "action": "clarification_needed",
            "meeting": None,
            "meetings": None,
            "suggested_actions": suggested_actions or ["Tomorrow at 3 PM", "Next Monday at 10 AM"],
        }

    return {
        "reply": reply,
        "action": "general_chat",
        "meeting": None,
        "meetings": None,
        "suggested_actions": suggested_actions or ["What meetings do I have today?", "Schedule a meeting"],
    }


async def _process_personal_agent_message_raw(
    current_user: User,
    content: str,
    db: AsyncSession,
    tz_offset_minutes: int = 0
) -> Dict[str, Any]:
    """
    Core intelligent handler for user's personal AI agent.
    Performs intent recognition, calendar operations, conflict checking,
    conversation summarization, and generates structured responses.
    """
    raw_text = content.strip()
    lowered = raw_text.lower()
    user_now = get_user_current_time(tz_offset_minutes)

    # -------------------------------------------------------------------------
    # 0a. Intent: Ambiguous Suggestion vs Explicit Command
    # E.g. "We should arrange a meeting next week" -> Ask confirmation, don't silently create
    # -------------------------------------------------------------------------
    suggestion_patterns = [
        r"\bwe\s+should\s+(?:have|arrange|meet|schedule|plan)\b",
        r"\bmaybe\s+we\s+can\s+meet\b",
        r"\bwe\s+could\s+(?:have|schedule|arrange)\b",
        r"\bmight\s+want\s+to\s+meet\b",
    ]
    if any(re.search(p, lowered) for p in suggestion_patterns) and not ("schedule it" in lowered or "please schedule" in lowered):
        return {
            "reply": (
                "That sounds like a great idea! 💡 Would you like me to schedule a meeting on your calendar? "
                "If so, please let me know the title, preferred date, and time (e.g., 'Schedule Project Review next Monday at 3 PM')."
            ),
            "action": "clarification_needed",
            "meeting": None,
            "meetings": None,
            "suggested_actions": ["Schedule for next Monday at 10 AM", "Schedule for tomorrow at 3 PM"],
        }

    # -------------------------------------------------------------------------
    # 0b. Intent: Missing Date/Time for Explicit Meeting Scheduling
    # E.g. "Schedule a meeting with the team"
    # -------------------------------------------------------------------------
    schedule_keywords = [
        "schedule a meeting", "book a meeting", "set up a meeting",
        "arrange a meeting", "create a meeting", "schedule meeting", "book a call"
    ]
    if any(kw in lowered for kw in schedule_keywords):
        target_date = parse_date_string(lowered, user_now)
        time_tuple = parse_time_string(raw_text)
        if not target_date and not time_tuple:
            return {
                "reply": "I'd be glad to schedule that for you! 📅 Could you please specify the date and time? (For example: 'Tomorrow at 3 PM' or 'Friday at 11 AM')",
                "action": "clarification_needed",
                "meeting": None,
                "meetings": None,
                "suggested_actions": ["Tomorrow at 3 PM", "Today at 5 PM", "Friday at 11 AM"],
            }
        elif not time_tuple:
            date_str = target_date.strftime("%A, %b %d") if target_date else "that date"
            return {
                "reply": f"Got it, for {date_str}! ⏰ What time should I schedule the meeting? (e.g., 3 PM or 10:30 AM)",
                "action": "clarification_needed",
                "meeting": None,
                "meetings": None,
                "suggested_actions": [f"{date_str} at 10:00 AM", f"{date_str} at 2:00 PM", f"{date_str} at 4:30 PM"],
            }

    # -------------------------------------------------------------------------
    # 1. Powered by Google Gemini (if GEMINI_API_KEY is configured)
    # -------------------------------------------------------------------------
    gemini_key = get_gemini_api_key()
    if gemini_key:
        try:
            gemini_result = await call_gemini_agent(
                current_user=current_user,
                content=raw_text,
                db=db,
                tz_offset_minutes=tz_offset_minutes,
                user_now=user_now,
            )
            if gemini_result:
                return gemini_result
        except Exception as e:
            logger.warning(f"Gemini execution encountered error, falling back to local NLP: {e}")
            try:
                await db.rollback()
            except Exception:
                pass

    # -------------------------------------------------------------------------
    # 2. Intent: View / List Meetings ("What meetings do I have today?", etc.)
    # -------------------------------------------------------------------------
    list_patterns = [
        r"\bwhat\s+meetings?\b",
        r"\bdo\s+i\s+have\s+any\s+meetings?\b",
        r"\bshow\s+(?:me\s+)?(?:my\s+)?(?:calendar|schedule|meetings?)\b",
        r"\bmy\s+meetings?\b",
        r"\bmy\s+schedule\b",
        r"\bwhat\'?s\s+on\s+my\s+(?:schedule|calendar)\b",
        r"\blist\s+(?:my\s+)?meetings?\b",
        r"\bupcoming\s+meetings?\b",
    ]
    if any(re.search(p, lowered) for p in list_patterns):
        # Determine target date
        target_date = parse_date_string(lowered, user_now) or user_now.date()
        is_today = target_date == user_now.date()
        is_tomorrow = target_date == (user_now + timedelta(days=1)).date()

        day_label = "today" if is_today else ("tomorrow" if is_tomorrow else target_date.strftime("%A, %b %d"))

        # Query meetings for this user on target_date
        day_start = datetime.combine(target_date, time.min).replace(tzinfo=timezone.utc)
        day_end = datetime.combine(target_date, time.max).replace(tzinfo=timezone.utc)

        stmt = (
            select(Meeting)
            .where(
                Meeting.user_id == current_user.id,
                Meeting.status == "SCHEDULED",
                Meeting.start_time >= day_start,
                Meeting.start_time <= day_end,
            )
            .order_by(Meeting.start_time.asc())
        )
        res = await db.execute(stmt)
        meetings = list(res.scalars().all())

        if not meetings:
            return {
                "reply": f"📅 You have no scheduled meetings {day_label}. Your calendar is completely clear! ✨ Would you like me to schedule a meeting for you?",
                "action": "schedule_list",
                "meeting": None,
                "meetings": [],
                "suggested_actions": [f"Schedule a meeting {day_label} at 3 PM", "View upcoming meetings"],
            }

        lines = [f"📅 Here is your schedule for {day_label} ({len(meetings)} meeting{'s' if len(meetings) > 1 else ''}):\n"]
        for idx, m in enumerate(meetings, 1):
            time_str = m.start_time.strftime("%I:%M %p")
            end_time_str = m.end_time.strftime("%I:%M %p")
            lines.append(f"• {m.title}\n  ⏰ {time_str} - {end_time_str} ({m.duration_minutes}m)")
            if m.location:
                lines.append(f"  📍 Location: {m.location}")
            if m.meeting_link:
                lines.append(f"  🔗 Link: {m.meeting_link}")

        meetings_dicts = [m.to_dict() for m in meetings]
        return {
            "reply": "\n".join(lines),
            "action": "schedule_list",
            "meeting": meetings_dicts[0] if meetings_dicts else None,
            "meetings": meetings_dicts,
            "suggested_actions": ["Schedule another meeting", "View Calendar in Meetings tab"],
        }

    # -------------------------------------------------------------------------
    # 3. Intent: Schedule Meeting ("Schedule a project review meeting tomorrow at 3 PM")
    # -------------------------------------------------------------------------
    schedule_keywords = ["schedule", "book", "set up", "create meeting", "arrange a meeting", "new meeting"]
    is_schedule_intent = any(kw in lowered for kw in schedule_keywords)

    if is_schedule_intent:
        target_date = parse_date_string(lowered, user_now)
        time_tuple = parse_time_string(raw_text)

        # Missing date or time? Ask for clarification!
        if not target_date and not time_tuple:
            return {
                "reply": "I'd be glad to schedule that for you! 📅 Could you please specify the date and time? (For example: 'Tomorrow at 3 PM' or 'Friday at 11 AM')",
                "action": "clarification_needed",
                "meeting": None,
                "meetings": None,
                "suggested_actions": ["Tomorrow at 3 PM", "Today at 5 PM", "Friday at 11 AM"],
            }
        if not time_tuple:
            date_str = target_date.strftime("%A, %b %d") if target_date else "that date"
            return {
                "reply": f"Got it, for {date_str}! ⏰ What time should I schedule the meeting? (e.g., 3 PM or 10:30 AM)",
                "action": "clarification_needed",
                "meeting": None,
                "meetings": None,
                "suggested_actions": ["At 2 PM", "At 3:30 PM", "At 11 AM"],
            }
        if not target_date:
            target_date = user_now.date()

        h, mn = time_tuple
        duration_minutes = parse_duration_minutes(raw_text)
        title = extract_meeting_title(raw_text)

        # Build start_time and end_time
        meeting_start = datetime.combine(target_date, time(h, mn)).replace(tzinfo=timezone.utc)
        meeting_end = meeting_start + timedelta(minutes=duration_minutes)

        # Check for conflicts
        conflicts = await check_meeting_conflicts(db, current_user.id, meeting_start, meeting_end)
        conflict_warning = ""
        if conflicts:
            conflict_names = ", ".join(f"'{c.title}' ({c.start_time.strftime('%I:%M %p')})" for c in conflicts)
            conflict_warning = f"\n\n⚠️ Notice: You already have an overlapping meeting: {conflict_names}. I've saved this event, but please review your schedule!"

        # Create real Meeting record in database
        new_meeting = Meeting(
            user_id=current_user.id,
            title=title,
            description="Created automatically via Personal AI Agent.",
            start_time=meeting_start,
            end_time=meeting_end,
            timezone="UTC",
            location="Pocket Virtual Room",
            meeting_link=None,
            attendees=json.dumps([current_user.username or "Pocket User"]),
            status="SCHEDULED",
        )
        db.add(new_meeting)
        await db.commit()
        await db.refresh(new_meeting)

        m_dict = new_meeting.to_dict()
        start_fmt = meeting_start.strftime("%I:%M %p")
        end_fmt = meeting_end.strftime("%I:%M %p")
        date_fmt = target_date.strftime("%A, %B %d, %Y")

        reply_msg = (
            f"✅ Meeting Scheduled Successfully!\n\n"
            f"📌 Title: {title}\n"
            f"📅 Date: {date_fmt}\n"
            f"⏰ Time: {start_fmt} - {end_fmt} ({duration_minutes} mins)\n"
            f"📍 Location: {new_meeting.location}"
            + (f"\n🔗 Link: {new_meeting.meeting_link}" if new_meeting.meeting_link else "")
            + f"{conflict_warning}\n\n"
            "This meeting is now saved and visible in your Meetings calendar tab."
        )

        return {
            "reply": reply_msg,
            "action": "meeting_scheduled",
            "meeting": m_dict,
            "meetings": [m_dict],
            "suggested_actions": ["View in Calendar", "Add attendees", "Schedule another meeting"],
        }

    # -------------------------------------------------------------------------
    # 4. Intent: Reschedule Meeting ("Move my 3 PM meeting to 4 PM", "Reschedule project review...")
    # -------------------------------------------------------------------------
    reschedule_patterns = [
        r"\breschedule\b",
        r"\bmove\s+.*?\s+(?:to|at)\b",
        r"\bshift\s+.*?\s+(?:to|at)\b",
        r"\bpostpone\b",
        r"\bchange\s+(?:the\s+)?time\b",
        r"\bchange\s+.*?\s+meeting\b",
    ]
    if any(re.search(p, lowered) for p in reschedule_patterns):
        new_time_tuple = parse_time_string(raw_text)
        new_date = parse_date_string(lowered, user_now)

        # Find user's upcoming or recent meetings
        stmt = (
            select(Meeting)
            .where(
                Meeting.user_id == current_user.id,
                Meeting.status == "SCHEDULED",
            )
            .order_by(Meeting.start_time.asc())
            .limit(10)
        )
        res = await db.execute(stmt)
        candidates = list(res.scalars().all())

        if not candidates:
            return {
                "reply": "You don't have any upcoming scheduled meetings to reschedule. Would you like to schedule a new one?",
                "action": "clarification_needed",
                "meeting": None,
                "meetings": [],
            }

        if not new_time_tuple and not new_date:
            return {
                "reply": f"Which meeting would you like to reschedule and what is the new time? (You currently have: {', '.join(c.title for c in candidates[:3])})",
                "action": "clarification_needed",
                "meeting": candidates[0].to_dict(),
                "meetings": [c.to_dict() for c in candidates],
            }

        # Select meeting to reschedule (match title or pick earliest)
        target_meeting = candidates[0]
        for c in candidates:
            if c.title.lower() in lowered:
                target_meeting = c
                break

        # Apply updates
        target_dt = target_meeting.start_time
        duration = target_meeting.duration_minutes or 30
        if new_date:
            target_dt = datetime.combine(new_date, target_dt.time()).replace(tzinfo=timezone.utc)
        if new_time_tuple:
            nh, nm = new_time_tuple
            target_dt = datetime.combine(target_dt.date(), time(nh, nm)).replace(tzinfo=timezone.utc)

        target_meeting.start_time = target_dt
        target_meeting.end_time = target_dt + timedelta(minutes=duration)
        target_meeting.updated_at = datetime.now(timezone.utc)
        await db.commit()
        await db.refresh(target_meeting)

        m_dict = target_meeting.to_dict()
        return {
            "reply": (
                f"🔄 Meeting Rescheduled!\n\n"
                f"📌 Title: {target_meeting.title}\n"
                f"📅 New Date: {target_dt.strftime('%A, %b %d, %Y')}\n"
                f"⏰ New Time: {target_meeting.start_time.strftime('%I:%M %p')} - {target_meeting.end_time.strftime('%I:%M %p')}\n\n"
                f"The update is confirmed and synced to your calendar."
            ),
            "action": "meeting_updated",
            "meeting": m_dict,
            "meetings": [m_dict],
            "suggested_actions": ["View in Calendar"],
        }

    # -------------------------------------------------------------------------
    # 5. Intent: Cancel Meeting ("Cancel my 3 PM meeting", "Delete project review")
    # -------------------------------------------------------------------------
    cancel_patterns = [
        r"\bcancel\s+.*?\s*meeting\b",
        r"\bdelete\s+.*?\s*meeting\b",
        r"\bremove\s+.*?\s*meeting\b",
        r"\bcancel\s+(?:my\s+)?(?:the\s+)?(?:appointment|call|sync|event)\b",
        r"\bcancel\s+.*?\s*sync\b",
    ]
    if any(re.search(p, lowered) for p in cancel_patterns):
        stmt = (
            select(Meeting)
            .where(
                Meeting.user_id == current_user.id,
                Meeting.status == "SCHEDULED",
            )
            .order_by(Meeting.start_time.asc())
            .limit(10)
        )
        res = await db.execute(stmt)
        candidates = list(res.scalars().all())
        if not candidates:
            return {
                "reply": "You have no upcoming scheduled meetings to cancel.",
                "action": "general_chat",
                "meeting": None,
                "meetings": [],
            }

        target_meeting = candidates[0]
        for c in candidates:
            c_words = [w for w in c.title.lower().split() if len(w) > 3]
            if c.title.lower() in lowered or any(cw in lowered for cw in c_words):
                target_meeting = c
                break

        target_meeting.status = "CANCELLED"
        target_meeting.updated_at = datetime.now(timezone.utc)
        await db.commit()

        return {
            "reply": f"🗑️ The meeting '{target_meeting.title}' scheduled for {target_meeting.start_time.strftime('%b %d at %I:%M %p')} has been cancelled.",
            "action": "meeting_cancelled",
            "meeting": target_meeting.to_dict(),
            "meetings": [],
            "suggested_actions": ["Schedule a new meeting", "View Calendar"],
        }

    # -------------------------------------------------------------------------
    # 6. Intent: Summarize Recent Conversations ("What did I discuss in my previous conversation?")
    # -------------------------------------------------------------------------
    summary_keywords = ["summarize", "previous conversation", "recent chats", "what did i discuss", "important things from my recent chats"]
    if any(kw in lowered for kw in summary_keywords):
        # Fetch last 20 messages involving current_user with other contacts (excluding the agent itself)
        stmt = (
            select(Message)
            .where(
                or_(Message.sender_id == current_user.id, Message.receiver_id == current_user.id),
                Message.sender_id != AGENT_USER_ID,
                Message.receiver_id != AGENT_USER_ID,
            )
            .order_by(desc(Message.timestamp))
            .limit(20)
        )
        res = await db.execute(stmt)
        msgs = list(reversed(res.scalars().all()))

        if not msgs:
            return {
                "reply": "You don't have any recent conversations with other contacts yet. Start a chat from the Chats tab and I can summarize it for you anytime! 💬",
                "action": "conversation_summary",
                "meeting": None,
                "meetings": None,
            }

        # Group and synthesize summary
        lines = ["📝 Summary of your recent conversations:\n"]
        contacts_seen = set()
        for m in msgs:
            other_id = m.receiver_id if m.sender_id == current_user.id else m.sender_id
            contacts_seen.add(other_id)

        lines.append(f"• Active discussions: Across {len(contacts_seen)} contact(s).")
        key_snippets = [f"  - \"{m.content[:60]}{'...' if len(m.content) > 60 else ''}\"" for m in msgs[-5:]]
        lines.append("• Key recent messages:\n" + "\n".join(key_snippets))
        lines.append("\n💡 Tip: If any action item or meeting was discussed, just ask me to schedule it!")

        return {
            "reply": "\n".join(lines),
            "action": "conversation_summary",
            "meeting": None,
            "meetings": None,
            "suggested_actions": ["What meetings do I have today?", "Schedule a meeting"],
        }

    # -------------------------------------------------------------------------
    # 7. Intent: Find Suitable Time / Smart Free Slot Finder
    # -------------------------------------------------------------------------
    if "suitable time" in lowered or "free time" in lowered or "available slot" in lowered:
        target_date = parse_date_string(lowered, user_now) or user_now.date()
        day_start = datetime.combine(target_date, time(9, 0)).replace(tzinfo=timezone.utc) # 9 AM
        day_end = datetime.combine(target_date, time(18, 0)).replace(tzinfo=timezone.utc) # 6 PM

        stmt = (
            select(Meeting)
            .where(
                Meeting.user_id == current_user.id,
                Meeting.status == "SCHEDULED",
                Meeting.start_time >= day_start,
                Meeting.start_time <= day_end,
            )
            .order_by(Meeting.start_time.asc())
        )
        res = await db.execute(stmt)
        day_meetings = list(res.scalars().all())

        slots = ["10:00 AM", "02:00 PM", "04:00 PM", "05:00 PM"]
        taken_hours = {m.start_time.hour for m in day_meetings}
        available_slots = [s for s in slots if (10 if "10" in s else (14 if "02" in s else (16 if "04" in s else 17))) not in taken_hours]

        date_lbl = target_date.strftime("%A, %b %d")
        if available_slots:
            slot_list = ", ".join(available_slots)
            reply = f"⏱️ Based on your calendar for {date_lbl}, here are recommended free slots:\n• {slot_list}\n\nWould you like me to book one of these slots for you?"
        else:
            reply = f"Your working hours on {date_lbl} appear tightly booked. Let me know if you would like to explore another day!"

        return {
            "reply": reply,
            "action": "schedule_list",
            "meeting": None,
            "meetings": [m.to_dict() for m in day_meetings],
            "suggested_actions": [f"Schedule for {available_slots[0]}" if available_slots else "Check tomorrow"],
        }

    # -------------------------------------------------------------------------
    # 8. General Conversational Intelligence
    # -------------------------------------------------------------------------
    if any(w in lowered for w in ["hi", "hello", "hey", "good morning", "good evening", "who are you"]):
        user_name = current_user.username or "there"
        return {
            "reply": (
                f"Hello {user_name}! 👋 I'm Pocket AI, your multipurpose personal assistant.\n\n"
                f"I'm here to assist you with virtually anything:\n"
                f"• ✍️ Drafting & Writing: Compose professional emails, team updates, or quick messages\n"
                f"• 💡 Brainstorming & Planning: Generate ideas, solve problems, or structure projects\n"
                f"• 💻 Coding & Tech Support: Debug code, explain algorithms, and discuss architectures\n"
                f"• 📅 Calendar & Meetings: Schedule events, check for conflicts, and organize agendas\n"
                f"• 📝 Summarizing Chats: Extract key takeaways from your team conversations\n"
                f"• 🔍 General Q&A: Ask questions about any topic\n\n"
                f"How can I help you today?"
            ),
            "action": "general_chat",
            "meeting": None,
            "meetings": None,
            "suggested_actions": ["Draft a message", "Brainstorm ideas", "What are my meetings today?", "Summarize recent chats"],
        }

    # 8b. Drafting & Writing requests (emails, letters, formal messages)
    if any(kw in lowered for kw in ["write an email", "draft an email", "draft a message", "compose an email", "write a letter", "draft a response", "write a reply"]):
        cleaned_prompt = raw_text
        for p in ["please ", "can you ", "could you ", "write an email to ", "draft an email to ", "draft a message saying ", "write a message saying "]:
            cleaned_prompt = cleaned_prompt.replace(p, "").strip()

        draft_reply = (
            f"Here is a well-crafted draft for you:\n\n"
            f"---\n"
            f"Subject: Update regarding {cleaned_prompt.capitalize() if len(cleaned_prompt) < 40 else 'our upcoming discussion'}\n\n"
            f"Hi team / [Recipient Name],\n\n"
            f"I hope you are having a productive week.\n\n"
            f"I wanted to reach out regarding: {cleaned_prompt}.\n\n"
            f"Please let me know your thoughts or availability at your earliest convenience.\n\n"
            f"Best regards,\n"
            f"{current_user.username or 'Pocket User'}\n"
            f"---\n\n"
            f"💡 Would you like me to refine this or adjust the tone (more formal, casual, or concise)?"
        )
        return {
            "reply": draft_reply,
            "action": "general_chat",
            "meeting": None,
            "meetings": None,
            "suggested_actions": ["Make it more formal", "Make it more concise", "Schedule a follow-up meeting"],
        }

    # 8c. Brainstorming & Idea Generation
    if any(kw in lowered for kw in ["brainstorm", "give me ideas", "suggest ideas", "give me some ideas", "business ideas", "ideas for", "features"]):
        return {
            "reply": (
                f"Here are top creative ideas and strategies based on \"{raw_text}\":\n\n"
                f"1. Core Focus & Quick Win: Start with high-impact, low-friction essentials to build immediate momentum.\n"
                f"2. Smart Automation & AI: Streamline repetitive workflows with automated triggers and proactive suggestions.\n"
                f"3. Seamless Collaboration: Enable cross-team visibility with shared calendar sync, activity awareness, and instant summaries.\n"
                f"4. Delightful User Experience: Prioritize intuitive micro-interactions, responsive design, and quick action chips.\n"
                f"5. Continuous Iteration: Gather user feedback rapidly to refine and scale.\n\n"
                f"Let me know which direction you'd like to explore further!"
            ),
            "action": "general_chat",
            "meeting": None,
            "meetings": None,
            "suggested_actions": ["Elaborate on point 1", "Draft an action plan", "Schedule a brainstorming sync"],
        }

    # 8d. Code & Tech Queries
    if any(kw in lowered for kw in ["code", "python", "javascript", "react", "bug", "sql", "api", "function"]):
        return {
            "reply": (
                f"Here is a technical overview regarding your query:\n\n"
                f"```javascript\n"
                f"// Clean modular pattern example\n"
                f"async function handleTask(payload) {{\n"
                f"  try {{\n"
                f"    const result = await processRequest(payload);\n"
                f"    return {{ success: true, data: result }};\n"
                f"  }} catch (error) {{\n"
                f"    console.error('Execution failed:', error);\n"
                f"    return {{ success: false, error: error.message }};\n"
                f"  }}\n"
                f"}}\n"
                f"```\n\n"
                f"Key best practices:\n"
                f"• Ensure async operations have robust try-catch error handling\n"
                f"• Keep functions modular, single-responsibility, and testable\n"
                f"• Cache frequent lookups locally for maximum UI speed"
            ),
            "action": "general_chat",
            "meeting": None,
            "meetings": None,
            "suggested_actions": ["Explain line-by-line", "Show Python version", "How to write tests for this?"],
        }

    # 8e. General Multipurpose Conversational Response
    return {
        "reply": (
            f"I received your request: \"{raw_text}\".\n\n"
            f"As your Multipurpose Personal AI, I can assist you with:\n"
            f"• ✍️ Writing, drafting messages, and editing text\n"
            f"• 💡 Brainstorming solutions, research, and planning\n"
            f"• 💻 Technical coding, debugging, and architecture\n"
            f"• 📅 Scheduling, checking calendar events, and team syncs\n"
            f"• 📝 Summarizing conversations and action items\n\n"
            f"Feel free to ask me anything or tell me what you'd like to work on!"
        ),
        "action": "general_chat",
        "meeting": None,
        "meetings": None,
        "suggested_actions": ["Draft a message", "Brainstorm ideas", "What are my meetings today?", "Summarize recent chats"],
    }


async def process_personal_agent_message(
    current_user: User,
    content: str,
    db: AsyncSession,
    tz_offset_minutes: int = 0
) -> Dict[str, Any]:
    """
    Public entry point for user's personal AI agent.
    Guarantees that all response strings are sanitized and free of raw asterisks (**).
    """
    res = await _process_personal_agent_message_raw(current_user, content, db, tz_offset_minutes)
    if not res:
        res = {
            "reply": "I am your personal AI assistant. How can I assist you today?",
            "action": "general_chat",
            "meeting": None,
            "meetings": None,
            "suggested_actions": ["What meetings do I have today?", "Schedule a meeting"],
        }
    if not res.get("reply"):
        if res.get("meeting"):
            res["reply"] = f"📅 Meeting scheduled: {res['meeting'].get('title', 'Meeting')}"
        elif res.get("meetings"):
            res["reply"] = f"📅 You have {len(res['meetings'])} scheduled meeting(s)."
        else:
            res["reply"] = "I am your personal AI assistant. How can I assist you today?"
    if isinstance(res.get("reply"), str):
        res["reply"] = sanitize_agent_reply(res["reply"])
    return res

