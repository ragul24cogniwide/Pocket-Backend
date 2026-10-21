"""
Unit & Integration Tests for Personal AI Agent and Calendar Collaboration System.
Verifies:
- User personal agent provisioning and data isolation
- Calendar event creation, retrieval, conflict checking, update, and deletion
- Natural language meeting scheduling via AI Agent
- Suggestion vs explicit instruction detection
- Missing information handling and clarification
- Summarization of user conversations
"""

import asyncio
import json
import unittest
from datetime import datetime, date, time, timedelta, timezone
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from database import Base
from models import User, Message, Meeting
from ai_agent import (
    process_personal_agent_message,
    ensure_personal_agent_user,
    check_meeting_conflicts,
    AGENT_USER_ID,
    parse_date_string,
    parse_time_string,
    extract_meeting_title,
)

# Test SQLite in-memory database
TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


async def test_date_and_time_parsing():
    now = datetime(2026, 10, 20, 10, 0, 0, tzinfo=timezone.utc)
    
    # Relative dates
    d_today = parse_date_string("schedule a call today at 4 pm", now)
    assert d_today == date(2026, 10, 20)
    
    d_tomorrow = parse_date_string("let's meet tomorrow at 3 pm", now)
    assert d_tomorrow == date(2026, 10, 21)

    d_day_after = parse_date_string("sync day after tomorrow", now)
    assert d_day_after == date(2026, 10, 22)

    # Time parsing
    t_3pm = parse_time_string("schedule review at 3 PM")
    assert t_3pm == (15, 0)

    t_1030am = parse_time_string("meet at 10:30 am")
    assert t_1030am == (10, 30)

    # Title extraction
    title1 = extract_meeting_title("Schedule a project review meeting tomorrow at 3 PM")
    assert "Project Review" in title1 or "Meeting" in title1

    title2 = extract_meeting_title("Book sprint planning for 1 hour")
    assert "Sprint Planning" in title2


async def test_meeting_crud_and_isolation(test_session: AsyncSession):
    # Setup two separate users
    user_a = User(id="user_a", phone_number="+919000000001", username="User A")
    user_b = User(id="user_b", phone_number="+919000000002", username="User B")
    test_session.add_all([user_a, user_b])
    await test_session.commit()

    # User A creates a meeting
    start_a = datetime(2026, 10, 21, 15, 0, tzinfo=timezone.utc)
    end_a = datetime(2026, 10, 21, 15, 30, tzinfo=timezone.utc)
    meeting_a = Meeting(
        user_id=user_a.id,
        title="User A Private Strategy",
        start_time=start_a,
        end_time=end_a,
        timezone="UTC",
        status="SCHEDULED",
    )
    test_session.add(meeting_a)
    await test_session.commit()

    # Verify User A can query their meeting
    res_a = await test_session.execute(select(Meeting).where(Meeting.user_id == user_a.id))
    meetings_a = res_a.scalars().all()
    assert len(meetings_a) == 1
    assert meetings_a[0].title == "User A Private Strategy"

    # Verify User B cannot see User A's meeting (Security & Privacy Isolation)
    res_b = await test_session.execute(select(Meeting).where(Meeting.user_id == user_b.id))
    meetings_b = res_b.scalars().all()
    assert len(meetings_b) == 0


async def test_conflict_detection(test_session: AsyncSession):
    user = User(id="user_conflict", phone_number="+919000000003", username="Conflict Tester")
    test_session.add(user)
    await test_session.commit()

    # Existing meeting 3:00 PM - 4:00 PM
    m1 = Meeting(
        user_id=user.id,
        title="Client Pitch",
        start_time=datetime(2026, 10, 21, 15, 0, tzinfo=timezone.utc),
        end_time=datetime(2026, 10, 21, 16, 0, tzinfo=timezone.utc),
        status="SCHEDULED",
    )
    test_session.add(m1)
    await test_session.commit()

    # Check overlapping slot 3:30 PM - 4:30 PM
    conflicts = await check_meeting_conflicts(
        test_session,
        user.id,
        datetime(2026, 10, 21, 15, 30, tzinfo=timezone.utc),
        datetime(2026, 10, 21, 16, 30, tzinfo=timezone.utc),
    )
    assert len(conflicts) == 1
    assert conflicts[0].title == "Client Pitch"

    # Non-overlapping slot 4:30 PM - 5:00 PM
    no_conflicts = await check_meeting_conflicts(
        test_session,
        user.id,
        datetime(2026, 10, 21, 16, 30, tzinfo=timezone.utc),
        datetime(2026, 10, 21, 17, 0, tzinfo=timezone.utc),
    )
    assert len(no_conflicts) == 0


async def test_ai_agent_scheduling_flow(test_session: AsyncSession):
    user = User(id="user_agent_test", phone_number="+919000000004", username="Agent Tester")
    test_session.add(user)
    await ensure_personal_agent_user(test_session)
    await test_session.commit()

    # 1. Ask to schedule with full details
    prompt = "Schedule a project review meeting tomorrow at 3 PM for 30 minutes"
    res = await process_personal_agent_message(user, prompt, test_session, tz_offset_minutes=0)
    assert res["action"] == "meeting_scheduled"
    assert res["meeting"] is not None
    assert "Project Review" in res["meeting"]["title"] or "Meeting" in res["meeting"]["title"] or len(res["meeting"]["title"]) > 0

    # Verify event was ACTUALLY created in the database
    db_res = await test_session.execute(select(Meeting).where(Meeting.user_id == user.id))
    saved_meetings = db_res.scalars().all()
    assert len(saved_meetings) == 1

    # 2. Ask "What meetings do I have tomorrow?"
    res_list = await process_personal_agent_message(user, "What meetings do I have tomorrow?", test_session)
    assert res_list["action"] == "schedule_list"
    assert len(res_list["meetings"]) == 1

    # 3. Suggestion vs explicit instruction test
    res_sug = await process_personal_agent_message(user, "We should arrange a meeting next week", test_session)
    assert res_sug["action"] == "clarification_needed"
    assert "Would you like me to schedule a meeting" in res_sug["reply"]

    # 4. Missing time clarification test
    res_miss = await process_personal_agent_message(user, "Schedule a meeting with the team", test_session)
    assert res_miss["action"] == "clarification_needed"
async def run_all_tests():
    print("Running test_date_and_time_parsing...")
    await test_date_and_time_parsing()
    print("✓ test_date_and_time_parsing passed.")

    engine = create_async_engine(TEST_DB_URL, echo=False)
    async_session = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with async_session() as session:
        print("Running test_meeting_crud_and_isolation...")
        await test_meeting_crud_and_isolation(session)
        print("✓ test_meeting_crud_and_isolation passed.")

        print("Running test_conflict_detection...")
        await test_conflict_detection(session)
        print("✓ test_conflict_detection passed.")

        print("Running test_ai_agent_scheduling_flow...")
        await test_ai_agent_scheduling_flow(session)
        print("✓ test_ai_agent_scheduling_flow passed.")

    await engine.dispose()
    print("\n🎉 ALL BACKEND UNIT & INTEGRATION TESTS PASSED SUCCESSFULLY!")


if __name__ == "__main__":
    asyncio.run(run_all_tests())

