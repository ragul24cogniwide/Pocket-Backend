import asyncio
from database import async_session_factory
from models import User, Message, MessageStatus
from sqlalchemy import select
from datetime import datetime, timezone

async def run_seed():
    async with async_session_factory() as db:
        users = [
            {"id": "1", "phone": "+919876543211", "name": "Rahul Sharma", "color": "#FFB800", "quote": "⚡ Always on the move!"},
            {"id": "2", "phone": "+919123456782", "name": "Priya Patel", "color": "#10B981", "quote": "✨ Living in the moment."},
            {"id": "3", "phone": "+919988776653", "name": "React Native Devs", "color": "#3B82F6", "quote": "🚀 Building mobile apps with Pocket!"},
            {"id": "4", "phone": "+919811223344", "name": "Alex Johnson", "color": "#8B5CF6", "quote": "🎧 In my zone."},
        ]
        for u in users:
            stmt = select(User).where(User.id == u["id"])
            res = await db.execute(stmt)
            existing = res.scalar_one_or_none()
            if not existing:
                p_stmt = select(User).where(User.phone_number == u["phone"])
                p_res = await db.execute(p_stmt)
                phone_val = u["phone"] if not p_res.scalar_one_or_none() else f"{u['phone']}_{u['id']}"
                db.add(User(
                    id=u["id"],
                    phone_number=phone_val,
                    username=u["name"],
                    avatar_color=u["color"],
                    quote=u["quote"],
                    is_online=True
                ))
                await db.commit()
                print(f"Seeded user {u['id']} ({u['name']})")
            else:
                print(f"User {u['id']} exists ({existing.username})")

        # Now test message to receiver '3'
        msg = Message(
            sender_id="+918946014462",
            receiver_id="3",
            content="Hello from test script!",
            status=MessageStatus.SENT,
            timestamp=datetime.now(timezone.utc)
        )
        db.add(msg)
        await db.commit()
        print(f"Successfully inserted test message to receiver '3'! ID: {msg.id}")

if __name__ == "__main__":
    asyncio.run(run_seed())
