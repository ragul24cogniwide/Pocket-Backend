"""
Database engine and async session factory.
Optimized for Neon PostgreSQL (asyncpg) with SSL and connection pooling,
plus fallback to SQLite for local development.
"""

import os
import ssl
from collections.abc import AsyncGenerator
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "sqlite+aiosqlite:///./messaging_engine.db"
)

# Convert postgres:// or postgresql:// to postgresql+asyncpg:// if needed
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://") and not DATABASE_URL.startswith("postgresql+asyncpg://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

# Normalize sslmode= to ssl= for asyncpg driver compatibility
if "sslmode=" in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("sslmode=", "ssl=")

# Configure SSL context for Neon PostgreSQL if ssl=require or neon.tech host
engine_kwargs = {
    "echo": False,
    "future": True,
    "pool_pre_ping": True,
}

if "postgresql+asyncpg" in DATABASE_URL:
    engine_kwargs.update({
        "pool_size": 20,
        "max_overflow": 10,
        "pool_recycle": 300,
    })

engine: AsyncEngine = create_async_engine(DATABASE_URL, **engine_kwargs)

async_session_factory = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


class Base(DeclarativeBase):
    """Base declarative class for all SQLAlchemy ORM models."""
    pass


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency yielding transactional async database sessions."""
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def init_db() -> None:
    """Creates database tables automatically on startup if missing and seeds default contacts."""
    from models import User
    from sqlalchemy import select

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Gentle column addition for PostgreSQL / SQLite if columns were added later
        try:
            await conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar_url VARCHAR(500);"))
        except Exception:
            pass
        try:
            await conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS quote VARCHAR(255) DEFAULT 'Hey there! I am using Pocket.';"))
        except Exception:
            pass
        try:
            await conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS fcm_token VARCHAR(255);"))
        except Exception:
            pass

    # Seed default mock/demo contacts so chatting with any ID (1, 2, 3, 4) never throws Foreign Key errors
    async with async_session_factory() as session:
        default_seed_users = [
            {"id": "1", "phone_number": "+919876543211", "username": "Rahul Sharma", "avatar_color": "#FFB800", "quote": "⚡ Always on the move!"},
            {"id": "2", "phone_number": "+919123456782", "username": "Priya Patel", "avatar_color": "#10B981", "quote": "✨ Living in the moment."},
            {"id": "3", "phone_number": "+919988776653", "username": "React Native Devs", "avatar_color": "#3B82F6", "quote": "🚀 Building awesome mobile apps with Pocket!"},
            {"id": "4", "phone_number": "+919811223344", "username": "Alex Johnson", "avatar_color": "#8B5CF6", "quote": "🎧 In my zone."},
        ]

        for item in default_seed_users:
            try:
                stmt = select(User).where(User.id == item["id"])
                res = await session.execute(stmt)
                existing = res.scalar_one_or_none()
                if not existing:
                    phone_stmt = select(User).where(User.phone_number == item["phone_number"])
                    phone_res = await session.execute(phone_stmt)
                    phone_existing = phone_res.scalar_one_or_none()
                    phone_val = item["phone_number"] if not phone_existing else f"{item['phone_number']}_{item['id']}"

                    user = User(
                        id=item["id"],
                        phone_number=phone_val,
                        username=item["username"],
                        avatar_color=item["avatar_color"],
                        quote=item["quote"],
                        is_online=True,
                    )
                    session.add(user)
                    await session.commit()
            except Exception:
                await session.rollback()
