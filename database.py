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
    """Dependency yielding transactional async database sessions with auto-schema healing."""
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception as exc:
            await session.rollback()
            err_str = str(exc)
            if "does not exist" in err_str and ("relation" in err_str or "table" in err_str or "column" in err_str):
                import logging
                logging.getLogger("Database").warning(
                    "Missing schema element detected (%s). Auto-healing database schema...", err_str
                )
                try:
                    await init_db()
                except Exception as heal_err:
                    logging.getLogger("Database").error("Auto-heal failed: %s", heal_err)
            raise
        finally:
            await session.close()


async def init_db() -> None:
    """Creates database tables automatically on startup if missing and seeds default contacts."""
    import asyncio
    import logging
    from models import User, Meeting, OtpRecord, Message, Status, ConversationChangeLog, UserSyncSequence, Group, GroupMember, GroupMessage, WebSession, MessageReaction, ChatArchive, CallRecord  # noqa: F401 (registers tables)
    from sqlalchemy import select

    db_logger = logging.getLogger("Database")

    # Neon PostgreSQL can pause/scale to zero; retry table creation up to 5 times
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
                # Gentle column addition for PostgreSQL / SQLite if columns were added later
                for ddl in [
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar_url TEXT;",
                    "ALTER TABLE users ALTER COLUMN avatar_url TYPE TEXT;",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS quote VARCHAR(255) DEFAULT 'Hey there! I am using Flick.';",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS fcm_token TEXT;",
                    "ALTER TABLE users ALTER COLUMN fcm_token TYPE TEXT;",
                    "ALTER TABLE messages ADD COLUMN IF NOT EXISTS reply_to TEXT;",
                    "ALTER TABLE messages ADD COLUMN IF NOT EXISTS is_edited BOOLEAN DEFAULT FALSE;",
                    "ALTER TABLE messages ADD COLUMN IF NOT EXISTS delivered_at TIMESTAMP WITH TIME ZONE;",
                    "ALTER TABLE messages ADD COLUMN IF NOT EXISTS read_at TIMESTAMP WITH TIME ZONE;",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS preferences TEXT;",
                    "ALTER TABLE messages ADD COLUMN IF NOT EXISTS client_msg_id VARCHAR(64);",
                    "ALTER TABLE messages ADD COLUMN IF NOT EXISTS receipt_hidden BOOLEAN DEFAULT FALSE;",
                    "ALTER TABLE messages ADD COLUMN IF NOT EXISTS hidden_for_sender BOOLEAN DEFAULT FALSE;",
                    "ALTER TABLE messages ADD COLUMN IF NOT EXISTS hidden_for_receiver BOOLEAN DEFAULT FALSE;",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS token_version INTEGER DEFAULT 0;",
                    "CREATE INDEX IF NOT EXISTS ix_messages_client_msg_id ON messages (client_msg_id);",
                ]:
                    try:
                        await conn.execute(text(ddl))
                    except Exception:
                        pass
            db_logger.info("Database schema initialized and verified successfully.")
            break
        except Exception as e:
            db_logger.warning("DB init attempt %d/%d failed: %s. Retrying in 2s...", attempt, max_retries, e)
            if attempt == max_retries:
                db_logger.error("Failed to initialize database tables after %d attempts: %s", max_retries, e)
                raise
            await asyncio.sleep(2)

    # Demo contacts are for local testing only: in production every real user would see them
    if os.getenv("SEED_DEMO_USERS", "false").lower() != "true":
        return

    # Seed default mock/demo contacts so chatting with any ID never throws Foreign Key errors
    async with async_session_factory() as session:
        default_seed_users = [
            {"id": "+918946014462", "phone_number": "+918946014462", "username": "Ragulvasanth Muthukumar", "avatar_color": "#10B981", "quote": "🚀 Building the future"},
            {"id": "1", "phone_number": "+919876543211", "username": "Rahul Sharma", "avatar_color": "#FFB800", "quote": "⚡ Always on the move!"},
            {"id": "2", "phone_number": "+919123456782", "username": "Priya Patel", "avatar_color": "#10B981", "quote": "✨ Living in the moment."},
            {"id": "3", "phone_number": "+919988776653", "username": "React Native Devs", "avatar_color": "#3B82F6", "quote": "🚀 Building awesome mobile apps with Flick!"},
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
