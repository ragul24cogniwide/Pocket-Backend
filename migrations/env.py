"""
Alembic environment.

Two ways in:
  - the app's startup (database.run_migrations) hands over its open connection in
    config.attributes["connection"], so migrations run on the app's own async engine;
  - the `alembic` command line (alembic upgrade head, alembic revision --autogenerate ...)
    connects with DATABASE_URL from the environment / .env, the same URL the app uses.
"""

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy.engine import Connection

from database import DATABASE_URL, MESSAGE_INDEXES, Base, engine
import models  # noqa: F401  (registers every table on Base.metadata, for --autogenerate)

config = context.config
if config.config_file_name is not None and not config.attributes.get("connection"):
    fileConfig(config.config_file_name, disable_existing_loggers=False)

target_metadata = Base.metadata

# Indexes created by raw SQL in database.py (one is a partial unique index the models can't
# describe). Autogenerate must not see them as "not in the models" and write DROP INDEX for them:
# uq_messages_sender_client_msg is what stops a retried send from being stored twice.
MANUAL_INDEXES = {name for name, _ in MESSAGE_INDEXES}


def include_object(obj, name, type_, reflected, compare_to) -> bool:
    return not (type_ == "index" and name in MANUAL_INDEXES)


def _configure(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        include_object=include_object,
        render_as_batch=connection.dialect.name == "sqlite",  # SQLite can't ALTER most things in place
    )


def run_migrations_offline() -> None:
    """`alembic upgrade head --sql`: prints the SQL instead of running it."""
    context.configure(
        url=DATABASE_URL, target_metadata=target_metadata, literal_binds=True, compare_type=True,
        include_object=include_object,
    )
    with context.begin_transaction():
        context.run_migrations()


def _run_sync(connection: Connection) -> None:
    _configure(connection)
    with context.begin_transaction():
        context.run_migrations()


async def _run_async() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(_run_sync)


def run_migrations_online() -> None:
    connection = config.attributes.get("connection")
    if connection is not None:  # called from the app, inside its own transaction
        _run_sync(connection)
    else:  # the alembic command line
        asyncio.run(_run_async())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
