from logging.config import fileConfig
import os
from sqlalchemy import engine_from_config
from sqlalchemy import pool
from alembic import context
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import create_async_engine
import asyncio

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config
fileConfig(config.config_file_name)

# Interpret the config file for Python logging.
# This line sets up loggers basically.

# add your model's MetaData object here
# for 'autogenerate' support
from sqlmodel import SQLModel
from mcp_platform.gateway.models import APIKey  # ensure models are imported

# target_metadata = SQLModel.metadata

def get_url():
    # Default to a local sqlite file in the user's home under ~/.mcp when
    # DATABASE_URL is not provided.
    from pathlib import Path

    default_db = Path.home() / ".mcp" / "gateway.db"
    return os.getenv("DATABASE_URL", f"sqlite:///{default_db}")

config.set_main_option("sqlalchemy.url", get_url())

target_metadata = SQLModel.metadata


def run_migrations_offline():
    url = config.get_main_option("sqlalchemy.url")
    context.configure(url=url, target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    # Support both sync and async database URLs. If an async driver is used
    # (e.g. postgresql+asyncpg or sqlite+aiosqlite), create an async engine
    # and run migrations using run_sync; otherwise use the regular
    # engine_from_config flow.
    url = config.get_main_option("sqlalchemy.url")
    sa_url = make_url(url)

    is_async = sa_url.drivername.endswith('+asyncpg') or sa_url.drivername.endswith('+aiosqlite') or 'async' in sa_url.drivername

    if is_async:
        # For async URLs, use the async engine and run migrations in a sync
        # function using run_sync.
        async_url = url
        async_engine = create_async_engine(async_url, poolclass=pool.NullPool)

        async def do_migrations():
            async with async_engine.connect() as connection:
                await connection.run_sync(lambda sync_conn: context.configure(connection=sync_conn, target_metadata=target_metadata) or None)
                # Run migrations inside a synchronous context provided by
                # run_sync. Alembic's context.run_migrations must be called in
                # that synchronous context.
                await connection.run_sync(lambda sync_conn: context.run_migrations())

        asyncio.get_event_loop().run_until_complete(do_migrations())
        async_engine.dispose()
    else:
        connectable = engine_from_config(
            config.get_section(config.config_ini_section),
            prefix="sqlalchemy.",
            poolclass=pool.NullPool,
        )

        with connectable.connect() as connection:
            context.configure(connection=connection, target_metadata=target_metadata)
            with context.begin_transaction():
                context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
