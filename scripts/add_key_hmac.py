#!/usr/bin/env python3
"""One-off script to add `key_hmac` column and index to the `api_keys` table.

This script uses SQLAlchemy/SQLModel to connect to the database using the
DATABASE_URL environment variable or a default sqlite file (./gateway.db).

It will:
- Check whether `key_hmac` column exists; if not, add it (nullable TEXT).
- Create an index on key_hmac if it doesn't exist.

Note: For PostgreSQL, creating an index CONCURRENTLY is recommended in
production. This script uses a simple CREATE INDEX which may take a lock on
large tables. For production Postgres, prefer using Alembic with
CREATE INDEX CONCURRENTLY.
"""

import argparse
import asyncio
import os
import sys
from pathlib import Path

from sqlalchemy import inspect, text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

DEFAULT_SQLITE = f"sqlite:///{Path.home() / '.mcp' / 'gateway.db'}"


def get_db_url():
    return (
        os.environ.get("DATABASE_URL")
        or os.environ.get("MCP_GATEWAY_DATABASE_URL")
        or DEFAULT_SQLITE
    )


async def run(db_url: str, dry_run: bool = False):
    # Convert sqlite URL to async driver if necessary
    if db_url.startswith("sqlite://") and not db_url.startswith("sqlite+aiosqlite://"):
        async_url = db_url.replace("sqlite://", "sqlite+aiosqlite://", 1)
    else:
        async_url = db_url

    engine = create_async_engine(async_url, future=True)

    async with engine.begin() as conn:
        # Detect dialect to make specific choices
        dialect = conn.dialect.name
        print(f"Connected to DB dialect: {dialect}")

        # Check if table exists
        insp = inspect(conn.sync_engine)
        if not insp.has_table("api_keys"):
            print("Table 'api_keys' does not exist. Nothing to do.")
            return

        # Column exists?
        columns = [c[1]["name"] for c in insp.get_columns("api_keys")]
        if "key_hmac" in columns:
            print("Column 'key_hmac' already exists on api_keys.")
        else:
            if dry_run:
                print("Would add column 'key_hmac' to api_keys")
            else:
                print("Adding column 'key_hmac' to api_keys...")
                # SQLite supports simple ALTER ADD COLUMN
                try:
                    await conn.execute(
                        text("ALTER TABLE api_keys ADD COLUMN key_hmac TEXT")
                    )
                    print("Added column key_hmac")
                except Exception as e:
                    print("Failed to add column key_hmac:", e)
                    raise

        # Create index if missing
        indexes = [idx["name"] for idx in insp.get_indexes("api_keys")]
        idx_name = "ix_api_keys_key_hmac"
        if idx_name in indexes:
            print(f"Index {idx_name} already exists")
        else:
            if dry_run:
                print(f"Would create index {idx_name} on api_keys(key_hmac)")
            else:
                print(f"Creating index {idx_name} on api_keys(key_hmac)...")
                try:
                    await conn.execute(
                        text(f"CREATE INDEX {idx_name} ON api_keys(key_hmac)")
                    )
                    print("Index created")
                except Exception as e:
                    print("Failed to create index:", e)
                    raise

    await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", help="Database URL to use (overrides env)")
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be done"
    )
    args = parser.parse_args()

    db_url = args.db or get_db_url()
    print("Using DB URL:", db_url)

    try:
        asyncio.run(run(db_url, dry_run=args.dry_run))
    except Exception as e:
        print("Error:", e)
        sys.exit(2)
    print("Done")
