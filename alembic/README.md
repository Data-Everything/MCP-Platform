Alembic migrations for MCP Gateway

Run migrations (example):

# Install alembic
pip install alembic

# Run the migration against the DB specified in DATABASE_URL (or default in alembic.ini)
export DATABASE_URL="sqlite:///./gateway.db"
alembic upgrade head

Notes:
- For PostgreSQL in production, prefer CREATE INDEX CONCURRENTLY for large tables.
- Adjust alembic.ini sqlalchemy.url or set DATABASE_URL env var.
