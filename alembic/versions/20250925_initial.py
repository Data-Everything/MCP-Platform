"""initial migration - create all tables

Revision ID: 20250925_initial
Revises: 
Create Date: 2025-09-25
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20250925_initial'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    # Import models and create all tables using SQLModel metadata. We
    # use the bind from Alembic's op to run the creation on the current
    # connection; this keeps the migration database-agnostic.
    from sqlmodel import SQLModel
    # Importing application models ensures metadata is populated
    from mcp_platform.gateway import models as _models  # noqa: F401

    bind = op.get_bind()
    SQLModel.metadata.create_all(bind=bind)


def downgrade():
    from sqlmodel import SQLModel
    from mcp_platform.gateway import models as _models  # noqa: F401

    bind = op.get_bind()
    SQLModel.metadata.drop_all(bind=bind)
