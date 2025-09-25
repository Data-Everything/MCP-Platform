"""add key_hmac column and index

Revision ID: 20250925_add_key_hmac
Revises: 
Create Date: 2025-09-25
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20250925_add_key_hmac'
down_revision = '20250925_initial'
branch_labels = None
depends_on = None


def upgrade():
    # Add nullable column
    op.add_column('api_keys', sa.Column('key_hmac', sa.String(length=128), nullable=True))

    # Create index. For PostgreSQL we prefer to create the index CONCURRENTLY
    # to avoid locking the table for writes; creating/dropping an index
    # CONCURRENTLY must be done outside of a transaction, so we use
    # autocommit_block for the PostgreSQL path. For other dialects (sqlite,
    # mysql, etc.) a regular index creation is fine.
    bind = op.get_bind()
    dialect_name = getattr(bind, 'dialect', None)
    dialect_name = dialect_name.name if dialect_name is not None else None

    if dialect_name == 'postgresql':
        # autocommit_block ensures the CREATE INDEX CONCURRENTLY runs
        # outside the surrounding transaction which PostgreSQL requires.
        with op.get_context().autocommit_block():
            op.create_index('ix_api_keys_key_hmac', 'api_keys', ['key_hmac'], unique=False, postgresql_concurrently=True)
    else:
        op.create_index('ix_api_keys_key_hmac', 'api_keys', ['key_hmac'], unique=False)


def downgrade():
    bind = op.get_bind()
    dialect_name = getattr(bind, 'dialect', None)
    dialect_name = dialect_name.name if dialect_name is not None else None

    if dialect_name == 'postgresql':
        with op.get_context().autocommit_block():
            op.drop_index('ix_api_keys_key_hmac', table_name='api_keys', postgresql_concurrently=True)
    else:
        op.drop_index('ix_api_keys_key_hmac', table_name='api_keys')

    op.drop_column('api_keys', 'key_hmac')
