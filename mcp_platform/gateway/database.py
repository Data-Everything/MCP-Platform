"""Database management for the MCP Platform Gateway."""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator, List, Optional

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel
import asyncio

from .models import (
    APIKey,
    APIKeyCreate,
    GatewayConfig,
    LoadBalancerConfig,
    ServerInstance,
    ServerInstanceCreate,
    ServerTemplate,
    User,
    UserCreate,
)

logger = logging.getLogger(__name__)

# Database URL constants
SQLITE_PREFIX = "sqlite://"
SQLITE_PREFIX_FULL = "sqlite:///"
POSTGRESQL_PREFIX = "postgresql://"
MYSQL_PREFIX = "mysql://"
ORACLE_PREFIX = "oracle://"
MSSQL_PREFIX = "mssql://"


def _validate_postgresql_driver():
    """Validate PostgreSQL driver availability."""
    try:
        import asyncpg  # noqa: F401
    except ImportError:
        raise ImportError(
            "PostgreSQL support requires asyncpg. Install with: "
            "pip install mcp-platform[postgresql]"
        )


def _validate_mysql_driver():
    """Validate MySQL driver availability."""
    try:
        import aiomysql  # noqa: F401
    except ImportError:
        raise ImportError(
            "MySQL support requires aiomysql. Install with: "
            "pip install mcp-platform[mysql]"
        )


def _validate_oracle_driver():
    """Validate Oracle driver availability."""
    try:
        import cx_Oracle_async  # noqa: F401
    except ImportError:
        raise ImportError(
            "Oracle support requires cx_Oracle_async. Install with: "
            "pip install mcp-platform[oracle]"
        )


def _validate_mssql_driver():
    """Validate SQL Server driver availability."""
    try:
        import aioodbc  # noqa: F401
    except ImportError:
        raise ImportError(
            "SQL Server support requires aioodbc. Install with: "
            "pip install mcp-platform[mssql]"
        )


def _convert_database_url_and_validate_driver(db_url: str) -> str:
    """Convert database URL to async format and validate driver availability."""
    if db_url.startswith(SQLITE_PREFIX_FULL):
        return db_url.replace(SQLITE_PREFIX_FULL, "sqlite+aiosqlite:///")
    elif db_url.startswith(SQLITE_PREFIX):
        return db_url.replace(SQLITE_PREFIX, "sqlite+aiosqlite://")
    elif db_url.startswith(POSTGRESQL_PREFIX):
        _validate_postgresql_driver()
        return db_url.replace(POSTGRESQL_PREFIX, "postgresql+asyncpg://")
    elif db_url.startswith("postgresql+"):
        _validate_postgresql_driver()
        return db_url
    elif db_url.startswith(MYSQL_PREFIX):
        _validate_mysql_driver()
        return db_url.replace(MYSQL_PREFIX, "mysql+aiomysql://")
    elif db_url.startswith("mysql+"):
        _validate_mysql_driver()
        return db_url
    elif db_url.startswith(ORACLE_PREFIX):
        _validate_oracle_driver()
        return db_url.replace(ORACLE_PREFIX, "oracle+cx_oracle_async://")
    elif db_url.startswith("oracle+"):
        _validate_oracle_driver()
        return db_url
    elif db_url.startswith(MSSQL_PREFIX):
        _validate_mssql_driver()
        return db_url.replace(MSSQL_PREFIX, "mssql+aioodbc://")
    elif db_url.startswith("mssql+"):
        _validate_mssql_driver()
        return db_url

    # Return original URL if no conversion needed
    return db_url


class DatabaseManager:
    """Manages database connections and operations."""

    def __init__(self, config: GatewayConfig):
        self.config = config
        self.engine: Optional[AsyncEngine] = None
        self.session_factory: Optional[sessionmaker] = None
        self._initialized = False

    async def initialize(self):
        """Initialize database connection and create tables."""
        if self._initialized:
            return

        try:
            # Convert database URL to async format and validate driver availability
            db_url = _convert_database_url_and_validate_driver(self.config.database.url)

            self.engine = create_async_engine(
                db_url,
                echo=self.config.database.echo,
                pool_size=self.config.database.pool_size,
                max_overflow=self.config.database.max_overflow,
                future=True,
            )

        except Exception as e:
            logger.error(f"Failed to create database engine: {e}")
            raise

        self.session_factory = async_sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

        # Only set up the engine and session factory here. We do NOT
        # apply Alembic migrations automatically at startup. The CLI
        # (`db-init`) should be used to apply migrations explicitly by
        # an operator. This avoids surprising schema changes during
        # process start in production environments.
        self._initialized = True
        logger.info("Database engine and session factory initialized (migrations not applied automatically)")

    async def _create_tables(self):
        """Create database tables."""
        async with self.engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.create_all)

    async def _apply_alembic_migrations(self):
        """Attempt to run Alembic migrations against the configured DB.

        This imports alembic.command and points Alembic at the same
        database URL used by the running application. It runs in a
        thread-safe manner by invoking Alembic's command API.
        """
        # Keep this method for backwards compatibility. Prefer using
        # apply_migrations() which is the public API used by the CLI.
        return await self.apply_migrations()

    async def migrations_applied(self) -> bool:
        """Return True if the database has Alembic migrations applied up to head.

        This checks the `alembic_version` table for the currently applied
        revision and compares it to the repository head(s). If Alembic is not
        installed or the DB does not contain the alembic_version table, this
        returns False.
        """
        try:
            import alembic.config as alembic_config
            from alembic.script import ScriptDirectory
        except Exception:
            # Alembic not available -> treat as not migrated
            return False

        alembic_cfg = alembic_config.Config("alembic.ini")
        alembic_cfg.set_main_option("sqlalchemy.url", self.config.database.url)

        sd = ScriptDirectory.from_config(alembic_cfg)
        heads = sd.get_heads() or []
        if not heads:
            return False

        # Read alembic_version from the DB
        try:
            async with self.get_session() as session:
                result = await session.execute(text("SELECT version_num FROM alembic_version"))
                row = result.scalar_one_or_none()
        except Exception:
            # Table missing or query failed => not migrated
            return False

        return bool(row) and row in heads

    async def apply_migrations(self):
        """Apply Alembic migrations (upgrade to head) for the configured DB.

        This uses Alembic's command API in a thread executor since Alembic
        performs blocking I/O and uses its own engine/connection lifecycle.
        """
        try:
            import alembic.config as alembic_config
            import alembic.command as alembic_command
        except Exception as e:
            raise RuntimeError("Alembic is not available in this environment") from e

        alembic_cfg = alembic_config.Config("alembic.ini")
        db_url = self.config.database.url
        alembic_cfg.set_main_option("sqlalchemy.url", db_url)

        loop = asyncio.get_event_loop()

        def run_upgrade():
            alembic_command.upgrade(alembic_cfg, "head")

        await loop.run_in_executor(None, run_upgrade)

    async def close(self):
        """Close database connections."""
        if self.engine:
            await self.engine.dispose()
            self._initialized = False
            logger.info("Database connections closed")

    async def health_check(self) -> bool:
        """Check database health by performing a simple query."""
        try:
            if not self._initialized:
                return False

            async with self.get_session() as session:
                # Simple query to test database connectivity
                await session.execute(text("SELECT 1"))
                return True
        except Exception as e:
            logger.warning(f"Database health check failed: {e}")
            return False

    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get database session."""
        if not self._initialized:
            await self.initialize()

        async with self.session_factory() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    # Server operations
    async def create_server_instance(self, server_data: dict) -> ServerInstance:
        """Create a new server instance."""
        async with self.get_session() as session:
            server = ServerInstance(**server_data)
            session.add(server)
            await session.commit()
            await session.refresh(server)
            return server

    async def get_server_instance(self, server_id: str) -> Optional[ServerInstance]:
        """Get server instance by ID."""
        async with self.get_session() as session:
            result = await session.get(ServerInstance, server_id)
            return result

    async def get_server_instances(
        self, skip: int = 0, limit: int = 100
    ) -> List[ServerInstance]:
        """Get all server instances with pagination."""
        async with self.get_session() as session:
            from sqlalchemy import select

            result = await session.execute(
                select(ServerInstance).offset(skip).limit(limit)
            )
            return result.scalars().all()

    async def update_server_instance(
        self, server_id: str, server_data: dict
    ) -> Optional[ServerInstance]:
        """Update server instance."""
        async with self.get_session() as session:
            server = await session.get(ServerInstance, server_id)
            if server:
                for key, value in server_data.items():
                    setattr(server, key, value)
                await session.commit()
                await session.refresh(server)
            return server

    async def delete_server_instance(self, server_id: str) -> bool:
        """Delete server instance."""
        async with self.get_session() as session:
            server = await session.get(ServerInstance, server_id)
            if server:
                await session.delete(server)
                await session.commit()
                return True
            return False

    # Template operations
    async def create_server_template(self, template_data: dict) -> ServerTemplate:
        """Create a new server template."""
        async with self.get_session() as session:
            template = ServerTemplate(**template_data)
            session.add(template)
            await session.commit()
            await session.refresh(template)
            return template

    async def get_server_template(self, template_id: str) -> Optional[ServerTemplate]:
        """Get server template by ID."""
        async with self.get_session() as session:
            result = await session.get(ServerTemplate, template_id)
            return result

    async def get_server_templates(
        self, skip: int = 0, limit: int = 100
    ) -> List[ServerTemplate]:
        """Get all server templates with pagination."""
        async with self.get_session() as session:
            from sqlalchemy import select

            result = await session.execute(
                select(ServerTemplate).offset(skip).limit(limit)
            )
            return result.scalars().all()

    async def update_server_template(
        self, template_id: str, template_data: dict
    ) -> Optional[ServerTemplate]:
        """Update server template."""
        async with self.get_session() as session:
            template = await session.get(ServerTemplate, template_id)
            if template:
                for key, value in template_data.items():
                    setattr(template, key, value)
                await session.commit()
                await session.refresh(template)
            return template

    async def delete_server_template(self, template_id: str) -> bool:
        """Delete server template."""
        async with self.get_session() as session:
            template = await session.get(ServerTemplate, template_id)
            if template:
                await session.delete(template)
                await session.commit()
                return True
            return False

    # User operations
    async def create_user(self, user_data: dict) -> User:
        """Create a new user."""
        async with self.get_session() as session:
            user = User(**user_data)
            session.add(user)
            await session.commit()
            await session.refresh(user)
            return user

    async def get_user(self, user_id: str) -> Optional[User]:
        """Get user by ID."""
        async with self.get_session() as session:
            result = await session.get(User, user_id)
            return result

    async def get_users(self, skip: int = 0, limit: int = 100) -> List[User]:
        """Get all users with pagination."""
        async with self.get_session() as session:
            from sqlalchemy import select

            result = await session.execute(select(User).offset(skip).limit(limit))
            return result.scalars().all()

    async def update_user(self, user_id: str, user_data: dict) -> Optional[User]:
        """Update user."""
        async with self.get_session() as session:
            user = await session.get(User, user_id)
            if user:
                for key, value in user_data.items():
                    setattr(user, key, value)
                await session.commit()
                await session.refresh(user)
            return user

    async def delete_user(self, user_id: str) -> bool:
        """Delete user."""
        async with self.get_session() as session:
            user = await session.get(User, user_id)
            if user:
                await session.delete(user)
                await session.commit()
                return True
            return False

    # API Key operations
    async def create_api_key(self, api_key_data: dict) -> APIKey:
        """Create a new API key."""
        async with self.get_session() as session:
            api_key = APIKey(**api_key_data)
            session.add(api_key)
            await session.commit()
            await session.refresh(api_key)
            return api_key

    async def get_api_key(self, key_id: str) -> Optional[APIKey]:
        """Get API key by ID."""
        async with self.get_session() as session:
            result = await session.get(APIKey, key_id)
            return result

    async def get_api_key_by_hmac(self, key_hmac: str) -> Optional[APIKey]:
        """Get API key by keyed HMAC value."""
        async with self.get_session() as session:
            from sqlalchemy import select

            result = await session.execute(
                select(APIKey).where(APIKey.key_hmac == key_hmac)
            )
            return result.scalar_one_or_none()

    async def get_api_keys(self, skip: int = 0, limit: int = 100) -> List[APIKey]:
        """Get all API keys with pagination."""
        async with self.get_session() as session:
            from sqlalchemy import select

            result = await session.execute(select(APIKey).offset(skip).limit(limit))
            return result.scalars().all()

    async def update_api_key(self, key_id: str, api_key_data: dict) -> Optional[APIKey]:
        """Update API key."""
        async with self.get_session() as session:
            api_key = await session.get(APIKey, key_id)
            if api_key:
                for key, value in api_key_data.items():
                    setattr(api_key, key, value)
                await session.commit()
                await session.refresh(api_key)
            return api_key

    async def delete_api_key(self, key_id: str) -> bool:
        """Delete API key."""
        async with self.get_session() as session:
            api_key = await session.get(APIKey, key_id)
            if api_key:
                await session.delete(api_key)
                await session.commit()
                return True
            return False

    # Load balancer config operations
    async def create_load_balancer_config(self, lb_data: dict) -> LoadBalancerConfig:
        """Create a new load balancer config."""
        async with self.get_session() as session:
            load_balancer = LoadBalancerConfig(**lb_data)
            session.add(load_balancer)
            await session.commit()
            await session.refresh(load_balancer)
            return load_balancer

    async def get_load_balancer_config(
        self, lb_id: str
    ) -> Optional[LoadBalancerConfig]:
        """Get load balancer config by ID."""
        async with self.get_session() as session:
            result = await session.get(LoadBalancerConfig, lb_id)
            return result

    async def get_load_balancer_configs(
        self, skip: int = 0, limit: int = 100
    ) -> List[LoadBalancerConfig]:
        """Get all load balancer configs with pagination."""
        async with self.get_session() as session:
            from sqlalchemy import select

            result = await session.execute(
                select(LoadBalancerConfig).offset(skip).limit(limit)
            )
            return result.scalars().all()

    async def update_load_balancer_config(
        self, lb_id: str, lb_data: dict
    ) -> Optional[LoadBalancerConfig]:
        """Update load balancer config."""
        async with self.get_session() as session:
            load_balancer = await session.get(LoadBalancerConfig, lb_id)
            if load_balancer:
                for key, value in lb_data.items():
                    setattr(load_balancer, key, value)
                await session.commit()
                await session.refresh(load_balancer)
            return load_balancer

    async def delete_load_balancer_config(self, lb_id: str) -> bool:
        """Delete load balancer config."""
        async with self.get_session() as session:
            load_balancer = await session.get(LoadBalancerConfig, lb_id)
            if load_balancer:
                await session.delete(load_balancer)
                await session.commit()
                return True
            return False


class ServerInstanceCRUD:
    """CRUD operations for server instances."""

    def __init__(self, db: DatabaseManager):
        self.db = db

    async def create(self, server_data: ServerInstanceCreate) -> ServerInstance:
        """Create a new server instance."""
        server_dict = server_data.model_dump()
        return await self.create_server_instance(server_dict)

    async def create_server_instance(self, server_data: dict) -> ServerInstance:
        """Create a new server instance."""
        return await self.db.create_server_instance(server_data)

    async def get_server_instance(self, server_id: str) -> Optional[ServerInstance]:
        """Get server instance by ID."""
        return await self.db.get_server_instance(server_id)

    async def get_active(self) -> List[ServerInstance]:
        """Get all active server instances."""
        async with self.db.get_session() as session:

            result = await session.execute(
                select(ServerInstance).where(ServerInstance.is_active is True)
            )
            return result.scalars().all()

    async def get_server_instances(
        self, skip: int = 0, limit: int = 100
    ) -> List[ServerInstance]:
        """Get all server instances."""
        return await self.db.get_server_instances(skip, limit)

    async def update_server_instance(
        self, server_id: str, server_data: dict
    ) -> Optional[ServerInstance]:
        """Update server instance."""
        return await self.db.update_server_instance(server_id, server_data)

    async def delete_server_instance(self, server_id: str) -> bool:
        """Delete server instance."""
        return await self.db.delete_server_instance(server_id)

    async def get_all(self) -> List[ServerInstance]:
        """Get all server instances."""
        return await self.get_server_instances()

    async def list_all(self) -> List[ServerInstance]:
        """Get all server instances (alias for get_all)."""
        return await self.get_all()

    async def get_by_template(self, template_name: str) -> List[ServerInstance]:
        """Get server instances by template name."""
        async with self.db.get_session() as session:
            from sqlalchemy import select

            result = await session.execute(
                select(ServerInstance).where(
                    ServerInstance.template_name == template_name
                )
            )
            return result.scalars().all()

    async def get_healthy_by_template(self, template_name: str) -> List[ServerInstance]:
        """Get healthy server instances by template name."""
        async with self.db.get_session() as session:
            from sqlalchemy import select

            result = await session.execute(
                select(ServerInstance).where(
                    (ServerInstance.template_name == template_name)
                    & (ServerInstance.status == "healthy")
                )
            )
            return result.scalars().all()


class ServerTemplateCRUD:
    """CRUD operations for server templates."""

    def __init__(self, db: DatabaseManager):
        self.db = db

    async def create(self, template_data: dict) -> ServerTemplate:
        """Create a new server template."""
        return await self.create_server_template(template_data)

    async def create_server_template(self, template_data: dict) -> ServerTemplate:
        """Create a new server template."""
        return await self.db.create_server_template(template_data)

    async def get_server_template(self, template_id: str) -> Optional[ServerTemplate]:
        """Get server template by ID."""
        return await self.db.get_server_template(template_id)

    async def get_all(self) -> List[ServerTemplate]:
        """Get all server templates."""
        return await self.get_server_templates()

    async def list_all(self) -> List[ServerTemplate]:
        """Get all server templates (alias for get_all)."""
        return await self.get_all()

    async def get_server_templates(
        self, skip: int = 0, limit: int = 100
    ) -> List[ServerTemplate]:
        """Get all server templates."""
        return await self.db.get_server_templates(skip, limit)

    async def update_server_template(
        self, template_id: str, template_data: dict
    ) -> Optional[ServerTemplate]:
        """Update server template."""
        return await self.db.update_server_template(template_id, template_data)

    async def delete_server_template(self, template_id: str) -> bool:
        """Delete server template."""
        return await self.db.delete_server_template(template_id)


# Base CRUD class
class BaseCRUD:
    """Base CRUD operations."""

    def __init__(self, db: DatabaseManager):
        self.db = db


# CRUD classes for compatibility with existing tests and auth.py
class UserCRUD(BaseCRUD):
    """CRUD operations for users."""

    def __init__(self, db: DatabaseManager):
        super().__init__(db)

    async def create(self, user_data: UserCreate, hashed_password: str) -> User:
        """Create a new user with hashed password."""
        user_dict = user_data.model_dump()
        user_dict["hashed_password"] = hashed_password
        # Remove the plain password if it exists
        user_dict.pop("password", None)
        return await self.db.create_user(user_dict)

    async def create_user(self, user_data: dict) -> User:
        """Create a new user."""
        return await self.db.create_user(user_data)

    async def get_user(self, user_id: str) -> Optional[User]:
        """Get user by ID."""
        return await self.db.get_user(user_id)

    async def get_by_username(self, username: str) -> Optional[User]:
        """Get user by username."""
        return await self.get_user_by_username(username)

    async def get_user_by_username(self, username: str) -> Optional[User]:
        """Get user by username."""
        async with self.db.get_session() as session:
            from sqlalchemy import select

            result = await session.execute(
                select(User).where(User.username == username)
            )
            return result.scalar_one_or_none()

    async def get_user_by_email(self, email: str) -> Optional[User]:
        """Get user by email."""
        async with self.db.get_session() as session:
            from sqlalchemy import select

            result = await session.execute(select(User).where(User.email == email))
            return result.scalar_one_or_none()

    async def update(self, user_id: str, user_data: dict) -> Optional[User]:
        """Update user."""
        return await self.update_user(user_id, user_data)

    async def update_user(self, user_id: str, user_data: dict) -> Optional[User]:
        """Update user."""
        return await self.db.update_user(user_id, user_data)

    async def delete_user(self, user_id: str) -> bool:
        """Delete user."""
        return await self.db.delete_user(user_id)


class APIKeyCRUD(BaseCRUD):
    """CRUD operations for API keys."""

    def __init__(self, db: DatabaseManager):
        super().__init__(db)

    async def create(self, api_key_data: APIKeyCreate, key_hash: str) -> APIKey:
        """Create a new API key with hashed key."""
        # Convert to dict if it's a Pydantic model
        if hasattr(api_key_data, "model_dump"):
            api_key_dict = api_key_data.model_dump()
        elif hasattr(api_key_data, "dict"):
            api_key_dict = api_key_data.dict()
        else:
            api_key_dict = (
                dict(api_key_data)
                if not isinstance(api_key_data, dict)
                else api_key_data.copy()
            )

        api_key_dict["key_hash"] = key_hash
        return await self.db.create_api_key(api_key_dict)

    async def create_api_key(self, api_key_data: dict) -> APIKey:
        """Create a new API key."""
        return await self.db.create_api_key(api_key_data)

    async def get_api_key(self, key_id: str) -> Optional[APIKey]:
        """Get API key by ID."""
        return await self.db.get_api_key(key_id)

    async def get_by_user(self, user_id: str) -> List[APIKey]:
        """Get API keys by user ID."""
        async with self.db.get_session() as session:
            from sqlalchemy import select

            result = await session.execute(
                select(APIKey).where(APIKey.user_id == user_id)
            )
            return result.scalars().all()

    async def get_api_key_by_key(self, key: str) -> Optional[APIKey]:
        """Get API key by key value."""
        async with self.db.get_session() as session:
            from sqlalchemy import select

            result = await session.execute(select(APIKey).where(APIKey.key == key))
            return result.scalar_one_or_none()

    async def get_api_key_by_hmac(self, key_hmac: str) -> Optional[APIKey]:
        """Get API key by HMAC via DatabaseManager."""
        return await self.db.get_api_key_by_hmac(key_hmac)

    async def update(self, key_id: str, api_key_data: dict) -> Optional[APIKey]:
        """Update API key."""
        return await self.update_api_key(key_id, api_key_data)

    async def update_api_key(self, key_id: str, api_key_data: dict) -> Optional[APIKey]:
        """Update API key."""
        return await self.db.update_api_key(key_id, api_key_data)

    async def delete_api_key(self, key_id: str) -> bool:
        """Delete API key."""
        return await self.db.delete_api_key(key_id)


# Global database manager instance
_database_manager: Optional[DatabaseManager] = None


def get_database() -> DatabaseManager:
    """Get global database manager."""
    if _database_manager is None:
        raise RuntimeError(
            "Database not initialized. Call initialize_database() first."
        )
    return _database_manager


async def initialize_database(config: GatewayConfig) -> DatabaseManager:
    """Initialize global database manager."""
    global _database_manager
    _database_manager = DatabaseManager(config)
    await _database_manager.initialize()
    return _database_manager


async def close_database():
    """Close global database manager."""
    global _database_manager
    if _database_manager:
        await _database_manager.close()
        _database_manager = None
