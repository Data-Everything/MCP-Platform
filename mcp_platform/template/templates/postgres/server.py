#!/usr/bin/env python3
"""
PostgreSQL MCP Server - Production-ready implementation.

A secure PostgreSQL MCP server that provides controlled access to PostgreSQL
databases with configurable authentication, read-only mode, SSH tunneling,
and comprehensive query execution capabilities using FastMCP and SQLAlchemy.
"""

import logging
import os
import re
import sys
import time
from contextlib import contextmanager
from typing import Any, Dict, Optional, Tuple

import sqlparse
from fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from .config import PostgresServerConfig
except ImportError:
    try:
        from config import PostgresServerConfig
    except ImportError:
        # Fallback for Docker or direct script execution
        sys.path.append(os.path.dirname(__file__))
        from config import PostgresServerConfig

# PostgreSQL/SQLAlchemy imports
try:
    import psycopg
    from sqlalchemy import create_engine, inspect, text
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import SQLAlchemyError
except ImportError:
    # Check if we're in test mode
    if "pytest" in sys.modules or any("test" in module for module in sys.modules):
        # In test mode, create mock objects
        import types

        psycopg = types.ModuleType("psycopg")
        psycopg.Error = Exception
        psycopg.OperationalError = Exception
        psycopg.ProgrammingError = Exception

        def create_engine(*args, **kwargs):
            """Mock create_engine for testing."""

            class MockResult:
                def __init__(self):
                    self.returns_rows = True

                def fetchall(self):
                    return []

                def fetchone(self):
                    return None

                def keys(self):
                    return []

                @property
                def rowcount(self):
                    return 0

            class MockConnection:
                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

                def execute(self, query, *args, **kwargs):
                    return MockResult()

            class MockEngine:
                def connect(self):
                    return MockConnection()

                def dispose(self):
                    return None

                url = type(
                    "MockURL",
                    (),
                    {
                        "database": "test",
                        "host": "localhost",
                        "port": 5432,
                        "username": "testuser",
                        "password": None,
                    },
                )()

            return MockEngine()

        def text(query):
            """Mock text function."""
            return query

        def inspect(engine):
            """Mock inspect function."""

            class MockInspector:
                def get_schema_names(self):
                    return ["public"]

                def get_table_names(self, schema=None):
                    return ["test_table"]

                def get_columns(self, table, schema=None):
                    return [
                        {"name": "id", "type": "INTEGER", "nullable": False},
                        {"name": "name", "type": "VARCHAR", "nullable": True},
                    ]

            return MockInspector()

        Engine = type
        SQLAlchemyError = Exception
    else:
        logger.error(
            "PostgreSQL and SQLAlchemy libraries are not installed. "
            "Please install with: pip install psycopg-binary sqlalchemy"
        )
        sys.exit(1)

# SSH tunnel support
try:
    from sshtunnel import SSHTunnelForwarder
except ImportError:
    # Create a mock for testing or when SSH tunnel is not available
    if "pytest" in sys.modules or any("test" in module for module in sys.modules):

        class SSHTunnelForwarder:
            def __init__(self, *args, **kwargs):
                self.local_bind_port = 5432

            def start(self):
                pass

            def stop(self):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

    else:
        logger.warning(
            "sshtunnel library not installed. SSH tunnel functionality will be disabled. "
            "Install with: pip install sshtunnel"
        )
        SSHTunnelForwarder = None


class PostgresMCPServer:
    """
    PostgreSQL MCP Server implementation using FastMCP and SQLAlchemy.

    This server provides secure access to PostgreSQL databases with:
    - Multiple authentication methods (password, certificate, peer, etc.)
    - Read-only mode enforcement with configurable override
    - SSH tunneling for secure remote connections
    - Comprehensive query execution and schema inspection tools
    - SSL/TLS connection support
    """

    def __init__(self, config_dict: dict = None, skip_validation: bool = False):
        """Initialize the PostgreSQL MCP Server with configuration."""
        self._skip_validation = skip_validation
        self.config = PostgresServerConfig(
            config_dict=config_dict or {}, skip_validation=skip_validation
        )

        # Get configuration
        self.config_data = self.config.get_template_config()
        self.template_data = self.config.get_template_data()

        self.logger = logging.getLogger(__name__)
        self.engine: Optional[Engine] = None
        self.ssh_tunnel: Optional[SSHTunnelForwarder] = None
        self.version = self.template_data.get("version", "1.0.0")

        # Initialize FastMCP
        self.mcp = FastMCP(
            name=self.template_data.get("name", "postgres-server"),
            instructions="PostgreSQL database server for secure data access",
            version=self.version,
        )

        # Register tools
        self._register_tools()

        self.logger.info("PostgreSQL MCP server %s created", self.mcp.name)

    def _register_tools(self):
        """Register all MCP tools."""
        # Schema exploration tools
        self.mcp.tool(
            self.list_databases,
            name="list_databases",
            description="List all databases on the PostgreSQL server",
        )

        self.mcp.tool(
            self.list_schemas,
            name="list_schemas",
            description="List all accessible database schemas for a given database",
        )

        self.mcp.tool(
            self.list_tables,
            name="list_tables",
            description="List tables in a specific schema",
        )

        self.mcp.tool(
            self.describe_table,
            name="describe_table",
            description="Get detailed schema information for a table",
        )

        self.mcp.tool(
            self.list_columns,
            name="list_columns",
            description="List columns in a specific table",
        )

        # Query execution tools
        self.mcp.tool(
            self.execute_query,
            name="execute_query",
            description="Execute a SQL query against PostgreSQL (subject to read-only restrictions)",
        )

        self.mcp.tool(
            self.explain_query,
            name="explain_query",
            description="Get query execution plan for a SQL query",
        )

        # Database info tools
        self.mcp.tool(
            self.get_database_info,
            name="get_database_info",
            description="Get information about the PostgreSQL database",
        )

        self.mcp.tool(
            self.get_table_stats,
            name="get_table_stats",
            description="Get statistics for a specific table",
        )

        # Index and constraint tools
        self.mcp.tool(
            self.list_indexes,
            name="list_indexes",
            description="List indexes for a specific table",
        )

        self.mcp.tool(
            self.list_constraints,
            name="list_constraints",
            description="List constraints for a specific table",
        )

        # Connection management
        self.mcp.tool(
            self.test_connection,
            name="test_connection",
            description="Test the database connection",
        )

        self.mcp.tool(
            self.get_connection_info,
            name="get_connection_info",
            description="Get information about the current database connection",
        )

    @contextmanager
    def _get_connection(self):
        """Get a database connection with proper error handling."""
        if not self.engine:
            self._initialize_connection()

        try:
            # Use the engine.connect() context manager so tests that mock
            # __enter__/__exit__ behave correctly and real engines are
            # properly closed.
            with self.engine.connect() as connection:
                yield connection
        except SQLAlchemyError as e:
            self.logger.error("Database connection error: %s", e)
            raise
        except Exception as e:
            self.logger.error("Unexpected connection error: %s", e)
            raise

    def _initialize_connection(self):
        """Initialize the database connection and SSH tunnel if needed."""
        try:
            # Set up SSH tunnel if configured
            ssh_config = self.config.get_ssh_config()
            if ssh_config and SSHTunnelForwarder:
                self._setup_ssh_tunnel(ssh_config)

            # Create database connection
            connection_string = self.config.get_connection_string()

            # Modify connection string if SSH tunnel is active
            if self.ssh_tunnel:
                # Replace host and port with tunnel endpoint
                pattern = r"@[^:]+:\d+"
                replacement = f"@localhost:{self.ssh_tunnel.local_bind_port}"
                connection_string = re.sub(pattern, replacement, connection_string)

            self.engine = create_engine(
                connection_string,
                pool_pre_ping=True,
                pool_recycle=3600,
                connect_args={
                    "connect_timeout": self.config.get_template_config().get(
                        "connection_timeout", 10
                    ),
                },
            )

            # Test the connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            self.logger.info("Database connection established successfully")

        except Exception as e:
            self.logger.error("Failed to initialize database connection: %s", e)
            if self.ssh_tunnel:
                self.ssh_tunnel.stop()
                self.ssh_tunnel = None
            raise

    def _setup_ssh_tunnel(self, ssh_config: Dict[str, Any]):
        """Set up SSH tunnel for database connection."""
        try:
            if not SSHTunnelForwarder:
                raise ImportError("sshtunnel library not available")

            self.logger.info(
                "Setting up SSH tunnel to %s:%s",
                ssh_config["ssh_host"],
                ssh_config["ssh_port"],
            )

            # Prepare SSH authentication
            ssh_auth = {}
            if ssh_config["ssh_auth_method"] == "password":
                ssh_auth["ssh_password"] = ssh_config.get("ssh_password")
            elif ssh_config["ssh_auth_method"] == "key":
                ssh_auth["ssh_pkey"] = ssh_config.get("ssh_key_file")
                if ssh_config.get("ssh_key_passphrase"):
                    ssh_auth["ssh_private_key_password"] = ssh_config[
                        "ssh_key_passphrase"
                    ]

            self.ssh_tunnel = SSHTunnelForwarder(
                (ssh_config["ssh_host"], ssh_config["ssh_port"]),
                ssh_username=ssh_config["ssh_user"],
                remote_bind_address=(
                    ssh_config["remote_host"],
                    ssh_config["remote_port"],
                ),
                local_bind_address=("localhost", ssh_config["local_port"]),
                **ssh_auth,
            )

            self.ssh_tunnel.start()
            self.logger.info(
                "SSH tunnel established on local port %s",
                self.ssh_tunnel.local_bind_port,
            )

        except Exception as e:
            self.logger.error("Failed to establish SSH tunnel: %s", e)
            raise

    def _validate_query_safety(self, query: str) -> Tuple[bool, str]:
        """
        Validate if query is safe to execute based on read-only mode.

        Args:
            query: SQL query to validate

        Returns:
            Tuple of (is_safe, reason)
        """
        if not self.config.is_read_only():
            return True, "Write mode enabled"

        # Parse SQL to detect write operations
        try:
            parsed = sqlparse.parse(query)
            for statement in parsed:
                tokens = [
                    token for token in statement.flatten() if not token.is_whitespace
                ]
                if tokens:
                    first_value = tokens[0].value.upper()

                    # Check for write operations
                    write_operations = {
                        "INSERT",
                        "UPDATE",
                        "DELETE",
                        "CREATE",
                        "DROP",
                        "ALTER",
                        "TRUNCATE",
                        "REPLACE",
                        "MERGE",
                    }

                    if first_value in write_operations:
                        return (
                            False,
                            f"Write operation '{first_value}' not allowed in read-only mode",
                        )

                    # Check for functions that might modify data
                    query_upper = query.upper()
                    dangerous_functions = [
                        "NEXTVAL",
                        "SETVAL",
                        "PG_RELOAD_CONF",
                        "PG_ROTATE_LOGFILE",
                    ]

                    for func in dangerous_functions:
                        if func in query_upper:
                            return (
                                False,
                                f"Function '{func}' not allowed in read-only mode",
                            )

        except Exception as e:
            self.logger.warning("Could not parse query for safety check: %s", e)
            # If we can't parse, be conservative and allow only SELECT
            if not query.strip().upper().startswith("SELECT"):
                return False, "Only SELECT queries allowed when query parsing fails"

        return True, "Query appears safe"

    def _check_schema_access(self, schema: str) -> bool:
        """Check if access to schema is allowed."""
        allowed_schemas = self.config.get_allowed_schemas()

        if not allowed_schemas or allowed_schemas == "*":
            return True

        # Try regex match first
        try:
            if re.match(allowed_schemas, schema):
                return True
        except re.error:
            # Not a regex, fall through to comma-separated parsing
            pass

        # Treat as comma-separated list
        allowed_list = [s.strip() for s in allowed_schemas.split(",") if s.strip()]
        if "*" in allowed_list:
            return True
        return schema in allowed_list

    async def list_schemas(self, database: str = None) -> Dict[str, Any]:
        """List all accessible database schemas for the specified database.

        Args:
            database: Name of the database to inspect (optional)
        """
        temp_engine = None
        try:
            # Decide which engine to use:
            # - If a database override is provided and it differs from the current engine's database,
            #   create a temporary engine for that database.
            # - Otherwise, use the existing engine (initializing it if necessary).
            engine_to_use = None

            if database:
                current_db = None
                if self.engine and getattr(self.engine, "url", None) is not None:
                    try:
                        current_db = self.engine.url.database
                    except Exception:
                        current_db = None

                if self.engine and current_db == database:
                    engine_to_use = self.engine
                else:
                    # Create a temporary engine for the requested database
                    temp_conn_str = self.config.get_connection_string(
                        database_override=database
                    )
                    if not temp_conn_str:
                        raise RuntimeError(
                            "Connection string for requested database is empty"
                        )

                    temp_engine = create_engine(
                        temp_conn_str,
                        pool_pre_ping=True,
                        pool_recycle=3600,
                        connect_args={
                            "connect_timeout": self.config.get_template_config().get(
                                "connection_timeout", 10
                            ),
                        },
                    )
                    engine_to_use = temp_engine
            else:
                # No database override requested: ensure primary engine exists
                if not self.engine:
                    # Initialize the main connection if not already done
                    self._initialize_connection()
                engine_to_use = self.engine

            if engine_to_use is None:
                raise RuntimeError("No database engine available to inspect schemas")

            inspector = inspect(engine_to_use)
            all_schemas = inspector.get_schema_names()

            # Filter based on access control
            accessible_schemas = [
                schema for schema in all_schemas if self._check_schema_access(schema)
            ]

            # Determine reported database name
            reported_db = (
                database if database else getattr(engine_to_use.url, "database", None)
            )

            return {
                "database": reported_db,
                "schemas": accessible_schemas,
                "total_count": len(accessible_schemas),
                "filtered_count": len(all_schemas) - len(accessible_schemas),
            }

        except Exception as e:
            self.logger.error("Error listing schemas for database %s: %s", database, e)
            return {"error": f"Failed to list schemas: {str(e)}"}

        finally:
            if temp_engine:
                try:
                    temp_engine.dispose()
                except Exception:
                    pass

    async def list_databases(self) -> Dict[str, Any]:
        """List all databases on the server (subject to access controls).

        Returns:
            Dict with database names and counts.
        """
        try:
            with self._get_connection() as conn:
                # Query pg_database for non-template databases
                result = conn.execute(
                    text("SELECT datname FROM pg_database WHERE datistemplate = false;")
                )
                rows = result.fetchall()
                databases = [r[0] for r in rows]

                return {"databases": databases, "count": len(databases)}

        except Exception as e:
            self.logger.error("Error listing databases: %s", e)
            return {"error": f"Failed to list databases: {str(e)}"}

    async def list_tables(self, schema: str = "public") -> Dict[str, Any]:
        """List tables in a specific schema."""
        try:
            if not self._check_schema_access(schema):
                return {"error": f"Access denied to schema '{schema}'"}

            with self._get_connection():
                inspector = inspect(self.engine)
                tables = inspector.get_table_names(schema=schema)

                return {"schema": schema, "tables": tables, "count": len(tables)}

        except Exception as e:
            self.logger.error("Error listing tables in schema %s: %s", schema, e)
            return {"error": f"Failed to list tables: {str(e)}"}

    async def describe_table(
        self, table: str, schema: str = "public"
    ) -> Dict[str, Any]:
        """Get detailed schema information for a table."""
        try:
            if not self._check_schema_access(schema):
                return {"error": f"Access denied to schema '{schema}'"}

            with self._get_connection():
                inspector = inspect(self.engine)

                # Get table columns
                columns = inspector.get_columns(table, schema=schema)

                # Get primary keys
                primary_keys = inspector.get_pk_constraint(table, schema=schema)

                # Get foreign keys
                foreign_keys = inspector.get_foreign_keys(table, schema=schema)

                # Get indexes
                indexes = inspector.get_indexes(table, schema=schema)

                return {
                    "schema": schema,
                    "table": table,
                    "columns": columns,
                    "primary_keys": primary_keys,
                    "foreign_keys": foreign_keys,
                    "indexes": indexes,
                }

        except Exception as e:
            self.logger.error("Error describing table %s.%s: %s", schema, table, e)
            return {"error": f"Failed to describe table: {str(e)}"}

    async def list_columns(self, table: str, schema: str = "public") -> Dict[str, Any]:
        """List columns in a specific table."""
        try:
            if not self._check_schema_access(schema):
                return {"error": f"Access denied to schema '{schema}'"}

            with self._get_connection():
                inspector = inspect(self.engine)
                columns = inspector.get_columns(table, schema=schema)

                column_info = []
                for col in columns:
                    column_info.append(
                        {
                            "name": col["name"],
                            "type": str(col["type"]),
                            "nullable": col.get("nullable", True),
                            "default": col.get("default"),
                            "comment": col.get("comment"),
                        }
                    )

                return {
                    "schema": schema,
                    "table": table,
                    "columns": column_info,
                    "count": len(column_info),
                }

        except Exception as e:
            self.logger.error("Error listing columns for %s.%s: %s", schema, table, e)
            return {"error": f"Failed to list columns: {str(e)}"}

    async def execute_query(
        self, query: str, limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """Execute a SQL query against PostgreSQL."""
        try:
            # Validate query safety
            is_safe, reason = self._validate_query_safety(query)
            if not is_safe:
                return {"error": f"Query rejected: {reason}"}

            # Apply limit if not specified and it's a SELECT query
            if limit is None:
                limit = self.config.get_max_results()

            # Add LIMIT clause for SELECT queries if not present
            query_upper = query.strip().upper()
            if query_upper.startswith("SELECT") and "LIMIT " not in query_upper:
                query = f"{query.rstrip(';')} LIMIT {limit}"

            with self._get_connection() as conn:
                start_time = time.time()
                result = conn.execute(text(query))
                execution_time = time.time() - start_time

                # Handle different result types
                if result.returns_rows:
                    rows = result.fetchall()
                    columns = list(result.keys())

                    # Convert rows to dictionaries for JSON serialization
                    data = []
                    for row in rows:
                        data.append(dict(zip(columns, row)))

                    return {
                        "query": query,
                        "columns": columns,
                        "data": data,
                        "row_count": len(data),
                        "execution_time": round(execution_time, 3),
                        "limited": len(data) == limit,
                    }
                else:
                    # For non-SELECT queries (if write mode enabled)
                    return {
                        "query": query,
                        "rows_affected": result.rowcount,
                        "execution_time": round(execution_time, 3),
                    }

        except Exception as e:
            self.logger.error("Error executing query: %s", e)
            return {"error": f"Query execution failed: {str(e)}"}

    async def explain_query(self, query: str) -> Dict[str, Any]:
        """Get query execution plan for a SQL query."""
        try:
            explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"

            with self._get_connection() as conn:
                result = conn.execute(text(explain_query))
                plan = result.fetchone()[0]

                return {"query": query, "execution_plan": plan}

        except Exception as e:
            self.logger.error("Error explaining query: %s", e)
            return {"error": f"Query explain failed: {str(e)}"}

    async def get_database_info(self) -> Dict[str, Any]:
        """Get information about the PostgreSQL database."""
        try:
            with self._get_connection() as conn:
                # Get database version
                version_result = conn.execute(text("SELECT version()"))
                version = version_result.fetchone()[0]

                # Get database name
                db_name = self.engine.url.database

                # Get current user
                user_result = conn.execute(text("SELECT current_user"))
                current_user = user_result.fetchone()[0]

                # Get database size
                size_query = text(
                    """
                    SELECT pg_size_pretty(pg_database_size(current_database())) as size
                """
                )
                size_result = conn.execute(size_query)
                db_size = size_result.fetchone()[0]

                # Get connection info
                conn_info = {
                    "host": self.engine.url.host,
                    "port": self.engine.url.port,
                    "database": db_name,
                    "username": self.engine.url.username,
                }

                return {
                    "version": version,
                    "database": db_name,
                    "current_user": current_user,
                    "size": db_size,
                    "connection": conn_info,
                    "read_only_mode": self.config.is_read_only(),
                    "ssh_tunnel_active": self.ssh_tunnel is not None,
                }

        except Exception as e:
            self.logger.error("Error getting database info: %s", e)
            return {"error": f"Failed to get database info: {str(e)}"}

    async def get_table_stats(
        self, table: str, schema: str = "public"
    ) -> Dict[str, Any]:
        """Get statistics for a specific table."""
        try:
            if not self._check_schema_access(schema):
                return {"error": f"Access denied to schema '{schema}'"}

            with self._get_connection() as conn:
                # Get row count
                count_query = text(f"SELECT COUNT(*) FROM {schema}.{table}")
                count_result = conn.execute(count_query)
                row_count = count_result.fetchone()[0]

                # Get table size
                size_query = text(
                    """
                    SELECT
                        pg_size_pretty(pg_total_relation_size($1)) as total_size,
                        pg_size_pretty(pg_relation_size($1)) as table_size,
                        pg_size_pretty(pg_total_relation_size($1) - pg_relation_size($1)) as index_size
                """
                )
                size_result = conn.execute(size_query, f"{schema}.{table}")
                size_data = size_result.fetchone()

                return {
                    "schema": schema,
                    "table": table,
                    "row_count": row_count,
                    "total_size": size_data[0],
                    "table_size": size_data[1],
                    "index_size": size_data[2],
                }

        except Exception as e:
            self.logger.error(
                "Error getting table stats for %s.%s: %s", schema, table, e
            )
            return {"error": f"Failed to get table stats: {str(e)}"}

    async def list_indexes(self, table: str, schema: str = "public") -> Dict[str, Any]:
        """List indexes for a specific table."""
        try:
            if not self._check_schema_access(schema):
                return {"error": f"Access denied to schema '{schema}'"}

            with self._get_connection():
                inspector = inspect(self.engine)
                indexes = inspector.get_indexes(table, schema=schema)

                return {
                    "schema": schema,
                    "table": table,
                    "indexes": indexes,
                    "count": len(indexes),
                }

        except Exception as e:
            self.logger.error("Error listing indexes for %s.%s: %s", schema, table, e)
            return {"error": f"Failed to list indexes: {str(e)}"}

    async def list_constraints(
        self, table: str, schema: str = "public"
    ) -> Dict[str, Any]:
        """List constraints for a specific table."""
        try:
            if not self._check_schema_access(schema):
                return {"error": f"Access denied to schema '{schema}'"}

            with self._get_connection():
                inspector = inspect(self.engine)

                # Get primary key constraints
                pk_constraint = inspector.get_pk_constraint(table, schema=schema)

                # Get foreign key constraints
                fk_constraints = inspector.get_foreign_keys(table, schema=schema)

                # Get check constraints
                check_constraints = inspector.get_check_constraints(
                    table, schema=schema
                )

                # Get unique constraints
                unique_constraints = inspector.get_unique_constraints(
                    table, schema=schema
                )

                return {
                    "schema": schema,
                    "table": table,
                    "primary_key": pk_constraint,
                    "foreign_keys": fk_constraints,
                    "check_constraints": check_constraints,
                    "unique_constraints": unique_constraints,
                }

        except Exception as e:
            self.logger.error(
                "Error listing constraints for %s.%s: %s", schema, table, e
            )
            return {"error": f"Failed to list constraints: {str(e)}"}

    async def test_connection(self) -> Dict[str, Any]:
        """Test the database connection."""
        try:
            if not self.engine:
                self._initialize_connection()

            with self._get_connection() as conn:
                start_time = time.time()
                result = conn.execute(text("SELECT 1 as test"))
                response_time = time.time() - start_time

                test_value = result.fetchone()[0]

                return {
                    "status": "success",
                    "response_time": round(response_time, 3),
                    "test_query_result": test_value,
                    "ssh_tunnel_active": self.ssh_tunnel is not None,
                }

        except Exception as e:
            self.logger.error("Connection test failed: %s", e)
            return {"status": "failed", "error": str(e)}

    async def get_connection_info(self) -> Dict[str, Any]:
        """Get information about the current database connection."""
        try:
            connection_info = {
                "engine_initialized": self.engine is not None,
                "read_only_mode": self.config.is_read_only(),
                "max_results": self.config.get_max_results(),
                "query_timeout": self.config.get_query_timeout(),
                "ssh_tunnel_active": self.ssh_tunnel is not None,
                "allowed_schemas": self.config.get_allowed_schemas(),
            }

            if self.engine:
                # Build a masked database URL without relying on str(engine.url)
                try:
                    url_user = getattr(self.engine.url, "username", None)
                    url_host = getattr(self.engine.url, "host", None)
                    url_port = getattr(self.engine.url, "port", None)
                    url_db = getattr(self.engine.url, "database", None)
                    url_password = getattr(self.engine.url, "password", None)

                    if url_user and url_host:
                        if url_password:
                            masked = f"{url_user}:***@{url_host}:{url_port}/{url_db}"
                        else:
                            masked = f"{url_user}@{url_host}:{url_port}/{url_db}"
                    else:
                        masked = str(getattr(self.engine, "url", ""))

                except Exception:
                    masked = str(getattr(self.engine, "url", ""))

                connection_info.update(
                    {
                        "database_url": masked,
                        "pool_size": getattr(self.engine.pool, "size", None),
                        "checked_out_connections": getattr(
                            self.engine.pool, "checkedout", None
                        ),
                    }
                )

            if self.ssh_tunnel:
                ssh_config = self.config.get_ssh_config()
                connection_info["ssh_tunnel_info"] = {
                    "ssh_host": ssh_config["ssh_host"],
                    "ssh_port": ssh_config["ssh_port"],
                    "local_port": self.ssh_tunnel.local_bind_port,
                    "remote_host": ssh_config["remote_host"],
                    "remote_port": ssh_config["remote_port"],
                }

            return connection_info

        except Exception as e:
            self.logger.error("Error getting connection info: %s", e)
            return {"error": f"Failed to get connection info: {str(e)}"}

    def run(self):
        """Run the MCP server."""
        transport = os.getenv(
            "MCP_TRANSPORT",
            self.template_data.get("transport", {}).get("default", "http"),
        )
        run_kwargs = {
            "transport": transport,
        }
        if transport != "stdio":
            run_kwargs["host"] = os.getenv("MCP_HOST", "0.0.0.0")
            run_kwargs["port"] = (
                int(
                    os.getenv(
                        "MCP_PORT",
                        self.template_data.get("transport", {}).get("port", 7080),
                    )
                )
                if not os.getenv("MCP_TRANSPORT") == "stdio"
                else None
            )
        try:
            self.mcp.run(**run_kwargs)
        except KeyboardInterrupt:
            self.logger.info("Server interrupted by user")
        except Exception as e:
            self.logger.error("Server error: %s", e)
            raise
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up resources."""
        try:
            if self.engine:
                self.engine.dispose()
                self.logger.info("Database engine disposed")

            if self.ssh_tunnel:
                self.ssh_tunnel.stop()
                self.logger.info("SSH tunnel closed")
        except Exception as e:
            self.logger.error("Error during cleanup: %s", e)


def setup_health_check(server_instance: PostgresMCPServer):
    """Set up health check endpoint for the server."""

    @server_instance.mcp.custom_route(path="/health", methods=["GET"])
    async def health_check(request: Request):
        """
        Health check endpoint to verify server status.
        """
        try:
            # Test PostgreSQL connection
            with server_instance.engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            return JSONResponse(
                {
                    "status": "healthy",
                    "server": "Postgres MCP Server",
                    "version": server_instance.version,
                    "postgres_connection": "ok",
                    "read_only_mode": server_instance.config.is_read_only(),
                    "pg_host": server_instance.config_data.get("pg_host"),
                    "pg_port": server_instance.config_data.get("pg_port"),
                }
            )
        except Exception as e:
            return JSONResponse(
                {
                    "status": "unhealthy",
                    "error": str(e),
                    "server": "Postgres MCP Server",
                },
                status_code=503,
            )


def main():
    """Main entry point for the server."""
    # Parse any command line arguments or environment variables
    config_dict = {}

    # You can add CLI argument parsing here if needed
    # For now, rely on environment variables and defaults

    # Create and run the server
    server = PostgresMCPServer(config_dict=config_dict)
    setup_health_check(server)
    server.run()


if __name__ == "__main__":
    main()
