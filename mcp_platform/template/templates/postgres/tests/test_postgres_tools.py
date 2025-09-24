"""
Unit tests for PostgreSQL MCP server tools and functionality.

Tests the PostgreSQL template's tool implementations, query execution,
and database operations using mocked database connections.
"""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# Add the template directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    from server import PostgresMCPServer
except ImportError:
    # Handle import in different environments
    import importlib.util

    server_path = os.path.join(os.path.dirname(__file__), "..", "server.py")
    spec = importlib.util.spec_from_file_location("server", server_path)
    server_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(server_module)
    PostgresMCPServer = server_module.PostgresMCPServer

    config_path = os.path.join(os.path.dirname(__file__), "..", "config.py")
    spec = importlib.util.spec_from_file_location("config", config_path)
    config_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config_module)
    PostgresServerConfig = config_module.PostgresServerConfig


class TestPostgresServerTools:
    """Test PostgreSQL MCP server tool implementations."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock configuration for testing."""
        return {
            "pg_host": "localhost",
            "pg_user": "testuser",
            "pg_password": "testpass",
            "pg_database": "testdb",
            "read_only": True,
            "max_results": 100,
            "query_timeout": 30,
            "allowed_schemas": "*",
        }

    @pytest.fixture
    def mock_server(self, mock_config):
        """Create a mock PostgreSQL server for testing."""
        with (
            patch("server.create_engine"),
            patch("server.SSHTunnelForwarder"),
            patch("server.psycopg"),
        ):
            server = PostgresMCPServer(config_dict=mock_config, skip_validation=True)

            # Mock the connection context manager
            mock_connection = MagicMock()
            mock_engine = MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_connection
            mock_engine.connect.return_value.__exit__.return_value = None
            server.engine = mock_engine

            return server

    @pytest.mark.asyncio
    async def test_list_schemas(self, mock_server):
        """Test list_schemas tool."""
        # Mock the inspector
        mock_inspector = MagicMock()
        mock_inspector.get_schema_names.return_value = ["public", "analytics", "logs"]

        with patch("server.inspect", return_value=mock_inspector):
            result = await mock_server.list_schemas(
                mock_server.config_data.get("pg_database", "postgres")
            )

        assert "schemas" in result
        assert result["schemas"] == ["public", "analytics", "logs"]
        assert result["total_count"] == 3
        assert result["filtered_count"] == 0

    @pytest.mark.asyncio
    async def test_list_schemas_with_filtering(self, mock_config):
        """Test list_schemas with schema filtering."""
        mock_config["allowed_schemas"] = "public,analytics"

        with (
            patch("server.create_engine"),
            patch("server.SSHTunnelForwarder"),
            patch("server.psycopg"),
        ):
            server = PostgresMCPServer(config_dict=mock_config, skip_validation=True)

            # Mock the connection and inspector
            mock_connection = MagicMock()
            mock_engine = MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_connection
            mock_engine.connect.return_value.__exit__.return_value = None
            server.engine = mock_engine

            mock_inspector = MagicMock()
            mock_inspector.get_schema_names.return_value = [
                "public",
                "analytics",
                "logs",
                "system",
            ]

            with patch("server.inspect", return_value=mock_inspector):
                result = await server.list_schemas(
                    server.config_data.get("pg_database", "testdb")
                )

        assert result["schemas"] == ["public", "analytics"]
        assert result["total_count"] == 2
        assert result["filtered_count"] == 2

    @pytest.mark.asyncio
    async def test_list_tables(self, mock_server):
        """Test list_tables tool."""
        mock_inspector = MagicMock()
        mock_inspector.get_table_names.return_value = ["users", "orders", "products"]

        with patch("server.inspect", return_value=mock_inspector):
            result = await mock_server.list_tables(schema="public")

        assert result["schema"] == "public"
        assert result["tables"] == ["users", "orders", "products"]
        assert result["count"] == 3

    @pytest.mark.asyncio
    async def test_list_tables_access_denied(self, mock_config):
        """Test list_tables with access denied to schema."""
        mock_config["allowed_schemas"] = "public"

        with (
            patch("server.create_engine"),
            patch("server.SSHTunnelForwarder"),
            patch("server.psycopg"),
        ):
            server = PostgresMCPServer(config_dict=mock_config, skip_validation=True)

            result = await server.list_tables(schema="restricted")

        assert "error" in result
        assert "Access denied" in result["error"]

    @pytest.mark.asyncio
    async def test_describe_table(self, mock_server):
        """Test describe_table tool."""
        mock_inspector = MagicMock()
        mock_inspector.get_columns.return_value = [
            {"name": "id", "type": "INTEGER", "nullable": False},
            {"name": "name", "type": "VARCHAR", "nullable": True},
        ]
        mock_inspector.get_pk_constraint.return_value = {"constrained_columns": ["id"]}
        mock_inspector.get_foreign_keys.return_value = []
        mock_inspector.get_indexes.return_value = []

        with patch("server.inspect", return_value=mock_inspector):
            result = await mock_server.describe_table(table="users", schema="public")

        assert result["schema"] == "public"
        assert result["table"] == "users"
        assert len(result["columns"]) == 2
        assert result["columns"][0]["name"] == "id"

    @pytest.mark.asyncio
    async def test_list_columns(self, mock_server):
        """Test list_columns tool."""
        mock_inspector = MagicMock()
        mock_inspector.get_columns.return_value = [
            {"name": "id", "type": "INTEGER", "nullable": False, "default": None},
            {"name": "email", "type": "VARCHAR", "nullable": False, "default": None},
            {
                "name": "created_at",
                "type": "TIMESTAMP",
                "nullable": True,
                "default": "now()",
            },
        ]

        with patch("server.inspect", return_value=mock_inspector):
            result = await mock_server.list_columns(table="users", schema="public")

        assert result["schema"] == "public"
        assert result["table"] == "users"
        assert result["count"] == 3
        assert len(result["columns"]) == 3
        assert result["columns"][0]["name"] == "id"
        assert result["columns"][0]["type"] == "INTEGER"

    @pytest.mark.asyncio
    async def test_execute_query_select(self, mock_server):
        """Test execute_query with SELECT query."""
        mock_connection = mock_server.engine.connect.return_value.__enter__.return_value
        mock_result = MagicMock()
        mock_result.returns_rows = True
        mock_result.fetchall.return_value = [
            (1, "John Doe", "john@example.com"),
            (2, "Jane Smith", "jane@example.com"),
        ]
        mock_result.keys.return_value = ["id", "name", "email"]
        mock_connection.execute.return_value = mock_result

        result = await mock_server.execute_query("SELECT * FROM users")

        assert "data" in result
        assert len(result["data"]) == 2
        assert result["columns"] == ["id", "name", "email"]
        assert result["data"][0]["name"] == "John Doe"
        assert result["row_count"] == 2

    @pytest.mark.asyncio
    async def test_execute_query_with_limit(self, mock_server):
        """Test execute_query with LIMIT clause addition."""
        mock_connection = mock_server.engine.connect.return_value.__enter__.return_value
        mock_result = MagicMock()
        mock_result.returns_rows = True
        mock_result.fetchall.return_value = []
        mock_result.keys.return_value = []
        mock_connection.execute.return_value = mock_result

        await mock_server.execute_query("SELECT * FROM users")

        # Verify LIMIT was added to query
        executed_query = mock_connection.execute.call_args[0][0].text
        assert "LIMIT 100" in executed_query

    @pytest.mark.asyncio
    async def test_execute_query_read_only_violation(self, mock_server):
        """Test execute_query rejects write operations in read-only mode."""
        result = await mock_server.execute_query(
            "INSERT INTO users (name) VALUES ('test')"
        )

        assert "error" in result
        assert "not allowed in read-only mode" in result["error"]

    @pytest.mark.asyncio
    async def test_execute_query_write_mode(self, mock_config):
        """Test execute_query allows write operations when read-only is disabled."""
        mock_config["read_only"] = False

        with (
            patch("server.create_engine"),
            patch("server.SSHTunnelForwarder"),
            patch("server.psycopg"),
        ):
            server = PostgresMCPServer(config_dict=mock_config, skip_validation=True)

            mock_connection = MagicMock()
            mock_engine = MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_connection
            mock_engine.connect.return_value.__exit__.return_value = None
            server.engine = mock_engine

            mock_result = MagicMock()
            mock_result.returns_rows = False
            mock_result.rowcount = 1
            mock_connection.execute.return_value = mock_result

            result = await server.execute_query(
                "INSERT INTO users (name) VALUES ('test')"
            )

        assert "rows_affected" in result
        assert result["rows_affected"] == 1

    @pytest.mark.asyncio
    async def test_explain_query(self, mock_server):
        """Test explain_query tool."""
        mock_connection = mock_server.engine.connect.return_value.__enter__.return_value
        mock_result = MagicMock()
        mock_result.fetchone.return_value = [{"Plan": {"Node Type": "Seq Scan"}}]
        mock_connection.execute.return_value = mock_result

        result = await mock_server.explain_query("SELECT * FROM users")

        assert "execution_plan" in result
        assert result["query"] == "SELECT * FROM users"

    @pytest.mark.asyncio
    async def test_get_database_info(self, mock_server):
        """Test get_database_info tool."""
        mock_connection = mock_server.engine.connect.return_value.__enter__.return_value

        # Mock version query
        version_result = MagicMock()
        version_result.fetchone.return_value = ["PostgreSQL 14.5"]

        # Mock user query
        user_result = MagicMock()
        user_result.fetchone.return_value = ["testuser"]

        # Mock size query
        size_result = MagicMock()
        size_result.fetchone.return_value = ["10 MB"]

        mock_connection.execute.side_effect = [version_result, user_result, size_result]

        # Mock engine URL
        mock_server.engine.url.database = "testdb"
        mock_server.engine.url.host = "localhost"
        mock_server.engine.url.port = 5432
        mock_server.engine.url.username = "testuser"

        result = await mock_server.get_database_info()

        assert result["version"] == "PostgreSQL 14.5"
        assert result["database"] == "testdb"
        assert result["current_user"] == "testuser"
        assert result["size"] == "10 MB"
        assert result["read_only_mode"] is True

    @pytest.mark.asyncio
    async def test_get_table_stats(self, mock_server):
        """Test get_table_stats tool."""
        mock_connection = mock_server.engine.connect.return_value.__enter__.return_value

        # Mock count query
        count_result = MagicMock()
        count_result.fetchone.return_value = [1000]

        # Mock size query
        size_result = MagicMock()
        size_result.fetchone.return_value = ["1024 kB", "800 kB", "224 kB"]

        mock_connection.execute.side_effect = [count_result, size_result]

        result = await mock_server.get_table_stats(table="users", schema="public")

        assert result["schema"] == "public"
        assert result["table"] == "users"
        assert result["row_count"] == 1000
        assert result["total_size"] == "1024 kB"

    @pytest.mark.asyncio
    async def test_list_indexes(self, mock_server):
        """Test list_indexes tool."""
        mock_inspector = MagicMock()
        mock_inspector.get_indexes.return_value = [
            {"name": "users_pkey", "column_names": ["id"], "unique": True},
            {"name": "idx_users_email", "column_names": ["email"], "unique": False},
        ]

        with patch("server.inspect", return_value=mock_inspector):
            result = await mock_server.list_indexes(table="users", schema="public")

        assert result["schema"] == "public"
        assert result["table"] == "users"
        assert result["count"] == 2
        assert len(result["indexes"]) == 2

    @pytest.mark.asyncio
    async def test_list_constraints(self, mock_server):
        """Test list_constraints tool."""
        mock_inspector = MagicMock()
        mock_inspector.get_pk_constraint.return_value = {"constrained_columns": ["id"]}
        mock_inspector.get_foreign_keys.return_value = []
        mock_inspector.get_check_constraints.return_value = []
        mock_inspector.get_unique_constraints.return_value = []

        with patch("server.inspect", return_value=mock_inspector):
            result = await mock_server.list_constraints(table="users", schema="public")

        assert result["schema"] == "public"
        assert result["table"] == "users"
        assert "primary_key" in result
        assert "foreign_keys" in result

    @pytest.mark.asyncio
    async def test_test_connection_success(self, mock_server):
        """Test test_connection tool success."""
        mock_connection = mock_server.engine.connect.return_value.__enter__.return_value
        mock_result = MagicMock()
        mock_result.fetchone.return_value = [1]
        mock_connection.execute.return_value = mock_result

        result = await mock_server.test_connection()

        assert result["status"] == "success"
        assert result["test_query_result"] == 1
        assert "response_time" in result

    @pytest.mark.asyncio
    async def test_test_connection_failure(self, mock_config):
        """Test test_connection tool failure."""
        with (
            patch("server.create_engine"),
            patch("server.SSHTunnelForwarder"),
            patch("server.psycopg"),
        ):
            server = PostgresMCPServer(config_dict=mock_config, skip_validation=True)

            # Mock connection failure
            mock_engine = MagicMock()
            mock_engine.connect.side_effect = Exception("Connection failed")
            server.engine = mock_engine

            result = await server.test_connection()

        assert result["status"] == "failed"
        assert "error" in result

    @pytest.mark.asyncio
    async def test_get_connection_info(self, mock_server):
        """Test get_connection_info tool."""
        # Mock engine URL
        mock_server.engine.url.database = "testdb"
        mock_server.engine.url.host = "localhost"
        mock_server.engine.url.username = "testuser"
        mock_server.engine.url.password = "secret"

        result = await mock_server.get_connection_info()

        assert result["engine_initialized"] is True
        assert result["read_only_mode"] is True
        assert result["max_results"] == 100
        assert "***" in result["database_url"]  # password should be masked

    def test_validate_query_safety_read_only(self, mock_server):
        """Test query safety validation in read-only mode."""
        # Safe query
        is_safe, reason = mock_server._validate_query_safety("SELECT * FROM users")
        assert is_safe is True

        # Unsafe queries
        unsafe_queries = [
            "INSERT INTO users VALUES (1, 'test')",
            "UPDATE users SET name = 'test'",
            "DELETE FROM users WHERE id = 1",
            "CREATE TABLE test (id INT)",
            "DROP TABLE users",
            "TRUNCATE users",
        ]

        for query in unsafe_queries:
            is_safe, reason = mock_server._validate_query_safety(query)
            assert is_safe is False
            assert "not allowed in read-only mode" in reason

    def test_validate_query_safety_write_mode(self, mock_config):
        """Test query safety validation with write mode enabled."""
        mock_config["read_only"] = False

        with (
            patch("server.create_engine"),
            patch("server.SSHTunnelForwarder"),
            patch("server.psycopg"),
        ):
            server = PostgresMCPServer(config_dict=mock_config, skip_validation=True)

            # All queries should be allowed in write mode
            queries = [
                "SELECT * FROM users",
                "INSERT INTO users VALUES (1, 'test')",
                "UPDATE users SET name = 'test'",
                "DELETE FROM users WHERE id = 1",
            ]

            for query in queries:
                is_safe, reason = server._validate_query_safety(query)
                assert is_safe is True

    def test_check_schema_access_wildcard(self, mock_server):
        """Test schema access check with wildcard."""
        assert mock_server._check_schema_access("public") is True
        assert mock_server._check_schema_access("any_schema") is True

    def test_check_schema_access_list(self, mock_config):
        """Test schema access check with comma-separated list."""
        mock_config["allowed_schemas"] = "public,analytics,logs"

        with (
            patch("server.create_engine"),
            patch("server.SSHTunnelForwarder"),
            patch("server.psycopg"),
        ):
            server = PostgresMCPServer(config_dict=mock_config, skip_validation=True)

            assert server._check_schema_access("public") is True
            assert server._check_schema_access("analytics") is True
            assert server._check_schema_access("restricted") is False

    def test_check_schema_access_regex(self, mock_config):
        """Test schema access check with regex pattern."""
        mock_config["allowed_schemas"] = "^(public|test_.*)$"

        with (
            patch("server.create_engine"),
            patch("server.SSHTunnelForwarder"),
            patch("server.psycopg"),
        ):
            server = PostgresMCPServer(config_dict=mock_config, skip_validation=True)

            assert server._check_schema_access("public") is True
            assert server._check_schema_access("test_schema") is True
            assert server._check_schema_access("test_data") is True
            assert server._check_schema_access("production") is False


class TestPostgresServerConnection:
    """Test PostgreSQL server connection and initialization."""

    def test_server_initialization(self):
        """Test server initialization."""
        config_dict = {
            "pg_host": "localhost",
            "pg_user": "testuser",
            "pg_password": "testpass",
        }

        with (
            patch("server.create_engine"),
            patch("server.SSHTunnelForwarder"),
            patch("server.psycopg"),
        ):
            server = PostgresMCPServer(config_dict=config_dict, skip_validation=True)

            assert server.config is not None
            assert server.mcp is not None
            assert server.engine is None  # Not initialized until first use

    def test_ssh_tunnel_initialization(self):
        """Test SSH tunnel initialization."""
        config_dict = {
            "pg_host": "localhost",
            "pg_user": "testuser",
            "pg_password": "testpass",
            "ssh_tunnel": True,
            "ssh_host": "bastion.example.com",
            "ssh_user": "admin",
            "ssh_password": "sshpass",
        }

        with (
            patch("server.create_engine"),
            patch("server.SSHTunnelForwarder") as mock_tunnel,
            patch("server.psycopg"),
        ):

            mock_tunnel_instance = MagicMock()
            mock_tunnel_instance.local_bind_port = 12345
            mock_tunnel.return_value = mock_tunnel_instance

            server = PostgresMCPServer(config_dict=config_dict, skip_validation=True)

            # Trigger connection initialization
            server._initialize_connection()

            mock_tunnel.assert_called_once()
            mock_tunnel_instance.start.assert_called_once()

    def test_cleanup(self):
        """Test server cleanup."""
        config_dict = {
            "pg_host": "localhost",
            "pg_user": "testuser",
            "pg_password": "testpass",
        }

        with (
            patch("server.create_engine"),
            patch("server.SSHTunnelForwarder"),
            patch("server.psycopg"),
        ):
            server = PostgresMCPServer(config_dict=config_dict, skip_validation=True)

            # Mock engine and tunnel
            mock_engine = MagicMock()
            mock_tunnel = MagicMock()
            server.engine = mock_engine
            server.ssh_tunnel = mock_tunnel

            server.cleanup()

            mock_engine.dispose.assert_called_once()
            mock_tunnel.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_error_handling(self):
        """Test error handling in tools."""
        # Build a local mock server similar to other connection tests
        config_dict = {
            "pg_host": "localhost",
            "pg_user": "testuser",
            "pg_password": "testpass",
        }

        with (
            patch("server.create_engine"),
            patch("server.SSHTunnelForwarder"),
            patch("server.psycopg"),
        ):
            server = PostgresMCPServer(config_dict=config_dict, skip_validation=True)

            # Mock connection to raise exception
            mock_engine = MagicMock()
            mock_engine.connect.side_effect = Exception("Database error")
            server.engine = mock_engine

            result = await server.list_schemas(
                server.config_data.get("pg_database", "postgres")
            )

        assert "error" in result
        assert "Failed to list schemas" in result["error"]
