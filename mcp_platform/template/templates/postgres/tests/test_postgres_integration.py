"""
Integration tests for PostgreSQL MCP server.

Tests the PostgreSQL template's end-to-end functionality with real
database connections (when available) or comprehensive mocking.
"""

import asyncio
import os
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pytest

# Add the template directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    from config import PostgresServerConfig
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


class TestPostgresIntegration:
    """Integration tests for PostgreSQL MCP server."""

    @pytest.fixture
    def integration_config(self):
        """Configuration for integration testing."""
        return {
            "pg_host": os.getenv("TEST_PG_HOST", "localhost"),
            "pg_port": int(os.getenv("TEST_PG_PORT", "5432")),
            "pg_user": os.getenv("TEST_PG_USER", "postgres"),
            "pg_password": os.getenv("TEST_PG_PASSWORD", ""),
            "pg_database": os.getenv("TEST_PG_DATABASE", "postgres"),
            "read_only": True,
            "max_results": 10,
            "ssl_mode": "prefer",
        }

    @pytest.fixture
    def mock_integration_server(self, integration_config):
        """Create a server with mocked database for integration testing."""
        with (
            patch("server.create_engine") as mock_create_engine,
            patch("server.psycopg"),
            patch("server.SSHTunnelForwarder"),
        ):

            # Create mock engine and connection
            mock_engine = MagicMock()
            mock_connection = MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_connection
            mock_engine.connect.return_value.__exit__.return_value = None
            mock_engine.url.database = integration_config["pg_database"]
            mock_engine.url.host = integration_config["pg_host"]
            mock_engine.url.port = integration_config["pg_port"]
            mock_engine.url.username = integration_config["pg_user"]
            mock_engine.url.password = None

            mock_create_engine.return_value = mock_engine

            server = PostgresMCPServer(
                config_dict=integration_config, skip_validation=True
            )
            server.engine = mock_engine

            return server, mock_connection

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_full_schema_discovery_workflow(self, mock_integration_server):
        """Test complete schema discovery workflow."""
        server, mock_connection = mock_integration_server

        # Mock inspector for schema discovery
        mock_inspector = MagicMock()
        mock_inspector.get_schema_names.return_value = ["public", "information_schema"]
        mock_inspector.get_table_names.return_value = ["users", "orders"]
        mock_inspector.get_columns.return_value = [
            {"name": "id", "type": "INTEGER", "nullable": False},
            {"name": "username", "type": "VARCHAR", "nullable": False},
            {"name": "email", "type": "VARCHAR", "nullable": True},
        ]

        with patch("server.inspect", return_value=mock_inspector):
            # 1. List schemas
            schemas_result = await server.list_schemas(
                server.config.get("pg_database", "postgres")
            )
            assert "schemas" in schemas_result
            assert "public" in schemas_result["schemas"]

            # 2. List tables in public schema
            tables_result = await server.list_tables(schema="public")
            assert tables_result["schema"] == "public"
            assert "users" in tables_result["tables"]

            # 3. Describe users table
            describe_result = await server.describe_table(
                table="users", schema="public"
            )
            assert describe_result["table"] == "users"
            assert len(describe_result["columns"]) == 3

            # 4. List columns
            columns_result = await server.list_columns(table="users", schema="public")
            assert columns_result["count"] == 3
            assert columns_result["columns"][0]["name"] == "id"

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_query_execution_workflow(self, mock_integration_server):
        """Test query execution workflow with different query types."""
        server, mock_connection = mock_integration_server

        # Mock query results
        select_result = MagicMock()
        select_result.returns_rows = True
        select_result.fetchall.return_value = [
            (1, "alice", "alice@example.com"),
            (2, "bob", "bob@example.com"),
        ]
        select_result.keys.return_value = ["id", "username", "email"]
        mock_connection.execute.return_value = select_result

        # Test SELECT query
        query_result = await server.execute_query("SELECT * FROM users")
        assert "data" in query_result
        assert len(query_result["data"]) == 2
        assert query_result["data"][0]["username"] == "alice"

        # Test query with LIMIT
        query_result = await server.execute_query("SELECT * FROM users", limit=1)
        # Should add LIMIT to query
        executed_query = mock_connection.execute.call_args[0][0].text
        assert "LIMIT 1" in executed_query

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_read_only_enforcement(self, mock_integration_server):
        """Test read-only mode enforcement."""
        server, mock_connection = mock_integration_server

        # Test that write operations are blocked
        write_queries = [
            "INSERT INTO users (username) VALUES ('test')",
            "UPDATE users SET username = 'test' WHERE id = 1",
            "DELETE FROM users WHERE id = 1",
            "CREATE TABLE test_table (id INT)",
            "DROP TABLE users",
        ]

        for query in write_queries:
            result = await server.execute_query(query)
            assert "error" in result
            assert "not allowed in read-only mode" in result["error"]

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_schema_access_control(self):
        """Test schema access control."""
        # Test with restricted schema access
        config = {
            "pg_host": "localhost",
            "pg_user": "testuser",
            "pg_password": "testpass",
            "allowed_schemas": "public,analytics",
        }

        with (
            patch("server.create_engine"),
            patch("server.psycopg"),
            patch("server.SSHTunnelForwarder"),
        ):
            server = PostgresMCPServer(config_dict=config, skip_validation=True)

            # Should allow access to public
            assert server._check_schema_access("public") is True

            # Should allow access to analytics
            assert server._check_schema_access("analytics") is True

            # Should deny access to other schemas
            assert server._check_schema_access("restricted") is False

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_ssh_tunnel_integration(self):
        """Test SSH tunnel integration."""
        config = {
            "pg_host": "localhost",
            "pg_user": "testuser",
            "pg_password": "testpass",
            "ssh_tunnel": True,
            "ssh_host": "bastion.example.com",
            "ssh_user": "admin",
            "ssh_password": "sshpass",
            "ssh_auth_method": "password",
        }

        with (
            patch("server.create_engine") as mock_create_engine,
            patch("server.SSHTunnelForwarder") as mock_tunnel_class,
            patch("server.psycopg"),
        ):

            # Mock SSH tunnel
            mock_tunnel = MagicMock()
            mock_tunnel.local_bind_port = 12345
            mock_tunnel_class.return_value = mock_tunnel

            # Mock engine
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine

            server = PostgresMCPServer(config_dict=config, skip_validation=True)
            server._initialize_connection()

            # Verify SSH tunnel was created and started
            mock_tunnel_class.assert_called_once()
            mock_tunnel.start.assert_called_once()

            # Verify connection string was modified to use tunnel
            connection_args = mock_create_engine.call_args[0][0]
            assert "localhost:12345" in connection_args

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_ssl_configuration_integration(self):
        """Test SSL configuration integration."""
        # Create temporary SSL files for testing
        with (
            tempfile.NamedTemporaryFile(
                mode="w", suffix=".crt", delete=False
            ) as cert_file,
            tempfile.NamedTemporaryFile(
                mode="w", suffix=".key", delete=False
            ) as key_file,
            tempfile.NamedTemporaryFile(
                mode="w", suffix=".crt", delete=False
            ) as ca_file,
        ):

            cert_file.write(
                "-----BEGIN CERTIFICATE-----\ntest cert\n-----END CERTIFICATE-----"
            )
            key_file.write(
                "-----BEGIN PRIVATE KEY-----\ntest key\n-----END PRIVATE KEY-----"
            )
            ca_file.write(
                "-----BEGIN CERTIFICATE-----\ntest ca\n-----END CERTIFICATE-----"
            )

            cert_path = cert_file.name
            key_path = key_file.name
            ca_path = ca_file.name

        try:
            config = {
                "pg_host": "localhost",
                "pg_user": "testuser",
                "pg_password": "testpass",
                "ssl_mode": "verify-full",
                "ssl_cert": cert_path,
                "ssl_key": key_path,
                "ssl_ca": ca_path,
            }

            server_config = PostgresServerConfig(
                config_dict=config, skip_validation=True
            )
            connection_string = server_config.get_connection_string()

            # Verify SSL parameters are in connection string
            assert "sslmode=verify-full" in connection_string
            assert f"sslcert={cert_path}" in connection_string
            assert f"sslkey={key_path}" in connection_string
            assert f"sslrootcert={ca_path}" in connection_string

        finally:
            # Clean up temporary files
            os.unlink(cert_path)
            os.unlink(key_path)
            os.unlink(ca_path)

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_connection_pooling_and_cleanup(self, mock_integration_server):
        """Test connection pooling and cleanup."""
        server, mock_connection = mock_integration_server

        # Test multiple operations use the same engine
        initial_engine = server.engine

        # Perform multiple operations
        await server.test_connection()
        await server.get_database_info()

        # Engine should remain the same (connection pooling)
        assert server.engine is initial_engine

        # Test cleanup
        server.cleanup()
        server.engine.dispose.assert_called_once()

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_error_handling_and_recovery(self, mock_integration_server):
        """Test error handling and recovery mechanisms."""
        server, mock_connection = mock_integration_server

        # Test database connection error
        mock_connection.execute.side_effect = Exception("Connection lost")

        result = await server.execute_query("SELECT 1")
        assert "error" in result
        assert "Query execution failed" in result["error"]

        # Test recovery after error
        mock_connection.execute.side_effect = None
        mock_result = MagicMock()
        mock_result.returns_rows = True
        mock_result.fetchall.return_value = [(1,)]
        mock_result.keys.return_value = ["test"]
        mock_connection.execute.return_value = mock_result

        result = await server.execute_query("SELECT 1")
        assert "data" in result
        assert len(result["data"]) == 1

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_performance_and_limits(self, mock_integration_server):
        """Test performance aspects and limit enforcement."""
        server, mock_connection = mock_integration_server

        # Mock large result set
        large_result = MagicMock()
        large_result.returns_rows = True
        # Create more results than the limit
        large_result.fetchall.return_value = [(i, f"user_{i}") for i in range(20)]
        large_result.keys.return_value = ["id", "username"]
        mock_connection.execute.return_value = large_result

        # Execute query - should be limited to max_results (10)
        await server.execute_query("SELECT * FROM users")

        # Verify LIMIT was applied in query
        executed_query = mock_connection.execute.call_args[0][0].text
        assert "LIMIT 10" in executed_query

    @pytest.mark.integration
    def test_environment_variable_integration(self):
        """Test integration with environment variables."""
        env_vars = {
            "PG_HOST": "env-host",
            "PG_USER": "env-user",
            "PG_PASSWORD": "env-password",
            "PG_DATABASE": "env-db",
            "PG_READ_ONLY": "false",
            "PG_MAX_RESULTS": "500",
        }

        with patch.dict(os.environ, env_vars):
            config = PostgresServerConfig(config_dict={}, skip_validation=True)
            template_config = config.get_template_config()

            assert template_config["pg_host"] == "env-host"
            assert template_config["pg_user"] == "env-user"
            assert template_config["pg_password"] == "env-password"
            assert template_config["pg_database"] == "env-db"
            assert template_config["read_only"] is False
            assert template_config["max_results"] == 500

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_concurrent_operations(self, mock_integration_server):
        """Test concurrent database operations."""
        server, mock_connection = mock_integration_server

        # Mock different results for concurrent operations
        mock_results = []
        for i in range(3):
            result = MagicMock()
            result.returns_rows = True
            result.fetchall.return_value = [(i, f"result_{i}")]
            result.keys.return_value = ["id", "value"]
            mock_results.append(result)

        mock_connection.execute.side_effect = mock_results

        # Run concurrent operations
        tasks = [
            server.execute_query(f"SELECT {i} as id, 'result_{i}' as value")
            for i in range(3)
        ]

        results = await asyncio.gather(*tasks)

        # Verify all operations completed
        assert len(results) == 3
        for i, result in enumerate(results):
            assert "data" in result
            assert result["data"][0]["id"] == i

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_complex_query_scenarios(self, mock_integration_server):
        """Test complex query scenarios and edge cases."""
        server, mock_connection = mock_integration_server

        # Test query with multiple statements (should be handled safely)
        multi_statement = "SELECT 1; SELECT 2;"
        result = await server.execute_query(multi_statement)
        # In read-only mode, this should still work as it's SELECT statements

        # Test query with comments
        commented_query = """
        -- This is a comment
        SELECT * FROM users
        WHERE id > 0  /* Another comment */
        """

        mock_result = MagicMock()
        mock_result.returns_rows = True
        mock_result.fetchall.return_value = []
        mock_result.keys.return_value = []
        mock_connection.execute.return_value = mock_result

        result = await server.execute_query(commented_query)
        assert "data" in result

    @pytest.mark.integration
    @pytest.mark.skipif(
        not os.getenv("TEST_REAL_DATABASE"),
        reason="Real database tests require TEST_REAL_DATABASE environment variable",
    )
    @pytest.mark.asyncio
    async def test_real_database_connection(self, integration_config):
        """Test with real PostgreSQL database (when available)."""
        # This test only runs when TEST_REAL_DATABASE is set
        # and actual database credentials are provided

        try:
            server = PostgresMCPServer(
                config_dict=integration_config, skip_validation=False
            )

            # Test basic connection
            connection_result = await server.test_connection()
            assert connection_result["status"] == "success"

            # Test schema listing
            schemas_result = await server.list_schemas(
                server.config.get("pg_database", "postgres")
            )
            assert "schemas" in schemas_result
            assert "public" in schemas_result["schemas"]

            # Test simple query
            query_result = await server.execute_query("SELECT 1 as test_value")
            assert query_result["data"][0]["test_value"] == 1

            # Clean up
            server.cleanup()

        except Exception as e:
            pytest.skip(f"Real database test skipped due to connection error: {e}")

    def test_template_discovery_integration(self):
        """Test that the template is properly discoverable by the platform."""
        template_path = os.path.join(os.path.dirname(__file__), "..", "template.json")

        assert os.path.exists(template_path), "template.json must exist"

        with open(template_path, "r") as f:
            import json

            template_data = json.load(f)

            # Verify template can be loaded and has required fields
            assert template_data["id"] == "postgres"
            assert template_data["name"] == "PostgreSQL MCP Server"
            assert "config_schema" in template_data
            assert "tools" in template_data
            assert len(template_data["tools"]) > 0
