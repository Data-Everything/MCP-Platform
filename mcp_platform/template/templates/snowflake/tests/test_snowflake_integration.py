"""
Integration tests for Snowflake MCP server.

Tests the Snowflake template's end-to-end functionality with mocked
Snowflake connections and comprehensive tool testing.
"""

import os
import sys
import tempfile
from unittest.mock import MagicMock, Mock, patch

import pytest

# Add the template directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    from config import SnowflakeServerConfig
    from response_formatter import SnowflakeResponseFormatter
    from server import SnowflakeMCPServer
except ImportError:
    # Handle import in different environments
    import importlib.util

    server_path = os.path.join(os.path.dirname(__file__), "..", "server.py")
    spec = importlib.util.spec_from_file_location("server", server_path)
    server_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(server_module)
    SnowflakeMCPServer = server_module.SnowflakeMCPServer

    config_path = os.path.join(os.path.dirname(__file__), "..", "config.py")
    spec = importlib.util.spec_from_file_location("config", config_path)
    config_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config_module)
    SnowflakeServerConfig = config_module.SnowflakeServerConfig

    formatter_path = os.path.join(
        os.path.dirname(__file__), "..", "response_formatter.py"
    )
    spec = importlib.util.spec_from_file_location("response_formatter", formatter_path)
    formatter_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(formatter_module)
    SnowflakeResponseFormatter = formatter_module.SnowflakeResponseFormatter


class TestSnowflakeIntegration:
    """Integration tests for Snowflake MCP server."""

    def test_template_discovery_integration(self):
        """Test that the template is properly discoverable by the platform."""
        template_path = os.path.join(os.path.dirname(__file__), "..", "template.json")

        assert os.path.exists(template_path), "template.json must exist"

        with open(template_path, "r") as f:
            import json

            template_data = json.load(f)

            # Verify template can be loaded and has required fields
            assert template_data["id"] == "snowflake"
            assert template_data["name"] == "Snowflake MCP Server"
            assert "config_schema" in template_data
            assert "tools" in template_data
            assert len(template_data["tools"]) > 0

    @pytest.mark.integration
    @patch("snowflake.connector.connect")
    def test_server_initialization_password_auth(self, mock_connect):
        """Test server initialization with password authentication."""
        # Mock Snowflake connection
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        config_dict = {
            "account": "test-account",
            "user": "testuser",
            "password": "testpass",
            "database": "TESTDB",
            "warehouse": "COMPUTE_WH",
        }

        server = SnowflakeMCPServer(config_dict=config_dict)

        # Verify server was initialized
        assert server.config_data["account"] == "test-account"
        assert server.config_data["user"] == "testuser"
        assert server.config_data["authenticator"] == "snowflake"

        # Verify connection was attempted
        mock_connect.assert_called_once()
        call_args = mock_connect.call_args[1]
        assert call_args["account"] == "test-account"
        assert call_args["user"] == "testuser"
        assert call_args["password"] == "testpass"

    @pytest.mark.integration
    @patch("snowflake.connector.connect")
    def test_server_initialization_jwt_auth(self, mock_connect):
        """Test server initialization with JWT authentication."""
        # Mock Snowflake connection
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        # Create temporary private key file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False) as f:
            f.write(
                "-----BEGIN PRIVATE KEY-----\ntest_key_content\n-----END PRIVATE KEY-----"
            )
            f.flush()

            config_dict = {
                "account": "test-account",
                "user": "testuser",
                "authenticator": "jwt",
                "private_key_file": f.name,
            }

            server = SnowflakeMCPServer(config_dict=config_dict, skip_validation=True)

            # Verify server was initialized
            assert server.config_data["account"] == "test-account"
            assert server.config_data["authenticator"] == "jwt"

            # Verify connection was attempted with JWT auth
            mock_connect.assert_called_once()
            call_args = mock_connect.call_args[1]
            assert call_args["authenticator"] == "jwt"
            assert "private_key" in call_args

            os.unlink(f.name)

    @pytest.mark.integration
    @patch("snowflake.connector.connect")
    def test_list_databases_tool(self, mock_connect):
        """Test list_databases tool functionality."""
        # Mock Snowflake connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connect.return_value = mock_connection

        # Mock database list result
        mock_databases = [
            {"name": "TESTDB", "owner": "SYSADMIN", "comment": "Test database"},
            {"name": "ANALYTICS", "owner": "SYSADMIN", "comment": "Analytics database"},
        ]
        mock_cursor.fetchall.return_value = mock_databases

        config_dict = {
            "account": "test-account",
            "user": "testuser",
            "password": "testpass",
        }

        server = SnowflakeMCPServer(config_dict=config_dict)

        # Test list_databases tool
        with patch.object(server, "_get_cursor") as mock_get_cursor:
            mock_get_cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
            mock_get_cursor.return_value.__exit__ = Mock(return_value=False)

            # Call the tool through the server's registered tools
            result = server.mcp.tools["list_databases"]()

        # Verify result
        assert "databases" in result
        assert result["count"] == 2
        assert result["databases"][0]["name"] == "TESTDB"
        assert result["databases"][1]["name"] == "ANALYTICS"

    @pytest.mark.integration
    @patch("snowflake.connector.connect")
    def test_execute_query_tool(self, mock_connect):
        """Test execute_query tool functionality."""
        # Mock Snowflake connection and cursor
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connect.return_value = mock_connection

        # Mock query result
        mock_cursor.description = [
            ("ID", 0, None, None, None, None, None),
            ("NAME", 2, None, None, None, None, None),
        ]
        mock_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]

        config_dict = {
            "account": "test-account",
            "user": "testuser",
            "password": "testpass",
            "read_only": True,
        }

        server = SnowflakeMCPServer(config_dict=config_dict)

        # Test execute_query tool with SELECT
        with patch.object(server, "_get_cursor") as mock_get_cursor:
            mock_get_cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
            mock_get_cursor.return_value.__exit__ = Mock(return_value=False)

            result = server.mcp.tools["execute_query"]("SELECT * FROM users")

        # Verify result
        assert "query" in result
        assert "columns" in result
        assert "rows" in result
        assert result["row_count"] == 2
        assert len(result["columns"]) == 2
        assert result["columns"][0]["name"] == "ID"
        assert result["rows"][0] == [1, "Alice"]

    @pytest.mark.integration
    @patch("snowflake.connector.connect")
    def test_read_only_mode_enforcement(self, mock_connect):
        """Test read-only mode enforcement."""
        # Mock Snowflake connection
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connect.return_value = mock_connection

        config_dict = {
            "account": "test-account",
            "user": "testuser",
            "password": "testpass",
            "read_only": True,
        }

        server = SnowflakeMCPServer(config_dict=config_dict)

        # Test that write queries are blocked
        with patch.object(server, "_get_cursor") as mock_get_cursor:
            mock_get_cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
            mock_get_cursor.return_value.__exit__ = Mock(return_value=False)

            result = server.mcp.tools["execute_query"](
                "INSERT INTO users VALUES (1, 'Test')"
            )

        # Should return error due to read-only mode
        assert "error" in result
        assert "read-only" in result["message"].lower()

    @pytest.mark.integration
    @patch("snowflake.connector.connect")
    def test_describe_table_tool(self, mock_connect):
        """Test describe_table tool functionality."""
        # Mock Snowflake connection
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connect.return_value = mock_connection

        # Mock table description result
        mock_columns = [
            {
                "name": "ID",
                "type": "NUMBER(38,0)",
                "kind": "COLUMN",
                "null?": "N",
                "default": None,
                "primary key": "Y",
                "unique key": "N",
                "check": None,
                "expression": None,
                "comment": "Primary key",
            },
            {
                "name": "NAME",
                "type": "VARCHAR(100)",
                "kind": "COLUMN",
                "null?": "Y",
                "default": None,
                "primary key": "N",
                "unique key": "N",
                "check": None,
                "expression": None,
                "comment": "User name",
            },
        ]
        mock_cursor.fetchall.return_value = mock_columns

        config_dict = {
            "account": "test-account",
            "user": "testuser",
            "password": "testpass",
        }

        server = SnowflakeMCPServer(config_dict=config_dict)

        # Test describe_table tool
        with patch.object(server, "_get_cursor") as mock_get_cursor:
            mock_get_cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
            mock_get_cursor.return_value.__exit__ = Mock(return_value=False)

            result = server.mcp.tools["describe_table"]("users")

        # Verify result
        assert "table" in result
        assert "columns" in result
        assert result["table"] == "users"
        assert result["column_count"] == 2
        assert result["columns"][0]["name"] == "ID"
        assert result["columns"][0]["type"] == "NUMBER(38,0)"
        assert result["columns"][1]["name"] == "NAME"

    @pytest.mark.integration
    @patch("snowflake.connector.connect")
    def test_database_access_control(self, mock_connect):
        """Test database access control functionality."""
        # Mock Snowflake connection
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connect.return_value = mock_connection

        config_dict = {
            "account": "test-account",
            "user": "testuser",
            "password": "testpass",
            "allowed_databases": "TESTDB,ANALYTICS",
        }

        server = SnowflakeMCPServer(config_dict=config_dict)

        # Test allowed database access
        assert server._is_database_allowed("TESTDB") is True
        assert server._is_database_allowed("ANALYTICS") is True

        # Test denied database access
        assert server._is_database_allowed("FORBIDDEN") is False

    @pytest.mark.integration
    @patch("snowflake.connector.connect")
    def test_warehouse_tools(self, mock_connect):
        """Test warehouse-related tools."""
        # Mock Snowflake connection
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connect.return_value = mock_connection

        # Mock warehouse data
        mock_warehouse = {
            "name": "COMPUTE_WH",
            "state": "STARTED",
            "size": "X-SMALL",
            "type": "STANDARD",
            "is_current": "Y",
            "is_default": "Y",
        }
        mock_cursor.fetchone.return_value = mock_warehouse
        mock_cursor.fetchall.return_value = [mock_warehouse]

        config_dict = {
            "account": "test-account",
            "user": "testuser",
            "password": "testpass",
        }

        server = SnowflakeMCPServer(config_dict=config_dict)

        # Test get_warehouse_info
        with patch.object(server, "_get_cursor") as mock_get_cursor:
            mock_get_cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
            mock_get_cursor.return_value.__exit__ = Mock(return_value=False)

            result = server.mcp.tools["get_warehouse_info"]()

        assert result["name"] == "COMPUTE_WH"
        assert result["state"] == "STARTED"
        assert result["size"] == "X-SMALL"

        # Test list_warehouses
        with patch.object(server, "_get_cursor") as mock_get_cursor:
            mock_get_cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
            mock_get_cursor.return_value.__exit__ = Mock(return_value=False)

            result = server.mcp.tools["list_warehouses"]()

        assert "warehouses" in result
        assert result["count"] == 1
        assert result["warehouses"][0]["name"] == "COMPUTE_WH"

    @pytest.mark.integration
    @patch("snowflake.connector.connect")
    def test_connection_info_tool(self, mock_connect):
        """Test get_connection_info tool."""
        # Mock Snowflake connection
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connect.return_value = mock_connection

        # Mock connection info query result
        mock_info = (
            "TEST_ACCOUNT",
            "TESTUSER",
            "TESTDB",
            "PUBLIC",
            "COMPUTE_WH",
            "USERROLE",
            "US-EAST-1",
            "7.5.0",
        )
        mock_cursor.fetchone.return_value = mock_info

        config_dict = {
            "account": "test-account",
            "user": "testuser",
            "password": "testpass",
        }

        server = SnowflakeMCPServer(config_dict=config_dict)

        # Test get_connection_info
        with patch.object(server, "_get_cursor") as mock_get_cursor:
            mock_get_cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
            mock_get_cursor.return_value.__exit__ = Mock(return_value=False)

            result = server.mcp.tools["get_connection_info"]()

        assert result["account"] == "TEST_ACCOUNT"
        assert result["user"] == "TESTUSER"
        assert result["database"] == "TESTDB"
        assert result["warehouse"] == "COMPUTE_WH"
        assert result["authenticator"] == "snowflake"

    @pytest.mark.integration
    @patch("snowflake.connector.connect")
    def test_test_connection_tool(self, mock_connect):
        """Test test_connection tool."""
        # Mock Snowflake connection
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connect.return_value = mock_connection

        # Mock test query result
        mock_cursor.fetchone.return_value = (1,)

        config_dict = {
            "account": "test-account",
            "user": "testuser",
            "password": "testpass",
        }

        server = SnowflakeMCPServer(config_dict=config_dict)

        # Test connection test
        with patch.object(server, "_get_cursor") as mock_get_cursor:
            mock_get_cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
            mock_get_cursor.return_value.__exit__ = Mock(return_value=False)

            result = server.mcp.tools["test_connection"]()

        assert result["connection_status"] == "success"
        assert result["test_result"] == 1
        assert "successful" in result["message"]

    @pytest.mark.integration
    @patch.dict(
        os.environ,
        {
            "SNOWFLAKE_ACCOUNT": "env-account",
            "SNOWFLAKE_USER": "env-user",
            "SNOWFLAKE_PASSWORD": "env-password",
            "SNOWFLAKE_DATABASE": "env-db",
            "SNOWFLAKE_READ_ONLY": "false",
            "SNOWFLAKE_MAX_RESULTS": "500",
        },
    )
    @patch("snowflake.connector.connect")
    def test_environment_variable_integration(self, mock_connect):
        """Test integration with environment variables."""
        # Mock Snowflake connection
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        config = SnowflakeServerConfig(config_dict={}, skip_validation=True)
        template_config = config.get_template_config()

        assert template_config["account"] == "env-account"
        assert template_config["user"] == "env-user"
        assert template_config["password"] == "env-password"
        assert template_config["database"] == "env-db"
        assert template_config["read_only"] is False
        assert template_config["max_results"] == 500

    @pytest.mark.integration
    @patch("snowflake.connector.connect")
    def test_error_handling(self, mock_connect):
        """Test error handling in server operations."""
        # Mock Snowflake connection that raises errors
        mock_connect.side_effect = Exception("Connection failed")

        config_dict = {
            "account": "test-account",
            "user": "testuser",
            "password": "testpass",
        }

        # Should raise exception during initialization
        with pytest.raises(Exception, match="Connection failed"):
            SnowflakeMCPServer(config_dict=config_dict)

    def test_response_formatter_integration(self):
        """Test response formatter integration."""
        formatter = SnowflakeResponseFormatter()

        # Test database list formatting
        databases = [
            {"name": "DB1", "owner": "SYSADMIN", "comment": "Test DB"},
            {"name": "DB2", "owner": "SYSADMIN", "comment": "Another DB"},
        ]
        result = formatter.format_database_list(databases)

        assert "databases" in result
        assert "count" in result
        assert result["count"] == 2
        assert result["databases"][0]["name"] == "DB1"

        # Test error formatting
        error = ValueError("Test error")
        error_result = formatter.format_error(error, "test_context")

        assert error_result["error"] is True
        assert error_result["error_type"] == "ValueError"
        assert error_result["message"] == "Test error"
        assert error_result["context"] == "test_context"
