#!/usr/bin/env python3
"""
Unit tests for Snowflake MCP Server.

Tests server functionality, tool registration, query validation,
and connection management using mocked Snowflake connector.
"""

import os
import pytest
import re
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

# Import the server class
import sys
sys.path.append(str(Path(__file__).parent.parent))
from server import SnowflakeMCPServer


class TestSnowflakeMCPServer:
    """Test cases for SnowflakeMCPServer."""

    @pytest.fixture
    def mock_snowflake_connector(self):
        """Mock snowflake.connector for testing."""
        with patch('server.snowflake.connector') as mock_connector:
            mock_connection = Mock()
            mock_cursor = Mock()
            
            # Set up mock connection
            mock_connection.cursor.return_value = mock_cursor
            mock_connection.is_closed.return_value = False
            mock_connector.connect.return_value = mock_connection
            
            yield mock_connector, mock_connection, mock_cursor

    @pytest.fixture
    def basic_config(self):
        """Basic valid configuration for testing."""
        return {
            "snowflake_account": "testaccount",
            "snowflake_user": "testuser",
            "snowflake_password": "testpass",
            "snowflake_warehouse": "TEST_WH"
        }

    def test_server_initialization(self, mock_snowflake_connector, basic_config):
        """Test server initialization with basic configuration."""
        mock_connector, mock_connection, mock_cursor = mock_snowflake_connector
        
        with patch.dict(os.environ, {}, clear=True):
            server = SnowflakeMCPServer(config_dict=basic_config)
            
            assert server.config is not None
            assert server.read_only is True  # Default should be True
            assert server.mcp is not None
            assert server.connection is None  # Connection created on demand

    def test_read_only_warning(self, mock_snowflake_connector, basic_config):
        """Test warning when read-only mode is disabled."""
        mock_connector, mock_connection, mock_cursor = mock_snowflake_connector
        config = {**basic_config, "read_only": False}
        
        with patch.dict(os.environ, {}, clear=True):
            with patch('server.logger') as mock_logger:
                server = SnowflakeMCPServer(config_dict=config)
                
                assert server.read_only is False
                mock_logger.warning.assert_called_once()
                warning_call = mock_logger.warning.call_args[0][0]
                assert "READ-ONLY MODE IS DISABLED" in warning_call

    def test_filter_pattern_compilation(self, mock_snowflake_connector, basic_config):
        """Test filter pattern compilation."""
        mock_connector, mock_connection, mock_cursor = mock_snowflake_connector
        config = {
            **basic_config,
            "database_filter_pattern": "^PROD_.*",
            "schema_filter_pattern": "^(PUBLIC|ANALYTICS)$"
        }
        
        with patch.dict(os.environ, {}, clear=True):
            server = SnowflakeMCPServer(config_dict=config)
            
            assert server.database_filter is not None
            assert server.schema_filter is not None
            assert isinstance(server.database_filter, re.Pattern)
            assert isinstance(server.schema_filter, re.Pattern)

    def test_invalid_filter_pattern(self, mock_snowflake_connector, basic_config):
        """Test handling of invalid regex patterns."""
        mock_connector, mock_connection, mock_cursor = mock_snowflake_connector
        config = {
            **basic_config,
            "database_filter_pattern": "["  # Invalid regex
        }
        
        with patch.dict(os.environ, {}, clear=True):
            with patch('server.logger') as mock_logger:
                server = SnowflakeMCPServer(config_dict=config)
                
                assert server.database_filter is None
                mock_logger.error.assert_called_once()

    def test_connection_creation_username_password(self, mock_snowflake_connector, basic_config):
        """Test connection creation with username/password authentication."""
        mock_connector, mock_connection, mock_cursor = mock_snowflake_connector
        
        with patch.dict(os.environ, {}, clear=True):
            server = SnowflakeMCPServer(config_dict=basic_config)
            connection = server._create_connection()
            
            mock_connector.connect.assert_called_once()
            call_args = mock_connector.connect.call_args[1]
            
            assert call_args["account"] == "testaccount"
            assert call_args["user"] == "testuser"
            assert call_args["password"] == "testpass"
            assert call_args["authenticator"] == "snowflake"

    def test_connection_creation_oauth(self, mock_snowflake_connector):
        """Test connection creation with OAuth authentication."""
        mock_connector, mock_connection, mock_cursor = mock_snowflake_connector
        config = {
            "snowflake_account": "testaccount",
            "snowflake_authenticator": "oauth",
            "snowflake_oauth_token": "test_token"
        }
        
        with patch.dict(os.environ, {}, clear=True):
            server = SnowflakeMCPServer(config_dict=config)
            connection = server._create_connection()
            
            call_args = mock_connector.connect.call_args[1]
            assert call_args["authenticator"] == "oauth"
            assert call_args["token"] == "test_token"

    def test_connection_creation_jwt(self, mock_snowflake_connector):
        """Test connection creation with JWT authentication."""
        mock_connector, mock_connection, mock_cursor = mock_snowflake_connector
        
        # Mock private key loading
        mock_private_key = b"mock_private_key_bytes"
        
        config = {
            "snowflake_account": "testaccount",
            "snowflake_user": "testuser",
            "snowflake_authenticator": "snowflake_jwt",
            "snowflake_private_key": "-----BEGIN PRIVATE KEY-----\nMOCK_KEY\n-----END PRIVATE KEY-----"
        }
        
        with patch.dict(os.environ, {}, clear=True):
            with patch.object(SnowflakeMCPServer, '_load_private_key', return_value=mock_private_key):
                server = SnowflakeMCPServer(config_dict=config)
                connection = server._create_connection()
                
                call_args = mock_connector.connect.call_args[1]
                assert call_args["authenticator"] == "snowflake_jwt"
                assert call_args["private_key"] == mock_private_key

    def test_read_only_query_validation(self, mock_snowflake_connector, basic_config):
        """Test read-only query validation."""
        mock_connector, mock_connection, mock_cursor = mock_snowflake_connector
        
        with patch.dict(os.environ, {}, clear=True):
            server = SnowflakeMCPServer(config_dict=basic_config)
            
            # Valid SELECT query should pass
            server._validate_read_only_query("SELECT * FROM table")
            
            # Prohibited queries should raise ValueError
            prohibited_queries = [
                "INSERT INTO table VALUES (1, 2)",
                "UPDATE table SET col = 1",
                "DELETE FROM table",
                "CREATE TABLE test (id INT)",
                "DROP TABLE test",
                "TRUNCATE TABLE test",
                "MERGE INTO target USING source ON condition"
            ]
            
            for query in prohibited_queries:
                with pytest.raises(ValueError, match="Query not allowed in read-only mode"):
                    server._validate_read_only_query(query)

    def test_read_only_disabled_allows_all_queries(self, mock_snowflake_connector, basic_config):
        """Test that disabling read-only mode allows all queries."""
        mock_connector, mock_connection, mock_cursor = mock_snowflake_connector
        config = {**basic_config, "read_only": False}
        
        with patch.dict(os.environ, {}, clear=True):
            server = SnowflakeMCPServer(config_dict=config)
            
            # All queries should be allowed when read-only is disabled
            queries = [
                "SELECT * FROM table",
                "INSERT INTO table VALUES (1, 2)",
                "UPDATE table SET col = 1",
                "DELETE FROM table"
            ]
            
            for query in queries:
                server._validate_read_only_query(query)  # Should not raise

    def test_filter_names(self, mock_snowflake_connector, basic_config):
        """Test name filtering functionality."""
        mock_connector, mock_connection, mock_cursor = mock_snowflake_connector
        config = {
            **basic_config,
            "database_filter_pattern": "^PROD_.*"
        }
        
        with patch.dict(os.environ, {}, clear=True):
            server = SnowflakeMCPServer(config_dict=config)
            
            names = ["PROD_DATABASE", "DEV_DATABASE", "TEST_DATABASE", "PROD_ANALYTICS"]
            filtered_names = server._filter_names(names, server.database_filter)
            
            assert "PROD_DATABASE" in filtered_names
            assert "PROD_ANALYTICS" in filtered_names
            assert "DEV_DATABASE" not in filtered_names
            assert "TEST_DATABASE" not in filtered_names

    def test_list_databases(self, mock_snowflake_connector, basic_config):
        """Test list_databases functionality."""
        mock_connector, mock_connection, mock_cursor = mock_snowflake_connector
        
        # Mock cursor results for SHOW DATABASES
        mock_cursor.fetchall.return_value = [
            ("2023-01-01", "PROD_DB", "owner", "comment"),
            ("2023-01-01", "DEV_DB", "owner", "comment"),
            ("2023-01-01", "TEST_DB", "owner", "comment")
        ]
        mock_cursor.description = [("created_on",), ("name",), ("owner",), ("comment",)]
        
        with patch.dict(os.environ, {}, clear=True):
            server = SnowflakeMCPServer(config_dict=basic_config)
            result = server.list_databases()
            
            assert "databases" in result
            assert "count" in result
            assert len(result["databases"]) == 3
            assert "PROD_DB" in result["databases"]
            assert "DEV_DB" in result["databases"]
            assert "TEST_DB" in result["databases"]

    def test_list_databases_with_filter(self, mock_snowflake_connector, basic_config):
        """Test list_databases with filtering."""
        mock_connector, mock_connection, mock_cursor = mock_snowflake_connector
        config = {**basic_config, "database_filter_pattern": "^PROD_.*"}
        
        # Mock cursor results
        mock_cursor.fetchall.return_value = [
            ("2023-01-01", "PROD_DB", "owner", "comment"),
            ("2023-01-01", "DEV_DB", "owner", "comment"),
            ("2023-01-01", "PROD_ANALYTICS", "owner", "comment")
        ]
        mock_cursor.description = [("created_on",), ("name",), ("owner",), ("comment",)]
        
        with patch.dict(os.environ, {}, clear=True):
            server = SnowflakeMCPServer(config_dict=config)
            result = server.list_databases()
            
            assert len(result["databases"]) == 2
            assert "PROD_DB" in result["databases"]
            assert "PROD_ANALYTICS" in result["databases"]
            assert "DEV_DB" not in result["databases"]
            assert result["filtered"] == 1

    def test_list_schemas(self, mock_snowflake_connector, basic_config):
        """Test list_schemas functionality."""
        mock_connector, mock_connection, mock_cursor = mock_snowflake_connector
        
        # Mock cursor results for SHOW SCHEMAS
        mock_cursor.fetchall.return_value = [
            ("2023-01-01", "PUBLIC", "owner", "comment"),
            ("2023-01-01", "ANALYTICS", "owner", "comment"),
            ("2023-01-01", "STAGING", "owner", "comment")
        ]
        mock_cursor.description = [("created_on",), ("name",), ("owner",), ("comment",)]
        
        with patch.dict(os.environ, {}, clear=True):
            server = SnowflakeMCPServer(config_dict=basic_config)
            result = server.list_schemas("TEST_DATABASE")
            
            assert "schemas" in result
            assert "database" in result
            assert result["database"] == "TEST_DATABASE"
            assert len(result["schemas"]) == 3
            assert "PUBLIC" in result["schemas"]

    def test_list_schemas_database_filter_check(self, mock_snowflake_connector, basic_config):
        """Test list_schemas respects database filter."""
        mock_connector, mock_connection, mock_cursor = mock_snowflake_connector
        config = {**basic_config, "database_filter_pattern": "^PROD_.*"}
        
        with patch.dict(os.environ, {}, clear=True):
            server = SnowflakeMCPServer(config_dict=config)
            result = server.list_schemas("DEV_DATABASE")
            
            assert "error" in result
            assert "not accessible due to filter restrictions" in result["error"]

    def test_execute_query(self, mock_snowflake_connector, basic_config):
        """Test execute_query functionality."""
        mock_connector, mock_connection, mock_cursor = mock_snowflake_connector
        
        # Mock cursor results
        mock_cursor.fetchall.return_value = [
            (1, "John", "Doe"),
            (2, "Jane", "Smith")
        ]
        mock_cursor.description = [("id",), ("first_name",), ("last_name",)]
        
        with patch.dict(os.environ, {}, clear=True):
            server = SnowflakeMCPServer(config_dict=basic_config)
            result = server.execute_query("SELECT * FROM users", limit=10)
            
            assert "query" in result
            assert "columns" in result
            assert "rows" in result
            assert "row_count" in result
            assert len(result["rows"]) == 2
            assert result["columns"] == ["id", "first_name", "last_name"]
            assert result["read_only_mode"] is True

    def test_execute_query_with_limit(self, mock_snowflake_connector, basic_config):
        """Test execute_query with LIMIT clause addition."""
        mock_connector, mock_connection, mock_cursor = mock_snowflake_connector
        mock_cursor.fetchall.return_value = []
        mock_cursor.description = []
        
        with patch.dict(os.environ, {}, clear=True):
            server = SnowflakeMCPServer(config_dict=basic_config)
            
            # Query without LIMIT should get one added
            result = server.execute_query("SELECT * FROM users", limit=50)
            
            # Check that the query was modified to include LIMIT
            mock_cursor.execute.assert_called_once()
            executed_query = mock_cursor.execute.call_args[0][0]
            assert "LIMIT 50" in executed_query

    def test_execute_query_read_only_violation(self, mock_snowflake_connector, basic_config):
        """Test execute_query with read-only violation."""
        mock_connector, mock_connection, mock_cursor = mock_snowflake_connector
        
        with patch.dict(os.environ, {}, clear=True):
            server = SnowflakeMCPServer(config_dict=basic_config)
            result = server.execute_query("DELETE FROM users")
            
            assert "error" in result
            assert "Query not allowed in read-only mode" in result["error"]

    def test_get_connection_info(self, mock_snowflake_connector, basic_config):
        """Test get_connection_info functionality."""
        mock_connector, mock_connection, mock_cursor = mock_snowflake_connector
        
        # Mock session info query result
        mock_cursor.fetchone.return_value = (
            "testaccount", "testuser", "testrole", "testdb", "testschema", "testwh"
        )
        
        with patch.dict(os.environ, {}, clear=True):
            server = SnowflakeMCPServer(config_dict=basic_config)
            result = server.get_connection_info()
            
            assert "account" in result
            assert "user" in result
            assert "role" in result
            assert "read_only_mode" in result
            assert result["account"] == "testaccount"
            assert result["user"] == "testuser"
            assert result["read_only_mode"] is True

    def test_describe_table(self, mock_snowflake_connector, basic_config):
        """Test describe_table functionality."""
        mock_connector, mock_connection, mock_cursor = mock_snowflake_connector
        
        # Mock DESCRIBE TABLE results
        mock_cursor.fetchall.return_value = [
            ("id", "NUMBER(38,0)", "COLUMN", "Y", None, "N", "N", None, None, "Primary key"),
            ("name", "VARCHAR(100)", "COLUMN", "N", None, "N", "N", None, None, "User name")
        ]
        mock_cursor.description = [
            ("name",), ("type",), ("kind",), ("null?",), ("default",),
            ("primary key",), ("unique key",), ("check",), ("expression",), ("comment",)
        ]
        
        with patch.dict(os.environ, {}, clear=True):
            server = SnowflakeMCPServer(config_dict=basic_config)
            result = server.describe_table("DB", "SCHEMA", "TABLE")
            
            assert "columns" in result
            assert "database" in result
            assert "schema" in result
            assert "table" in result
            assert len(result["columns"]) == 2
            assert result["columns"][0]["name"] == "id"
            assert result["columns"][1]["name"] == "name"

    def test_error_handling_in_tools(self, mock_snowflake_connector, basic_config):
        """Test error handling in tool methods."""
        mock_connector, mock_connection, mock_cursor = mock_snowflake_connector
        
        # Mock an exception during query execution
        mock_cursor.execute.side_effect = Exception("Database connection failed")
        
        with patch.dict(os.environ, {}, clear=True):
            server = SnowflakeMCPServer(config_dict=basic_config)
            
            # All methods should return error dictionaries instead of raising
            result = server.list_databases()
            assert "error" in result
            
            result = server.list_schemas("DB")
            assert "error" in result
            
            result = server.execute_query("SELECT 1")
            assert "error" in result

    def test_private_key_loading_from_content(self, mock_snowflake_connector):
        """Test private key loading from string content."""
        mock_connector, mock_connection, mock_cursor = mock_snowflake_connector
        
        # Mock the cryptography module
        with patch('server.load_pem_private_key') as mock_load_key:
            mock_private_key = Mock()
            mock_private_key.private_bytes.return_value = b"serialized_key"
            mock_load_key.return_value = mock_private_key
            
            config = {
                "snowflake_account": "testaccount",
                "snowflake_user": "testuser",
                "snowflake_authenticator": "snowflake_jwt",
                "snowflake_private_key": "-----BEGIN PRIVATE KEY-----\nMOCK_KEY\n-----END PRIVATE KEY-----"
            }
            
            with patch.dict(os.environ, {}, clear=True):
                server = SnowflakeMCPServer(config_dict=config)
                result = server._load_private_key(config["snowflake_private_key"])
                
                assert result == b"serialized_key"
                mock_load_key.assert_called_once()

    def test_tool_registration(self, mock_snowflake_connector, basic_config):
        """Test that all tools are properly registered."""
        mock_connector, mock_connection, mock_cursor = mock_snowflake_connector
        
        with patch.dict(os.environ, {}, clear=True):
            server = SnowflakeMCPServer(config_dict=basic_config)
            
            # Check that mcp.tool was called for each expected tool
            # This is a basic check - in reality you'd need to mock the mcp.tool decorator
            assert hasattr(server, 'list_databases')
            assert hasattr(server, 'list_schemas')
            assert hasattr(server, 'list_tables')
            assert hasattr(server, 'describe_table')
            assert hasattr(server, 'execute_query')
            assert hasattr(server, 'get_connection_info')


if __name__ == "__main__":
    pytest.main([__file__])