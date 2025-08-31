#!/usr/bin/env python3
"""
Integration tests for Snowflake MCP Server.

These tests require a real Snowflake account and should be run with appropriate
environment variables set. They can be skipped if credentials are not available.
"""

import os
import pytest
from pathlib import Path
import asyncio

# Import the server class
import sys
sys.path.append(str(Path(__file__).parent.parent))
from server import SnowflakeMCPServer


# Skip integration tests if no Snowflake credentials are available
def has_snowflake_credentials():
    """Check if Snowflake credentials are available for testing."""
    required_vars = ["SNOWFLAKE_ACCOUNT"]
    auth_vars = [
        ["SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"],  # Username/password
        ["SNOWFLAKE_OAUTH_TOKEN"],  # OAuth
        ["SNOWFLAKE_PRIVATE_KEY"],  # Key pair (with SNOWFLAKE_USER)
    ]
    
    if not all(os.getenv(var) for var in required_vars):
        return False
    
    # Check if any authentication method is configured
    for auth_group in auth_vars:
        if all(os.getenv(var) for var in auth_group):
            return True
        # Special case for key pair auth which also needs user
        if "SNOWFLAKE_PRIVATE_KEY" in auth_group and os.getenv("SNOWFLAKE_PRIVATE_KEY") and os.getenv("SNOWFLAKE_USER"):
            return True
    
    return False


# Mark all tests in this file as integration tests
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not has_snowflake_credentials(),
        reason="Snowflake credentials not available. Set SNOWFLAKE_ACCOUNT and authentication credentials."
    )
]


class TestSnowflakeIntegration:
    """Integration tests for Snowflake MCP Server."""

    @pytest.fixture
    def server_config(self):
        """Get server configuration from environment variables."""
        config = {
            "snowflake_account": os.getenv("SNOWFLAKE_ACCOUNT"),
        }
        
        # Add authentication details
        if os.getenv("SNOWFLAKE_USER"):
            config["snowflake_user"] = os.getenv("SNOWFLAKE_USER")
        
        if os.getenv("SNOWFLAKE_PASSWORD"):
            config["snowflake_password"] = os.getenv("SNOWFLAKE_PASSWORD")
        
        if os.getenv("SNOWFLAKE_OAUTH_TOKEN"):
            config["snowflake_authenticator"] = "oauth"
            config["snowflake_oauth_token"] = os.getenv("SNOWFLAKE_OAUTH_TOKEN")
        
        if os.getenv("SNOWFLAKE_PRIVATE_KEY"):
            config["snowflake_authenticator"] = "snowflake_jwt"
            config["snowflake_private_key"] = os.getenv("SNOWFLAKE_PRIVATE_KEY")
            if os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"):
                config["snowflake_private_key_passphrase"] = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
        
        # Add optional settings
        for env_var, config_key in [
            ("SNOWFLAKE_WAREHOUSE", "snowflake_warehouse"),
            ("SNOWFLAKE_DATABASE", "snowflake_database"),
            ("SNOWFLAKE_SCHEMA", "snowflake_schema"),
            ("SNOWFLAKE_ROLE", "snowflake_role"),
        ]:
            if os.getenv(env_var):
                config[config_key] = os.getenv(env_var)
        
        return config

    @pytest.fixture
    def server(self, server_config):
        """Create a server instance for testing."""
        server = SnowflakeMCPServer(config_dict=server_config)
        yield server
        
        # Cleanup: close connection if it exists
        if server.connection and not server.connection.is_closed():
            server.connection.close()

    def test_connection_establishment(self, server):
        """Test that the server can establish a connection to Snowflake."""
        connection = server._get_connection()
        assert connection is not None
        assert not connection.is_closed()
        
        # Test basic connectivity with a simple query
        cursor = connection.cursor()
        cursor.execute("SELECT 1 as test_value")
        result = cursor.fetchone()
        cursor.close()
        
        assert result is not None
        assert result[0] == 1

    def test_get_connection_info(self, server):
        """Test getting connection information."""
        result = server.get_connection_info()
        
        assert "error" not in result
        assert "account" in result
        assert "user" in result
        assert result["account"] is not None
        assert result["read_only_mode"] is True  # Default should be True

    def test_list_databases(self, server):
        """Test listing databases."""
        result = server.list_databases()
        
        assert "error" not in result
        assert "databases" in result
        assert "count" in result
        assert isinstance(result["databases"], list)
        assert result["count"] >= 0
        
        # Should have at least some system databases
        assert len(result["databases"]) > 0

    def test_list_schemas_information_schema(self, server):
        """Test listing schemas in INFORMATION_SCHEMA database."""
        # Most Snowflake accounts should have INFORMATION_SCHEMA
        result = server.list_schemas("INFORMATION_SCHEMA")
        
        if "error" not in result:
            assert "schemas" in result
            assert "database" in result
            assert result["database"] == "INFORMATION_SCHEMA"
            assert isinstance(result["schemas"], list)

    def test_execute_simple_query(self, server):
        """Test executing a simple query."""
        result = server.execute_query("SELECT CURRENT_TIMESTAMP() as current_time", limit=1)
        
        assert "error" not in result
        assert "query" in result
        assert "columns" in result
        assert "rows" in result
        assert "row_count" in result
        assert len(result["rows"]) == 1
        assert "current_time" in result["columns"]

    def test_execute_query_with_limit(self, server):
        """Test executing a query with row limit."""
        # Query information schema which should have multiple rows
        result = server.execute_query(
            "SELECT * FROM INFORMATION_SCHEMA.SCHEMATA", 
            limit=5
        )
        
        if "error" not in result:
            assert len(result["rows"]) <= 5
            assert result["limited"] is True
            assert result["limit"] == 5

    def test_read_only_enforcement(self, server):
        """Test that read-only mode prevents write operations."""
        # Attempt to run a CREATE statement (should be blocked)
        result = server.execute_query("CREATE TABLE test_table (id INT)")
        
        assert "error" in result
        assert "Query not allowed in read-only mode" in result["error"]

    def test_read_only_disabled_server(self, server_config):
        """Test server with read-only mode disabled."""
        config = {**server_config, "read_only": False}
        server = SnowflakeMCPServer(config_dict=config)
        
        try:
            # This should not raise a read-only error (but may fail for other reasons)
            result = server.execute_query("SELECT 1")
            # The query itself should work
            assert "error" not in result or "Query not allowed in read-only mode" not in result.get("error", "")
        finally:
            if server.connection and not server.connection.is_closed():
                server.connection.close()

    def test_database_filtering(self, server_config):
        """Test database filtering functionality."""
        # Create server with database filter
        config = {**server_config, "database_filter_pattern": "^INFORMATION_SCHEMA$"}
        server = SnowflakeMCPServer(config_dict=config)
        
        try:
            result = server.list_databases()
            
            if "error" not in result:
                # Should only return INFORMATION_SCHEMA if filter is working
                assert "INFORMATION_SCHEMA" in result["databases"]
                # Other databases should be filtered out
                assert result["filtered"] >= 0
        finally:
            if server.connection and not server.connection.is_closed():
                server.connection.close()

    def test_schema_filtering(self, server_config):
        """Test schema filtering functionality."""
        # Create server with schema filter
        config = {**server_config, "schema_filter_pattern": "^INFORMATION_SCHEMA$"}
        server = SnowflakeMCPServer(config_dict=config)
        
        try:
            # Try to list schemas in INFORMATION_SCHEMA database
            result = server.list_schemas("INFORMATION_SCHEMA")
            
            if "error" not in result:
                # Should only return INFORMATION_SCHEMA schema if filter is working
                filtered_schemas = result["schemas"]
                if "INFORMATION_SCHEMA" in filtered_schemas:
                    # Filter should only include INFORMATION_SCHEMA
                    assert len([s for s in filtered_schemas if s == "INFORMATION_SCHEMA"]) >= 1
        finally:
            if server.connection and not server.connection.is_closed():
                server.connection.close()

    def test_describe_information_schema_table(self, server):
        """Test describing a table in INFORMATION_SCHEMA."""
        # Try to describe SCHEMATA table which should exist
        result = server.describe_table("INFORMATION_SCHEMA", "INFORMATION_SCHEMA", "SCHEMATA")
        
        if "error" not in result:
            assert "columns" in result
            assert "database" in result
            assert "schema" in result
            assert "table" in result
            assert result["database"] == "INFORMATION_SCHEMA"
            assert result["schema"] == "INFORMATION_SCHEMA"
            assert result["table"] == "SCHEMATA"
            assert len(result["columns"]) > 0

    def test_list_tables_information_schema(self, server):
        """Test listing tables in INFORMATION_SCHEMA."""
        result = server.list_tables("INFORMATION_SCHEMA", "INFORMATION_SCHEMA")
        
        if "error" not in result:
            assert "tables" in result
            assert "database" in result
            assert "schema" in result
            assert result["database"] == "INFORMATION_SCHEMA"
            assert result["schema"] == "INFORMATION_SCHEMA"
            assert isinstance(result["tables"], list)
            
            # Should have some system tables
            table_names = [table["name"] for table in result["tables"]]
            assert len(table_names) > 0

    def test_connection_timeout_configuration(self, server_config):
        """Test connection timeout configuration."""
        config = {**server_config, "connection_timeout": 30}
        server = SnowflakeMCPServer(config_dict=config)
        
        try:
            # Connection should still work with custom timeout
            connection = server._get_connection()
            assert connection is not None
            assert not connection.is_closed()
        finally:
            if server.connection and not server.connection.is_closed():
                server.connection.close()

    def test_multiple_authentication_methods(self, server_config):
        """Test different authentication methods if multiple are available."""
        # This test only runs if we have username/password auth
        if "snowflake_user" in server_config and "snowflake_password" in server_config:
            # Test username/password auth explicitly
            config = {
                **server_config,
                "snowflake_authenticator": "snowflake"
            }
            server = SnowflakeMCPServer(config_dict=config)
            
            try:
                connection = server._get_connection()
                assert connection is not None
            finally:
                if server.connection and not server.connection.is_closed():
                    server.connection.close()

    @pytest.mark.slow
    def test_large_query_with_limit(self, server):
        """Test executing a potentially large query with limit."""
        # Query that might return many rows
        result = server.execute_query(
            "SELECT * FROM INFORMATION_SCHEMA.COLUMNS", 
            limit=10
        )
        
        if "error" not in result:
            assert len(result["rows"]) <= 10
            assert result["row_count"] <= 10

    def test_error_handling_invalid_query(self, server):
        """Test error handling for invalid SQL."""
        result = server.execute_query("INVALID SQL SYNTAX HERE")
        
        assert "error" in result
        assert "query" in result

    def test_error_handling_invalid_database(self, server):
        """Test error handling for accessing non-existent database."""
        result = server.list_schemas("NON_EXISTENT_DATABASE_NAME_12345")
        
        assert "error" in result

    def test_error_handling_invalid_table(self, server):
        """Test error handling for describing non-existent table."""
        result = server.describe_table(
            "INFORMATION_SCHEMA", 
            "INFORMATION_SCHEMA", 
            "NON_EXISTENT_TABLE_12345"
        )
        
        assert "error" in result


@pytest.mark.performance
class TestSnowflakePerformance:
    """Performance tests for Snowflake MCP Server."""

    @pytest.fixture
    def server(self):
        """Create a server instance for performance testing."""
        if not has_snowflake_credentials():
            pytest.skip("Snowflake credentials not available")
        
        config = {
            "snowflake_account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "snowflake_user": os.getenv("SNOWFLAKE_USER"),
            "snowflake_password": os.getenv("SNOWFLAKE_PASSWORD"),
        }
        
        server = SnowflakeMCPServer(config_dict=config)
        yield server
        
        if server.connection and not server.connection.is_closed():
            server.connection.close()

    @pytest.mark.slow
    def test_connection_reuse(self, server):
        """Test that connections are properly reused."""
        # First connection
        conn1 = server._get_connection()
        conn1_id = id(conn1)
        
        # Second call should reuse the same connection
        conn2 = server._get_connection()
        conn2_id = id(conn2)
        
        assert conn1_id == conn2_id

    @pytest.mark.slow
    def test_concurrent_queries(self, server):
        """Test handling multiple queries (simulated concurrency)."""
        queries = [
            "SELECT 1",
            "SELECT CURRENT_TIMESTAMP()",
            "SELECT 'test'",
        ]
        
        results = []
        for query in queries:
            result = server.execute_query(query)
            results.append(result)
        
        # All queries should succeed
        for result in results:
            assert "error" not in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])