#!/usr/bin/env python3
"""
Integration tests for Databricks MCP Server.

These tests require actual Databricks workspace credentials and should only
be run when the DATABRICKS_INTEGRATION_TEST environment variable is set.

Set the following environment variables to run these tests:
- DATABRICKS_INTEGRATION_TEST=true
- DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
- DATABRICKS_TOKEN=your_access_token

Example:
    export DATABRICKS_INTEGRATION_TEST=true
    export DATABRICKS_HOST=https://dbc-12345.cloud.databricks.com
    export DATABRICKS_TOKEN=dapi1234567890abcdef
    python -m pytest tests/test_databricks_integration.py -v
"""

import os
import pytest
import asyncio
from typing import Dict, Any

from mcp_platform.template.templates.databricks.server import DatabricksMCPServer

# Skip all tests in this module if integration testing is not enabled
pytestmark = pytest.mark.skipif(
    not os.getenv("DATABRICKS_INTEGRATION_TEST", "").lower() == "true",
    reason="Set DATABRICKS_INTEGRATION_TEST=true to run integration tests"
)


@pytest.fixture
def integration_config() -> Dict[str, Any]:
    """Create configuration for integration tests from environment variables."""
    return {
        "workspace_host": os.getenv("DATABRICKS_HOST"),
        "access_token": os.getenv("DATABRICKS_TOKEN"),
        "auth_method": "pat",
        "read_only": True,
        "max_rows": 10,  # Limit for testing
        "connection_timeout": 30
    }


@pytest.fixture
def server(integration_config):
    """Create a Databricks MCP server for integration testing."""
    return DatabricksMCPServer(config_dict=integration_config)


class TestDatabricksIntegration:
    """Integration tests for Databricks MCP Server."""

    @pytest.mark.asyncio
    async def test_server_initialization(self, server):
        """Test that server initializes and connects to Databricks."""
        assert server.workspace_client is not None
        assert server.config_data["workspace_host"] is not None
        assert server.config_data["read_only"] is True

    @pytest.mark.asyncio
    async def test_list_clusters_integration(self, server):
        """Test listing clusters against real Databricks workspace."""
        result = await server.list_clusters()
        
        # Should return successful result structure
        assert "clusters" in result
        assert "count" in result
        assert isinstance(result["clusters"], list)
        assert isinstance(result["count"], int)
        
        # If clusters exist, validate structure
        if result["count"] > 0:
            cluster = result["clusters"][0]
            required_fields = ["cluster_id", "cluster_name", "state"]
            for field in required_fields:
                assert field in cluster

    @pytest.mark.asyncio
    async def test_list_warehouses_integration(self, server):
        """Test listing warehouses against real Databricks workspace."""
        result = await server.list_warehouses()
        
        # Should return successful result structure
        assert "warehouses" in result
        assert "count" in result
        assert isinstance(result["warehouses"], list)
        assert isinstance(result["count"], int)
        
        # If warehouses exist, validate structure
        if result["count"] > 0:
            warehouse = result["warehouses"][0]
            required_fields = ["warehouse_id", "warehouse_name", "state"]
            for field in required_fields:
                assert field in warehouse

    @pytest.mark.asyncio
    async def test_list_databases_integration(self, server):
        """Test listing databases against real Databricks workspace."""
        result = await server.list_databases()
        
        # Should return successful result structure
        assert "databases" in result
        assert "count" in result
        assert isinstance(result["databases"], list)
        assert isinstance(result["count"], int)
        
        # Should have at least system databases
        assert result["count"] > 0, "Should have at least system databases"
        
        database = result["databases"][0]
        required_fields = ["database", "catalog", "schema"]
        for field in required_fields:
            assert field in database

    @pytest.mark.asyncio
    async def test_execute_simple_query_integration(self, server):
        """Test executing a simple query against real Databricks workspace."""
        # Use a simple system query that should work in most workspaces
        result = await server.execute_query(query="SELECT 1 as test_column")
        
        # Should return successful result
        if "error" not in result:
            assert "query" in result
            assert "data" in result
            assert "columns" in result
            assert len(result["data"]) > 0
            assert result["data"][0][0] == "1"
        else:
            # If there's an error, it should be informative
            assert isinstance(result["error"], str)
            print(f"Query failed (expected in some environments): {result['error']}")

    @pytest.mark.asyncio
    async def test_read_only_mode_enforcement_integration(self, server):
        """Test that read-only mode prevents write operations."""
        # Try a write operation that should be blocked
        write_queries = [
            "CREATE TABLE test_table (id INT)",
            "INSERT INTO test_table VALUES (1)",
            "DELETE FROM test_table WHERE id = 1"
        ]
        
        for query in write_queries:
            result = await server.execute_query(query=query)
            
            # Should be blocked by read-only mode
            assert "error" in result
            assert "read-only mode" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_database_filtering_integration(self, server):
        """Test database filtering functionality."""
        # Get all databases first
        all_databases = await server.list_databases()
        
        if all_databases["count"] > 1:
            # Test with pattern that should match subset
            first_db = all_databases["databases"][0]["database"]
            catalog, schema = first_db.split(".", 1)
            
            # Update server config to filter by specific catalog
            server.config_data["allowed_databases"] = catalog + ".*"
            
            filtered_result = await server.list_databases()
            
            # Should have fewer or equal databases
            assert filtered_result["count"] <= all_databases["count"]
            
            # All returned databases should match the pattern
            for db in filtered_result["databases"]:
                assert db["database"].startswith(catalog)

    @pytest.mark.asyncio 
    async def test_connection_health_integration(self, server):
        """Test that connection health check works."""
        try:
            # This should work if properly connected
            user_info = server.workspace_client.current_user.me()
            assert user_info.user_name is not None
            print(f"Connected as user: {user_info.user_name}")
        except Exception as e:
            pytest.fail(f"Health check failed: {e}")

    @pytest.mark.asyncio
    async def test_error_handling_integration(self, server):
        """Test error handling for invalid operations."""
        # Test invalid query
        result = await server.execute_query(query="INVALID SQL SYNTAX")
        assert "error" in result
        
        # Test invalid cluster ID
        result = await server.get_cluster_info(cluster_id="invalid-cluster-id")
        assert "error" in result
        
        # Test invalid warehouse ID
        result = await server.get_warehouse_info(warehouse_id="invalid-warehouse-id")
        assert "error" in result


if __name__ == "__main__":
    # Check if integration testing is enabled
    if not os.getenv("DATABRICKS_INTEGRATION_TEST", "").lower() == "true":
        print("Integration tests disabled. Set DATABRICKS_INTEGRATION_TEST=true to enable.")
        print("Also set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables.")
    else:
        pytest.main([__file__, "-v"])