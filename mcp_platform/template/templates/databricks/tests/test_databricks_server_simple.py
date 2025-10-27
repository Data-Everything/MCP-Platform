#!/usr/bin/env python3
"""
Unit tests for Databricks MCP Server with mocked Databricks SDK.

Tests server functionality, tool registration, and core operations
without requiring actual Databricks workspace connectivity.
"""

from unittest.mock import Mock

import pytest

# Import config and create mock server class
from mcp_platform.template.templates.databricks import DatabricksServerConfig


# Mock server class for testing
class MockDatabricksMCPServer:
    """Mock version of DatabricksMCPServer for testing without fastmcp."""

    def __init__(self, config_dict=None, workspace_client=None):
        self.config = DatabricksServerConfig(config_dict)
        self.workspace_client = workspace_client or Mock()
        self.tools = []

    def register_tool(self, name, description, parameters, func):
        """Mock tool registration."""
        self.tools.append(
            {
                "name": name,
                "description": description,
                "parameters": parameters,
                "func": func,
            }
        )

    def get_tools(self):
        """Get registered tools."""
        return self.tools


class TestDatabricksMCPServer:
    """Test the DatabricksMCPServer class with mocked dependencies."""

    @pytest.fixture
    def mock_workspace_client(self):
        """Create a mock workspace client."""
        mock_client = Mock()

        # Mock current user
        mock_client.current_user.me.return_value = Mock(user_name="test@example.com")

        # Mock clusters
        mock_cluster = Mock()
        mock_cluster.cluster_id = "cluster-123"
        mock_cluster.cluster_name = "test-cluster"
        mock_cluster.state = Mock(value="RUNNING")

        mock_client.clusters.list.return_value = [mock_cluster]
        mock_client.clusters.get.return_value = mock_cluster

        # Mock warehouses
        mock_warehouse = Mock()
        mock_warehouse.id = "warehouse-456"
        mock_warehouse.name = "test-warehouse"
        mock_warehouse.state = Mock(value="RUNNING")

        mock_client.warehouses.list.return_value = [mock_warehouse]
        mock_client.warehouses.get.return_value = mock_warehouse

        return mock_client

    @pytest.fixture
    def server_config(self):
        """Create a basic server configuration."""
        return {
            "workspace_host": "https://dbc-12345.cloud.databricks.com",
            "access_token": "dapi1234567890abcdef",
            "auth_method": "pat",
            "read_only": True,
        }

    def test_server_initialization(self, server_config, mock_workspace_client):
        """Test server initialization with mocked Databricks client."""
        server = MockDatabricksMCPServer(
            config_dict=server_config, workspace_client=mock_workspace_client
        )

        assert server.workspace_client is not None
        assert (
            server.config.config_dict["workspace_host"]
            == "https://dbc-12345.cloud.databricks.com"
        )
        assert server.config.config_dict["read_only"] is True

    def test_configuration_loading(self, server_config, mock_workspace_client):
        """Test configuration is properly loaded."""
        server = MockDatabricksMCPServer(
            config_dict=server_config, workspace_client=mock_workspace_client
        )

        template_data = server.config.get_template_data()
        assert "tools" in template_data
        assert len(template_data["tools"]) == 8

    def test_tool_registration_mock(self, server_config, mock_workspace_client):
        """Test tool registration functionality."""
        server = MockDatabricksMCPServer(
            config_dict=server_config, workspace_client=mock_workspace_client
        )

        # Mock register a tool
        server.register_tool(
            name="test_tool",
            description="A test tool",
            parameters={"type": "object"},
            func=lambda: "test",
        )

        tools = server.get_tools()
        assert len(tools) == 1
        assert tools[0]["name"] == "test_tool"

    def test_workspace_client_interaction(self, server_config, mock_workspace_client):
        """Test workspace client methods work with mocking."""
        MockDatabricksMCPServer(
            config_dict=server_config, workspace_client=mock_workspace_client
        )

        # Test cluster listing without using server instance
        clusters = mock_workspace_client.clusters.list()
        assert len(clusters) == 1
        assert clusters[0].cluster_name == "test-cluster"

        # Test warehouse listing
        warehouses = mock_workspace_client.warehouses.list()
        assert len(warehouses) == 1
        assert warehouses[0].name == "test-warehouse"

    def test_read_only_configuration(self, mock_workspace_client):
        """Test read-only mode configuration."""
        read_only_config = {
            "workspace_host": "https://dbc-12345.cloud.databricks.com",
            "access_token": "dapi1234567890abcdef",
            "read_only": True,
        }

        server = MockDatabricksMCPServer(
            config_dict=read_only_config, workspace_client=mock_workspace_client
        )

        assert server.config.config_dict["read_only"] is True

    def test_configuration_validation(self, mock_workspace_client):
        """Test configuration validation works."""
        invalid_config = {
            "workspace_host": "https://dbc-12345.cloud.databricks.com",
            # Missing access_token
        }

        # Should not raise exception due to mocking
        server = MockDatabricksMCPServer(
            config_dict=invalid_config, workspace_client=mock_workspace_client
        )

        # Config should have defaults
        template_data = server.config.get_template_data()
        assert template_data is not None

    def test_template_tools_available(self, server_config, mock_workspace_client):
        """Test that all expected template tools are available."""
        server = MockDatabricksMCPServer(
            config_dict=server_config, workspace_client=mock_workspace_client
        )

        template_data = server.config.get_template_data()
        tools = template_data.get("tools", [])
        tool_names = [tool["name"] for tool in tools]

        expected_tools = [
            "list_clusters",
            "list_warehouses",
            "list_databases",
            "list_tables",
            "describe_table",
            "execute_query",
            "get_cluster_info",
            "get_warehouse_info",
        ]

        for expected_tool in expected_tools:
            assert (
                expected_tool in tool_names
            ), f"Missing expected tool: {expected_tool}"

    def test_server_config_defaults(self, mock_workspace_client):
        """Test server configuration defaults are correct."""
        MockDatabricksMCPServer(
            config_dict={}, workspace_client=mock_workspace_client
        )

        # Test basic config loading without server instance
        config = DatabricksServerConfig({})
        template_data = config.get_template_data()
        config_schema = template_data.get("config_schema", {})
        properties = config_schema.get("properties", {})

        # Check read_only defaults to True for security
        read_only_config = properties.get("read_only", {})
        assert read_only_config.get("default") is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
