#!/usr/bin/env python3
"""
Integration tests for Databricks MCP Server.

These tests verify the configuration and template structure work correctly
without requiring actual Databricks workspace connectivity.
All tests are mocked and focus on integration between components.
"""

import pytest

from mcp_platform.template.templates.databricks import DatabricksServerConfig


class TestDatabricksConfigIntegration:
    """Integration tests for Databricks configuration."""

    def test_config_template_integration(self):
        """Test that configuration integrates properly with template."""
        config = DatabricksServerConfig(
            {
                "workspace_host": "https://test.databricks.com",
                "access_token": "test_token",
                "read_only": True,
            }
        )

        template_data = config.get_template_data()
        template_config = config.get_template_config()

        # Verify integration between template data and config
        assert "tools" in template_data
        assert "workspace_host" in template_config
        assert template_config["read_only"] is True

    def test_tool_template_consistency(self):
        """Test that tools defined in template are consistent."""
        config = DatabricksServerConfig()
        template_data = config.get_template_data()

        tools = template_data.get("tools", [])
        assert len(tools) == 8

        # Verify each tool has consistent structure
        for tool in tools:
            assert "name" in tool
            assert "description" in tool
            assert "parameters" in tool
            assert isinstance(tool["parameters"], dict)

    def test_security_integration(self):
        """Test that security settings integrate properly."""
        config = DatabricksServerConfig()
        template_data = config.get_template_data()

        # Check that read-only is enforced at template level
        config_schema = template_data.get("config_schema", {})
        properties = config_schema.get("properties", {})
        read_only_config = properties.get("read_only", {})

        assert read_only_config.get("default") is True
        assert read_only_config.get("type") == "boolean"

    def test_transport_integration(self):
        """Test that transport configuration integrates correctly."""
        config = DatabricksServerConfig()
        template_data = config.get_template_data()

        transport = template_data.get("transport", {})
        ports = template_data.get("ports", {})

        # Verify transport and port configuration consistency
        assert transport.get("default") == "stdio"
        assert "stdio" in transport.get("supported", [])
        assert "http" in transport.get("supported", [])

        # Check port configuration exists for HTTP transport
        assert "7072" in ports
        assert ports["7072"] == 7072
        assert transport.get("port") == 7072

    def test_filtering_integration(self):
        """Test that database and schema filtering integrates properly."""
        config = DatabricksServerConfig(
            {
                "workspace_host": "https://test.databricks.com",
                "access_token": "test_token",
                "allowed_databases": "db1,db2",
                "allowed_schemas": "schema1,schema2",
            }
        )

        template_config = config.get_template_config()

        assert template_config["allowed_databases"] == "db1,db2"
        assert template_config["allowed_schemas"] == "schema1,schema2"

    def test_authentication_integration(self):
        """Test that authentication methods integrate correctly."""
        # Test PAT authentication
        pat_config = DatabricksServerConfig(
            {
                "workspace_host": "https://test.databricks.com",
                "auth_method": "pat",
                "access_token": "test_pat_token",
            }
        )

        template_config = pat_config.get_template_config()
        assert template_config["auth_method"] == "pat"
        assert template_config["access_token"] == "test_pat_token"

        # Test OAuth authentication
        oauth_config = DatabricksServerConfig(
            {
                "workspace_host": "https://test.databricks.com",
                "auth_method": "oauth",
                "oauth_token": "test_oauth_token",
            }
        )

        template_config = oauth_config.get_template_config()
        assert template_config["auth_method"] == "oauth"
        assert template_config["oauth_token"] == "test_oauth_token"

    def test_environment_variable_integration(self):
        """Test environment variable mapping integration."""
        config = DatabricksServerConfig()
        template_data = config.get_template_data()

        config_schema = template_data.get("config_schema", {})
        properties = config_schema.get("properties", {})

        # Check critical environment mappings
        critical_props = ["workspace_host", "access_token", "auth_method"]
        for prop_name in critical_props:
            if prop_name in properties:
                prop_config = properties[prop_name]
                assert "env_mapping" in prop_config
                assert prop_config["env_mapping"].startswith("DATABRICKS_")

    def test_docker_integration(self):
        """Test Docker configuration integration."""
        config = DatabricksServerConfig()
        template_data = config.get_template_data()

        # Verify Docker configuration is complete
        assert template_data.get("docker_image") == "dataeverything/mcp-databricks"
        assert template_data.get("docker_tag") == "latest"

        # Check that ports and transport are compatible with Docker
        ports = template_data.get("ports", {})
        transport = template_data.get("transport", {})

        assert "7072" in ports
        assert transport.get("port") == 7072

    def test_capability_integration(self):
        """Test that capabilities integrate with available tools."""
        config = DatabricksServerConfig()
        template_data = config.get_template_data()

        capabilities = template_data.get("capabilities", [])
        tools = template_data.get("tools", [])

        # Verify capabilities match tool categories
        capability_names = [cap["name"] for cap in capabilities]
        expected_capabilities = [
            "Cluster Management",
            "Database Schema Discovery",
            "SQL Query Execution",
            "Access Control & Filtering",
        ]

        for expected_cap in expected_capabilities:
            assert expected_cap in capability_names

        # Verify tools support the advertised capabilities
        tool_names = [tool["name"] for tool in tools]

        # Cluster management tools
        assert "list_clusters" in tool_names
        assert "get_cluster_info" in tool_names

        # Schema discovery tools
        assert "list_databases" in tool_names
        assert "list_tables" in tool_names
        assert "describe_table" in tool_names

        # Query execution tools
        assert "execute_query" in tool_names

        # Warehouse tools
        assert "list_warehouses" in tool_names
        assert "get_warehouse_info" in tool_names


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
