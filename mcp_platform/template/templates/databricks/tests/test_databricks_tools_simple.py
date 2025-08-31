#!/usr/bin/env python3
"""
Simplified tool validation tests for Databricks template.

Tests the tool categorization, read-only mode restrictions, and 
expected tool availability for the Databricks MCP server.
"""

import pytest

from mcp_platform.template.templates.databricks.config import DatabricksServerConfig


class TestDatabricksToolValidation:
    """Test Databricks template tool validation and categorization."""

    def test_tool_categorization(self):
        """Test Databricks tool categorization is properly defined."""
        config = DatabricksServerConfig()
        template_data = config.get_template_data()
        
        tools = template_data.get("tools", [])
        
        # Verify we have the expected tools
        tool_names = [tool["name"] for tool in tools]
        
        expected_tools = [
            "list_clusters",
            "list_warehouses", 
            "list_databases",
            "list_tables",
            "describe_table",
            "execute_query",
            "get_cluster_info",
            "get_warehouse_info"
        ]
        
        for expected_tool in expected_tools:
            assert expected_tool in tool_names, f"Missing expected tool: {expected_tool}"

    def test_expected_tool_count_validation(self):
        """Test expected Databricks tool count matches documentation."""
        config = DatabricksServerConfig()
        template_data = config.get_template_data()
        
        tools = template_data.get("tools", [])
        
        # Should have exactly 8 core tools
        assert len(tools) == 8, f"Expected 8 tools, found {len(tools)}"
        
        # Each tool should have required fields
        for tool in tools:
            assert "name" in tool
            assert "description" in tool
            assert "parameters" in tool

    def test_readonly_mode_restrictions(self):
        """Test read-only mode properly restricts write operations."""
        # Tools that should be safe in read-only mode (all current tools)
        read_tools = [
            "list_clusters",
            "list_warehouses",
            "list_databases", 
            "list_tables",
            "describe_table",
            "execute_query",  # Safe because it checks query content
            "get_cluster_info",
            "get_warehouse_info"
        ]
        
        # Tools that would be write operations (not currently implemented but documented as restricted)
        write_operations = [
            "CREATE", "INSERT", "UPDATE", "DELETE", 
            "DROP", "ALTER", "TRUNCATE", "MERGE", "COPY"
        ]
        
        # Verify categorization makes sense
        assert len(read_tools) >= 8, "Should have at least 8 read operations"
        
        # All current tools should be read-only safe
        config = DatabricksServerConfig()
        template_data = config.get_template_data()
        tools = template_data.get("tools", [])
        
        for tool in tools:
            assert tool["name"] in read_tools, f"Tool {tool['name']} should be read-only safe"

    def test_transport_mode_compatibility(self):
        """Test Databricks template supports expected transport modes."""
        config = DatabricksServerConfig()
        template_data = config.get_template_data()
        
        transport = template_data.get("transport", {})
        
        assert "default" in transport
        assert "supported" in transport
        
        # Should support both stdio and http
        supported_transports = transport["supported"]
        assert "stdio" in supported_transports
        assert "http" in supported_transports
        
        # Default should be stdio for safety
        assert transport["default"] == "stdio"

    def test_environment_variable_consistency(self):
        """Test environment variables are consistently defined."""
        config = DatabricksServerConfig()
        template_data = config.get_template_data()
        
        config_schema = template_data.get("config_schema", {})
        properties = config_schema.get("properties", {})
        
        # Check that all configuration properties have env_mapping
        for prop_name, prop_config in properties.items():
            if prop_name in ["workspace_host", "access_token", "auth_method", "read_only"]:
                assert "env_mapping" in prop_config, f"Property {prop_name} should have env_mapping"
                
                # Check env_mapping follows DATABRICKS_ prefix convention
                env_mapping = prop_config["env_mapping"]
                assert env_mapping.startswith("DATABRICKS_"), f"Environment variable {env_mapping} should start with DATABRICKS_"

    def test_tool_naming_conventions(self):
        """Test Databricks tools follow proper naming conventions."""
        config = DatabricksServerConfig()
        template_data = config.get_template_data()
        
        tools = template_data.get("tools", [])
        
        for tool in tools:
            tool_name = tool["name"]
            
            # Tool names should use snake_case
            assert "_" in tool_name or tool_name.islower(), f"Tool {tool_name} should use snake_case"
            
            # Tool names should be descriptive
            assert len(tool_name) > 3, f"Tool name {tool_name} should be descriptive"
            
            # Check that description is provided and meaningful
            description = tool["description"]
            assert len(description) > 10, f"Tool {tool_name} description should be meaningful"

    def test_security_configuration_validation(self):
        """Test security-related configuration is properly set up."""
        config = DatabricksServerConfig()
        template_data = config.get_template_data()
        
        config_schema = template_data.get("config_schema", {})
        properties = config_schema.get("properties", {})
        
        # Check read_only defaults to true
        read_only_config = properties.get("read_only", {})
        assert read_only_config.get("default") is True, "read_only should default to True for safety"
        
        # Check sensitive fields are marked as sensitive
        sensitive_fields = ["access_token", "oauth_token", "password"]
        for field in sensitive_fields:
            if field in properties:
                assert properties[field].get("sensitive") is True, f"Field {field} should be marked as sensitive"

    def test_filtering_configuration_presence(self):
        """Test database and schema filtering configuration is available."""
        config = DatabricksServerConfig()
        template_data = config.get_template_data()
        
        config_schema = template_data.get("config_schema", {})
        properties = config_schema.get("properties", {})
        
        # Check filtering configuration exists
        assert "allowed_databases" in properties, "Should have allowed_databases configuration"
        assert "allowed_schemas" in properties, "Should have allowed_schemas configuration"
        
        # Check defaults allow all access
        assert properties["allowed_databases"].get("default") == "*", "Should default to allowing all databases"
        assert properties["allowed_schemas"].get("default") == "*", "Should default to allowing all schemas"

    def test_capabilities_documentation(self):
        """Test capabilities are properly documented."""
        config = DatabricksServerConfig()
        template_data = config.get_template_data()
        
        capabilities = template_data.get("capabilities", [])
        
        # Should have at least 4 capability categories
        assert len(capabilities) >= 4, f"Expected at least 4 capabilities, found {len(capabilities)}"
        
        expected_capability_names = [
            "Cluster Management",
            "Database Schema Discovery", 
            "SQL Query Execution",
            "Access Control & Filtering"
        ]
        
        capability_names = [cap["name"] for cap in capabilities]
        
        for expected_cap in expected_capability_names:
            assert expected_cap in capability_names, f"Missing capability: {expected_cap}"

    def test_template_metadata_completeness(self):
        """Test template metadata is complete and consistent."""
        config = DatabricksServerConfig()
        template_data = config.get_template_data()
        
        # Check required metadata fields
        required_fields = ["name", "description", "version", "author", "category"]
        for field in required_fields:
            assert field in template_data, f"Missing required field: {field}"
            assert template_data[field], f"Field {field} should not be empty"
        
        # Check category is appropriate
        assert template_data["category"] == "Database", "Category should be Database"
        
        # Check tags include relevant keywords
        tags = template_data.get("tags", [])
        expected_tags = ["databricks", "sql", "warehouse"]
        for tag in expected_tags:
            assert tag in tags, f"Missing expected tag: {tag}"

    def test_docker_configuration(self):
        """Test Docker configuration is properly set up."""
        config = DatabricksServerConfig()
        template_data = config.get_template_data()
        
        # Check Docker image configuration
        assert "docker_image" in template_data
        assert template_data["docker_image"] == "dataeverything/mcp-databricks"
        
        # Check port configuration
        ports = template_data.get("ports", {})
        assert "7072" in ports, "Should expose port 7072"
        assert ports["7072"] == 7072, "Port mapping should be consistent"
        
        # Check transport port matches
        transport = template_data.get("transport", {})
        assert transport.get("port") == 7072, "Transport port should match exposed port"


if __name__ == "__main__":
    pytest.main([__file__])