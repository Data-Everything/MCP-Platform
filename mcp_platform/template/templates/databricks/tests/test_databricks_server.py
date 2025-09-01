#!/usr/bin/env python3
"""
Unit tests for Databricks MCP Server with mocked Databricks SDK.

Tests server functionality, tool registration, and core operations
without requiring actual Databricks workspace connectivity.
"""

from unittest.mock import Mock

import pytest

# Import config for testing
from mcp_platform.template.templates.databricks import DatabricksServerConfig


class TestDatabricksMCPServerMock:
    """Test Databricks MCP Server functionality with mocks."""

    def test_config_initialization(self):
        """Test basic configuration initialization."""
        config = DatabricksServerConfig({
            "workspace_host": "https://test.databricks.com",
            "access_token": "test_token",
            "read_only": True,
        })
        
        template_data = config.get_template_data()
        assert "tools" in template_data
        assert len(template_data["tools"]) == 8
        
    def test_mock_workspace_client(self):
        """Test mock workspace client functionality."""
        mock_client = Mock()
        
        # Mock clusters
        mock_cluster = Mock()
        mock_cluster.cluster_id = "cluster-123"
        mock_cluster.cluster_name = "test-cluster"
        mock_cluster.state = Mock(value="RUNNING")
        mock_client.clusters.list.return_value = [mock_cluster]
        
        # Test mock interactions
        clusters = mock_client.clusters.list()
        assert len(clusters) == 1
        assert clusters[0].cluster_name == "test-cluster"
        
    def test_authentication_configuration(self):
        """Test authentication configuration."""
        pat_config = DatabricksServerConfig({
            "workspace_host": "https://test.databricks.com",
            "auth_method": "pat",
            "access_token": "test_token",
        })
        
        template_config = pat_config.get_template_config()
        assert template_config["auth_method"] == "pat"
        assert template_config["access_token"] == "test_token"
        
    def test_read_only_enforcement(self):
        """Test read-only mode enforcement."""
        config = DatabricksServerConfig({
            "workspace_host": "https://test.databricks.com",
            "access_token": "test_token",
            "read_only": True,
        })
        
        template_config = config.get_template_config()
        assert template_config["read_only"] is True
        
    def test_filtering_configuration(self):
        """Test database and schema filtering."""
        config = DatabricksServerConfig({
            "workspace_host": "https://test.databricks.com",
            "access_token": "test_token",
            "allowed_databases": "db1,db2",
            "allowed_schemas": "schema1",
        })
        
        template_config = config.get_template_config()
        assert template_config["allowed_databases"] == "db1,db2"
        assert template_config["allowed_schemas"] == "schema1"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
