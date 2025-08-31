#!/usr/bin/env python3
"""
Unit tests for Databricks MCP Server with mocked Databricks SDK.

Tests server functionality, tool registration, and core operations
without requiring actual Databricks workspace connectivity.
"""

from unittest.mock import MagicMock, Mock, patch

import pytest

# Mock the databricks SDK before importing our server
databricks_mock = MagicMock()
sys_modules_patch = patch.dict(
    "sys.modules",
    {
        "databricks": databricks_mock,
        "databricks.sdk": databricks_mock,
        "databricks.sdk.core": databricks_mock,
        "databricks.sdk.service": databricks_mock,
        "databricks.sdk.service.catalog": databricks_mock,
        "databricks.sdk.service.compute": databricks_mock,
        "databricks.sdk.service.sql": databricks_mock,
    },
)

sys_modules_patch.start()

from mcp_platform.template.templates.databricks.server import (
    DatabricksMCPServer,
)


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
        mock_cluster.node_type_id = "i3.xlarge"
        mock_cluster.num_workers = 2
        mock_cluster.spark_version = "11.3.x-scala2.12"
        mock_cluster.runtime_engine = Mock(value="STANDARD")
        mock_cluster.creator_user_name = "test@example.com"
        mock_cluster.state_message = "Running"
        mock_cluster.driver_node_type_id = "i3.xlarge"
        mock_cluster.autoscale = None
        mock_cluster.start_time = 1640995200
        mock_cluster.default_tags = {}
        mock_cluster.custom_tags = {}

        mock_client.clusters.list.return_value = [mock_cluster]
        mock_client.clusters.get.return_value = mock_cluster

        # Mock warehouses
        mock_warehouse = Mock()
        mock_warehouse.id = "warehouse-456"
        mock_warehouse.name = "test-warehouse"
        mock_warehouse.state = Mock(value="RUNNING")
        mock_warehouse.cluster_size = "Medium"
        mock_warehouse.min_num_clusters = 1
        mock_warehouse.max_num_clusters = 3
        mock_warehouse.auto_stop_mins = 120
        mock_warehouse.auto_resume = True
        mock_warehouse.warehouse_type = Mock(value="PRO")
        mock_warehouse.channel = Mock(value="CHANNEL_NAME_CURRENT")
        mock_warehouse.creator_name = "test@example.com"
        mock_warehouse.tags = Mock()
        mock_warehouse.tags.as_dict.return_value = {}
        mock_warehouse.spot_instance_policy = Mock(value="COST_OPTIMIZED")
        mock_warehouse.enable_photon = True
        mock_warehouse.enable_serverless_compute = False

        mock_client.warehouses.list.return_value = [mock_warehouse]
        mock_client.warehouses.get.return_value = mock_warehouse

        # Mock catalogs
        mock_catalog = Mock()
        mock_catalog.name = "main"
        mock_client.catalogs.list.return_value = [mock_catalog]

        # Mock schemas
        mock_schema = Mock()
        mock_schema.name = "default"
        mock_schema.owner = "test@example.com"
        mock_schema.comment = "Default schema"
        mock_client.schemas.list.return_value = [mock_schema]

        # Mock tables
        mock_table = Mock()
        mock_table.name = "test_table"
        mock_table.table_type = Mock(value="MANAGED")
        mock_table.data_source_format = Mock(value="DELTA")
        mock_table.owner = "test@example.com"
        mock_table.comment = "Test table"
        mock_client.tables.list.return_value = [mock_table]

        # Mock table details
        mock_column = Mock()
        mock_column.name = "id"
        mock_column.type_name = Mock(value="INT")
        mock_column.type_text = "int"
        mock_column.nullable = False
        mock_column.comment = "ID column"
        mock_column.position = 0

        mock_table_detail = Mock()
        mock_table_detail.name = "test_table"
        mock_table_detail.full_name = "main.default.test_table"
        mock_table_detail.table_type = Mock(value="MANAGED")
        mock_table_detail.data_source_format = Mock(value="DELTA")
        mock_table_detail.owner = "test@example.com"
        mock_table_detail.comment = "Test table"
        mock_table_detail.columns = [mock_column]
        mock_client.tables.get.return_value = mock_table_detail

        # Mock statement execution
        mock_result = Mock()
        mock_result.data_array = [["1", "test"], ["2", "data"]]

        mock_column_schema = Mock()
        mock_column_schema.name = "id"
        mock_column_schema.type_name = "INT"

        mock_manifest = Mock()
        mock_manifest.schema = Mock()
        mock_manifest.schema.columns = [mock_column_schema]

        mock_status = Mock()
        mock_status.state = Mock(value="SUCCEEDED")

        mock_response = Mock()
        mock_response.result = mock_result
        mock_response.manifest = mock_manifest
        mock_response.status = mock_status

        mock_client.statement_execution.execute_statement.return_value = mock_response

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

    @patch("mcp_platform.template.templates.databricks.server.WorkspaceClient")
    def test_server_initialization(
        self, mock_workspace_client_class, server_config, mock_workspace_client
    ):
        """Test server initialization with mocked Databricks client."""
        mock_workspace_client_class.return_value = mock_workspace_client

        server = DatabricksMCPServer(config_dict=server_config)

        assert server.workspace_client is not None
        assert (
            server.config_data["workspace_host"]
            == "https://dbc-12345.cloud.databricks.com"
        )
        assert server.config_data["read_only"] is True

        # Verify workspace client was initialized
        mock_workspace_client_class.assert_called_once()

    @patch("mcp_platform.template.templates.databricks.server.Config")
    @patch("mcp_platform.template.templates.databricks.server.WorkspaceClient")
    def test_authentication_methods(
        self, mock_workspace_client_class, mock_config_class, mock_workspace_client
    ):
        """Test different authentication methods create proper config."""
        mock_workspace_client_class.return_value = mock_workspace_client
        mock_config_class.return_value = MagicMock()

        # Test PAT authentication
        pat_config = {
            "workspace_host": "https://dbc-12345.cloud.databricks.com",
            "auth_method": "pat",
            "access_token": "dapi123",
        }
        server = DatabricksMCPServer(config_dict=pat_config)
        assert server.config_data["auth_method"] == "pat"

        # Test OAuth authentication
        oauth_config = {
            "workspace_host": "https://dbc-12345.cloud.databricks.com",
            "auth_method": "oauth",
            "oauth_token": "oauth123",
        }
        server = DatabricksMCPServer(config_dict=oauth_config)
        assert server.config_data["auth_method"] == "oauth"

        # Test username/password authentication
        userpass_config = {
            "workspace_host": "https://dbc-12345.cloud.databricks.com",
            "auth_method": "username_password",
            "username": "user@example.com",
            "password": "password123",
        }
        server = DatabricksMCPServer(config_dict=userpass_config)
        assert server.config_data["auth_method"] == "username_password"

    @patch("mcp_platform.template.templates.databricks.server.WorkspaceClient")
    @pytest.mark.asyncio
    async def test_list_clusters(
        self, mock_workspace_client_class, server_config, mock_workspace_client
    ):
        """Test listing clusters functionality."""
        mock_workspace_client_class.return_value = mock_workspace_client

        server = DatabricksMCPServer(config_dict=server_config)
        result = await server.list_clusters()

        assert "clusters" in result
        assert "count" in result
        assert result["count"] > 0
        assert len(result["clusters"]) > 0

        cluster = result["clusters"][0]
        assert "cluster_id" in cluster
        assert "cluster_name" in cluster
        assert "state" in cluster

    @patch("mcp_platform.template.templates.databricks.server.WorkspaceClient")
    @pytest.mark.asyncio
    async def test_list_warehouses(
        self, mock_workspace_client_class, server_config, mock_workspace_client
    ):
        """Test listing warehouses functionality."""
        mock_workspace_client_class.return_value = mock_workspace_client

        server = DatabricksMCPServer(config_dict=server_config)
        result = await server.list_warehouses()

        assert "warehouses" in result
        assert "count" in result
        assert result["count"] > 0
        assert len(result["warehouses"]) > 0

        warehouse = result["warehouses"][0]
        assert "warehouse_id" in warehouse
        assert "warehouse_name" in warehouse
        assert "state" in warehouse

    @patch("mcp_platform.template.templates.databricks.server.WorkspaceClient")
    @pytest.mark.asyncio
    async def test_list_databases(
        self, mock_workspace_client_class, server_config, mock_workspace_client
    ):
        """Test listing databases functionality."""
        mock_workspace_client_class.return_value = mock_workspace_client

        server = DatabricksMCPServer(config_dict=server_config)
        result = await server.list_databases()

        assert "databases" in result
        assert "count" in result
        assert len(result["databases"]) > 0

        database = result["databases"][0]
        assert "database" in database
        assert "catalog" in database
        assert "schema" in database

    @patch("mcp_platform.template.templates.databricks.server.WorkspaceClient")
    @pytest.mark.asyncio
    async def test_list_tables(
        self, mock_workspace_client_class, server_config, mock_workspace_client
    ):
        """Test listing tables functionality."""
        mock_workspace_client_class.return_value = mock_workspace_client

        server = DatabricksMCPServer(config_dict=server_config)
        result = await server.list_tables(database="main.default")

        assert "tables" in result
        assert "database" in result
        assert "count" in result
        assert len(result["tables"]) > 0

        table = result["tables"][0]
        assert "table_name" in table
        assert "table_type" in table
        assert "full_name" in table

    @patch("mcp_platform.template.templates.databricks.server.WorkspaceClient")
    @pytest.mark.asyncio
    async def test_describe_table(
        self, mock_workspace_client_class, server_config, mock_workspace_client
    ):
        """Test describing table functionality."""
        mock_workspace_client_class.return_value = mock_workspace_client

        server = DatabricksMCPServer(config_dict=server_config)
        result = await server.describe_table(
            database="main.default", table="test_table"
        )

        assert "table_name" in result
        assert "full_name" in result
        assert "columns" in result
        assert "column_count" in result
        assert len(result["columns"]) > 0

        column = result["columns"][0]
        assert "name" in column
        assert "type_name" in column
        assert "nullable" in column

    @patch("mcp_platform.template.templates.databricks.server.WorkspaceClient")
    @pytest.mark.asyncio
    async def test_execute_query_read_only_mode(
        self, mock_workspace_client_class, server_config, mock_workspace_client
    ):
        """Test query execution in read-only mode."""
        mock_workspace_client_class.return_value = mock_workspace_client

        server = DatabricksMCPServer(config_dict=server_config)

        # Test allowed read query
        result = await server.execute_query(
            query="SELECT * FROM main.default.test_table LIMIT 10"
        )

        assert "query" in result
        assert "columns" in result
        assert "data" in result
        assert "row_count" in result

        # Test blocked write query
        result = await server.execute_query(
            query="INSERT INTO main.default.test_table VALUES (1, 'test')"
        )
        assert "error" in result
        assert "read-only mode" in result["error"]

    @patch("mcp_platform.template.templates.databricks.server.WorkspaceClient")
    @pytest.mark.asyncio
    async def test_execute_query_write_mode(
        self, mock_workspace_client_class, mock_workspace_client
    ):
        """Test query execution with write mode enabled."""
        mock_workspace_client_class.return_value = mock_workspace_client

        config = {
            "workspace_host": "https://dbc-12345.cloud.databricks.com",
            "access_token": "token123",
            "read_only": False,
        }

        server = DatabricksMCPServer(config_dict=config)

        # Test write query is allowed when read_only=False
        result = await server.execute_query(
            query="INSERT INTO main.default.test_table VALUES (1, 'test')"
        )

        # Should not have error about read-only mode
        assert "error" not in result or "read-only mode" not in result.get("error", "")

    @patch("mcp_platform.template.templates.databricks.server.WorkspaceClient")
    @pytest.mark.asyncio
    async def test_get_cluster_info(
        self, mock_workspace_client_class, server_config, mock_workspace_client
    ):
        """Test getting detailed cluster information."""
        mock_workspace_client_class.return_value = mock_workspace_client

        server = DatabricksMCPServer(config_dict=server_config)
        result = await server.get_cluster_info(cluster_id="cluster-123")

        assert "cluster_id" in result
        assert "cluster_name" in result
        assert "state" in result
        assert "spark_version" in result

    @patch("mcp_platform.template.templates.databricks.server.WorkspaceClient")
    @pytest.mark.asyncio
    async def test_get_warehouse_info(
        self, mock_workspace_client_class, server_config, mock_workspace_client
    ):
        """Test getting detailed warehouse information."""
        mock_workspace_client_class.return_value = mock_workspace_client

        server = DatabricksMCPServer(config_dict=server_config)
        result = await server.get_warehouse_info(warehouse_id="warehouse-456")

        assert "warehouse_id" in result
        assert "warehouse_name" in result
        assert "state" in result
        assert "cluster_size" in result

    def test_read_only_check(self):
        """Test read-only mode operation checking."""
        # Create server with read-only mode
        config = {
            "workspace_host": "https://dbc-12345.cloud.databricks.com",
            "access_token": "token123",
            "read_only": True,
        }

        with patch("mcp_platform.template.templates.databricks.server.WorkspaceClient"):
            server = DatabricksMCPServer(config_dict=config)

            # Test that write operations are blocked
            with pytest.raises(ValueError) as excinfo:
                server._check_read_only_mode("INSERT INTO table VALUES (1)")
            assert "read-only mode" in str(excinfo.value)

            with pytest.raises(ValueError) as excinfo:
                server._check_read_only_mode("DELETE FROM table WHERE id = 1")
            assert "read-only mode" in str(excinfo.value)

            # Test that read operations are allowed
            server._check_read_only_mode("SELECT * FROM table")  # Should not raise

    def test_pattern_matching(self):
        """Test pattern matching for database and schema filtering."""
        config = {
            "workspace_host": "https://dbc-12345.cloud.databricks.com",
            "access_token": "token123",
        }

        with patch("mcp_platform.template.templates.databricks.server.WorkspaceClient"):
            server = DatabricksMCPServer(config_dict=config)

            # Test wildcard pattern
            assert server._match_pattern("any_database", "*") is True

            # Test exact match
            assert server._match_pattern("analytics", "analytics,reporting") is True
            assert server._match_pattern("staging", "analytics,reporting") is False

            # Test regex pattern
            assert server._match_pattern("prod_analytics", "prod_.*") is True
            assert server._match_pattern("dev_analytics", "prod_.*") is False

    def test_database_filtering(self):
        """Test database filtering functionality."""
        config = {
            "workspace_host": "https://dbc-12345.cloud.databricks.com",
            "access_token": "token123",
            "allowed_databases": "analytics,reporting",
        }

        with patch("mcp_platform.template.templates.databricks.server.WorkspaceClient"):
            server = DatabricksMCPServer(config_dict=config)

            databases = ["analytics", "reporting", "staging", "development"]
            filtered = server._filter_databases(databases)

            assert "analytics" in filtered
            assert "reporting" in filtered
            assert "staging" not in filtered
            assert "development" not in filtered

    def test_schema_filtering(self):
        """Test schema filtering functionality."""
        config = {
            "workspace_host": "https://dbc-12345.cloud.databricks.com",
            "access_token": "token123",
            "allowed_schemas": "default,public",
        }

        with patch("mcp_platform.template.templates.databricks.server.WorkspaceClient"):
            server = DatabricksMCPServer(config_dict=config)

            schemas = ["default", "public", "staging", "private"]
            filtered = server._filter_schemas(schemas)

            assert "default" in filtered
            assert "public" in filtered
            assert "staging" not in filtered
            assert "private" not in filtered

    def test_tool_registration(self):
        """Test that all expected tools are registered with the MCP server."""
        config = {
            "workspace_host": "https://dbc-12345.cloud.databricks.com",
            "access_token": "token123",
        }

        with patch("mcp_platform.template.templates.databricks.server.WorkspaceClient"):
            server = DatabricksMCPServer(config_dict=config)

            # Check that FastMCP instance was created
            assert server.mcp is not None

            # The tools should be registered during initialization
            # We can verify this by checking the server has the expected methods
            assert hasattr(server, "list_clusters")
            assert hasattr(server, "list_warehouses")
            assert hasattr(server, "list_databases")
            assert hasattr(server, "list_tables")
            assert hasattr(server, "describe_table")
            assert hasattr(server, "execute_query")
            assert hasattr(server, "get_cluster_info")
            assert hasattr(server, "get_warehouse_info")


if __name__ == "__main__":
    pytest.main([__file__])
