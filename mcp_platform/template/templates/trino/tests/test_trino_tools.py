"""
Unit tests for Trino MCP server tool implementations.

Tests tool functionality, parameter validation, and response formats
for the new Python FastMCP implementation.
"""

import json
import os
# Import the server module
import sys
# Import unittest.mock for the cancel_query test
import unittest.mock
from unittest.mock import Mock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    from config import TrinoServerConfig
    from server import TrinoMCPServer, create_server
except ImportError:
    # Handle import in different environments
    import importlib.util

    server_path = os.path.join(os.path.dirname(__file__), "..", "server.py")
    spec = importlib.util.spec_from_file_location("server", server_path)
    server_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(server_module)
    TrinoMCPServer = server_module.TrinoMCPServer
    create_server = server_module.create_server

    config_path = os.path.join(os.path.dirname(__file__), "..", "config.py")
    spec = importlib.util.spec_from_file_location("config", config_path)
    config_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config_module)
    TrinoServerConfig = config_module.TrinoServerConfig


class TestTrinoServerTools:
    """Test Trino MCP server tool implementations."""

    def setup_method(self):
        """Set up test fixtures."""
        self.test_config = {
            "trino_host": "localhost",
            "trino_user": "admin",
            "trino_port": 8080,
            "trino_catalog": "memory",
            "trino_schema": "default",
        }

    def create_test_server(self, config_override=None):
        """Create a test server instance with mocked dependencies."""
        config = {**self.test_config}
        if config_override:
            config.update(config_override)

        with patch("server.create_engine") as mock_engine:
            # Mock the engine and connection
            mock_conn = Mock()
            mock_result = Mock()
            mock_result.fetchall.return_value = [
                ("test_catalog",),
                ("another_catalog",),
            ]
            mock_result.fetchone.return_value = ("test_value",)
            mock_result.__iter__ = Mock(
                return_value=iter([{"col1": "val1", "col2": "val2"}])
            )
            mock_conn.execute.return_value = mock_result
            mock_conn.__enter__ = Mock(return_value=mock_conn)
            mock_conn.__exit__ = Mock(return_value=None)

            mock_engine_instance = Mock()
            mock_engine_instance.connect.return_value = mock_conn
            mock_engine.return_value = mock_engine_instance

            server = TrinoMCPServer(config_dict=config, skip_validation=True)
            server.engine = mock_engine_instance
            return server, mock_conn

    def test_server_initialization(self):
        """Test server initialization with configuration."""
        server, _ = self.create_test_server()

        assert server is not None
        assert server.config_data["trino_host"] == "localhost"
        assert server.config_data["trino_user"] == "admin"
        assert hasattr(server, "mcp")
        assert hasattr(server, "engine")

    def test_list_catalogs_tool(self):
        """Test list_catalogs tool functionality."""
        server, mock_conn = self.create_test_server()

        # Mock the SQL result
        mock_conn.execute.return_value.fetchall.return_value = [
            ("catalog1",),
            ("catalog2",),
            ("catalog3",),
        ]

        result = server.list_catalogs()

        assert result["success"] is True
        assert "catalogs" in result
        assert len(result["catalogs"]) == 3
        assert result["catalogs"] == ["catalog1", "catalog2", "catalog3"]
        assert result["total_count"] == 3
        mock_conn.execute.assert_called_once()

    def test_list_catalogs_error_handling(self):
        """Test list_catalogs error handling."""
        server, mock_conn = self.create_test_server()

        # Mock an exception
        mock_conn.execute.side_effect = Exception("Connection failed")

        result = server.list_catalogs()

        assert result["success"] is False
        assert "error" in result
        assert result["catalogs"] == []

    def test_list_schemas_tool(self):
        """Test list_schemas tool functionality."""
        server, mock_conn = self.create_test_server()

        # Mock the SQL result
        mock_conn.execute.return_value.fetchall.return_value = [
            ("schema1",),
            ("schema2",),
            ("information_schema",),
        ]

        result = server.list_schemas("test_catalog")

        assert result["success"] is True
        assert "schemas" in result
        assert len(result["schemas"]) == 3
        assert result["catalog"] == "test_catalog"
        assert result["total_count"] == 3

    def test_list_tables_tool(self):
        """Test list_tables tool functionality."""
        server, mock_conn = self.create_test_server()

        # Mock the SQL result
        mock_conn.execute.return_value.fetchall.return_value = [
            ("table1",),
            ("table2",),
            ("view1",),
        ]

        result = server.list_tables("test_catalog", "test_schema")

        assert result["success"] is True
        assert "tables" in result
        assert len(result["tables"]) == 3
        assert result["catalog"] == "test_catalog"
        assert result["schema"] == "test_schema"

    def test_describe_table_tool(self):
        """Test describe_table tool functionality."""
        server, mock_conn = self.create_test_server()

        # Mock the describe result
        mock_conn.execute.return_value.fetchall.return_value = [
            ("id", "bigint", "", "Primary key"),
            ("name", "varchar(255)", "", "User name"),
            ("created_at", "timestamp", "", "Creation time"),
        ]

        result = server.describe_table("catalog", "schema", "table")

        assert result["success"] is True
        assert "columns" in result
        assert len(result["columns"]) == 3
        assert result["catalog"] == "catalog"
        assert result["schema"] == "schema"
        assert result["table"] == "table"
        assert result["full_table_name"] == "catalog.schema.table"

        # Check column details
        columns = result["columns"]
        assert columns[0]["name"] == "id"
        assert columns[0]["type"] == "bigint"
        assert columns[0]["comment"] == "Primary key"

    def test_execute_query_read_only_mode(self):
        """Test execute_query respects read-only mode."""
        server, mock_conn = self.create_test_server(
            {"trino_allow_write_queries": False}  # Read-only mode
        )

        # Test read query (should work)
        read_query = "SELECT * FROM catalog.schema.table"

        # Mock query result
        mock_result = Mock()
        mock_result.__iter__ = Mock(
            return_value=iter(
                [
                    Mock(_mapping={"col1": "val1", "col2": "val2"}),
                    Mock(_mapping={"col1": "val3", "col2": "val4"}),
                ]
            )
        )
        mock_conn.execute.return_value = mock_result

        result = server.execute_query(read_query)

        assert result["success"] is True
        assert "rows" in result
        assert result["num_rows"] == 2

    def test_execute_query_write_operations_blocked(self):
        """Test that write operations are blocked in read-only mode."""
        server, _ = self.create_test_server({"trino_allow_write_queries": False})

        write_queries = [
            "INSERT INTO table VALUES (1, 'test')",
            "UPDATE table SET col = 'value'",
            "DELETE FROM table WHERE id = 1",
            "CREATE TABLE test (id int)",
            "DROP TABLE test",
            "ALTER TABLE test ADD COLUMN col2 varchar",
        ]

        for query in write_queries:
            result = server.execute_query(query)
            assert result["success"] is False
            assert "Write operations are not allowed" in result["error"]

    def test_execute_query_with_catalog_schema(self):
        """Test execute_query with catalog and schema parameters."""
        server, mock_conn = self.create_test_server()

        # Mock query result
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter([Mock(_mapping={"count": 100})]))
        mock_conn.execute.return_value = mock_result

        result = server.execute_query(
            "SELECT COUNT(*) as count FROM my_table", catalog="hive", schema="default"
        )

        assert result["success"] is True
        assert result["catalog"] == "hive"
        assert result["schema"] == "default"

        # Should have executed a USE statement for catalog/schema and then the query
        execute_calls = mock_conn.execute.call_args_list
        # Expect at least the USE (or USE <catalog>.<schema>) and the actual query
        assert len(execute_calls) >= 2
        # Verify one of the execute calls included a USE statement
        called_sqls = [str(c.args[0]).upper() for c in execute_calls]
        assert any(
            "USE" in s for s in called_sqls
        ), f"No USE statement found in calls: {called_sqls}"

    def test_execute_query_max_results_limit(self):
        """Test that execute_query respects max_results limit."""
        server, mock_conn = self.create_test_server(
            {"trino_max_results": 2}  # Small limit for testing
        )

        # Mock result with more rows than limit
        mock_rows = [Mock(_mapping={"id": i, "value": f"val{i}"}) for i in range(5)]
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter(mock_rows))
        mock_conn.execute.return_value = mock_result

        result = server.execute_query("SELECT * FROM big_table")

        assert result["success"] is True
        assert result["num_rows"] == 2  # Should be limited
        assert result["max_results"] == 2

    def test_get_query_status_tool(self):
        """Test get_query_status tool functionality."""
        server, mock_conn = self.create_test_server()

        # Mock query status result
        mock_conn.execute.return_value.fetchone.return_value = [
            "query_123",
            "SELECT * FROM table",
            "FINISHED",
            "2023-01-01 10:00:00",
        ]

        result = server.get_query_status("query_123")

        assert result["success"] is True
        assert result["query_id"] == "query_123"
        assert result["state"] == "FINISHED"

    def test_get_query_status_not_found(self):
        """Test get_query_status when query is not found."""
        server, mock_conn = self.create_test_server()

        # Mock no result
        mock_conn.execute.return_value.fetchone.return_value = None

        result = server.get_query_status("nonexistent_query")

        assert result["success"] is False
        assert "not found" in result["error"]

    def test_cancel_query_tool(self):
        """Test cancel_query tool functionality."""
        server, mock_conn = self.create_test_server()

        result = server.cancel_query("query_123")

        assert result["success"] is True
        assert result["query_id"] == "query_123"
        assert "cancelled successfully" in result["message"]
        mock_conn.execute.assert_called_with(unittest.mock.ANY)

    def test_get_cluster_info_tool(self):
        """Test get_cluster_info tool functionality."""
        server, mock_conn = self.create_test_server()

        # Mock different queries for cluster info
        def mock_execute_side_effect(query):
            mock_result = Mock()
            if "nodes" in str(query):
                mock_result.fetchall.return_value = [
                    {"node_id": "node1", "state": "active"},
                    {"node_id": "node2", "state": "active"},
                ]
            elif "version" in str(query):
                mock_result.fetchone.return_value = ["Trino 404"]
            elif "SESSION" in str(query):
                mock_result.fetchall.return_value = [
                    ("catalog", "memory"),
                    ("schema", "default"),
                ]
            else:
                mock_result.fetchall.return_value = []
                mock_result.fetchone.return_value = None
            return mock_result

        mock_conn.execute.side_effect = mock_execute_side_effect

        result = server.get_cluster_info()

        assert result["success"] is True
        assert "cluster_info" in result

    def test_write_mode_warning(self):
        """Test that write mode shows warning on server creation."""
        with patch("builtins.print") as mock_print:
            server, _ = self.create_test_server({"trino_allow_write_queries": True})

            # Should have printed warning
            mock_print.assert_called()
            warning_call = str(mock_print.call_args_list)
            assert "WARNING" in warning_call
            assert "write mode" in warning_call

    def test_write_query_allowed_in_write_mode(self):
        """Test that write queries are allowed when write mode is enabled."""
        server, mock_conn = self.create_test_server({"trino_allow_write_queries": True})

        # Mock successful write operation
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter([]))
        mock_conn.execute.return_value = mock_result

        result = server.execute_query("CREATE TABLE test_table (id int)")

        assert result["success"] is True

    def test_tool_registration(self):
        """Test that all expected tools are registered."""
        server, _ = self.create_test_server()

        # Check that MCP has the expected tools registered
        assert hasattr(server, "mcp")

        # Verify tool methods exist
        expected_tools = [
            "list_catalogs",
            "list_schemas",
            "list_tables",
            "describe_table",
            "execute_query",
            "get_query_status",
            "cancel_query",
            "get_cluster_info",
        ]

        for tool_name in expected_tools:
            assert hasattr(server, tool_name)
            assert callable(getattr(server, tool_name))

    def test_error_handling_consistency(self):
        """Test that all tools handle errors consistently."""
        server, mock_conn = self.create_test_server()

        # Make all database calls fail
        mock_conn.execute.side_effect = Exception("Database error")

        tools_to_test = [
            (server.list_catalogs, []),
            (server.list_schemas, ["catalog"]),
            (server.list_tables, ["catalog", "schema"]),
            (server.describe_table, ["catalog", "schema", "table"]),
            (server.execute_query, ["SELECT 1"]),
            (server.get_query_status, ["query_id"]),
            (server.cancel_query, ["query_id"]),
            (server.get_cluster_info, []),
        ]

        for tool_func, args in tools_to_test:
            result = tool_func(*args)
            assert result["success"] is False
            assert "error" in result
            assert isinstance(result["error"], str)
            # Should be descriptive and action-oriented (no tools list required)
            assert len(result["error"]) > 0

    def test_discovery_tools_structure(self):
        """Test discovery tools have proper parameter structure."""
        template_path = os.path.join(os.path.dirname(__file__), "..", "template.json")

        with open(template_path, "r") as f:
            template_config = json.load(f)

        tools = {tool["name"]: tool for tool in template_config["tools"]}

        # list_catalogs should have no parameters
        list_catalogs = tools["list_catalogs"]
        assert list_catalogs["parameters"] == []
        assert "List all accessible Trino catalogs" in list_catalogs["description"]

        # list_schemas should require catalog parameter
        list_schemas = tools["list_schemas"]
        schema_params = {p["name"]: p for p in list_schemas["parameters"]}
        assert "catalog" in schema_params
        assert schema_params["catalog"]["required"] is True
        assert schema_params["catalog"]["type"] == "string"

        # list_tables should require catalog and schema
        list_tables = tools["list_tables"]
        table_params = {p["name"]: p for p in list_tables["parameters"]}
        assert "catalog" in table_params
        assert "schema" in table_params
        assert table_params["catalog"]["required"] is True
        assert table_params["schema"]["required"] is True

    def test_query_execution_tool_parameters(self):
        """Test query execution tool has proper parameters."""
        template_path = os.path.join(os.path.dirname(__file__), "..", "template.json")

        with open(template_path, "r") as f:
            template_config = json.load(f)

        tools = {tool["name"]: tool for tool in template_config["tools"]}
        execute_query = tools["execute_query"]

        params = {p["name"]: p for p in execute_query["parameters"]}

        # Required query parameter
        assert "query" in params
        assert params["query"]["required"] is True
        assert params["query"]["type"] == "string"
        assert "SQL query" in params["query"]["description"]

        # Optional context parameters
        assert "catalog" in params
        assert params["catalog"]["required"] is False
        assert params["catalog"]["type"] == "string"

        assert "schema" in params
        assert params["schema"]["required"] is False
        assert params["schema"]["type"] == "string"

        # Should mention read-only restrictions
        assert "read-only" in execute_query["description"].lower()

    def test_table_inspection_tools(self):
        """Test table inspection tools have proper structure."""
        template_path = os.path.join(os.path.dirname(__file__), "..", "template.json")

        with open(template_path, "r") as f:
            template_config = json.load(f)

        tools = {tool["name"]: tool for tool in template_config["tools"]}
        describe_table = tools["describe_table"]

        params = {p["name"]: p for p in describe_table["parameters"]}

        # Should require catalog, schema, and table
        required_params = ["catalog", "schema", "table"]
        for param_name in required_params:
            assert param_name in params
            assert params[param_name]["required"] is True
            assert params[param_name]["type"] == "string"

    def test_query_management_tools(self):
        """Test query management tools are properly defined."""
        template_path = os.path.join(os.path.dirname(__file__), "..", "template.json")

        with open(template_path, "r") as f:
            template_config = json.load(f)

        tools = {tool["name"]: tool for tool in template_config["tools"]}

        # get_query_status tool
        get_status = tools["get_query_status"]
        status_params = {p["name"]: p for p in get_status["parameters"]}
        assert "query_id" in status_params
        assert status_params["query_id"]["required"] is True
        assert "query ID" in status_params["query_id"]["description"]

        # cancel_query tool
        cancel_query = tools["cancel_query"]
        cancel_params = {p["name"]: p for p in cancel_query["parameters"]}
        assert "query_id" in cancel_params
        assert cancel_params["query_id"]["required"] is True

    def test_access_control_integration(self):
        """Test tools integrate with access control configuration."""
        template_path = os.path.join(os.path.dirname(__file__), "..", "template.json")

        with open(template_path, "r") as f:
            template_config = json.load(f)

        # Access control should be mentioned in capabilities
        capabilities = template_config["capabilities"]
        access_control_cap = next(
            cap for cap in capabilities if "Access Control" in cap["name"]
        )

        assert "catalog" in access_control_cap["description"].lower()
        assert "schema" in access_control_cap["description"].lower()
        assert "filter" in access_control_cap["description"].lower()

        # Configuration should support filtering
        properties = template_config["config_schema"]["properties"]

        filtering_configs = [
            "allowed_catalogs",
            "catalog_regex",
            "allowed_schemas",
            "schema_regex",
        ]

        for config_name in filtering_configs:
            assert config_name in properties

    def test_read_only_mode_restrictions(self):
        """Test read-only mode restrictions are documented."""
        template_path = os.path.join(os.path.dirname(__file__), "..", "template.json")

        with open(template_path, "r") as f:
            template_config = json.load(f)

        # Read-only should be default
        properties = template_config["config_schema"]["properties"]
        assert properties["read_only"]["default"] is True

        # execute_query should mention restrictions
        tools = {tool["name"]: tool for tool in template_config["tools"]}
        execute_query = tools["execute_query"]
        assert "read-only" in execute_query["description"].lower()

        # Should have capability explaining read-only
        capabilities = template_config["capabilities"]
        query_capability = next(
            cap for cap in capabilities if "Query Execution" in cap["name"]
        )
        assert "read-only" in query_capability["example"].lower()

    def test_environment_variable_consistency(self):
        """Test environment variables are consistent with upstream."""
        template_path = os.path.join(os.path.dirname(__file__), "..", "template.json")

        with open(template_path, "r") as f:
            template_config = json.load(f)

        properties = template_config["config_schema"]["properties"]

        # Core Trino connection variables
        trino_vars = {
            "trino_host": "TRINO_HOST",
            "trino_port": "TRINO_PORT",
            "trino_user": "TRINO_USER",
        }

        for prop_name, expected_env in trino_vars.items():
            assert properties[prop_name]["env_mapping"] == expected_env

        # Authentication variables
        auth_vars = {
            "auth_method": "TRINO_AUTH_METHOD",
            "jwt_token": "TRINO_JWT_TOKEN",
            "oauth2_client_id": "TRINO_OAUTH2_CLIENT_ID",
            "oauth2_client_secret": "TRINO_OAUTH2_CLIENT_SECRET",
            "oauth2_token_url": "TRINO_OAUTH2_TOKEN_URL",
        }

        for prop_name, expected_env in auth_vars.items():
            assert properties[prop_name]["env_mapping"] == expected_env

        # Access control variables
        access_vars = {
            "read_only": "TRINO_READ_ONLY",
            "allowed_catalogs": "TRINO_ALLOWED_CATALOGS",
            "catalog_regex": "TRINO_CATALOG_REGEX",
            "allowed_schemas": "TRINO_ALLOWED_SCHEMAS",
            "schema_regex": "TRINO_SCHEMA_REGEX",
            "trino_query_timeout": "TRINO_QUERY_TIMEOUT",
            "trino_max_results": "TRINO_MAX_RESULTS",
        }

        for prop_name, expected_env in access_vars.items():
            assert properties[prop_name]["env_mapping"] == expected_env

    def test_tool_parameter_descriptions(self):
        """Test all tool parameters have clear descriptions."""
        template_path = os.path.join(os.path.dirname(__file__), "..", "template.json")

        with open(template_path, "r") as f:
            template_config = json.load(f)

        tools = template_config["tools"]

        for tool in tools:
            assert "description" in tool
            assert len(tool["description"]) > 10  # Meaningful description

            for param in tool["parameters"]:
                assert "name" in param
                assert "description" in param
                assert "type" in param
                assert "required" in param

                # Description should be helpful
                assert len(param["description"]) > 5

                # Type should be valid
                assert param["type"] in [
                    "string",
                    "integer",
                    "boolean",
                    "array",
                    "object",
                ]
